"""HTTP client for The Odds API v4."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from math import ceil
from time import perf_counter
from typing import Any

import httpx
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt

from prop_ev.settings import Settings

FEATURED_MARKETS = {"h2h", "spreads", "totals", "outrights"}
CORE_PROP_MARKETS = {
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_points_rebounds_assists",
}


class OddsAPIError(RuntimeError):
    """Raised on Odds API failures."""


class RetryableStatusError(RuntimeError):
    """Raised for retryable status codes."""

    def __init__(self, response: httpx.Response) -> None:
        self.response = response
        message = f"retryable status {response.status_code}"
        super().__init__(message)

    def retry_after_seconds(self) -> float | None:
        raw_value = self.response.headers.get("Retry-After")
        if not raw_value:
            return None
        try:
            return max(0.0, float(raw_value))
        except ValueError:
            try:
                date_value = parsedate_to_datetime(raw_value)
            except (TypeError, ValueError):
                return None
            now = datetime.now(UTC)
            return max(0.0, (date_value - now).total_seconds())


@dataclass(frozen=True)
class OddsResponse:
    """Response data and metadata from an API call."""

    data: Any
    status_code: int
    headers: dict[str, str]
    duration_ms: int
    retry_count: int


def parse_csv(raw_value: str | None) -> list[str]:
    """Parse comma-separated values."""
    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def regions_equivalent(regions: str | None, bookmakers: str | None) -> int:
    """Compute region-equivalent multiplier."""
    books = parse_csv(bookmakers)
    if books:
        return max(1, ceil(len(books) / 10))
    region_list = parse_csv(regions)
    if region_list:
        return len(region_list)
    return 1


def estimate_featured_credits(markets: list[str], regions_factor: int) -> int:
    """Estimate featured market request credits."""
    return len(set(markets)) * regions_factor


def estimate_event_credits(markets: list[str], regions_factor: int, event_count: int) -> int:
    """Estimate per-event request credits (worst case)."""
    return len(set(markets)) * regions_factor * max(event_count, 0)


def _wait_for_retry(retry_state) -> float:
    """Wait strategy for tenacity retries."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, RetryableStatusError):
        retry_after = exc.retry_after_seconds()
        if retry_after is not None:
            return min(retry_after, 60.0)
    return min(2 ** (retry_state.attempt_number - 1), 30.0)


def _unwrap_historical_data(payload: Any) -> Any:
    if isinstance(payload, dict) and "data" in payload:
        return payload.get("data")
    return payload


class OddsAPIClient:
    """Thin HTTP client around The Odds API v4."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        base_url = settings.odds_api_base_url.rstrip("/")
        self._base_url = base_url
        limits = httpx.Limits(max_connections=8, max_keepalive_connections=4)
        self._http = httpx.Client(timeout=settings.odds_api_timeout_s, limits=limits)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> OddsAPIClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _request(self, *, path: str, params: dict[str, Any]) -> OddsResponse:
        api_key = str(self.settings.odds_api_key).strip()
        if not api_key:
            raise OddsAPIError(
                "missing Odds API key; set ODDS_API_KEY or configure "
                "odds_api.key_files in runtime.toml"
            )
        params_with_key = dict(params)
        params_with_key["apiKey"] = api_key
        url = f"{self._base_url}/{path.lstrip('/')}"
        retries = 0
        started = perf_counter()
        response: httpx.Response | None = None
        try:
            for attempt in Retrying(
                stop=stop_after_attempt(4),
                retry=retry_if_exception_type(RetryableStatusError),
                wait=_wait_for_retry,
                reraise=True,
            ):
                with attempt:
                    retries = attempt.retry_state.attempt_number - 1
                    response = self._http.get(url, params=params_with_key)
                    if response.status_code == 429 or 500 <= response.status_code <= 599:
                        raise RetryableStatusError(response)
                    response.raise_for_status()
        except RetryableStatusError as exc:
            raise OddsAPIError(
                f"{path} failed with status {exc.response.status_code} after retries"
            ) from exc
        except httpx.HTTPError as exc:
            raise OddsAPIError(f"{path} failed with transport error: {exc}") from exc
        if response is None:
            raise OddsAPIError(f"{path} failed without a response")

        duration_ms = int((perf_counter() - started) * 1000)
        headers = {
            "x-requests-last": response.headers.get("x-requests-last", ""),
            "x-requests-used": response.headers.get("x-requests-used", ""),
            "x-requests-remaining": response.headers.get("x-requests-remaining", ""),
            "retry-after": response.headers.get("retry-after", ""),
        }
        return OddsResponse(
            data=response.json(),
            status_code=response.status_code,
            headers=headers,
            duration_ms=duration_ms,
            retry_count=retries,
        )

    def list_sports(self) -> OddsResponse:
        """List sports (free endpoint)."""
        return self._request(path="/sports", params={})

    def list_events(
        self,
        *,
        sport_key: str,
        commence_from: str | None = None,
        commence_to: str | None = None,
        date_format: str = "iso",
        historical_date: str | None = None,
    ) -> OddsResponse:
        """List events for a sport (free endpoint)."""
        if historical_date:
            params: dict[str, Any] = {"dateFormat": date_format, "date": historical_date}
            raw = self._request(path=f"/historical/sports/{sport_key}/events", params=params)
            data = _unwrap_historical_data(raw.data)
            if not isinstance(data, list):
                raise OddsAPIError(
                    "historical events payload missing list data for "
                    f"sport={sport_key} date={historical_date}"
                )
            return OddsResponse(
                data=data,
                status_code=raw.status_code,
                headers=raw.headers,
                duration_ms=raw.duration_ms,
                retry_count=raw.retry_count,
            )

        params = {"dateFormat": date_format}
        if commence_from:
            params["commenceTimeFrom"] = commence_from
        if commence_to:
            params["commenceTimeTo"] = commence_to
        return self._request(path=f"/sports/{sport_key}/events", params=params)

    def get_featured_odds(
        self,
        *,
        sport_key: str,
        markets: list[str],
        regions: str | None,
        bookmakers: str | None,
        commence_from: str | None = None,
        commence_to: str | None = None,
        event_ids: list[str] | None = None,
        odds_format: str = "american",
        date_format: str = "iso",
    ) -> OddsResponse:
        """Fetch featured market odds."""
        market_set = set(markets)
        if not market_set.issubset(FEATURED_MARKETS):
            invalid = sorted(market_set - FEATURED_MARKETS)
            raise ValueError(f"invalid featured markets: {','.join(invalid)}")

        params: dict[str, Any] = {
            "markets": ",".join(sorted(market_set)),
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        }
        if bookmakers:
            params["bookmakers"] = bookmakers
        elif regions:
            params["regions"] = regions
        if commence_from:
            params["commenceTimeFrom"] = commence_from
        if commence_to:
            params["commenceTimeTo"] = commence_to
        if event_ids:
            params["eventIds"] = ",".join(sorted(set(event_ids)))

        return self._request(path=f"/sports/{sport_key}/odds", params=params)

    def get_event_odds(
        self,
        *,
        sport_key: str,
        event_id: str,
        markets: list[str],
        regions: str | None,
        bookmakers: str | None,
        include_links: bool = False,
        include_sids: bool = False,
        odds_format: str = "american",
        date_format: str = "iso",
        historical_date: str | None = None,
    ) -> OddsResponse:
        """Fetch odds for one event, including player props markets."""
        invalid = sorted(set(markets) - CORE_PROP_MARKETS)
        if invalid:
            raise ValueError(f"invalid prop markets: {','.join(invalid)}")

        params: dict[str, Any] = {
            "markets": ",".join(sorted(set(markets))),
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        }
        if bookmakers:
            params["bookmakers"] = bookmakers
        elif regions:
            params["regions"] = regions
        if include_links:
            params["includeLinks"] = "true"
        if include_sids:
            params["includeSids"] = "true"
        if historical_date:
            params["date"] = historical_date
            raw = self._request(
                path=f"/historical/sports/{sport_key}/events/{event_id}/odds",
                params=params,
            )
            data = _unwrap_historical_data(raw.data)
            if not isinstance(data, dict):
                raise OddsAPIError(
                    "historical event odds payload missing object data for "
                    f"event={event_id} date={historical_date}"
                )
            return OddsResponse(
                data=data,
                status_code=raw.status_code,
                headers=raw.headers,
                duration_ms=raw.duration_ms,
                retry_count=raw.retry_count,
            )

        return self._request(path=f"/sports/{sport_key}/events/{event_id}/odds", params=params)
