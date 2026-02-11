import json
from pathlib import Path

import pytest

from prop_ev.storage import SnapshotStore, make_snapshot_id, request_hash


def test_request_hash_excludes_api_key() -> None:
    path = "/sports/basketball_nba/odds"
    params_a = {"apiKey": "secret-a", "markets": "spreads,totals", "regions": "us"}
    params_b = {"apiKey": "secret-b", "markets": "spreads,totals", "regions": "us"}

    hash_a = request_hash("GET", path, params_a)
    hash_b = request_hash("GET", path, params_b)

    assert hash_a == hash_b


def test_cache_hit_prevents_fetch_path(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path / "data")
    snapshot_id = make_snapshot_id()
    store.ensure_snapshot(snapshot_id)
    key = request_hash("GET", "/sports/basketball_nba/odds", {"markets": "spreads"})
    store.write_response(snapshot_id, key, {"cached": True})

    def fetch() -> dict[str, bool]:
        raise AssertionError("network fetch should not be called")

    if store.has_response(snapshot_id, key):
        payload = store.load_response(snapshot_id, key)
    else:
        payload = fetch()

    assert payload == {"cached": True}


def test_atomic_write_cleans_temp_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = SnapshotStore(tmp_path / "data")
    snapshot_id = make_snapshot_id()
    store.ensure_snapshot(snapshot_id)
    key = request_hash("GET", "/sports/basketball_nba/events", {})

    def replace_fail(_: Path, __: Path) -> None:
        raise OSError("replace failure")

    monkeypatch.setattr("prop_ev.storage.os.replace", replace_fail)

    with pytest.raises(OSError):
        store.write_response(snapshot_id, key, {"ok": True})

    response_dir = store.snapshot_dir(snapshot_id) / "responses"
    assert not any(path.name.startswith(".tmp-") for path in response_dir.glob(".*"))
    assert not (response_dir / f"{key}.json").exists()


def test_append_usage_writes_jsonl(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path / "data")
    store.append_usage(
        endpoint="/sports/basketball_nba/odds",
        request_key="abc",
        snapshot_id="snap",
        status_code=200,
        duration_ms=120,
        retry_count=0,
        headers={"x-requests-last": "2", "x-requests-used": "10", "x-requests-remaining": "490"},
        cached=False,
    )

    usage_files = sorted((tmp_path / "data" / "usage").glob("usage-*.jsonl"))
    assert len(usage_files) == 1
    rows = [json.loads(line) for line in usage_files[0].read_text(encoding="utf-8").splitlines()]
    assert rows[0]["x_requests_last"] == "2"
