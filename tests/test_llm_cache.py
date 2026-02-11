import json
from pathlib import Path

import httpx
import pytest

from prop_ev.llm_client import LLMClient, LLMClientError, _default_post
from prop_ev.settings import Settings


def test_llm_cache_hit_prevents_repeat_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    settings = Settings(_env_file=None)

    calls = {"count": 0}

    def fake_post(url: str, headers: dict[str, str], payload: dict, timeout: float) -> dict:
        calls["count"] += 1
        assert url.endswith("/v1/responses")
        assert "Authorization" in headers
        output_text = (
            '{"slate_summary":"ok","top_plays_explained":[],'
            '"watchouts":[],"data_quality_flags":[],"confidence_notes":[]}'
        )
        return {
            "output_text": output_text,
            "usage": {
                "input_tokens": 50,
                "output_tokens": 20,
                "total_tokens": 70,
            },
        }

    client = LLMClient(settings=settings, data_root=tmp_path / "data", post_fn=fake_post)
    payload = {"x": 1}
    first = client.cached_completion(
        task="playbook_pass1",
        prompt_version="v1",
        prompt="hello",
        payload=payload,
        snapshot_id="snap-1",
        model="gpt-5-mini",
        max_output_tokens=200,
        temperature=0.1,
        refresh=False,
        offline=False,
    )
    second = client.cached_completion(
        task="playbook_pass1",
        prompt_version="v1",
        prompt="hello",
        payload=payload,
        snapshot_id="snap-1",
        model="gpt-5-mini",
        max_output_tokens=200,
        temperature=0.1,
        refresh=False,
        offline=False,
    )

    assert calls["count"] == 1
    assert first["cached"] is False
    assert second["cached"] is True

    usage_files = sorted((tmp_path / "data" / "llm_usage").glob("usage-*.jsonl"))
    assert usage_files
    rows = [json.loads(line) for line in usage_files[0].read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert rows[0]["cached"] is False
    assert rows[1]["cached"] is True


def test_default_post_wraps_http_status_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    request = httpx.Request("POST", "https://api.openai.com/v1/responses")
    response = httpx.Response(status_code=400, request=request, text='{"error":"bad request"}')

    def fake_post(url: str, headers: dict[str, str], json: dict, timeout: float) -> httpx.Response:
        del url, headers, json, timeout
        raise httpx.HTTPStatusError("bad request", request=request, response=response)

    monkeypatch.setattr("prop_ev.llm_client.httpx.post", fake_post)
    with pytest.raises(LLMClientError, match="status=400"):
        _default_post(
            "https://api.openai.com/v1/responses",
            {"Authorization": "Bearer x"},
            {"model": "gpt-5-mini"},
            30.0,
        )


def test_gpt5_payload_omits_temperature(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    settings = Settings(_env_file=None)

    captured: dict[str, dict] = {}

    def fake_post(url: str, headers: dict[str, str], payload: dict, timeout: float) -> dict:
        del url, headers, timeout
        captured["payload"] = payload
        return {"output_text": "ok", "usage": {"input_tokens": 1, "output_tokens": 1}}

    client = LLMClient(settings=settings, data_root=tmp_path / "data", post_fn=fake_post)
    client.cached_completion(
        task="playbook_pass1",
        prompt_version="v1",
        prompt="hello",
        payload={"x": 1},
        snapshot_id="snap-1",
        model="gpt-5-mini",
        max_output_tokens=120,
        temperature=0.1,
        refresh=True,
        offline=False,
    )

    payload = captured["payload"]
    assert payload["model"] == "gpt-5-mini"
    assert "temperature" not in payload
