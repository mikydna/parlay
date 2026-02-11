import json
from pathlib import Path

import httpx
import pytest

from prop_ev.llm_client import LLMClient, LLMClientError, LLMResponseFormatError, _default_post
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
    assert payload.get("reasoning") == {"effort": "minimal"}


def test_incomplete_response_raises_clear_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    settings = Settings(_env_file=None)

    def fake_post(url: str, headers: dict[str, str], payload: dict, timeout: float) -> dict:
        del url, headers, payload, timeout
        return {
            "id": "resp_test",
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [{"type": "reasoning", "summary": []}],
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }

    client = LLMClient(settings=settings, data_root=tmp_path / "data", post_fn=fake_post)
    with pytest.raises(LLMResponseFormatError, match="status=incomplete"):
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


def test_web_sources_extracted_and_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ODDS_API_KEY", "odds-test")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    settings = Settings(_env_file=None)

    def fake_post(url: str, headers: dict[str, str], payload: dict, timeout: float) -> dict:
        del url, headers, timeout
        assert payload.get("tools") == [{"type": "web_search"}]
        assert "reasoning" not in payload
        output_text = (
            '{"analysis_summary":"ok","supporting_facts":[],'
            '"refuting_facts":[],"bottom_line":"ok"}'
        )
        return {
            "output_text": output_text,
            "output": [
                {
                    "type": "web_search_call",
                    "action": {
                        "sources": [
                            {
                                "title": "Source A",
                                "url": "https://example.com/a",
                                "domain": "example.com",
                            }
                        ]
                    },
                }
            ],
            "usage": {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5},
        }

    client = LLMClient(settings=settings, data_root=tmp_path / "data", post_fn=fake_post)
    first = client.cached_completion(
        task="playbook_analyst_web",
        prompt_version="v1",
        prompt="hello",
        payload={"x": 1},
        snapshot_id="snap-1",
        model="gpt-5-mini",
        max_output_tokens=180,
        temperature=0.1,
        refresh=False,
        offline=False,
        request_options={"tools": [{"type": "web_search"}], "tool_choice": "auto"},
    )
    second = client.cached_completion(
        task="playbook_analyst_web",
        prompt_version="v1",
        prompt="hello",
        payload={"x": 1},
        snapshot_id="snap-1",
        model="gpt-5-mini",
        max_output_tokens=180,
        temperature=0.1,
        refresh=False,
        offline=False,
        request_options={"tools": [{"type": "web_search"}], "tool_choice": "auto"},
    )

    assert first["cached"] is False
    assert first["web_sources"][0]["url"] == "https://example.com/a"
    assert second["cached"] is True
    assert second["web_sources"][0]["title"] == "Source A"
