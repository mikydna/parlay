from prop_ev.brief_builder import (
    build_brief_input,
    default_pass1,
    enforce_readability_labels,
    move_disclosures_to_end,
    normalize_pass2_markdown,
    render_fallback_markdown,
    strip_risks_and_watchouts_section,
    strip_tier_b_view_section,
)


def _sample_report() -> dict:
    return {
        "snapshot_id": "snap-1",
        "generated_at_utc": "2026-02-11T17:00:00Z",
        "strategy_status": "modeled_with_gates",
        "summary": {
            "events": 2,
            "candidate_lines": 10,
            "tier_a_lines": 8,
            "tier_b_lines": 2,
            "eligible_lines": 3,
            "injury_source_official": "yes",
            "injury_source_secondary": "yes",
            "roster_source": "no",
            "roster_team_rows": 0,
            "quota": {"remaining": "490", "used": "10", "last": "1"},
        },
        "gaps": ["Roster feed returned no active rows."],
        "ranked_plays": [
            {
                "event_id": "event-1",
                "home_team": "A",
                "away_team": "B",
                "market": "player_points",
                "player": "Player A",
                "point": 20.5,
                "tier": "A",
                "recommended_side": "under",
                "under_best_price": 110,
                "under_best_book": "book_b",
                "under_link": "",
                "over_best_price": -120,
                "over_best_book": "book_a",
                "over_link": "",
                "best_ev": 0.08,
                "best_kelly": 0.06,
                "p_under_model": 0.56,
                "p_under_fair": 0.51,
                "hold": 0.03,
                "injury_status": "questionable",
                "roster_status": "active",
                "reason": "",
            },
            {
                "event_id": "event-2",
                "home_team": "C",
                "away_team": "D",
                "market": "player_points",
                "player": "Player C",
                "point": 12.5,
                "tier": "A",
                "recommended_side": "over",
                "over_best_price": 100,
                "over_best_book": "book_c",
                "over_link": "",
                "under_best_price": -120,
                "under_best_book": "book_d",
                "under_link": "",
                "best_ev": 0.07,
                "best_kelly": 0.05,
                "p_over_model": 0.57,
                "p_over_fair": 0.52,
                "hold": 0.02,
                "injury_status": "probable",
                "roster_status": "active",
                "reason": "",
            },
        ],
        "watchlist": [
            {
                "event_id": "event-1",
                "home_team": "A",
                "away_team": "B",
                "player": "Player B",
                "market": "player_points",
                "point": 14.5,
                "tier": "B",
                "reason": "tier_b_blocked",
                "best_ev": 0.09,
                "injury_status": "unknown",
                "roster_status": "unknown_roster",
            }
        ],
    }


def test_brief_input_and_fallback_markdown() -> None:
    brief = build_brief_input(_sample_report(), top_n=5)
    assert brief["snapshot_id"] == "snap-1"
    assert len(brief["top_plays"]) == 2
    assert brief["top_plays"][0]["player"] == "Player C"
    assert brief["top_plays"][0]["action_default"] == "GO"
    assert brief["top_plays"][0]["bet_type"] == "player_prop"
    assert brief["top_plays"][0]["action_default"] in {"GO", "LEAN", "NO-GO"}
    assert "Ticket" not in brief["top_plays"][0]["ticket"]
    assert brief["top_plays"][0]["game"] == "D @ C"
    assert "edge" in brief["top_plays"][0]["edge_note"].lower()
    assert "tier_b_spotlight" in brief
    assert len(brief["tier_b_spotlight"]) >= 1
    assert "game_cards" in brief
    assert len(brief["game_cards"]) >= 1

    pass1 = default_pass1(brief)
    assert "slate_summary" in pass1
    assert "top_plays_explained" in pass1
    assert pass1["top_plays_explained"][0]["action"] in {"GO", "LEAN", "NO-GO"}

    markdown = render_fallback_markdown(
        brief_input=brief, pass1=pass1, source_label="deterministic"
    )
    assert "## What The Bet Is" in markdown
    assert "## Executive Summary" in markdown
    assert "## Action Plan (GO / LEAN / NO-GO)" in markdown
    assert "### Top 2 Across All Games" in markdown
    assert "| Action | Game | Tier | Ticket | Edge Note | Why |" in markdown
    assert "## Risks and Watchouts" not in markdown
    assert "## Tier B View (Single-Book Lines)" not in markdown
    assert "## Game Cards by Matchup" in markdown
    assert "<!-- pagebreak -->" in markdown
    assert "\n<!-- pagebreak -->\n\n## Game Cards by Matchup\n" in markdown
    assert markdown.count("<!-- pagebreak -->") >= len(brief["game_cards"]) + 1
    assert "`unknown_roster` means" in markdown
    assert "Player A" in markdown


def test_normalize_pass2_markdown_uses_fallback() -> None:
    fallback = "# Strategy Brief\n\n## Snapshot\n"
    normalized = normalize_pass2_markdown("plain text", fallback)
    assert normalized == fallback


def test_enforce_readability_labels_inserts_required_lines() -> None:
    markdown = "## Action Plan (GO / LEAN / NO-GO)\n\n| A | B |\n| --- | --- |\n| 1 | 2 |\n"
    labeled = enforce_readability_labels(markdown, top_n=5)
    assert "### Top 5 Across All Games" in labeled


def test_strip_risks_and_watchouts_section() -> None:
    markdown = (
        "## Snapshot\n\n"
        "## Risks and Watchouts\n\n"
        "- hidden\n\n"
        "## Tier B View (Single-Book Lines)\n\n"
        "- keep\n"
    )
    stripped = strip_risks_and_watchouts_section(markdown)
    assert "## Risks and Watchouts" not in stripped
    assert "## Tier B View (Single-Book Lines)" in stripped


def test_strip_tier_b_view_section() -> None:
    markdown = (
        "## Snapshot\n\n"
        "## Tier B View (Single-Book Lines)\n\n"
        "- hidden\n\n"
        "## Data Quality\n\n"
        "- keep\n"
    )
    stripped = strip_tier_b_view_section(markdown)
    assert "## Tier B View (Single-Book Lines)" not in stripped
    assert "## Data Quality" in stripped


def test_move_disclosures_to_end() -> None:
    markdown = (
        "## Snapshot\n\n"
        "## Data Quality\n\n"
        "- dq\n\n"
        "## Confidence\n\n"
        "- cf\n\n"
        "## Game Cards by Matchup\n\n"
        "### WAS @ CLE\n"
    )
    moved = move_disclosures_to_end(markdown)
    assert "## Game Cards by Matchup" in moved
    assert "<!-- pagebreak -->" in moved
    assert "<!-- pagebreak -->\n\n## Game Cards by Matchup" in moved
    assert moved.rfind("## Data Quality") > moved.rfind("## Game Cards by Matchup")
    assert moved.rfind("## Confidence") > moved.rfind("## Data Quality")


def test_move_disclosures_to_end_avoids_double_trailing_pagebreak() -> None:
    markdown = (
        "## Snapshot\n\n"
        "## Game Cards by Matchup\n\n"
        "### WAS @ CLE\n\n"
        "<!-- pagebreak -->\n\n"
        "## Data Quality\n\n"
        "- dq\n"
    )
    moved = move_disclosures_to_end(markdown)
    assert "<!-- pagebreak -->\n\n## Data Quality" in moved
    assert "<!-- pagebreak -->\n\n\n## Data Quality" not in moved
