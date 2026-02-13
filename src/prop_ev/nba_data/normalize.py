"""Shared NBA name normalization helpers."""

from __future__ import annotations

import re
import unicodedata

TEAM_NAME_ALIASES = {
    "atl": "atlanta hawks",
    "atlanta": "atlanta hawks",
    "boston": "boston celtics",
    "bos": "boston celtics",
    "brooklyn": "brooklyn nets",
    "bkn": "brooklyn nets",
    "brk": "brooklyn nets",
    "charlotte": "charlotte hornets",
    "cha": "charlotte hornets",
    "cho": "charlotte hornets",
    "chicago": "chicago bulls",
    "chi": "chicago bulls",
    "cle": "cleveland cavaliers",
    "cleveland": "cleveland cavaliers",
    "dallas": "dallas mavericks",
    "dal": "dallas mavericks",
    "den": "denver nuggets",
    "denver": "denver nuggets",
    "det": "detroit pistons",
    "detroit": "detroit pistons",
    "golden state": "golden state warriors",
    "gs": "golden state warriors",
    "gsw": "golden state warriors",
    "hou": "houston rockets",
    "houston": "houston rockets",
    "ind": "indiana pacers",
    "indiana": "indiana pacers",
    "la clippers": "los angeles clippers",
    "lac": "los angeles clippers",
    "los angeles clippers": "los angeles clippers",
    "la lakers": "los angeles lakers",
    "lal": "los angeles lakers",
    "los angeles lakers": "los angeles lakers",
    "mem": "memphis grizzlies",
    "memphis": "memphis grizzlies",
    "mia": "miami heat",
    "miami": "miami heat",
    "mil": "milwaukee bucks",
    "milwaukee": "milwaukee bucks",
    "min": "minnesota timberwolves",
    "minnesota": "minnesota timberwolves",
    "new orleans": "new orleans pelicans",
    "nop": "new orleans pelicans",
    "nor": "new orleans pelicans",
    "new york": "new york knicks",
    "ny": "new york knicks",
    "nyk": "new york knicks",
    "okc": "oklahoma city thunder",
    "oklahoma city": "oklahoma city thunder",
    "orlando": "orlando magic",
    "orl": "orlando magic",
    "phi": "philadelphia 76ers",
    "philadelphia": "philadelphia 76ers",
    "philadelphia sixers": "philadelphia 76ers",
    "phx": "phoenix suns",
    "pho": "phoenix suns",
    "phoenix": "phoenix suns",
    "por": "portland trail blazers",
    "portland": "portland trail blazers",
    "sac": "sacramento kings",
    "sacramento": "sacramento kings",
    "san antonio": "san antonio spurs",
    "sa": "san antonio spurs",
    "sas": "san antonio spurs",
    "tor": "toronto raptors",
    "toronto": "toronto raptors",
    "utah": "utah jazz",
    "uta": "utah jazz",
    "washington": "washington wizards",
    "was": "washington wizards",
}


def canonical_team_name(name: str) -> str:
    """Canonicalize team names for matching."""
    normalized = " ".join(name.lower().split())
    return TEAM_NAME_ALIASES.get(normalized, normalized)


def normalize_person_name(name: str) -> str:
    """Normalize person names for fuzzy joins."""
    lowered = name.lower().strip()
    normalized = unicodedata.normalize("NFKD", lowered)
    ascii_only = "".join(ch for ch in normalized if ord(ch) < 128)
    cleaned = re.sub(r"[^a-z0-9]+", "", ascii_only)
    return cleaned
