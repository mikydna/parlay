"""NBA data module: ingestion plus unified runtime repository."""

from prop_ev.nba_data.normalize import canonical_team_name, normalize_person_name
from prop_ev.nba_data.repo import NBARepository
from prop_ev.nba_data.schema_version import SCHEMA_VERSION
from prop_ev.nba_data.source_policy import ResultsSourceMode, normalize_results_source_mode

__all__ = [
    "NBARepository",
    "ResultsSourceMode",
    "SCHEMA_VERSION",
    "canonical_team_name",
    "normalize_person_name",
    "normalize_results_source_mode",
]
