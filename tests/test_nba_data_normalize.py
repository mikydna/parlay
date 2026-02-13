from prop_ev.nba_data.normalize import canonical_team_name, normalize_person_name


def test_canonical_team_name_aliases() -> None:
    assert canonical_team_name("LA Clippers") == "los angeles clippers"
    assert canonical_team_name("Los Angeles Lakers") == "los angeles lakers"
    assert canonical_team_name("BKN") == "brooklyn nets"
    assert canonical_team_name("PHI") == "philadelphia 76ers"


def test_normalize_person_name() -> None:
    assert normalize_person_name("Luka Dončić") == "lukadoncic"
    assert normalize_person_name("D'Angelo Russell") == "dangelorussell"
