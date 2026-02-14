from prop_ev.cli import main


def test_cli_smoke(capsys, tmp_path):
    code = main(["--data-dir", str(tmp_path / "data" / "odds_api"), "snapshot", "ls"])
    captured = capsys.readouterr()

    assert code == 0
    assert "no snapshots" in captured.out
