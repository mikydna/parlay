from prop_ev.cli import main


def test_cli_smoke(capsys, monkeypatch, tmp_path):
    monkeypatch.setenv("PROP_EV_DATA_DIR", str(tmp_path / "data" / "odds_api"))
    code = main(["snapshot", "ls"])
    captured = capsys.readouterr()

    assert code == 0
    assert "no snapshots" in captured.out
