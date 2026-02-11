from prop_ev.cli import main


def test_cli_smoke(capsys):
    code = main()
    captured = capsys.readouterr()

    assert code == 0
    assert "prop-ev CLI stub" in captured.out
