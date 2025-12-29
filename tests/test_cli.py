from migretti.__main__ import main
import sys


def test_cli_help(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["migretti", "--help"])
    try:
        main()
    except SystemExit:
        pass
    captured = capsys.readouterr()
    assert "migretti - Database Migration Tool" in captured.out


def test_cli_no_args(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["migretti"])
    main()  # prints help
    captured = capsys.readouterr()
    assert "Available commands" in captured.out
