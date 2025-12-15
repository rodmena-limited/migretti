from migretti import __version__
from migretti.__main__ import main
import sys


def test_version():
    assert __version__ == "0.9.2"


def test_main(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["migretti", "--help"])
    try:
        main()
    except SystemExit:
        pass
    captured = capsys.readouterr()
    assert "migretti - Database Migration Tool" in captured.out
