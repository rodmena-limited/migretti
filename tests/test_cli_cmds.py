import pytest
from migretti.__main__ import cmd_status, cmd_list, cmd_head, cmd_verify


def test_cmd_status_no_args(capsys, monkeypatch):
    class Args:
        env = None

    # Mock get_migration_status to avoid DB call
    monkeypatch.setattr("migretti.__main__.get_migration_status", lambda env=None: [])

    cmd_status(Args())
    captured = capsys.readouterr()
    assert "Total migrations: 0" in captured.out


def test_cmd_list_no_args(capsys, monkeypatch):
    class Args:
        env = None

    monkeypatch.setattr("migretti.__main__.get_migration_status", lambda env=None: [])

    cmd_list(Args())
    captured = capsys.readouterr()
    assert "No migrations found" in captured.out


def test_cmd_head_no_args(capsys, monkeypatch):
    class Args:
        env = None

    monkeypatch.setattr("migretti.__main__.get_head", lambda env=None: None)

    cmd_head(Args())
    captured = capsys.readouterr()
    assert "No migrations applied" in captured.out


def test_cmd_verify_success(capsys, monkeypatch):
    class Args:
        env = None

    monkeypatch.setattr("migretti.__main__.verify_checksums", lambda env=None: True)

    cmd_verify(Args())
    captured = capsys.readouterr()
    assert "Verification Successful" in captured.out


def test_cmd_verify_fail(capsys, monkeypatch):
    class Args:
        env = None

    monkeypatch.setattr("migretti.__main__.verify_checksums", lambda env=None: False)

    with pytest.raises(SystemExit):
        cmd_verify(Args())
    captured = capsys.readouterr()
    assert "Verification Failed" in captured.out
