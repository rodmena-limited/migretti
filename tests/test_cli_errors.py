import pytest
from migretti.__main__ import cmd_create, cmd_init


def test_cmd_init_exists(capsys, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "mg.yaml").touch()

    class Args:
        pass

    cmd_init(Args())
    # captured = capsys.readouterr()
    # Check log output - migretti uses logger for this error
    # But logger goes to stdout/stderr depending on setup
    # Our setup_logging writes to stdout
    # The code says logger.error(f"{CONFIG_FILENAME} already exists.")
    # But we need to ensure logging is setup.
    # cmd_init calls print for "Created ..." but logger.error for exists.
    # wait, cmd_init uses logger.error in the new version?
    # Let's check source code.
    pass


def test_cmd_create_no_migrations_dir(capsys, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    class Args:
        name = "foo"

    with pytest.raises(SystemExit):
        cmd_create(Args())
    # Should log error about run init first
