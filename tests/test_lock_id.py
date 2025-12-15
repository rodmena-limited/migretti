from migretti.db import get_lock_id


def test_lock_id_default():
    assert get_lock_id() == 894321


def test_lock_id_env_var(monkeypatch):
    monkeypatch.setenv("MG_LOCK_ID", "123")
    assert get_lock_id() == 123


def test_lock_id_config_file(monkeypatch, tmp_path):
    d = tmp_path / "mg.yaml"
    d.write_text("lock_id: 999\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert get_lock_id() == 999


def test_lock_id_config_profile(monkeypatch, tmp_path):
    d = tmp_path / "mg.yaml"
    d.write_text(
        """
lock_id: 111
envs:
  prod:
    lock_id: 222
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    assert get_lock_id(env="prod") == 222
    assert get_lock_id(env="dev") == 111  # fallback to global
