from migretti.config import load_config


def test_config_env_var(monkeypatch):
    monkeypatch.setenv("MG_DATABASE_URL", "postgresql://u:p@h:5432/d")
    config = load_config()
    assert config["database"]["conninfo"] == "postgresql://u:p@h:5432/d"


def test_config_overrides(monkeypatch):
    # Mock file loading? No, let's just test env var overrides
    # If no file exists, load_config returns empty or throws if we rely on file
    # But we want to test specific overrides like MG_DB_HOST
    monkeypatch.setenv("MG_DB_HOST", "myhost")
    config = load_config()
    assert config["database"]["host"] == "myhost"


def test_config_profile(monkeypatch, tmp_path):
    # Create a dummy mg.yaml
    d = tmp_path / "mg.yaml"
    d.write_text("""
envs:
  prod:
    database:
        dbname: prod_db
  dev:
    database:
        dbname: dev_db
""")
    monkeypatch.chdir(tmp_path)

    # Test dev
    monkeypatch.setenv("MG_ENV", "dev")
    config = load_config()
    assert config["database"]["dbname"] == "dev_db"

    # Test prod
    config = load_config(env="prod")
    assert config["database"]["dbname"] == "prod_db"
