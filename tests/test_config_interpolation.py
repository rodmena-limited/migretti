from migretti.config import load_config


def test_config_interpolation(monkeypatch, tmp_path):
    monkeypatch.setenv("MY_HOST", "interpolated-host")
    monkeypatch.setenv("MY_PORT", "9999")

    d = tmp_path / "mg.yaml"
    d.write_text(
        """
database:
  host: ${MY_HOST}
  port: ${MY_PORT}
  user: postgres
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    config = load_config()
    assert config["database"]["host"] == "interpolated-host"
    assert (
        config["database"]["port"] == 9999
    )  # YAML parser might see digit? no expandvars returns str. YAML might re-parse?
    # os.path.expandvars on "port: ${MY_PORT}" -> "port: 9999".
    # yaml.safe_load will parse 9999 as int.
    # Let's see.
