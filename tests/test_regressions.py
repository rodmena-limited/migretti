"""
Regression tests for the 2026-07 production-readiness audit (unit level).

Each test names the audit finding it guards: C=critical, H=high, M=medium,
L=low. DB-dependent regressions live in tests/integration/test_regressions_db.py.
"""

import logging

import pytest

import migretti.config as config_mod
from migretti.__main__ import build_parser
from migretti.config import load_config, _interpolate_env_vars
from migretti.core import get_migration_files, rollback_migrations
from migretti.db import get_lock_id
from migretti.safety import check_prod_protection
from migretti.seed import cmd_seed


# --- C3: rollback steps must be a positive integer ---------------------------


def test_c3_cli_rejects_zero_steps():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["rollback", "0"])


def test_c3_cli_rejects_negative_steps():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["rollback", "-1"])


def test_c3_core_rejects_nonpositive_steps():
    with pytest.raises(ValueError, match="positive"):
        rollback_migrations(steps=0)
    with pytest.raises(ValueError, match="positive"):
        rollback_migrations(steps=-1)


# --- H1: an unknown environment must never fall back to another database -----


def _write_envs_config(tmp_path):
    (tmp_path / "mg.yaml").write_text(
        "database:\n  dbname: rootdb\n"
        "envs:\n  dev:\n    database:\n      dbname: devdb\n",
        encoding="utf-8",
    )


def test_h1_unknown_env_rejected(monkeypatch, tmp_path):
    _write_envs_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match="not defined"):
        load_config(env="stagng")


def test_h1_unknown_mg_env_rejected(monkeypatch, tmp_path):
    _write_envs_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MG_ENV", "stagng")
    with pytest.raises(RuntimeError, match="not defined"):
        load_config()


def test_h1_error_lists_available_envs(monkeypatch, tmp_path):
    _write_envs_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match="dev"):
        load_config(env="stagng")


def test_h1_explicit_env_without_envs_section_rejected(monkeypatch, tmp_path):
    (tmp_path / "mg.yaml").write_text("database:\n  dbname: rootdb\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match="not defined"):
        load_config(env="prod")


def test_h1_root_config_without_env_still_works(monkeypatch, tmp_path):
    _write_envs_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert load_config()["database"]["dbname"] == "rootdb"


# --- H3: env-var interpolation must not corrupt values containing '$' --------


def test_h3_bare_dollar_preserved(monkeypatch, tmp_path):
    monkeypatch.setenv("USERs3cret", "LEAKED")
    (tmp_path / "mg.yaml").write_text(
        "database:\n  dbname: d\n  password: Sup3r$USERs3cret\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    assert load_config()["database"]["password"] == "Sup3r$USERs3cret"


def test_h3_braced_form_interpolated(monkeypatch, tmp_path):
    monkeypatch.setenv("MY_DB_PW", "s3cret")
    (tmp_path / "mg.yaml").write_text(
        "database:\n  dbname: d\n  password: ${MY_DB_PW}\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    assert load_config()["database"]["password"] == "s3cret"


def test_h3_unset_braced_var_is_an_error(monkeypatch, tmp_path):
    monkeypatch.delenv("MG_AUDIT_UNSET_VAR", raising=False)
    (tmp_path / "mg.yaml").write_text(
        "database:\n  dbname: d\n  password: ${MG_AUDIT_UNSET_VAR}\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match="MG_AUDIT_UNSET_VAR"):
        load_config()


def test_h3_escaped_dollar_brace_is_literal(monkeypatch):
    monkeypatch.delenv("FOO", raising=False)
    assert _interpolate_env_vars("password: $${FOO}") == "password: ${FOO}"


def test_h3_url_override_of_explicit_env_warns(monkeypatch, tmp_path, caplog):
    config_mod._warned_url_override.clear()
    _write_envs_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MG_DATABASE_URL", "postgresql://u:p@h/x")
    with caplog.at_level(logging.WARNING):
        cfg = load_config(env="dev")
    assert cfg["database"]["conninfo"] == "postgresql://u:p@h/x"
    assert "overrides" in caplog.text


# --- H2: the fix command exists ----------------------------------------------


def test_h2_fix_command_registered():
    args = build_parser().parse_args(["fix", "01ABC", "--applied"])
    assert args.func.__name__ == "cmd_fix"
    assert args.applied is True
    assert args.remove is False


def test_h2_fix_requires_an_action_flag():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["fix", "01ABC"])


def test_h2_fix_flags_mutually_exclusive():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["fix", "01ABC", "--applied", "--remove"])


# --- M6: shared flags work before and after the subcommand -------------------


def test_m6_env_flag_after_subcommand():
    args = build_parser().parse_args(["apply", "--env", "staging"])
    assert args.env == "staging"


def test_m6_env_flag_before_subcommand():
    args = build_parser().parse_args(["--env", "staging", "apply"])
    assert args.env == "staging"


def test_m6_shared_flag_defaults_when_absent():
    args = build_parser().parse_args(["apply"])
    assert args.env is None
    assert args.json_log is False
    assert args.verbose is False


# --- M2: the prod gate covers dry runs (they execute SQL) --------------------


def test_m2_dry_run_still_prompts_on_prod(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "no")

    class Args:
        env = "prod"
        dry_run = True
        yes = False

    with pytest.raises(SystemExit) as e:
        check_prod_protection(Args())
    assert e.value.code == 0


def test_m2_yes_flag_skips_prompt():
    class Args:
        env = "prod"
        dry_run = True
        yes = True

    check_prod_protection(Args())  # must not prompt or exit


# --- M6: seed run is gated like other SQL-executing commands -----------------


def test_m6_seed_run_gated_on_prod(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "no")

    class Args:
        seed_command = None
        env = "prod"
        yes = False

    with pytest.raises(SystemExit) as e:
        cmd_seed(Args())
    assert e.value.code == 0


# --- M3/M7: migration scanner validation -------------------------------------


def _mig(tmp_path, fname):
    (tmp_path / "migrations").mkdir(exist_ok=True)
    (tmp_path / "migrations" / fname).write_text(
        "-- migrate: up\nSELECT 1;\n-- migrate: down\nSELECT 1;\n", encoding="utf-8"
    )


def test_m3_skipped_file_warns(monkeypatch, tmp_path, caplog):
    _mig(tmp_path, "nounderscore.sql")
    monkeypatch.chdir(tmp_path)
    with caplog.at_level(logging.WARNING):
        migs = get_migration_files()
    assert migs == []
    assert "Skipping" in caplog.text


def test_m7_duplicate_id_rejected(monkeypatch, tmp_path):
    _mig(tmp_path, "01_a.sql")
    _mig(tmp_path, "01_b.sql")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="Duplicate"):
        get_migration_files()


def test_m7_invalid_id_rejected(monkeypatch, tmp_path):
    _mig(tmp_path, "bad-id!_x.sql")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="Invalid migration id"):
        get_migration_files()


def test_m7_oversized_id_rejected(monkeypatch, tmp_path):
    _mig(tmp_path, "A" * 27 + "_x.sql")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="Invalid migration id"):
        get_migration_files()


def test_m7_mixed_width_ids_warn(monkeypatch, tmp_path, caplog):
    _mig(tmp_path, "2_first.sql")
    _mig(tmp_path, "10_second.sql")
    monkeypatch.chdir(tmp_path)
    with caplog.at_level(logging.WARNING):
        migs = get_migration_files()
    assert [m[0] for m in migs] == ["10", "2"]  # string order — hence the warning
    assert "mixed lengths" in caplog.text


# --- L8: lock_id must be a real integer --------------------------------------


def test_l8_bool_lock_id_rejected(monkeypatch, tmp_path):
    (tmp_path / "mg.yaml").write_text("lock_id: true\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert get_lock_id() == 894321


def test_l8_string_lock_id_rejected(monkeypatch, tmp_path):
    (tmp_path / "mg.yaml").write_text("lock_id: not_a_number\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert get_lock_id() == 894321


# --- L9: seed names are sanitized into seeds/ --------------------------------


def test_l9_seed_create_sanitizes_name(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    class Args:
        seed_command = "create"
        name = "../Evil Name!"

    cmd_seed(Args())
    assert (tmp_path / "seeds" / "evil_name.sql").exists()
    assert not (tmp_path / "Evil Name!.sql").exists()
