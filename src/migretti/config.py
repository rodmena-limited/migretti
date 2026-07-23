import os
import re
import yaml
from typing import Dict, Any, Optional, Set
from dotenv import load_dotenv
from migretti.logging_setup import get_logger

logger = get_logger()

CONFIG_FILENAME = "mg.yaml"

# Load .env file if present
load_dotenv()

_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_ESCAPE_SENTINEL = "\x00MG_LITERAL_DOLLAR_BRACE\x00"

# Warn-once bookkeeping so repeated load_config() calls in one run don't spam.
_warned_url_override: Set[str] = set()
_warned_bad_lock_id = False


def _interpolate_env_vars(content: str) -> str:
    """
    Replace ${VAR} with the value of the environment variable VAR.

    Only the braced form is special: a bare `$` (for example inside a
    password) is left untouched, and `$${VAR}` escapes interpolation,
    producing a literal `${VAR}`. Referencing an unset variable is an error
    rather than a silently wrong literal value.
    """
    content = content.replace("$${", _ESCAPE_SENTINEL)

    def _sub(match: "re.Match[str]") -> str:
        name = match.group(1)
        value = os.environ.get(name)
        if value is None:
            raise RuntimeError(
                f"Environment variable '{name}' referenced in {CONFIG_FILENAME} is not set."
            )
        return value

    content = _ENV_VAR_RE.sub(_sub, content)
    return content.replace(_ESCAPE_SENTINEL, "${")


def load_config(env: Optional[str] = None) -> Dict[str, Any]:
    """
    Loads configuration.
    Priority:
    1. Environment Variables (MG_DATABASE_URL)
    2. mg.yaml (environment specific profile)
    3. mg.yaml (default/root)

    An explicitly requested environment (--env or MG_ENV) that is not defined
    in mg.yaml is an error: a typo must never silently fall back to another
    database.
    """
    global _warned_bad_lock_id

    # Load from file
    file_config: Dict[str, Any] = {}
    if os.path.exists(CONFIG_FILENAME):
        try:
            with open(CONFIG_FILENAME, "r", encoding="utf-8") as f:
                content = f.read()
            content = _interpolate_env_vars(content)
            file_config = yaml.safe_load(content) or {}
        except (yaml.YAMLError, OSError) as e:
            raise RuntimeError(f"Error parsing {CONFIG_FILENAME}: {e}")

    # Resolve environment profile. "default" (the implicit value when neither
    # --env nor MG_ENV is set) means the root config unless a profile named
    # "default" exists.
    explicit_env = env or os.getenv("MG_ENV")
    target_env = explicit_env or "default"

    envs = file_config.get("envs")
    env_config: Optional[Dict[str, Any]] = None

    if explicit_env and explicit_env != "default":
        if not isinstance(envs, dict) or explicit_env not in envs:
            available = sorted(envs) if isinstance(envs, dict) else []
            detail = (
                f" (available: {', '.join(available)})"
                if available
                else f" (no 'envs' section in {CONFIG_FILENAME})"
            )
            raise RuntimeError(
                f"Environment '{explicit_env}' is not defined in {CONFIG_FILENAME}{detail}."
            )
        candidate = envs[explicit_env]
        if not isinstance(candidate, dict):
            raise RuntimeError(
                f"Environment '{explicit_env}' in {CONFIG_FILENAME} is not a mapping."
            )
        env_config = candidate
    elif (
        isinstance(envs, dict)
        and target_env in envs
        and isinstance(envs[target_env], dict)
    ):
        env_config = envs[target_env]

    final_db_config: Dict[str, Any] = {}
    if env_config is not None:
        final_db_config = env_config.get("database", {}) or {}
    elif isinstance(file_config.get("database"), dict):
        # Use root config (legacy support or simple setup)
        final_db_config = file_config["database"]

    # Override with specific env vars if set (e.g. MG_DB_HOST)
    if os.getenv("MG_DB_HOST"):
        final_db_config["host"] = os.getenv("MG_DB_HOST")
    if os.getenv("MG_DB_PORT"):
        final_db_config["port"] = os.getenv("MG_DB_PORT")
    if os.getenv("MG_DB_USER"):
        final_db_config["user"] = os.getenv("MG_DB_USER")
    if os.getenv("MG_DB_PASSWORD"):
        final_db_config["password"] = os.getenv("MG_DB_PASSWORD")
    if os.getenv("MG_DB_NAME"):
        final_db_config["dbname"] = os.getenv("MG_DB_NAME")

    final_config: Dict[str, Any] = {"database": final_db_config}

    # Lock ID handling: env profile wins over root
    if env_config is not None and "lock_id" in env_config:
        final_config["lock_id"] = env_config["lock_id"]
    elif "lock_id" in file_config:
        final_config["lock_id"] = file_config["lock_id"]

    # Forward hooks
    if "hooks" in file_config:
        final_config["hooks"] = file_config["hooks"]

    # Lock ID via Env Var
    if os.getenv("MG_LOCK_ID"):
        try:
            final_config["lock_id"] = int(os.getenv("MG_LOCK_ID", ""))
        except ValueError:
            if not _warned_bad_lock_id:
                logger.warning(
                    f"MG_LOCK_ID={os.getenv('MG_LOCK_ID')!r} is not an integer; ignoring it."
                )
                _warned_bad_lock_id = True

    # Database URL Override
    db_url = os.getenv("MG_DATABASE_URL")
    if db_url:
        if (
            explicit_env
            and explicit_env != "default"
            and target_env not in _warned_url_override
        ):
            logger.warning(
                f"MG_DATABASE_URL is set and overrides the '{target_env}' environment profile."
            )
            _warned_url_override.add(target_env)
        final_config["database"]["conninfo"] = db_url

    return final_config
