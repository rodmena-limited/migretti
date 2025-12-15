import os
import yaml
from typing import Dict, Any, Optional
from dotenv import load_dotenv

CONFIG_FILENAME = "mg.yaml"

# Load .env file if present
load_dotenv()


def load_config(env: Optional[str] = None) -> Dict[str, Any]:
    """
    Loads configuration.
    Priority:
    1. Environment Variables (MG_DATABASE_URL)
    2. mg.yaml (environment specific profile)
    3. mg.yaml (default/root)
    """

    # Load from file
    file_config: Dict[str, Any] = {}
    if os.path.exists(CONFIG_FILENAME):
        try:
            with open(CONFIG_FILENAME, "r", encoding="utf-8") as f:
                content = f.read()
                # Interpolate environment variables
                content = os.path.expandvars(content)
                file_config = yaml.safe_load(content) or {}
        except (yaml.YAMLError, OSError) as e:
            raise RuntimeError(f"Error parsing {CONFIG_FILENAME}: {e}")

    # Resolve environment profile
    # If env is not passed, check MG_ENV, default to 'default' or root
    target_env = env or os.getenv("MG_ENV", "default")

    final_db_config: Dict[str, Any] = {}

    if "envs" in file_config and isinstance(file_config["envs"], dict) and target_env in file_config["envs"]:
        # Use profile specific config
        env_config = file_config["envs"][target_env]
        if isinstance(env_config, dict):
            final_db_config = env_config.get("database", {})
    elif "database" in file_config and isinstance(file_config["database"], dict):
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

    # Lock ID handling
    if "envs" in file_config and isinstance(file_config["envs"], dict) and target_env in file_config["envs"]:
         env_config = file_config["envs"][target_env]
         if isinstance(env_config, dict) and "lock_id" in env_config:
             final_config["lock_id"] = env_config["lock_id"]
             
    if "lock_id" not in final_config and "lock_id" in file_config:
        final_config["lock_id"] = file_config["lock_id"]

    # Forward hooks
    if "hooks" in file_config:
        final_config["hooks"] = file_config["hooks"]

    # Lock ID via Env Var
    if os.getenv("MG_LOCK_ID"):
        try:
            final_config["lock_id"] = int(os.getenv("MG_LOCK_ID", ""))
        except ValueError:
            pass # Ignore invalid

    # Database URL Override
    db_url = os.getenv("MG_DATABASE_URL")
    if db_url:
        final_config["database"]["conninfo"] = db_url

    return final_config
