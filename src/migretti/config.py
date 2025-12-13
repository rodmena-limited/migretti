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
    target_env = env or os.getenv("MG_ENV", "default")

    final_config: Dict[str, Any] = {"database": {}}

    # 1. Base Config
    if "database" in file_config:
        final_config["database"] = file_config["database"]

    # 2. Env Profile Config
    if "envs" in file_config and target_env in file_config["envs"]:
        env_config = file_config["envs"][target_env]
        if "database" in env_config:
            final_config["database"] = env_config["database"]
        # Allow overriding lock_id per env
        if "lock_id" in env_config:
            final_config["lock_id"] = env_config["lock_id"]

    # 3. Global Config (if not set by env)
    if "lock_id" not in final_config and "lock_id" in file_config:
        final_config["lock_id"] = file_config["lock_id"]

    # Forward hooks
    if "hooks" in file_config:
        final_config["hooks"] = file_config["hooks"]

    # 4. Environment Variables Overrides
    db_url = os.getenv("MG_DATABASE_URL")
    if db_url:
        final_config["database"]["conninfo"] = db_url

    if os.getenv("MG_DB_HOST"):
        final_config["database"]["host"] = os.getenv("MG_DB_HOST")
    if os.getenv("MG_DB_PORT"):
        final_config["database"]["port"] = os.getenv("MG_DB_PORT")
    if os.getenv("MG_DB_USER"):
        final_config["database"]["user"] = os.getenv("MG_DB_USER")
    if os.getenv("MG_DB_PASSWORD"):
        final_config["database"]["password"] = os.getenv("MG_DB_PASSWORD")
    if os.getenv("MG_DB_NAME"):
        final_config["database"]["dbname"] = os.getenv("MG_DB_NAME")

    # Lock ID via Env Var
    if os.getenv("MG_LOCK_ID"):
        try:
            final_config["lock_id"] = int(os.getenv("MG_LOCK_ID", ""))
        except ValueError:
            pass  # Ignore invalid

    return final_config
