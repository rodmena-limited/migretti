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
    
    # check for direct env var config
    db_url = os.getenv("MG_DATABASE_URL")
    if db_url:
        return {"database": {"conninfo": db_url}}

    # Load from file
    file_config = {}
    if os.path.exists(CONFIG_FILENAME):
        with open(CONFIG_FILENAME, "r") as f:
            try:
                file_config = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                raise RuntimeError(f"Error parsing {CONFIG_FILENAME}: {e}")

    # Resolve environment profile
    # If env is not passed, check MG_ENV, default to 'default' or root
    target_env = env or os.getenv("MG_ENV", "default")
    
    final_db_config = {}
    
    if "envs" in file_config and target_env in file_config["envs"]:
        # Use profile specific config
        final_db_config = file_config["envs"][target_env].get("database", {})
    elif "database" in file_config:
        # Use root config (legacy support or simple setup)
        final_db_config = file_config["database"]
        
    # Override with specific env vars if set (e.g. MG_DB_HOST)
    if os.getenv("MG_DB_HOST"): final_db_config["host"] = os.getenv("MG_DB_HOST")
    if os.getenv("MG_DB_PORT"): final_db_config["port"] = os.getenv("MG_DB_PORT")
    if os.getenv("MG_DB_USER"): final_db_config["user"] = os.getenv("MG_DB_USER")
    if os.getenv("MG_DB_PASSWORD"): final_db_config["password"] = os.getenv("MG_DB_PASSWORD")
    if os.getenv("MG_DB_NAME"): final_db_config["dbname"] = os.getenv("MG_DB_NAME")

    return {"database": final_db_config}