import subprocess
from typing import Optional
from migretti.config import load_config
from migretti.logging_setup import get_logger

logger = get_logger()

def execute_hook(hook_name: str, env: Optional[str] = None) -> None:
    """
    Executes a configured hook.
    """
    config = load_config(env=env)
    hooks = config.get("hooks", {})
    if not isinstance(hooks, dict):
        return
        
    command = hooks.get(hook_name)
    
    if not command or not isinstance(command, str):
        return
        
    logger.info(f"Running {hook_name} hook: {command}")
    try:
        # Split command for security/correctness unless shell=True
        # Using shell=True for flexibility (e.g. pipes)
        result = subprocess.run(
            command, 
            shell=True, 
            check=True, 
            text=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        )
        if result.stdout.strip():
            logger.info(f"Hook output: {result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Hook failed: {e.stderr}")
        raise RuntimeError(f"Hook {hook_name} failed")
