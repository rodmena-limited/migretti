import logging
import sys
import json
from typing import Any, Dict

# Create a custom logger
logger = logging.getLogger("migretti")

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logging(json_format: bool = False, verbose: bool = False) -> None:
    """
    Setup logging configuration.
    """
    handler = logging.StreamHandler(sys.stdout)
    
    formatter: logging.Formatter
    if json_format:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        
    handler.setFormatter(formatter)
    
    logger.handlers = []
    logger.addHandler(handler)
    
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)

def get_logger() -> logging.Logger:
    return logger