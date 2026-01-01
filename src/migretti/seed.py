import os
import glob
import sys
import argparse
from typing import List, Optional
from migretti.db import get_connection
from migretti.logging_setup import get_logger
from migretti.io_utils import atomic_write
logger = get_logger()

def get_seed_files() -> List[str]:
    if not os.path.exists("seeds"):
        return []
    return sorted(glob.glob(os.path.join("seeds", "*.sql")))
