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

def run_seeds(env: Optional[str] = None) -> None:
    seeds = get_seed_files()
    if not seeds:
        logger.info("No seed files found in seeds/")
        return

    conn = get_connection(env=env)
    try:
        # Seeding should probably be transactional per file?
        with conn.cursor() as cur:
            for seed_file in seeds:
                logger.info(f"Running seed: {seed_file}")
                try:
                    with open(seed_file, "r", encoding="utf-8") as f:
                        sql = f.read()

                    # Transaction per file
                    with conn.transaction():
                        cur.execute(sql)

                    logger.info(f"Completed seed: {seed_file}")
                except Exception as e:
                    logger.error(f"Failed to run seed {seed_file}: {e}")
                    raise e
    finally:
        conn.close()
