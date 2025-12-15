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

def cmd_seed(args: argparse.Namespace) -> None:
    """Run data seeding scripts."""
    # Subcommand handling: seed run (default) or seed create
    if getattr(args, "seed_command", None) == "create":
        name = args.name
        filename = f"{name}.sql"
        if not os.path.exists("seeds"):
            os.makedirs("seeds")
            logger.info("Created seeds/ directory")
            
        filepath = os.path.join("seeds", filename)
        try:
            with atomic_write(filepath, exclusive=True) as f:
                f.write(f"-- Seed: {name}\n\nINSERT INTO ...\n")
            print(f"Created {filepath}")
        except Exception as e:
            logger.error(f"Failed to create seed file: {e}")
            sys.exit(1)
    else:
        # Run seeds
        run_seeds(env=args.env)