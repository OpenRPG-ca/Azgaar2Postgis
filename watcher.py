import os
import time
import subprocess
import sys
import shutil
import logging
from zipfile import ZipFile
from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler

load_dotenv()

WATCH_DIR = sys.argv[1] if len(sys.argv) > 1 else "/var/www/html/azgaar"
DATA_DIR = "/srv/data-loader/data"
ARCHIVE_DIR = "/srv/data-loader/processed_zips"
FAILED_DIR = "/srv/data-loader/failed_zips"
LOG_FILE = "data-loader.log"
POLL_INTERVAL = 10  # seconds
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_DATABASE = os.environ.get("PG_DATABASE", "")
PG_DB_URL = os.getenv("PG_DB_URL")  # Set via environment or .env
env = dict(os.environ)
env["PGPASSWORD"] = PG_PASSWORD
env["PG_DB_URL"] = PG_DB_URL
DDL_SQL = "01_spatial_schema.sql"
CLEAN_PY = "02_extract_and_clean.py"
OGR2OGR_SH = "03_ogr2ogr_import.sh"
ATTR_SQL = "04_bulk_attribute_import.sql"

REQUIRED_FILES = [
    "cells.geojson",
    "openheim.svg",
    "biomes.csv",
    "burgs.csv",
    "cultures.csv",
    "markers.csv",
    "provinces.csv",
    "religions.csv",
    "rivers.csv",
    "routes.csv",
    "markers.geojson",
    "rivers.geojson",
    "routes.geojson",
]

# ---- TimedRotatingFileHandler for daily log rotation ----
handler = TimedRotatingFileHandler(LOG_FILE, when="midnight", interval=1, backupCount=7)
handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logging.getLogger().handlers = [handler]
logging.getLogger().setLevel(logging.INFO)
# --------------------------------------------------------


def log(msg):
    logging.info(msg)
    print(msg, flush=True)


def prune_archive_dir():
    """
    Keep only the last 3 zip files (by modification time) in the ARCHIVE_DIR.
    """
    zip_files = [f for f in os.listdir(ARCHIVE_DIR) if f.endswith(".zip")]
    if len(zip_files) <= 3:
        return
    # Sort by modification time, newest last
    zip_files_full = [os.path.join(ARCHIVE_DIR, f) for f in zip_files]
    zip_files_full.sort(key=lambda f: os.path.getmtime(f), reverse=True)
    # Keep the 3 most recent
    for old_file in zip_files_full[3:]:
        try:
            os.remove(old_file)
            log(f"Deleted old archived zip: {old_file}")
        except Exception as e:
            log(f"Failed to delete old zip {old_file}: {e}")


def run_cmd(cmd, **kwargs):
    log(f"Running: {cmd if isinstance(cmd, str) else ' '.join(cmd)}")
    result = subprocess.run(
        cmd, shell=isinstance(cmd, str), capture_output=True, text=True, **kwargs
    )
    if result.returncode != 0:
        log(f"ERROR: {result.stderr.strip()}")
        raise RuntimeError(f"Command failed: {cmd}")
    else:
        log(result.stdout.strip())
    return result


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR, exist_ok=True)


def check_zip_contents(zip_path):
    """Return True if all required files are present, else False and log missing ones."""
    with ZipFile(zip_path, "r") as zipf:
        names = set(zipf.namelist())
        missing = [f for f in REQUIRED_FILES if f not in names]
        if missing:
            log(f"ERROR: openheim.zip is missing required files: {missing}")
            return False
        return True


def process_zip(zip_path):
    log(f"Found zip: {zip_path}")

    # 1. Check contents before extraction
    if not check_zip_contents(zip_path):
        log("Aborting import. Zip is missing required files.")
        return

    # 2. Unzip to DATA_DIR (overwrite)
    shutil.unpack_archive(zip_path, DATA_DIR)
    log(f"Extracted zip to {DATA_DIR}")

    # 3. Run DDL SQL
    run_cmd(
        [
            "psql",
            PG_DATABASE,
            "-U",
            PG_USER,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            DDL_SQL,
        ],
        env=env,
    )

    # 4. Run Python cleaning script
    run_cmd([sys.executable, CLEAN_PY])

    # 5. Run ogr2ogr import script
    run_cmd(
        ["bash", OGR2OGR_SH],
        env=env,
    )

    # 6. Run bulk attribute SQL
    run_cmd(
        [
            "psql",
            PG_DATABASE,
            "-U",
            PG_USER,
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            ATTR_SQL,
        ],
        env=env,
    )

    log("All steps completed successfully.")

    # 7. Move the zip to ARCHIVE
    archive_path = os.path.join(ARCHIVE_DIR, os.path.basename(zip_path))
    shutil.move(zip_path, archive_path)
    log(f"Archived {zip_path}.")
    prune_archive_dir()


def move_to_failed(zip_path):
    try:
        failed_path = os.path.join(FAILED_DIR, os.path.basename(zip_path))
        shutil.move(zip_path, failed_path)
        log(f"Moved failed zip {zip_path} to {FAILED_DIR}")
    except Exception as ex:
        log(f"ERROR: Could not move failed zip {zip_path} to {FAILED_DIR}: {ex}")


def main():
    ensure_dirs()
    log(f"Watching folder: {WATCH_DIR} for openheim.zip ...")
    while True:
        zip_path = os.path.join(WATCH_DIR, "openheim.zip")
        if os.path.isfile(zip_path):
            try:
                process_zip(zip_path)
            except Exception as e:
                log(f"Failed to process {zip_path}: {e}")
                move_to_failed(zip_path)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
