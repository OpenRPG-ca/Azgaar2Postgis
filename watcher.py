import os
import time
import subprocess
import sys
import shutil
import psycopg2
import logging
from datetime import datetime, timezone
from zipfile import ZipFile
from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler
from db_utils import (
    set_previous_active_to_passed,
    insert_fileupload_entry,
    update_fileupload_status,
    get_next_version,
)

load_dotenv()

WATCH_DIR = next(
    (arg for arg in sys.argv[1:] if not arg.startswith("-")), "/var/www/html/azgaar"
)
DATA_DIR = "/srv/data-loader/data"
ARCHIVE_DIR = "/srv/data-loader/processed_zips"
FAILED_DIR = "/srv/data-loader/failed_zips"
LOG_FILE = "data-loader.log"
POLL_INTERVAL = 10  # seconds
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_DATABASE = os.environ.get("PG_DATABASE", "")
PG_DB_URL = os.getenv("PG_DB_URL")  # Set via environment or .env
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
    result = subprocess.run(
        cmd,
        shell=isinstance(cmd, str),
        capture_output=True,
        text=True,
        **kwargs,
    )
    if result.returncode != 0:
        error_msg = (
            f"Command failed: {cmd if isinstance(cmd, str) else ' '.join(cmd)}\n"
            f"stdout: {result.stdout.strip()}\n"
            f"stderr: {result.stderr.strip()}"
        )
        print(error_msg, flush=True)
        logging.error(error_msg)
        raise RuntimeError(error_msg)
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


def process_zip(zip_path, env, version):
    # 1. Check contents before extraction
    if not check_zip_contents(zip_path):
        log("Aborting import. Zip is missing required files.")
        move_to_failed(zip_path, env)
        return

    # 2. Unzip to DATA_DIR (overwrite)
    shutil.unpack_archive(zip_path, DATA_DIR)
    log(f"Extracted zip to {DATA_DIR}")

    # 3. Run DDL SQL
    result = run_cmd(
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
    if result.returncode != 0:
        print(result.stderr, flush=True)

    log(f"SUCCESS: spatial schema created with {DDL_SQL}")

    # 4. Run Python cleaning script
    run_cmd([sys.executable, CLEAN_PY])
    log(f"SUCCESS: cleaning script {CLEAN_PY} executed successfully")

    # 5. Run ogr2ogr import script
    run_cmd(
        ["bash", OGR2OGR_SH],
        env=env,
    )
    log(f"SUCCESS: ogr2ogr import script {OGR2OGR_SH} executed successfully")

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
    log(f"SUCCESS: bulk attribute import with {ATTR_SQL}")

    log("All steps completed successfully.")

    # 7. Move the zip to ARCHIVE
    archive_path = os.path.join(ARCHIVE_DIR, os.path.basename(zip_path))
    shutil.move(zip_path, archive_path)
    log(f"Archived {zip_path}.")

    # DB update for PASSED and ACTIVE
    try:
        # Set last as active
        update_fileupload_status(
            path=archive_path,
            baseName="openheim.zip",
            name=os.path.basename(zip_path),
            status="active",
            version=version,
            pg_url=PG_DB_URL,
        )
        log(f"Updated regular.fileupload with passed/active status, version {version}")
    except Exception as e:
        log(f"ERROR: Failed to update regular.fileupload DB on archive: {e}")

    prune_archive_dir()


def move_to_failed(zip_path, env):
    try:
        failed_path = os.path.join(FAILED_DIR, os.path.basename(zip_path))
        shutil.move(zip_path, failed_path)
        log(f"Moved failed zip {zip_path} to {FAILED_DIR}")

        # DB update for FAILED
        update_fileupload_status(
            path=failed_path,
            baseName="openheim.zip",
            name=os.path.basename(zip_path),
            status="failed",
            version=None,  # No version for failed
            pg_url=PG_DB_URL,
        )
    except Exception as ex:
        log(f"ERROR: Could not move failed zip {zip_path} to {FAILED_DIR}: {ex}")


def main():
    is_test_mode = "-test" in sys.argv
    if is_test_mode:
        log("Running in TEST MODE — will pre-create FileUpload entry.")

    if PG_PASSWORD is None:
        raise ValueError("PG_PASSWORD environment variable must not be None")
    if PG_DB_URL is None:
        raise ValueError("PG_DB_URL environment variable must not be None")

    env = dict(os.environ)
    env["PGPASSWORD"] = PG_PASSWORD
    env["PG_DB_URL"] = PG_DB_URL

    ensure_dirs()
    log(f"Watching folder: {WATCH_DIR} for openheim.zip ...")
    while True:
        original_zip = os.path.join(WATCH_DIR, "openheim.zip")
        log(f"Polling for file: {original_zip}")
        if os.path.isfile(original_zip):
            try:
                version = get_next_version(PG_DB_URL, "openheim.zip")
                renamed_zip = os.path.join(WATCH_DIR, f"openheim_{version}.zip")
                os.rename(original_zip, renamed_zip)
                log(f"Found and moved file to {renamed_zip}")

                if is_test_mode:
                    test_filename = f"openheim_{version}.zip"
                    insert_fileupload_entry(
                        name=test_filename,
                        baseName="openheim",
                        path=renamed_zip,
                        version=version,
                        status="uploaded",
                        pg_url=PG_DB_URL,
                    )

                process_zip(renamed_zip, env, version)
            except Exception as e:
                log(f"ERROR: Exception during zip processing: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
