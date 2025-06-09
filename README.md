# Azgaar2Postgis

An attempt at converting Azgaar's Fantasy Map Generator (FMG) outputs into a PostGIS database.

---

## Overview

This project provides a workflow to extract, clean, and import data from Azgaar's Fantasy Map Generator (FMG) into a PostGIS-enabled PostgreSQL database. It includes scripts for parsing SVG and GeoJSON data, cleaning and transforming it, and importing it using ogr2ogr and SQL scripts.

---

## Repository Contents

- `01_spatial_schema.sql`: SQL script to set up the PostGIS schema.
- `02_extract_and_clean.py`: Main Python script for extracting and cleaning SVG/GeoJSON data.
- `03_ogr2ogr_import.sh`: Shell script to import data into PostGIS using ogr2ogr.
- `04_bulk_attribute_import.sql`: SQL script for bulk attribute imports.
- `requirements.txt`: Python dependencies.
- `watcher.py`: Likely a utility script for file monitoring or automation.
- Data files (`.geojson`, `.csv`, `.svg`) should be placed in the expected directories as referenced in the scripts.

---

## Database Setup Instructions

### assumptions:

1 you are using postgres 15 or higher
2 you have the postgis extension installed

### 1. Create the Database and User

Connect to PostgreSQL as a superuser (e.g., `postgres`):

```sh
psql -U postgres
```

Then run the following SQL commands (replace `DB_NAME` and `DB_OWNER` with your desired database name and owner):

```sql
CREATE USER DB_OWNER WITH PASSWORD 'choose_a_strong_password';
CREATE DATABASE DB_NAME OWNER DB_OWNER;
\c DB_NAME
```

### 2. Create Schemas

Connect to the new database as `DB_OWNER` (or a superuser):

```sql
CREATE SCHEMA regular AUTHORIZATION DB_OWNER;
CREATE SCHEMA spatial AUTHORIZATION DB_OWNER;
```

### 3. Install PostGIS Extension

Still in the database, run:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

---

## Python Environment Setup

Install Python 3.8+ and pip if needed, then install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Data Preparation

- Place your Azgaar FMG SVG output (e.g., `openheim.svg`) and relevant GeoJSON/CSV files into the `data` directory (`/srv/data-loader/data` by default).
- Adjust paths in `02_extract_and_clean.py` if your directories differ.

---

## Schema and Data Import

1. **Set up the schema:**

   ```bash
   psql -U DB_OWNER -d DB_NAME -f 01_spatial_schema.sql
   ```

2. **Extract and clean data:**

   ```bash
   python 02_extract_and_clean.py
   ```

3. **Import data into PostGIS:**

   ```bash
   bash 03_ogr2ogr_import.sh
   psql -U DB_OWNER -d DB_NAME -f 04_bulk_attribute_import.sql
   ```

---

## Dependencies

- Python packages:
  - `svgpathtools`
  - `geojson`
  - `shapely`
  - `tqdm`
  - `python-dotenv`
- System dependencies:
  - PostgreSQL
  - PostGIS
  - ogr2ogr (`gdal` package)

Install system dependencies via your OS package manager.

---

## License

See [LICENSE](LICENSE).

---

## Acknowledgements

- [Azgaar's Fantasy Map Generator](https://azgaar.github.io/Fantasy-Map-Generator/)
- [PostGIS](https://postgis.net/)
