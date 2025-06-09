#!/bin/bash

LOGFILE="data-loader.log"
exec >> "$LOGFILE" 2>&1

echo "========== Starting ogr2ogr import at $(date) =========="

# Directory where your GeoJSON files are stored
DATA_DIR="/srv/data-loader/data"

echo "[INFO] Importing openheim_rivers_cleaned.geojson..."
ogr2ogr -f "PostgreSQL" \
  PG:"$PG_DB_URL" \
  "$DATA_DIR/openheim_rivers_cleaned.geojson" \
  -nln spatial.rivers_geom \
  -overwrite \
  -nlt LINESTRING \
  -lco GEOMETRY_NAME=geom \
  -lco SCHEMA=spatial \
  -lco FID=id

echo "[INFO] Importing routes.geojson..."
ogr2ogr -f "PostgreSQL" \
  PG:"$PG_DB_URL" \
  "$DATA_DIR/routes.geojson" \
  -nln spatial.routes_geom_staging \
  -overwrite \
  -nlt LINESTRING \
  -lco GEOMETRY_NAME=geom \
  -lco SCHEMA=spatial \
  -lco FID=id

echo "[INFO] Importing cells.geojson..."
ogr2ogr -f "PostgreSQL" \
  PG:"$PG_DB_URL" \
  "$DATA_DIR/cells.geojson" \
  -nln spatial.cells_geom_staging \
  -overwrite \
  -nlt POLYGON \
  -lco GEOMETRY_NAME=geom \
  -lco SCHEMA=spatial \
  -lco FID=id

echo "[INFO] Importing openheim_land_cleaned.geojson..."
ogr2ogr -f "PostgreSQL" PG:"$PG_DB_URL" \
  "$DATA_DIR/openheim_land_cleaned.geojson" \
  -nln spatial.landmass_staging \
  -overwrite \
  -nlt MULTIPOLYGON \
  -lco GEOMETRY_NAME=geom \
  -lco FID=id

echo "[INFO] Importing openheim_rivers_cleaned.geojson..."
ogr2ogr -f "PostgreSQL" PG:"$PG_DB_URL" \
  "$DATA_DIR/openheim_rivers_cleaned.geojson" \
  -nln spatial.rivers_staging \
  -overwrite \
  -nlt LINESTRING \
  -lco GEOMETRY_NAME=geom \
  -lco FID=id

echo "All ogr2ogr imports completed at $(date)."