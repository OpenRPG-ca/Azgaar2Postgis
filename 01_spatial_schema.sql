-- Drop all spatial tables
DROP TABLE IF EXISTS spatial.burgs_geom CASCADE;

DROP TABLE IF EXISTS spatial.burgs_pixel_geom CASCADE;

DROP TABLE IF EXISTS spatial.biomes_geom CASCADE;

DROP TABLE IF EXISTS spatial.cultures_geom CASCADE;

DROP TABLE IF EXISTS spatial.markers_geom CASCADE;

DROP TABLE IF EXISTS spatial.provinces_geom CASCADE;

DROP TABLE IF EXISTS spatial.religions_geom CASCADE;

DROP TABLE IF EXISTS spatial.routes_geom CASCADE;

DROP TABLE IF EXISTS spatial.routes_geom_staging CASCADE;

DROP TABLE IF EXISTS spatial.rivers_geom CASCADE;

DROP TABLE IF EXISTS spatial.cells_geom CASCADE;

DROP TABLE IF EXISTS spatial.cells_geom_staging CASCADE;

DROP TABLE IF EXISTS spatial.landmass_geom CASCADE;

DROP TABLE IF EXISTS spatial.landmass CASCADE;

DROP TABLE IF EXISTS spatial.landmass_staging CASCADE;

SET
  search_path = spatial,
  regular,
  public;

-- Burgs geometry
CREATE TABLE
  IF NOT EXISTS spatial.burgs_geom (
    id INT PRIMARY KEY REFERENCES regular."BurgsAttr" (id) ON DELETE CASCADE,
    geom geometry (Point, 4326) NOT NULL
  );

CREATE INDEX IF NOT EXISTS idx_burgs_geom_geom ON spatial.burgs_geom USING GIST (geom);

CREATE TABLE
  IF NOT EXISTS spatial.burgs_pixel_geom (
    id INT PRIMARY KEY REFERENCES regular."BurgsAttr" (id) ON DELETE CASCADE,
    geom geometry (Point, 0) NOT NULL -- Explicitly define SRID 0
  );

CREATE INDEX IF NOT EXISTS idx_burgs_pixel_geom_geom ON spatial.burgs_pixel_geom USING GIST (geom);

-- Biomes geometry
CREATE TABLE
  spatial.biomes_geom (
    id INTEGER PRIMARY KEY,
    geom geometry (MultiPolygon, 4326)
  );

-- Cultures geometry
CREATE TABLE
  spatial.cultures_geom (
    id INTEGER PRIMARY KEY,
    geom geometry (MultiPolygon, 4326)
  );

-- Markers geometry
CREATE TABLE
  spatial.markers_geom (id TEXT PRIMARY KEY, geom geometry (Point, 4326));

-- Provinces geometry
CREATE TABLE
  spatial.provinces_geom (
    id INTEGER PRIMARY KEY,
    geom geometry (MultiPolygon, 4326)
  );

-- Religions geometry
CREATE TABLE
  spatial.religions_geom (
    id INTEGER PRIMARY KEY,
    geom geometry (MultiPolygon, 4326)
  );

-- Routes geometry
CREATE TABLE
  spatial.routes_geom (
    id INTEGER PRIMARY KEY,
    geom geometry (LineString, 4326),
    geojsondata jsonb
  );

-- Routes staging geometry
CREATE TABLE
  spatial.routes_geom_staging (
    id TEXT PRIMARY KEY,
    geom geometry (LineString, 4326),
    geojsondata jsonb
  );

-- Rivers geometry
CREATE TABLE
  spatial.rivers_geom (
    id INTEGER PRIMARY KEY,
    name TEXT,
    basin TEXT,
    length_km DOUBLE PRECISION,
    width_m DOUBLE PRECISION,
    discharge_cms DOUBLE PRECISION,
    geom geometry (LineString), -- no SRID because you’re in pixel space
    geojsondata JSONB
  );

CREATE INDEX rivers_geom_gix ON spatial.rivers_geom USING GIST (geom);

-- Cells geometry
CREATE TABLE
  spatial.cells_geom (
    id INTEGER PRIMARY KEY,
    geom geometry (Polygon, 4326),
    geojsondata jsonb
  );

-- Cells staging geometry
CREATE TABLE
  spatial.cells_geom_staging (
    id INTEGER PRIMARY KEY,
    geom geometry (Polygon, 4326),
    geojsondata jsonb
  );

-- Landmass (for full polygons with holes)
CREATE TABLE
  spatial.landmass_geom (
    id SERIAL PRIMARY KEY,
    geom geometry (Polygon, 4326),
    name TEXT
  );

CREATE TABLE
  IF NOT EXISTS spatial.landmass (
    id SERIAL PRIMARY KEY,
    geom geometry (MultiPolygon, 4326),
    type TEXT -- 'landmass' or 'hole'
  );

CREATE TABLE
  IF NOT EXISTS spatial.landmass_staging (
    id INTEGER PRIMARY KEY,
    geom geometry (MultiPolygon, 4326),
    geojsondata jsonb,
    type TEXT -- 'landmass' or 'hole'
  );