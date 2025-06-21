SET search_path = spatial, regular, public;

DROP TABLE IF EXISTS burgsattr_staging;
CREATE TEMP TABLE burgsattr_staging (
    "Id" INTEGER,
    "Burg" TEXT,
    "Province" TEXT,
    "Province Full Name" TEXT,
    "State" TEXT,
    "State Full Name" TEXT,
    "Culture" TEXT,
    "Religion" TEXT,
    "Population" INTEGER,
    "X" DOUBLE PRECISION,
    "Y" DOUBLE PRECISION,
    "Latitude" DOUBLE PRECISION,
    "Longitude" DOUBLE PRECISION,
    "Elevation (m)" INTEGER,
    "Temperature" TEXT,
    "Temperature likeness" TEXT,
    "Capital" TEXT,
    "Port" TEXT,
    "Citadel" TEXT,
    "Walls" TEXT,
    "Plaza" TEXT,
    "Temple" TEXT,
    "Shanty Town" TEXT,
    "Emblem" TEXT,
    "City Generator Link" TEXT
);

\copy burgsattr_staging FROM '/srv/data-loader/data/burgs.csv' DELIMITER ',' CSV HEADER;

INSERT INTO
    regular."BurgsAttr" (
        id,
        burg,
        province,
        state,
        culture,
        religion,
        population,
        capital,
        port,
        citadel,
        walls,
        plaza,
        temple,
        "shantyTown",
        emblem
    )
SELECT
    "Id",
    "Burg",
    "Province",
    "State",
    "Culture",
    "Religion",
    "Population",
    "Capital",
    "Port",
    "Citadel",
    "Walls",
    "Plaza",
    "Temple",
    "Shanty Town",
    "Emblem"
FROM
    burgsattr_staging
ON CONFLICT (id) DO UPDATE
SET
    burg = EXCLUDED.burg,
    province = EXCLUDED.province,
    state = EXCLUDED.state,
    culture = EXCLUDED.culture,
    religion = EXCLUDED.religion,
    population = EXCLUDED.population,
    capital = EXCLUDED.capital,
    port = EXCLUDED.port,
    citadel = EXCLUDED.citadel,
    walls = EXCLUDED.walls,
    plaza = EXCLUDED.plaza,
    temple = EXCLUDED.temple,
    "shantyTown" = EXCLUDED."shantyTown",
    emblem = EXCLUDED.emblem;

INSERT INTO
    spatial.burgs_geom (id, geom)
SELECT
    "Id",
    ST_SetSRID (ST_MakePoint ("Longitude", "Latitude"), 4326)
FROM
    burgsattr_staging
WHERE
    "Longitude" IS NOT NULL
    AND "Latitude" IS NOT NULL ON CONFLICT (id) DO
UPDATE
SET
    geom = EXCLUDED.geom;

INSERT INTO spatial.burgs_pixel_geom (id, geom)
SELECT
    "Id",
    ST_SetSRID(ST_MakePoint("X", 2000 - "Y"), 0)  -- flip Y
FROM
    burgsattr_staging
WHERE
    "X" IS NOT NULL
    AND "Y" IS NOT NULL
ON CONFLICT (id) DO UPDATE
SET
    geom = EXCLUDED.geom;


----------------------------------------------------------------
-- Repeat for each attribute table:
-- Culture
-- 1. Create a staging table matching your CSV header
DROP TABLE IF EXISTS culture_staging;

CREATE TEMP TABLE culture_staging (
    "Id" INTEGER,
    "Name" TEXT,
    "Color" TEXT,
    "Cells" INTEGER,
    "Expansionism" DOUBLE PRECISION,
    "Type" TEXT,
    "Area km2" DOUBLE PRECISION,
    "Population" INTEGER,
    "Namesbase" TEXT,
    "Emblems Shape" TEXT,
    "Origins" TEXT
);

-- 2. Bulk load the CSV
\copy culture_staging FROM '/srv/data-loader/data/cultures.csv' DELIMITER ',' CSV HEADER;

-- 3. Upsert from staging into your full Culture table, mapping columns
INSERT INTO
    regular."Culture" (
        id,
        name,
        color,
        cells,
        expansionism,
        type,
        area_km2,
        population,
        namesbase,
        emblems_shape,
        origins,
        description,
        "primaryAttribute",
        "secondaryAttribute",
        "tertiaryAttribute",
        "primarySkill",
        "secondarySkill",
        "tertiarySkill"
    )
SELECT
    "Id",
    "Name",
    "Color",
    "Cells",
    "Expansionism",
    "Type",
    "Area km2",
    "Population",
    "Namesbase",
    NULLIF("Emblems Shape", 'undefined'),
    "Origins",
    '', -- or just '' if not in your staging table
    'None',
    'None',
    'None',
    'None',
    'None',
    'None'
FROM
    culture_staging ON CONFLICT (id) DO
UPDATE
SET
    name = EXCLUDED.name,
    color = EXCLUDED.color,
    cells = EXCLUDED.cells,
    expansionism = EXCLUDED.expansionism,
    type = EXCLUDED.type,
    area_km2 = EXCLUDED.area_km2,
    population = EXCLUDED.population,
    namesbase = EXCLUDED.namesbase,
    emblems_shape = EXCLUDED.emblems_shape,
    origins = EXCLUDED.origins,
    description = EXCLUDED.description,
    "primaryAttribute" = EXCLUDED."primaryAttribute",
    "secondaryAttribute" = EXCLUDED."secondaryAttribute",
    "tertiaryAttribute" = EXCLUDED."tertiaryAttribute",
    "primarySkill" = EXCLUDED."primarySkill",
    "secondarySkill" = EXCLUDED."secondarySkill",
    "tertiarySkill" = EXCLUDED."tertiarySkill";

    INSERT INTO spatial.cultures_geom (id, geom)
    SELECT id, NULL::geometry(MultiPolygon, 4326)
    FROM regular."Culture"
    ON CONFLICT (id) DO UPDATE
    SET geom = EXCLUDED.geom;

-- MarkersAttr
-- 1. Drop staging table if exists
DROP TABLE IF EXISTS markersattr_staging;

CREATE TEMP TABLE markersattr_staging (
    id INTEGER,
    type TEXT,
    icon TEXT,
    name TEXT,
    note TEXT,
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

-- 3. Import data from CSV into staging table
\copy markersattr_staging FROM '/srv/data-loader/data/markers_cleaned.csv' DELIMITER ',' CSV HEADER;

-- 4. Upsert from staging into the target table
INSERT INTO
    regular."MarkersAttr" (
        id,
        type,
        icon,
        name,
        note,
        x,
        y,
        latitude,
        longitude
    )
SELECT
    id,
    type,
    icon,
    name,
    note,
    x,
    y,
    latitude,
    longitude
FROM
    markersattr_staging ON CONFLICT (id) DO
UPDATE
SET
    type = EXCLUDED.type,
    icon = EXCLUDED.icon,
    name = EXCLUDED.name,
    note = EXCLUDED.note,
    x = EXCLUDED.x,
    y = EXCLUDED.y,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;

INSERT INTO
    spatial.markers_geom (id, geom)
SELECT
    id,
    ST_SetSRID (ST_MakePoint (longitude, latitude), 4326)
FROM
    regular."MarkersAttr"
WHERE
    longitude IS NOT NULL
    AND latitude IS NOT NULL ON CONFLICT (id) DO
UPDATE
SET
    geom = EXCLUDED.geom;

-- ProvincesAttr
DROP TABLE IF EXISTS provincesattr_staging;

-- Create temp staging table with correct types and columns
CREATE TEMP TABLE provincesattr_staging (
    id INTEGER,
    province TEXT,
    full_name TEXT,
    form TEXT,
    state TEXT,
    color TEXT,
    capital TEXT,
    area_km2 DOUBLE PRECISION,
    total_population INTEGER,
    rural_population INTEGER,
    urban_population INTEGER,
    burgs INTEGER
);

-- \copy CSV data into the temp table
\copy provincesattr_staging FROM '/srv/data-loader/data/provinces.csv' DELIMITER ',' CSV HEADER;

-- Upsert from staging to target table
INSERT INTO
    regular."ProvincesAttr" (
        id,
        province,
        full_name,
        form,
        state,
        color,
        capital,
        area_km2,
        total_population,
        rural_population,
        urban_population,
        burgs
    )
SELECT
    id,
    province,
    full_name,
    form,
    state,
    color,
    capital,
    area_km2,
    total_population,
    rural_population,
    urban_population,
    burgs
FROM
    provincesattr_staging ON CONFLICT (id) DO
UPDATE
SET
    province = EXCLUDED.province,
    full_name = EXCLUDED.full_name,
    form = EXCLUDED.form,
    state = EXCLUDED.state,
    color = EXCLUDED.color,
    capital = EXCLUDED.capital,
    area_km2 = EXCLUDED.area_km2,
    total_population = EXCLUDED.total_population,
    rural_population = EXCLUDED.rural_population,
    urban_population = EXCLUDED.urban_population,
    burgs = EXCLUDED.burgs;

INSERT INTO spatial.provinces_geom (id, geom)
SELECT
    id,
    NULL::geometry(MultiPolygon, 4326)
FROM
    regular."ProvincesAttr"
ON CONFLICT (id) DO UPDATE
SET geom = EXCLUDED.geom;
-- Religion
DROP TABLE IF EXISTS religionsattr_staging;

CREATE TEMP TABLE religionsattr_staging (
    id INTEGER,
    name TEXT,
    color TEXT,
    type TEXT,
    form TEXT,
    supreme_deity TEXT,
    area_km2 DOUBLE PRECISION,
    believers INTEGER,
    origins TEXT,
    potential TEXT,
    expansionism DOUBLE PRECISION
);

\copy religionsattr_staging ( id, name, color, type, form, supreme_deity, area_km2, believers, origins, potential, expansionism) FROM '/srv/data-loader/data/religions.csv' DELIMITER ',' CSV HEADER;

INSERT INTO
    regular."Religion" (
        id,
        name,
        color,
        type,
        form,
        supreme_deity,
        area_km2,
        believers,
        origins,
        potential,
        expansionism,
        description,
        "primaryAttribute",
        "secondaryAttribute",
        "tertiaryAttribute",
        "primarySkill",
        "secondarySkill",
        "tertiarySkill"
    )
SELECT
    id,
    name,
    color,
    type,
    form,
    supreme_deity,
    area_km2,
    believers,
    origins,
    potential,
    expansionism,
    'None' AS description,
    'None' AS "primaryAttribute",
    'None' AS "secondaryAttribute",
    'None' AS "tertiaryAttribute",
    'None' AS "primarySkill",
    'None' AS "secondarySkill",
    'None' AS "tertiarySkill"
FROM
    religionsattr_staging ON CONFLICT (id) DO
UPDATE
SET
    name = EXCLUDED.name,
    color = EXCLUDED.color,
    type = EXCLUDED.type,
    form = EXCLUDED.form,
    supreme_deity = EXCLUDED.supreme_deity,
    area_km2 = EXCLUDED.area_km2,
    believers = EXCLUDED.believers,
    origins = EXCLUDED.origins,
    potential = EXCLUDED.potential,
    expansionism = EXCLUDED.expansionism,
    description = EXCLUDED.description,
    "primaryAttribute" = EXCLUDED."primaryAttribute",
    "secondaryAttribute" = EXCLUDED."secondaryAttribute",
    "tertiaryAttribute" = EXCLUDED."tertiaryAttribute",
    "primarySkill" = EXCLUDED."primarySkill",
    "secondarySkill" = EXCLUDED."secondarySkill",
    "tertiarySkill" = EXCLUDED."tertiarySkill";

INSERT INTO spatial.religions_geom (id, geom)
SELECT
    id,
    NULL::geometry(MultiPolygon, 4326)
FROM
    regular."Religion"
ON CONFLICT (id) DO UPDATE
SET geom = EXCLUDED.geom;

-- RiversAttr
-- Drop temp staging table if it exists
DROP TABLE IF EXISTS rivers_staging;
CREATE TEMP TABLE rivers_staging (
    id INTEGER,
    river TEXT,
    type TEXT, -- Assuming you have a 'type' column in rivers.csv
    length DOUBLE PRECISION,
    width DOUBLE PRECISION,
    discharge DOUBLE PRECISION,
    basin TEXT
    -- add or adjust columns as needed to match your rivers.csv
);

-- Load data from CSV (ensure columns/order match!)
\copy rivers_staging (id, river, type, length, width, discharge, basin) FROM '/srv/data-loader/data/rivers_cleaned.csv' DELIMITER ',' CSV HEADER;

-- Upsert into target table, setting extra fields to 'None' if needed
INSERT INTO regular."RiversAttr" (id, name)
SELECT
    id,
    river
FROM rivers_staging
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name;

-- RoutesAttr
DROP TABLE IF EXISTS routesattr_staging;

CREATE TEMP TABLE routesattr_staging (id INT, route TEXT, group_col TEXT, length TEXT);

\copy routesattr_staging (id, route, group_col, length) FROM '/srv/data-loader/data/routes.csv' DELIMITER ',' CSV HEADER;

INSERT INTO
    regular."RoutesAttr" (id, name, group_name)
SELECT
    id,
    route,
    group_col
FROM
    routesattr_staging ON CONFLICT (id) DO
UPDATE
SET
    name = EXCLUDED.name,
    group_name = EXCLUDED.group_name;

INSERT INTO spatial.routes_geom (id, geom)
SELECT
    id,
    NULL::geometry(LineString, 4326)
FROM
    regular."RoutesAttr"
ON CONFLICT (id) DO UPDATE
SET geom = EXCLUDED.geom;

-- Landmass upsert from staging
INSERT INTO spatial.landmass (id, geom, type)
SELECT
  id,
  ST_SetSRID(geom, 0),
  type
FROM spatial.landmass_staging
ON CONFLICT (id) DO UPDATE
SET
  geom = ST_SetSRID(EXCLUDED.geom, 0),
  type = EXCLUDED.type;

-- Cells upsert from staging
INSERT INTO spatial.cells_geom (id, geom, geojsondata)
SELECT
    id,
    geom,
    NULL
FROM
    spatial.cells_geom_staging
ON CONFLICT (id) DO UPDATE
SET
    geom = EXCLUDED.geom,
    geojsondata = EXCLUDED.geojsondata;

-- Cells attribute upsert
DROP TABLE IF EXISTS cellsattr_staging;

CREATE TEMP TABLE cellsattr_staging AS
SELECT
    *
FROM
    regular."CellsAttr"
WITH
    NO DATA;

INSERT INTO regular."CellsAttr" (
    id,
    height,
    biome,
    type,
    population,
    state,
    province,
    culture,
    religion,
    neighbors,
    geojsondata
)
SELECT
    (geojsondata->>'id')::INTEGER,
    (geojsondata->>'height')::INTEGER,
    (geojsondata->>'biome')::INTEGER,
    geojsondata->>'type',
    (geojsondata->>'population')::INTEGER,
    (geojsondata->>'state')::INTEGER,
    (geojsondata->>'province')::INTEGER,
    (geojsondata->>'culture')::INTEGER,
    (geojsondata->>'religion')::INTEGER,
    ARRAY(
        SELECT CAST(unnest(string_to_array(
            regexp_replace(geojsondata->>'neighbors', '[\[\] ]', '', 'g'),
            ','
        )) AS INTEGER)
    ),
    geojsondata
FROM spatial.cells_geom
WHERE geojsondata->>'id' IS NOT NULL
ON CONFLICT (id) DO UPDATE
SET
    height = EXCLUDED.height,
    biome = EXCLUDED.biome,
    type = EXCLUDED.type,
    population = EXCLUDED.population,
    state = EXCLUDED.state,
    province = EXCLUDED.province,
    culture = EXCLUDED.culture,
    religion = EXCLUDED.religion,
    neighbors = EXCLUDED.neighbors,
    geojsondata = EXCLUDED.geojsondata;