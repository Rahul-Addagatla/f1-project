-- COMMAND ----------

DROP VIEW IF EXISTS gold.circuits;
CREATE VIEW IF NOT EXISTS gold.circuits AS 
SELECT
  circuit_id,
  circuit_Ref,
  name,
  circuit_location,
  country,
  latitude,
  longitude,
  altitude,
  Ingestion_date
FROM silver.circuits;

-- COMMAND ----------

DROP VIEW IF EXISTS gold.races;
CREATE VIEW IF NOT EXISTS gold.races AS 
SELECT
  race_id,
  race_year,
  round,
  circuit_id,
  race_name,
  race_date,
  ingestion_date
FROM silver.races;

-- COMMAND ----------

DROP VIEW IF EXISTS gold.constructor;
CREATE VIEW IF NOT EXISTS gold.constructor AS 
SELECT
  constructor_id,
  constructor_ref,
  team,
  nationality
FROM silver.constructor;

-- COMMAND ----------

DROP VIEW IF EXISTS gold.drivers;
CREATE VIEW IF NOT EXISTS gold.drivers AS 
SELECT
  driver_id,
  driver_ref,
  name,
  driver_number,
  driver_name,
  driver_nationality 
FROM silver.drivers;

-- COMMAND ----------

DROP VIEW IF EXISTS gold.results;
CREATE VIEW IF NOT EXISTS gold.results AS 
SELECT
  result_id,
  result_race_id,
  driver_id,
  constructor_id,
  position_text,
  position_order,
  race_time,
  fastest_lap,
  fastest_lap_time,
  fastest_lap_speed 
FROM silver.results;

-- COMMAND ----------

DROP VIEW IF EXISTS gold.pit_stops;
CREATE VIEW IF NOT EXISTS gold.pit_stops AS 
SELECT
  driver_id,
  race_id 
FROM silver.pit_stops;

-- COMMAND ----------

DROP VIEW IF EXISTS gold.lap_times;
CREATE VIEW IF NOT EXISTS gold.lap_times AS 
SELECT
  driver_id,
  race_id
FROM silver.lap_times;

-- COMMAND ----------

DROP VIEW IF EXISTS gold.qualifying;
CREATE VIEW IF NOT EXISTS gold.qualifying AS 
SELECT
  qualify_id,
  driver_id,
  race_id,
  constructor_id 
FROM silver.qualifying;
