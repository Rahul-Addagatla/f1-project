# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming circuits data

# COMMAND ----------

circuits_df = spark.read.parquet(f"{bronze_folder_path}/circuits")

# COMMAND ----------

# Import necessary functions for selecting and modifying DataFrame columns
from pyspark.sql.functions import col, lit, to_timestamp, concat

# COMMAND ----------

# Add a new column called 'ingestion_date' that records the current timestamp
circuits_metadata = add_ingestion_date(circuits_df)

# COMMAND ----------

# Select only the required columns from the raw circuits DataFrame
circuits_selected_df = circuits_metadata.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"), col("Ingestion_date"))

# COMMAND ----------

# Rename columns to follow snake_case naming conventions for consistency
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("location", "circuit_location") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# Write the final DataFrame to the Delta table in the silver layer
circuits_renamed_df.write.mode("overwrite").format("delta").saveAsTable("silver.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming races data

# COMMAND ----------

races_df = spark.read.parquet(f"{bronze_folder_path}/races")

# COMMAND ----------
races_metadata_df = races_df.withColumn("race_date", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                  .add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

races_selected_df = races_metadata_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("race_date"), col("ingestion_date"))

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("name", "race_name") 

# COMMAND ----------

races_renamed_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("silver.races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming constructors data

# COMMAND ----------

constructor_df = spark.read.parquet(f"{bronze_folder_path}/constructor")

# COMMAND ----------

constructor_metadata_df = add_ingestion_date(constructor_df)

# COMMAND ----------

constructor_renamed_df = constructor_metadata_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumnRenamed("name", "team") \
                                             .drop(col('url'))

# COMMAND ----------

constructor_renamed_df.write.mode("overwrite").format("delta").saveAsTable("silver.constructor")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming drivers data

# COMMAND ----------

drivers_df = spark.read.parquet(f"{bronze_folder_path}/drivers")

# COMMAND ----------

drivers_metadata_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_renamed_df = drivers_metadata_df.withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("driverRef", "driver_ref") \
                                            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                            .withColumnRenamed("number", "driver_number") \
                                            .withColumnRenamed("name", "driver_name") \
                                            .withColumnRenamed("nationality", "driver_nationality") \
                                            .drop(col("url")

# COMMAND ----------

drivers_renamed_df.write.mode("overwrite").format("delta").saveAsTable("silver.drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming results data

# COMMAND ----------

results_df = spark.read.parquet(f"{bronze_folder_path}/results")

# COMMAND ----------

results_metadata_df = add_ingestion_date(results_df)

# COMMAND ----------

results_renamed_df = results_metadata_df.withColumnRenamed("resultId", "result_id") \
                                            .withColumnRenamed("raceId", "result_race_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("positionText", "position_text") \
                                            .withColumnRenamed("positionOrder", "position_order") \
                                            .withColumnRenamed("time", "race_time") \
                                            .withColumnRenamed("fastestLap", "fastest_lap") \
                                            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                            .drop(col("statusId")

# COMMAND ----------

results_deduped_df = results_renamed_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

results_deduped_df.write.mode("overwrite").format("delta").saveAsTable("silver.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming pit stops data

# COMMAND ----------

pit_stops_df = spark.read.parquet(f"{bronze_folder_path}/pit_stops")

# COMMAND ----------

pit_stops_metadata_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

pit_stops_renamed_df = pit_stops_metadata_df.withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

pit_stops_renamed_df.write.mode("overwrite").format("delta").saveAsTable("silver.pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming lap times data

# COMMAND ----------

lap_times_df = spark.read.parquet(f"{bronze_folder_path}/lap_times")

# COMMAND ----------

lap_times_metadata_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_renamed_df = lap_times_metadata_df.withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

lap_times_renamed_df.write.mode("overwrite").format("delta").saveAsTable("silver.lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming qualifying data

# COMMAND ----------

qualifying_df = spark.read.parquet(f"{bronze_folder_path}/qualifying")

# COMMAND ----------

qualifying_metadata_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

qualifying_renamed_df = qualifying_metadata_df.withColumnRenamed("qualifyId", "qualify_id") \
                                              .withColumnRenamed("driverId", "driver_id") \
                                              .withColumnRenamed("raceId", "race_id") \
                                              .withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

qualifying_renamed_df.write.mode("overwrite").format("delta").saveAsTable("silver.qualifying")
