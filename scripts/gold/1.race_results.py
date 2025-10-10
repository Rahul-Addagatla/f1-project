# Databricks notebook source
# Load drivers data from the Silver layer
drivers_df = spark.read.format("delta").load(f"{silver_folder_path}/drivers")

# COMMAND ----------

# Load constructors data from the Silver layer
constructor_df = spark.read.format("delta").load(f"{silver_folder_path}/constructor")

# COMMAND ----------

# Load circuits data from the Silver layer
circuits_df = spark.read.format("delta").load(f"{silver_folder_path}/circuits")

# COMMAND ----------

# Load races data from the Silver layer
races_df = spark.read.format("delta").load(f"{silver_folder_path}/races")

# COMMAND ----------

# Load results data from the Silver layer
results_df = spark.read.format("delta").load(f"{silver_folder_path}/results")

# COMMAND ----------

# Join races and circuits to get race-circuit combination along with selected columns
race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                            .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# Join results with race-circuits, drivers, and constructors to get enriched race results data
race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id, "inner") \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                            .join(constructor_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

# Import function to add current timestamp
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Select final required columns and add a created timestamp
final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position") \
                          .withColumn("created_date", current_timestamp()) 

# COMMAND ----------

# Write the final DataFrame to the Gold layer as a Delta table partitioned by race_year
final_df.write.mode("overwrite").format("delta").partitionBy("race_year").saveAsTable("gold.race_results")
