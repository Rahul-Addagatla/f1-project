# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

constructor_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                    StructField("constructorRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

constructor_df.write.mode("overwrite").parquet(f"{bronze_folder_path}/constructor")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

drivers_df.write.mode("overwrite").parquet(f"{bronze_folder_path}/drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

pit_stops_df.write.mode("overwrite").parquet(f"{bronze_folder_path}/pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/results.json")

# COMMAND ----------

results_df.write.mode("overwrite").parquet(f"{bronze_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest qualifying folder

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

qualifying_df.write.mode("overwrite").parquet(f"{bronze_folder_path}/qualifying")
