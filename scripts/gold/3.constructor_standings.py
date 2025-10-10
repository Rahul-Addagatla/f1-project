# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank, asc

constructor_standing_df = race_results_df.groupBy("race_year", "team") \
                                          .agg(sum("points").alias("total_points"),
                                           count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standing_df.withColumn("rank", dense_rank().over(constructor_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").partitionBy("race_year").saveAsTable("gold.constructor_standing")
