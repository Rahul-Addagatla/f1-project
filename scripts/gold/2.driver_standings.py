# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# Load the DataFrame first
race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# Extract the list of race years
race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, count, desc, rank, asc
# Apply the filter
race_results_df = race_results_df.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality") \
                                     .agg(sum("points").alias("total_points"),
                                     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", dense_rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").partitionBy("race_year").saveAsTable("gold.driver_standings")
