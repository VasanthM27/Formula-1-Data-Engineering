# Databricks notebook source
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType, DecimalType, TimestampType

# COMMAND ----------

lap_times_schema = StructType([StructField("raceId", IntegerType(), False),
                              StructField("driverId", IntegerType(), True),
                              StructField("lap", IntegerType(), True),
                              StructField("postion", IntegerType(), True),
                              StructField("time", StringType(), True),
                              StructField("milliseconds", IntegerType(), True)
                              ])

# COMMAND ----------

# MAGIC %run  "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/functions"

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

lap_times_final = lap_times_df.withColumnRenamed("raceId","Race_ID") \
                            .withColumnRenamed("driverId","Driver_ID")

lap_times_final = add_ingestion_Date(lap_times_final)

# COMMAND ----------

lap_times_final.write.mode("overwrite").parquet(f"{processed_folder_path}/laptimes")