# Databricks notebook source
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType, DecimalType, TimestampType

# COMMAND ----------

pitstops_schema = StructType([StructField("raceId", IntegerType(), False),
                              StructField("driverId", IntegerType(), True),
                              StructField("stop", IntegerType(), True),
                              StructField("lap", IntegerType(), True),
                              StructField("time", TimestampType(), True),
                            StructField("duration", DecimalType(), True),
                              StructField("milliseconds", DoubleType(), True)
                              ])

# COMMAND ----------

pitstops_df = spark.read.schema(pitstops_schema).option("multiLine", True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/functions"

# COMMAND ----------

pitstops_final = pitstops_df.withColumnRenamed("raceId","Race_ID") \
                            .withColumnRenamed("driverId","Driver_ID") \

pitstops_final = add_ingestion_Date(pitstops_final)

# COMMAND ----------

pitstops_final.write.mode("overwrite").parquet(f"{processed_folder_path}/pitstops")