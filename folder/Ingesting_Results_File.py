# Databricks notebook source
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, DecimalType, StructType, StructField

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DoubleType, DecimalType

results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", TimestampType(), True),
    StructField("milliseconds", DoubleType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", TimestampType(), True),
    StructField("fastestLapSpeed", DecimalType(), True),
    StructField("statusId", IntegerType(), True)
])


# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/functions"

# COMMAND ----------

results_df = spark.read.\
    schema(results_schema)\
        .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

results_with_columns = results_df.withColumnRenamed("resultId", "Result_ID") \
            .withColumnRenamed("raceId", "Race_ID") \
            .withColumnRenamed("constructorId", "Constructor_ID") \
            .withColumnRenamed("driverId", "Driver_ID") \
            .withColumnRenamed("positionText", "Position_text") \
            .withColumnRenamed("positionOrder", "Position_order") \
            .withColumnRenamed("fastestLap", "fastest_lap") \
            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

results_with_columns = add_ingestion_Date(results_with_columns)
            

# COMMAND ----------

results_final = results_with_columns.drop(col('statusId'))

# COMMAND ----------

results_final.write.mode('overwrite').partitionBy('Race_ID').parquet(f'{processed_folder_path}/results1')