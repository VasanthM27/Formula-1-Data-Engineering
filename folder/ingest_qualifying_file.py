# Databricks notebook source
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType, DecimalType, TimestampType

# COMMAND ----------

qualifying_schema = StructType([StructField("raceId", IntegerType(), False),
                              StructField("driverId", IntegerType(), True),
                              StructField("constructorId", IntegerType(), True),
                              StructField("number", IntegerType(), True),
                              StructField("position", IntegerType(), True),
                              StructField("q1", StringType(), True),
                              StructField("q2", StringType(), True),
                              StructField("q3", StringType(), True),
                              ])

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/functions"

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

qualifying_final = qualifying_df.withColumnRenamed("raceId","Race_ID") \
                            .withColumnRenamed("driverId","Driver_ID") \
                            .withColumnRenamed("constructorId","Constructor_ID")

qualifying_final = add_ingestion_Date(qualifying_final)

# COMMAND ----------

qualifying_final.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")