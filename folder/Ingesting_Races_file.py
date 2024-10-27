# Databricks notebook source
# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/functions"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, DoubleType, StructField, DateType, TimestampType

# COMMAND ----------

races_schema = StructType([StructField("raceId", IntegerType(), False),
                       StructField("Year", IntegerType(), True),
                       StructField("round", IntegerType(), True),
                       StructField("circuitId", IntegerType(), True),
                       StructField("name", StringType(), True),
                       StructField("date", DateType(), True),
                       StructField("time", StringType(), True)
                       ])


# COMMAND ----------

races_df = spark.read.option("header", True)\
.schema(races_schema)\
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

selected_columns_df = races_df.select(col("raceId").alias("Race_Id"),col("year").alias("Year"),col("round"),col("circuitId").alias("Circuit_Id"),col("name"),col("date"),col("time"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

races_with_timestamps = races_df.withColumn("Ingestion_Date", current_timestamp())\
        .withColumn("Race_TimeStamp", to_timestamp(concat(col("date"),lit(" "),col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_with_timestamps.write.mode('overwrite').parquet(f"{processed_folder_path}/races")