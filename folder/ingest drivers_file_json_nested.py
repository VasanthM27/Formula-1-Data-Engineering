# Databricks notebook source
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType(), True),
                          StructField("surname", StringType(),True)
                        ])

# COMMAND ----------

drivers_schema = StructType([StructField("driverId", IntegerType(), False),
                             StructField("driverRef", StringType(), True),
                             StructField("number", IntegerType(), True),
                             StructField("code", StringType(), True),
                             StructField("name", name_schema, True),
                             StructField("dob", DateType(), True),
                             StructField("nationality", StringType(), True),
                             StructField("url", StringType(), True),
                             ])

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/functions"

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col

# COMMAND ----------

drivers_df_updated = drivers_df.withColumnRenamed("driverId","Driver_Id") \
                                .withColumnRenamed("driverRef", "Driver_Ref") \
                                .withColumn("name", concat(col("name.forename"),lit(" "),col("name.surname")))

drivers_df_updated = add_ingestion_Date(drivers_df_updated)


# COMMAND ----------

drivers_df_final = drivers_df_updated.drop(drivers_df_updated["url"])

# COMMAND ----------

drivers_df_final.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

