# Databricks notebook source
# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/containerformula1/raw

# COMMAND ----------

# MAGIC %md
# MAGIC using DDL method for schema defining
# MAGIC

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/functions"

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f'{raw_folder_path}/constructors.json')

# COMMAND ----------

constructors_drop_url = constructors_df.drop(constructors_df["url"])

# COMMAND ----------

# from pyspark.sql.functions import col

# COMMAND ----------

# constructors_dropped_df = constructors_df.drop(col("url"))

# COMMAND ----------

# display(constructors_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_drop_url.withColumnRenamed("constructorId", "constructor_ID") \
                                            .withColumnRenamed("constructorRef", "constructor_Ref")

constructors_final_df = add_ingestion_Date(constructors_final_df)

# COMMAND ----------

# MAGIC %md write to parquet file

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/constructors")