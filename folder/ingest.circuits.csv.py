# Databricks notebook source
# MAGIC %md
# MAGIC Ingesting circuits.csv file

# COMMAND ----------

# MAGIC %md Read the CSV file using the spark dataframe reader

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/containerformula1/raw

# COMMAND ----------

## circuits_df = spark.read.option("header",True)\
## .option("inferSchema",True) \
## .csv('dbfs:/mnt/containerformula1/raw/circuits.csv')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DoubleType, StringType

# COMMAND ----------

circuits_schema = StructType([StructField("circuitId", IntegerType(), False),
                              StructField("circuitRef", StringType(), True),
                              StructField("name", StringType(), True),
                              StructField("location", StringType(), True),
                              StructField("country", StringType(), True),
                              StructField("lat", DoubleType(), True),
                              StructField("lng", DoubleType(), True),
                              StructField("alt", IntegerType(), True),
                              StructField("url", StringType(), True)
                            ])

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/functions"

# COMMAND ----------

circuits_df = spark.read.option("header",True)\
.schema(circuits_schema) \
.csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

# display(circuits_df)

# COMMAND ----------

# circuits_df.show()

# COMMAND ----------

# circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md selecting only the reqd columns in the dataframe
# MAGIC

# COMMAND ----------

# reqd_columns = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

# display(reqd_columns)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

reqd_columns_colu = circuits_df.select(col("circuitId").alias("circuit_Id"),col("circuitRef").alias("circuit_Ref"),col("name"),col("location"),col("country").alias("race_country"),col("lat").alias("latitude"),col("lng").alias("longitude"),col("alt").alias("altitude"))

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

## circuits_final_df = reqd_columns_colu.withColumn("ingestion_date", current_timestamp())\
   ## .withColumn("env",lit("production"))

# COMMAND ----------

circuits_final_df = add_ingestion_Date(reqd_columns_colu)

# COMMAND ----------

circuits_last = circuits_final_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC **write dataframe to parquet**

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

circuits_last.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

display(circuits_last)

# COMMAND ----------

