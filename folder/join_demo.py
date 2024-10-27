# Databricks notebook source
# MAGIC %run "./includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "Circuit_Name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("Year=2019") \
    .withColumnRenamed("name","Races_Name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df["circuit_Id"] == races_df["circuitId"], "inner") \
    .select(
        col("Circuit_Name"), \
        col("location"), \
        col("race_country") \
        ,col("Races_Name") \
        ,col("round")
            )

# COMMAND ----------

race_circuits_df1 = circuits_df.join(races_df, circuits_df["circuit_Id"] == races_df["circuitId"]) \
    .select(
            circuits_df["Circuit_Name"],
            circuits_df["location"],
            circuits_df["race_country"],
            races_df["Races_Name"],
            races_df["round"]


    )

# COMMAND ----------

display(race_circuits_df1)

# COMMAND ----------

