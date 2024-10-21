# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
        .withColumnRenamed("Race_TimeStamp", "race_date") \
            .withColumnRenamed("Year", "race_year") \
                .withColumnRenamed("circuitId", "circuit_Id")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "drivers_name") \
        .withColumnRenamed("nationality", "drivers_nationality") \
            .withColumnRenamed("number", "drivers_number")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team") \
        .withColumnRenamed("nationality", "constructors_nationality")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "races_time")

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df,races_df["circuit_Id"]==circuits_df["circuit_Id"],"inner") \
    .select(
            races_df["race_year"],
            races_df["race_name"],
            races_df["race_date"],
            circuits_df["circuit_location"],
            races_df["raceId"].alias("Race_Id")
    )

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df,(results_df["Race_Id"]==races_circuits_df["Race_Id"]),"inner") \
                            .join(drivers_df,(results_df["Driver_ID"]==drivers_df["Driver_Id"]),"inner") \
                            .join(constructors_df,(results_df["Constructor_ID"]==constructors_df["constructor_ID"]),"inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year","race_name","race_date","circuit_location","drivers_name","drivers_number","drivers_nationality","team","grid","fastest_lap","races_time","points") \
    .withColumn("Created_Date",current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import desc, asc

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(desc("points")))

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{presentation_folder_path}/race_results")