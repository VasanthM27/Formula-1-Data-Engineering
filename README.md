# Formula 1 Data Engineering Project

## Project Overview
This project showcases a complete end-to-end data engineering pipeline based on Formula 1 race data. The project leverages various Azure services, including **Azure Data Lake Gen 2**, **Azure Data Factory**, **Databricks** for **PySpark** and **SparkSQL** processing, and **Delta Lake** for storage layers. The pipeline handles incremental and full loads, processes multi-line JSON datasets, and models data from raw to presentation layers.

## Tools and Technologies
- **Cloud Platform**: Azure
- **Storage**: Azure Data Lake Gen 2, Delta Lake
- **Data Processing**: Azure Databricks (PySpark, SparkSQL, Databricks Clusters & Workflows)
- **Orchestration**: Azure Data Factory
- **Data Governance**: Unity Catalog
- **Languages**: Python, SQL
- **Data Modeling**: Raw, Processed, Presentation layers

## Key Concepts
1. **Data Ingestion**: Ingest raw race data from multiple sources (multi-line JSON, CSV) into Azure Data Lake.
   - Scripts: `Ingesting_constructors_file.py`, `ingest.circuits.csv.py`, `ingest_lap_times_file.py`, `ingest drivers_file_json_nested.py`
   
2. **Data Processing with PySpark and SparkSQL**: Perform transformations on the ingested data.
   - Handle nested JSON, flatten, and clean the data.
   - Perform joins and aggregations across race results, drivers, constructors, and lap times.
   - Scripts: `join_demo.py`, `demo_join_result_all_datatables`

3. **Data Orchestration with Azure Data Factory**: Orchestrate the data pipeline to handle full and incremental loads.
   - Configure linked services, datasets, and pipelines for data processing.

4. **Delta Lake Storage**: Use Delta Lake for the processed data, supporting version control and incremental updates.
   - Full load vs. incremental load strategy for new race data.

5. **Data Governance**: Utilize Unity Catalog for data governance and security across different layers of data (Raw, Processed, Presentation).

## Data Layers
- **Raw Layer**: Stores the unprocessed race data from various sources.
- **Processed Layer**: Contains the cleaned, deduplicated, and transformed data.
- **Presentation Layer**: Optimized for analytics, including aggregated metrics like lap times, race results, and driver standings.

## Pipeline Structure
1. **Mounting Azure Data Lake Gen 2**: 
   - Script: `mount adls containers for project.py`
   - Mount ADLS Gen 2 containers for storing raw and processed data.

2. **Ingesting Raw Data**:
   - Multiple Python scripts for each dataset ingestion, such as race results, drivers,pit stops, constructors, qualifying and lap times etc

3. **Data Transformation**:
   - **PySpark**: Use SparkSQL and PySpark to clean and transform the data, handling nested structures and complex joins.

4. **Incremental Load Processing**:
   - Implement a mechanism to detect new data and process it incrementally, ensuring that only new race data is processed without affecting existing data.

5. **Final Data Modeling**:
   - Create a data model using Delta Lake tables to organize the data into meaningful entities like `races`, `drivers`, `lap_times`, and `results`.

## Project Benefits
- **Scalable Data Pipeline**: Designed to handle large volumes of data efficiently.
- **Incremental and Full Load Support**: Ensures data consistency and up-to-date processing.
- **Modular Design**: Each component of the pipeline (ingestion, transformation, presentation) is modular and reusable.
- **Advanced Analytics**: Data is modeled for easy integration with BI tools, supporting real-time analytics and reporting.

## Sample PySpark Code
```python
# Sample PySpark Code for Processing Data
df_races = spark.read.json('/mnt/raw/races.json')
df_drivers = spark.read.csv('/mnt/raw/drivers.csv')

# Perform Join
df_result = df_races.join(df_drivers, df_races.driverId == df_drivers.driverId)

# Write to Delta Lake
df_result.write.format("delta").mode("overwrite").save('/mnt/processed/results/')
