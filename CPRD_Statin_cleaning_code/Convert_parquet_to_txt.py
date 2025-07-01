# Databricks notebook source
from pyspark.sql import SparkSession
import shutil
import os

# COMMAND ----------



# Initialize Spark session
spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()

# Load the Parquet file
df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet/')

# Specify a temporary directory for output
temp_dir = "/mnt/data/cprd_statin_data/temp"
final_output = "/mnt/data/cprd_statin_data/temp/patient_statin.csv"

# Save as a single partition CSV file in the temporary directory
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

# Move and rename the file
for filename in os.listdir(temp_dir):
    if filename.startswith("part-") and filename.endswith(".csv"):
        shutil.move(os.path.join(temp_dir, filename), final_output)
        break

# Clean up temporary directory
shutil.rmtree(temp_dir)

# Stop the Spark session
spark.stop()


# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()

# Load the Delta table
df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet/')

# Specify a temporary directory for output
temp_dir = "/mnt/data/cprd_statin_data/temp"
final_output = "/mnt/data/cprd_statin_data/temp/patient_statin.csv"

# Save as a single partition CSV file in the temporary directory
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

# Use dbutils to move and rename the file
files = dbutils.fs.ls(temp_dir)
for file in files:
    if file.name.startswith("part-") and file.name.endswith(".csv"):
        dbutils.fs.mv(file.path, final_output)
        break

# Clean up temporary directory
dbutils.fs.rm(temp_dir, recurse=True)

# Stop the Spark session
spark.stop()

# COMMAND ----------

# Load the Delta table
df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet/')


# COMMAND ----------

# Specify a temporary directory for output
temp_dir = "/mnt/data/cprd_statin_data/temp"
final_output = "/mnt/data/cprd_statin_data/temp/patient_statin.txt"


# COMMAND ----------

# Save as a single partition TXT file in the temporary directory
df.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", "\t").csv(temp_dir)



# COMMAND ----------


# Use dbutils to move and rename the file
files = dbutils.fs.ls(temp_dir)
for file in files:
    if file.name.startswith("part-") and file.name.endswith(".csv"):
        dbutils.fs.mv(file.path, final_output)
        break


# COMMAND ----------


# Clean up temporary directory
dbutils.fs.rm(temp_dir, recurse=True)