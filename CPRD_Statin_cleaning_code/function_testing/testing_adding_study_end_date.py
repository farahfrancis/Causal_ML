# Databricks notebook source
# DBTITLE 1,e
# The purpose of this notebook is to work out the study end date for every patient in the study

import pandas as pd
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StructType, StructField, DateType, StringType

import datetime 

# COMMAND ----------

# MAGIC %run /Workspace/Users/griffin.farrow@mhra.gov.uk/CPRD_Statin/src/add_study_end_date

# COMMAND ----------

pat_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet') # read in patient_dataframe

# COMMAND ----------

# would like to censor patients after they have a year of no statins
di_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/DrugIssue/DrugIssue.parquet')

# COMMAND ----------

codelist_path = '/mnt/data/cprd_statin_data/Codelists/statins.txt'
statin_df = spark.read.csv(codelist_path, header=True, sep='\t')
statin_df = statin_df.drop('DMDCode', 'TermfromEMIS', 'formulation', 'routeofadministration', 'bnfcode', 'DrugIssues')
broadcasted_statin_codelist = F.broadcast(statin_df)

# COMMAND ----------

pat_df = add_study_end_date(pat_df, di_df, broadcasted_statin_codelist)

# COMMAND ----------

pat_df.filter(F.col('outcome') > 0).display()