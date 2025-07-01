# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run /Workspace/Users/griffin.farrow@mhra.gov.uk/CPRD_Statin/src/add_concomitant_medications

# COMMAND ----------

# read in patient and drug data
pat_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet')
di_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/DrugIssue/DrugIssue.parquet')

# COMMAND ----------

# take in dataframe of concomitant medications
codelist = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/oral_steroids.txt', sep='\t', dtype=str)
codelist_df = spark.createDataFrame(codelist)
codelist_bc = F.broadcast(codelist_df)

# COMMAND ----------

pat_df_annotated = add_binary_column_concomitant_medication(pat_df, di_df, codelist_bc)

# COMMAND ----------

pat_df_annotated2 = add_binary_column_concomitant_medication_efficient(pat_df, di_df, codelist_bc)

# COMMAND ----------

diff_df = pat_df_annotated.exceptAll(pat_df_annotated2)
display(diff_df)