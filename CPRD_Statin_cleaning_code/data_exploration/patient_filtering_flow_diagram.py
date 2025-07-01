# Databricks notebook source
# MAGIC %md
# MAGIC The purpose of this notebook is to filter the patients who have enough baseline data for us to use them  
# MAGIC - For each patient in the patient list, we want to extract their first ever statin prescription date  
# MAGIC - If that date is not *at least* a year after their registration, then we need to filter out those patients

# COMMAND ----------

pip install schemdraw

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StructType, StructField, DateType, StringType

import matplotlib.pyplot as plt 


spark = SparkSession.builder.appName("Custom Baselining").getOrCreate()

# COMMAND ----------

# read in statins codelist
codelist_path = '/mnt/data/cprd_statin_data/Codelists/statins.txt'
statin_df = spark.read.csv(codelist_path, header=True, sep='\t')
statin_df = statin_df.drop('DMDCode', 'TermfromEMIS', 'formulation', 'routeofadministration', 'bnfcode', 'DrugIssues')
broadcasted_statin_codelist = F.broadcast(statin_df)

# COMMAND ----------

di_df4 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List4/DrugIssue/DrugIssue.parquet')
patient_df4 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List4/Patient/Patient.parquet')

# COMMAND ----------

di_df5 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List5/DrugIssue/DrugIssue.parquet')
patient_df5 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List5/Patient/Patient.parquet')

# COMMAND ----------

di_df6 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List6/DrugIssue/DrugIssue.parquet')
patient_df6 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List6/Patient/Patient.parquet')

# COMMAND ----------

di_df7 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List7/DrugIssue/data/')
patient_df7 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List7/Patient/data/')

# COMMAND ----------

# merge all dataframes
di_df = di_df4.union(di_df5).union(di_df6).union(di_df7)
patient_df = patient_df4.union(patient_df5).union(patient_df6).union(patient_df7)

# COMMAND ----------

# filter drug issue dataframe to have only statin relevant codes
di_df_filt = di_df.join(broadcasted_statin_codelist, di_df.prodcodeid == broadcasted_statin_codelist.ProdCodeId, how='inner')

# convert issuedate to date object
di_df_filt = di_df_filt.withColumn("issuedate", F.to_date(F.col("issuedate"), "dd/MM/yyyy"))

# COMMAND ----------

# patients who only take one statin

# group by patid and drugsubstancename and only keep the patients who have only one drugsubstancename
pat_only_one = di_df_filt.groupBy('patid').agg(F.countDistinct('drugsubstancename').alias('unique_drugs')).filter(F.col('unique_drugs') == 1).select('patid') # list of patids with only one statin class prescribed to them

# COMMAND ----------

# what statins do those patients take?
di_pat_only_one = di_df_filt.join(pat_only_one, on='patid', how='inner') # full record of patients who only take one statin

# COMMAND ----------

# what is that one statin
di_one_statin = di_pat_only_one.withColumn("row_num", F.row_number().over(Window.partitionBy("patid").orderBy("issuedate"))).filter(F.col("row_num") == 1).drop("row_num")

# add first statin date to pat_only_one dataframe 
pat_only_one = pat_only_one.join(di_one_statin.select('patid', 'issuedate'), on='patid', how='left')

# add patient registration date to this dataframe
pat_only_one = pat_only_one.join(patient_df.select('patid', 'regstartdate', 'acceptable'), on='patid', how='left')

# COMMAND ----------

# convert regstartdate
pat_only_one = pat_only_one.withColumn("regstartdate", F.to_date(F.col("regstartdate"), "dd/MM/yyyy"))

# COMMAND ----------

# add a column to pat_only_one which is time in days between regstartdate and issuedate
pat_only_one = pat_only_one.withColumn("days_between", F.datediff(F.col("issuedate"), F.col("regstartdate")))

# COMMAND ----------

# define all the quantities 
patient_number = patient_df.count() # total patients
patient_just_one = pat_only_one.count() # patients with only one statin prescription

# COMMAND ----------

"""
patient_number # total patients
patient_just_one # patients who have only taken one statin class
statin_not_in_study # number of patients whose first statin prescription is not in the study period
patient_not_enough_reg # patients who don't have enough registration data as a baseline
not_acceptable_patients # patients without an acceptable patient flag
final_count # final number of patients we are taking forward 
"""

# COMMAND ----------

# filter out all patients whose first statin prescription is before January 1st 2010
pat_suitable = pat_only_one.filter(F.col("issuedate") >= "2010-01-01")

statin_valid = pat_suitable.count()
statin_not_in_study = patient_just_one - statin_valid

# COMMAND ----------

# filter out all patients whose first statin prescription is not at least a year after their registration date 
pat_suitable = pat_suitable.filter(F.col("days_between") >= 365)

enough_reg = pat_suitable.count()
patient_not_enough_reg = statin_valid -  enough_reg

# COMMAND ----------

# filter out all patients without an acceptable patient flag 
pat_suitable = pat_suitable.filter(F.col('acceptable') == "1")

acceptable = pat_suitable.count()
not_acceptable_patients = enough_reg - acceptable

# COMMAND ----------

final_count = acceptable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make a flow chart

# COMMAND ----------

from schemdraw import flow
import schemdraw

tri_h = 5
tri_w = 5
box_h = 2
box_w = 2
final_w = 3
final_h = 3

with schemdraw.Drawing() as d:
    d.config(fontsize=11)
    b = flow.Start().label(f'Input: {patient_number} unique patients')
    flow.Arrow().down(d.unit/3)
    d1 = flow.Decision(w=tri_w, h=tri_h, E='>1', S='1').label("How many \n unique statins \n has this \n patient taken?")
    flow.Arrow().length(d.unit/3)
    d2 = flow.Decision(w=tri_w, h=tri_h, E='No', S='Yes').label("Is the first statin \n prescription in \n the study period \n (1/1/2010 -> 31/3/2020)?")
    flow.Arrow().length(d.unit/3)
    d3 = flow.Decision(w=tri_w, h=tri_h, E='No', S='Yes').label("Is there a year \n between reg date and \n first statin prescription?")
    flow.Arrow().length(d.unit/3)
    d4 = flow.Decision(w=tri_w, h=tri_h, E='No', S='Yes').label("Does the patient \n have an acceptable flag?")
    flow.Arrow().length(d.unit/3)
    d5 = flow.Decision(w=tri_w, h=tri_h, E='?', S='?').label("Any other exclusion criteria?")
    flow.Arrow().length(d.unit/3)
    d6 = flow.Box(w=final_w, h=final_h).anchor('N').label(f"Output \n {final_count} \n patients \n in study")

    flow.Arrow().right(d.unit*3).at(d1.E)
    first_filt = flow.Box(w=box_w, h=box_h).anchor('W').label(f'Excluded \n {patient_number-patient_just_one} \n patients')

    flow.Arrow().right(d.unit*3).at(d2.E)
    second_filt = flow.Box(w=box_w, h=box_h).anchor('W').label(f'Excluded \n {statin_not_in_study} \n patients')

    flow.Arrow().right(d.unit*3).at(d3.E)
    third_filt = flow.Box(w=box_w, h=box_h).anchor('W').label(f'Excluded \n {patient_not_enough_reg} \n patients')

    flow.Arrow().right(d.unit*3).at(d4.E)
    fourth_filt = flow.Box(w=box_w, h=box_h).anchor('W').label(f'Excluded \n {not_acceptable_patients} \n patients')

    flow.Arrow().right(d.unit*3).at(d5.E)
    fifth_filt = flow.Box(w=box_w, h=box_h).anchor('W').label(f'Excluded \n ? \n patients')



# COMMAND ----------

## save the flow chart

#d.save('flow_diagram.png', transparent=False)