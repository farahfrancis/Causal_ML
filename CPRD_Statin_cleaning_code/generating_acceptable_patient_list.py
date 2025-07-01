# Databricks notebook source
# MAGIC %md
# MAGIC # Generating a (final) acceptable patient list  
# MAGIC   
# MAGIC - This notebook merges all of the individual observation, drug issue, patient and practice files to form an overall "merged" dataframe.  
# MAGIC - This will then be filtered to get only the acceptable patients, by applying the different exclusion criteria 
# MAGIC - This will then be used to filter the drug issue and observation files
# MAGIC - Finally, it will save the "final" files that need to be dealt with in `delta` format with zordering optimisation by patid

# COMMAND ----------

import pandas as pd
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StructType, StructField, DateType, StringType

import datetime 

# COMMAND ----------

# MAGIC %run /Workspace/Users/farah.francis1@mhra.gov.uk/CPRD_Statin/src/add_outcome_information

# COMMAND ----------

di_df1 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List1/DrugIssue/DrugIssue.parquet')
obs_df1 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List1/Observation/Observation.parquet')
prac_df1 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List1/Practice/Practice.parquet')
pat_df1 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List1/Patients/Patients.parquet')

di_df2 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List2/DrugIssue/DrugIssue.parquet')
obs_df2 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List2/Observation/Observation.parquet')
prac_df2 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List2/Practice/Practice.parquet')
pat_df2 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List2/Patient/Patient.parquet')

di_df3 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List3/DrugIssue/DrugIssue.parquet')
obs_df3 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List3/Observation/Observation.parquet')
prac_df3 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List3/Practice/Practice.parquet')
pat_df3 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List3/Patient/Patient.parquet')

di_df4 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List4/DrugIssue/DrugIssue.parquet')
obs_df4 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List4/Observation/Observation.parquet')
prac_df4 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List4/Practice/Practice.parquet')
pat_df4 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List4/Patient/Patient.parquet')

di_df5 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List5/DrugIssue/DrugIssue.parquet')
obs_df5 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List5/Observation/Observation.parquet')
prac_df5 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List5/Practice/Practice.parquet')
pat_df5 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List5/Patient/Patient.parquet')

di_df6 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List6/DrugIssue/DrugIssue.parquet')
obs_df6 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List6/Observation/Observation.parquet')
prac_df6 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List6/Practice/Practice.parquet')
pat_df6 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List6/Patient/Patient.parquet')

di_df7 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List7/DrugIssue/data/DrugIssue.parquet')
obs_df7 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List7/Observation/data/Observation.parquet')
prac_df7 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List7/Practice/data/Practice.parquet')
pat_df7 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List7/Patient/data/Patient.parquet')

# COMMAND ----------

# need to select to ensure columns are ordered in the correct way if we are going to use union 
di_df1 = di_df1.select('patid', 'pracid', 'drugrecid', 'issuedate', 'enterdate', 'prodcodeid', 'dosageid', 'quantity', 'quantunitid', 'duration')
di_df2 = di_df2.select('patid', 'pracid', 'drugrecid', 'issuedate', 'enterdate', 'prodcodeid', 'dosageid', 'quantity', 'quantunitid', 'duration')
di_df3 = di_df3.select('patid', 'pracid', 'drugrecid', 'issuedate', 'enterdate', 'prodcodeid', 'dosageid', 'quantity', 'quantunitid', 'duration')
di_df4 = di_df4.select('patid', 'pracid', 'drugrecid', 'issuedate', 'enterdate', 'prodcodeid', 'dosageid', 'quantity', 'quantunitid', 'duration')
di_df5 = di_df5.select('patid', 'pracid', 'drugrecid', 'issuedate', 'enterdate', 'prodcodeid', 'dosageid', 'quantity', 'quantunitid', 'duration')
di_df6 = di_df6.select('patid', 'pracid', 'drugrecid', 'issuedate', 'enterdate', 'prodcodeid', 'dosageid', 'quantity', 'quantunitid', 'duration')
di_df7 = di_df7.select('patid', 'pracid', 'drugrecid', 'issuedate', 'enterdate', 'prodcodeid', 'dosageid', 'quantity', 'quantunitid', 'duration')

# COMMAND ----------

obs_df1 = obs_df1.select('patid', 'pracid', 'obsid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid')
obs_df2 = obs_df2.select('patid', 'pracid', 'obsid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid')
obs_df3 = obs_df3.select('patid', 'pracid', 'obsid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid')
obs_df4 = obs_df4.select('patid', 'pracid', 'obsid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid')
obs_df5 = obs_df5.select('patid', 'pracid', 'obsid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid')
obs_df6 = obs_df6.select('patid', 'pracid', 'obsid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid')
obs_df7 = obs_df7.select('patid', 'pracid', 'obsid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid')

# COMMAND ----------

pat_df1 = pat_df1.select('patid', 'gender', 'yob', 'mob', 'emis_ddate', 'regstartdate', 'regenddate', 'acceptable')
pat_df2 = pat_df2.select('patid', 'gender', 'yob', 'mob', 'emis_ddate', 'regstartdate', 'regenddate', 'acceptable')
pat_df3 = pat_df3.select('patid', 'gender', 'yob', 'mob', 'emis_ddate', 'regstartdate', 'regenddate', 'acceptable')
pat_df4 = pat_df4.select('patid', 'gender', 'yob', 'mob', 'emis_ddate', 'regstartdate', 'regenddate', 'acceptable')
pat_df5 = pat_df5.select('patid', 'gender', 'yob', 'mob', 'emis_ddate', 'regstartdate', 'regenddate', 'acceptable')
pat_df6 = pat_df6.select('patid', 'gender', 'yob', 'mob', 'emis_ddate', 'regstartdate', 'regenddate', 'acceptable')
pat_df7 = pat_df7.select('patid', 'gender', 'yob', 'mob', 'emis_ddate', 'regstartdate', 'regenddate', 'acceptable')

# COMMAND ----------

prac_df1 = prac_df1.select('pracid', 'lcd', 'uts', 'region')
prac_df2 = prac_df2.select('pracid', 'lcd', 'uts', 'region')
prac_df3 = prac_df3.select('pracid', 'lcd', 'uts', 'region')
prac_df4 = prac_df4.select('pracid', 'lcd', 'uts', 'region')
prac_df5 = prac_df5.select('pracid', 'lcd', 'uts', 'region')
prac_df6 = prac_df6.select('pracid', 'lcd', 'uts', 'region')
prac_df7 = prac_df7.select('pracid', 'lcd', 'uts', 'region')

# COMMAND ----------

di_df = di_df1.union(di_df2).union(di_df3).union(di_df4).union(di_df5).union(di_df6).union(di_df7)
obs_df = obs_df1.union(obs_df2).union(obs_df3).union(obs_df4).union(obs_df5).union(obs_df6).union(obs_df7)
pat_df = pat_df1.union(pat_df2).union(pat_df3).union(pat_df4).union(pat_df5).union(pat_df6).union(pat_df7)
prac_df = prac_df1.union(prac_df2).union(prac_df3).union(prac_df4).union(prac_df5).union(prac_df6).union(prac_df7)

# COMMAND ----------

unique_patid_count = pat_df.select('patid').distinct().count()
unique_patid_count

# COMMAND ----------

l_count = False # whether to count the number of patients that are being excluded by each step 

# COMMAND ----------

for df in ['di_df1', 'obs_df1', 'prac_df1', 'pat_df1', 
           'di_df2', 'obs_df2', 'prac_df2', 'pat_df2', 
           'di_df3', 'obs_df3', 'prac_df3', 'pat_df3', 
           'di_df4', 'obs_df4', 'prac_df4', 'pat_df4', 
           'di_df5', 'obs_df5', 'prac_df5', 'pat_df5', 
           'di_df6', 'obs_df6', 'prac_df6', 'pat_df6', 
           'di_df7', 'obs_df7', 'prac_df7', 'pat_df7']:
    if (df in locals()):
        del locals()[df] # checks if array is in memory and if not, then deletes it to free up memory


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Delete all patients who do not have the acceptable patient flag

# COMMAND ----------

pat_df = pat_df.filter(pat_df['acceptable'] == '1') 

# COMMAND ----------

if (l_count): 
    print(f"After deleting patients without an acceptable flag {pat_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) Identify the first statin prescription for each patient and add it as a new column to the dataframe

# COMMAND ----------

# MAGIC %run /Workspace/Users/farah.francis1@mhra.gov.uk/CPRD_Statin/src/check_for_first_ever_statin_prescription

# COMMAND ----------

codelist_path = '/mnt/data/cprd_statin_data/Codelists/statins.txt'
statin_df = spark.read.csv(codelist_path, header=True, sep='\t')
statin_df = statin_df.drop('DMDCode', 'TermfromEMIS', 'formulation', 'routeofadministration', 'bnfcode', 'DrugIssues')
broadcasted_statin_codelist = F.broadcast(statin_df)

# COMMAND ----------

# add info about first statin
pat_df_annotated = add_earliest_statin_prescription(statin_df, di_df, pat_df)

# delete all patients who do not have at least a year before their first statin prescription
pat_df_annotated = pat_df_annotated.filter(pat_df_annotated['time_to_first_statin'] >= 365)

# calculate start of baseline date
pat_df_annotated = pat_df_annotated.withColumn('study_entry_date', F.expr('date_add(first_statin_date, -365)'))

# COMMAND ----------

display(pat_df_annotated)

# COMMAND ----------

# cleaning up variables

if (pat_df in locals()):
    del pat_df 

# rename pat_df_annotated to pat_df (this actually makes a copy)
pat_df = pat_df_annotated.select("*")

# clean up by deleting pat_df_annotated
del pat_df_annotated

# COMMAND ----------

if (l_count):
    print("after filtering for a year of baseline data = ", pat_df.count())

# COMMAND ----------

pat_df.count()

# COMMAND ----------

display(pat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) Delete all patients whose first statin prescription is not in the study period (01/01/2010 -> 31/03/2020)

# COMMAND ----------

start_date = datetime.date(2010, 1, 1)  # study start date
end_date = datetime.date(2020, 3, 31)   # study end date

pat_df = pat_df.filter(
    (pat_df['first_statin_date'] >= start_date) & (pat_df['first_statin_date'] <= end_date)
    )

# COMMAND ----------

if (l_count):
    print("after filtering for patients whose first prescription is not in the study period = ", pat_df.count())

# COMMAND ----------

pat_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Exclude all patients who are < 18 when the receive their first statin prescription (surely very small)
# MAGIC

# COMMAND ----------

# Convert 'yob' to integer
pat_df = pat_df.withColumn('yob', F.col('yob').cast('int'))

# Define the date that a patient turns 18 as July 31st in the year they turn 18
pat_df = pat_df.withColumn('eighteenth_birthday', F.expr("add_months(make_date(yob, 7, 31), 18*12)"))

# Define a new column which is time between 18th birthday and statin prescription
pat_df = pat_df.withColumn('time_between_18_statin', F.datediff(F.col('first_statin_date'), F.col('eighteenth_birthday')))

pat_df = pat_df.filter(pat_df['time_between_18_statin'] >= 0)

pat_df = pat_df.drop('eighteenth_birthday', 'time_between_18_statin', 'yob_int')

# COMMAND ----------

if (l_count):
    print("after excluding underage patients = ", pat_df.count())

# COMMAND ----------

pat_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) Exclude all patients who have any pregnancy in the baseline period, or in the study period at all

# COMMAND ----------

# read in pregnancy codelist
codelist_path = '/mnt/data/cprd_statin_data/Codelists/pregnancy.txt'
preg_df = spark.read.csv(codelist_path, header=True, sep='\t').drop('Observations', 'OriginalReadCode', 'CleansedReadCode', 'SnomedCTConceptId', 'SnomedCTDescriptionId', 'EmisCodeCategoryId')
broadcasted_preg_codelist = F.broadcast(preg_df)

# COMMAND ----------

pat_df_preg_filtering = pat_df.select("*") # copy patient dataframe to prevent strange behaviour

# COMMAND ----------

# Add start of baseline period for each patient to the study
obs_df_filt = obs_df.join(pat_df_preg_filtering.select('patid', 'study_entry_date'), on='patid', how='inner')

# Filter the observation code list to only include pregnancy codes and convert dates in one step
obs_df_preg = obs_df_filt.join(broadcasted_preg_codelist.select('MedCodeId'), 
                                broadcasted_preg_codelist.MedCodeId == obs_df_filt.medcodeid, 
                                how='inner') \
                         .withColumn('obsdate', F.to_date(F.col('obsdate'), 'dd/MM/yyyy')) \
                         .withColumn('enterdate', F.to_date(F.col('enterdate'), 'dd/MM/yyyy'))

# Check whether each patient has any observations in obs_df_preg after their 'study_entry_date'
obs_df_date_bound = obs_df_preg.filter(F.coalesce(F.col('obsdate'), F.col('enterdate')) > F.col('study_entry_date'))

# Form a list of patids from obs_df_date_bound and exclude these from our acceptable patient dataframe
obs_df_date_bound_patids = obs_df_date_bound.select('patid').distinct()

# COMMAND ----------

# remove these "pregnant" patients from our list of patids 
pat_df_preg_filtering = pat_df_preg_filtering.join(obs_df_date_bound_patids.select('patid'), on='patid', how='left_anti')

# COMMAND ----------

if (l_count):
    print("after filtering out pregnant patients = ", pat_df.count())

# COMMAND ----------

pat_df.count()

# COMMAND ----------

# freeing up memory by deleting old dataframes 

if (obs_df_preg in locals()):
    del obs_df_preg 

if (obs_df_date_bound in locals()):
    del obs_df_date_bound

if (obs_df_filt in locals()):
    del obs_df_filt

if (pat_df in locals()):
    del pat_df 

pat_df = pat_df_preg_filtering.select("*")

del pat_df_preg_filtering

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6) Exclude all patients who experienced the outcome before their first statin prescription 

# COMMAND ----------

# now we need to essentially establish which of our patients experience an outcome, and filter to only keep the patients who experience the outcome *after* exposure
pat_with_outcome = add_outcome_column(pat_df, obs_df)

# COMMAND ----------

# Patients who experience outcome before they take a statin

# Only consider rows where the outcome date is not null (since Null means no outcome)
pat_with_outcome_temp = pat_with_outcome.filter(F.col('outcome_date').isNotNull())

# Identify patients who experienced outcome before they took a statin
outcome_before_study_entry_date = pat_with_outcome_temp.filter(F.col('outcome_date') < F.col('first_statin_date'))

# Delete all patients who experienced outcome before they took a statin
pat_with_outcome = pat_with_outcome.join(outcome_before_study_entry_date.select('patid'), on='patid', how='left_anti')

# COMMAND ----------

# delete all patients who had outcome_code 3 (undefined, but introduces doubt)
pat_with_outcome = pat_with_outcome.filter(F.col('outcome') != 3)

# COMMAND ----------

if (pat_df in locals()):
    del pat_df # delete patient data frame so we can update with our new filtered one

# delete data processing temporary frames
if (pat_with_outcome_temp in locals()):
    del pat_with_outcome_temp

if (outcome_before_study_entry_date in locals()):
    del outcome_before_study_entry_date

pat_df = pat_with_outcome.select("*")  # update patient_dataframe

#del pat_with_outcome

# COMMAND ----------

if (l_count):
    print(f"after filtering for outcome before exposure = {pat_with_outcome.count()}")

# COMMAND ----------

pat_with_outcome.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7) Exclude all the patients who have had more than one statin class prescribed to them

# COMMAND ----------

# identifying patients who only take one statin class

# first filter di_df to only contain statin codes
di_df_filt = di_df.join(broadcasted_statin_codelist, di_df.prodcodeid == broadcasted_statin_codelist.ProdCodeId, how='inner')

# COMMAND ----------

# group by patid and drugsubstancename and only keep the patients who have only one drugsubstancename
pat_only_one = di_df_filt.groupBy('patid').agg(F.countDistinct('drugsubstancename').alias('unique_drugs')).filter(F.col('unique_drugs') == 1).select('patid') 

# COMMAND ----------

# only keep patients from pat_df who appear in pat_only_one
pat_df = pat_df.join(pat_only_one, on='patid', how='inner')

# COMMAND ----------

if (l_count): 
    print("after filtering for only a single statin class = ", pat_df.count())

# COMMAND ----------

pat_df.count()

# COMMAND ----------

# cleaning up defunct arrays 
if (pat_only_one in locals()):
    del pat_only_one

if (di_df_filt in locals()):
    del di_df_filt

# COMMAND ----------

pat_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7) Dealing with patients who have an outcome "out of study"  
# MAGIC   
# MAGIC This one could do with a conversation with Helen.  
# MAGIC There are patients in the dataset who first experience the outcome *after* the end of the study period (31/03/2020)  
# MAGIC How should we handle those patients? Drop them, treat them as "no outcome" patients or treat them as "outcome" patients even though the outcome is outside the study period
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9) Yields a final acceptable patient list 

# COMMAND ----------

if (l_count):
    print(f"Final number of acceptable patients = {pat_df.count()}")

# COMMAND ----------

obs_df = obs_df.join(pat_df, on='patid', how='inner')

# COMMAND ----------

# filter obseration and di_df to only contain reasonable patients 
di_df = di_df.join(pat_df.select('patid'), on='patid', how='inner')

# COMMAND ----------

save_data = True # if set to true, then data will be saved to dbfs 

# COMMAND ----------

obs_df = obs_df.toDF(*(col if col not in obs_df.columns[:i] else f"{col}_dup" for i, col in enumerate(obs_df.columns)))

# COMMAND ----------

obs_df.write.format('delta') \
    .option("mergeSchema", "true") \
    .mode('overwrite') \
    .save('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Observation/Observation.parquet')


# COMMAND ----------

if save_data: 
    di_df.write.format('delta').mode('overwrite').save('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/DrugIssue/DrugIssue.parquet')

# COMMAND ----------

if save_data: 
    pat_df.write.format('delta') \
        .option("mergeSchema", "true") \
        .mode('overwrite') \
        .save('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet')

# COMMAND ----------

if save_data: 
    prac_df.write.format('delta').mode('overwrite').save('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Practice/Practice.parquet')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Observation/Observation.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/DrugIssue/DrugIssue.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Practice/Practice.parquet`;