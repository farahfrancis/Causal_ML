# Databricks notebook source
# MAGIC %md
# MAGIC The purpose of this notebook is to take the "final" patient list and process it to a "research-ready" dataset  
# MAGIC   
# MAGIC This should have a structure with the following features:
# MAGIC - One patient per row
# MAGIC - One column per covariate (age, gender, each comorbidity etc)
# MAGIC - Should have which statin each patient was exposed to 
# MAGIC - Should have for each patient the "study_entry_date" which is the start of the baseline period, the "index date" which is the date of first statin and the "study_exit_date" which is defined in the protocol
# MAGIC - This is ultimately the dataset that will be used for TMLE/DML work

# COMMAND ----------

## import all the modules that we think are relevant
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd 

# COMMAND ----------

# MAGIC %run /Workspace/Users/farah.francis1@mhra.gov.uk/CPRD_Statin/src/add_study_end_date

# COMMAND ----------

# MAGIC %run /Workspace/Users/farah.francis1@mhra.gov.uk/CPRD_Statin/src/add_concomitant_medications

# COMMAND ----------

# read in the datasets 
di_df = spark.read.format("delta").load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/DrugIssue/DrugIssue.parquet')
obs_df = spark.read.format("delta").load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Observation/Observation.parquet')
prac_df = spark.read.format("delta").load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Practice/Practice.parquet')
pat_df = spark.read.format("delta").load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Add study end date for all of our patients

# COMMAND ----------

codelist_path = '/mnt/data/cprd_statin_data/Codelists/statins.txt'
statin_df = spark.read.csv(codelist_path, header=True, sep='\t')
statin_df = statin_df.drop('DMDCode', 'TermfromEMIS', 'formulation', 'routeofadministration', 'bnfcode', 'DrugIssues')
broadcasted_statin_codelist = F.broadcast(statin_df)

# COMMAND ----------

pat_df_annotated = add_study_end_date(pat_df, di_df, broadcasted_statin_codelist)

# COMMAND ----------

# MAGIC %md
# MAGIC rename columns to be epi standard names and drop any columns that are not going to be relevant

# COMMAND ----------

pat_df_annotated = pat_df_annotated.select(
    'patid', 'gender', 'yob', 'mob', 'first_statin_date', 
    'study_entry_date', 'outcome_date', 'Outcome', 'patient_leaves_study_date'
)

pat_df_annotated = pat_df_annotated.withColumnRenamed('first_statin_date', 'index_date')
pat_df_annotated = pat_df_annotated.withColumnRenamed('patient_leaves_study_date', 'study_exit_date')

# COMMAND ----------

# MAGIC %md
# MAGIC - find the patients where the outcome date is after the study_exit_date
# MAGIC - whilst these patients *do* experience the outcome, we have to classify them as No Outcome because we censor at the end of the study date
# MAGIC - this is to avoid selection bias (as recommended by Helen)

# COMMAND ----------

pat_df_annotated = pat_df_annotated.withColumn(
    'Outcome', 
    F.when(F.col('outcome_date') > F.col('study_exit_date'), 0).otherwise(F.col('Outcome'))
).withColumn(
    'outcome_date', 
    F.when(F.col('outcome_date') > F.col('study_exit_date'), None).otherwise(F.col('outcome_date'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Add ethnicity to the final dataset

# COMMAND ----------

final_dataset = pat_df_annotated.select("*") # our research-ready dataset is going to be based on the patient dataset

# COMMAND ----------

display(final_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Add comorbidities using Farah's Code

# COMMAND ----------

# MAGIC %run /Workspace/Users/farah.francis1@mhra.gov.uk/CPRD_Statin/Binarising_comorbidities

# COMMAND ----------

final_df.count()

# COMMAND ----------

#updated_df is the final dataset need to combine with concomitant medications df, drop duplicate columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Add concomitant medications using Griffin's Code

# COMMAND ----------

# oral steroids 

oral_steroids = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/oral_steroids.txt', sep='\t', dtype=str)
os_df = spark.createDataFrame(oral_steroids)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, os_df, 'oral_steroids')

# COMMAND ----------

del oral_steroids, os_df

# COMMAND ----------

diuretics = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/diuretics.txt', sep='\t', dtype=str)
diu_df = spark.createDataFrame(diuretics)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, diu_df, 'diuretics')

# COMMAND ----------

del diuretics, diu_df

# COMMAND ----------

arbs = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/ARB.txt', sep='\t', dtype=str)
arb_df = spark.createDataFrame(arbs)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, arb_df, 'angiotension_receptor_blockers')

# COMMAND ----------

del arbs, arb_df 

# COMMAND ----------

ace = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/ACE_proc_final.csv', sep=',', dtype=str)
ace_df = spark.createDataFrame(ace)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, ace_df, 'ace_inhibitors')

# COMMAND ----------

del ace, ace_df

# COMMAND ----------

ccb = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/ccb_proc_final.csv', sep=',', dtype=str)
ccb_df = spark.createDataFrame(ccb)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, ccb_df, 'calcium_channel_blockers')

# COMMAND ----------

del ccb, ccb_df

# COMMAND ----------

bbs = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/beta_blockers_proc.txt', dtype=str)
bb_df = spark.createDataFrame(bbs)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, bb_df, 'beta_blockers')

# COMMAND ----------

del bbs, bb_df

# COMMAND ----------

fibrates = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/fibrates.txt', dtype=str, sep = '\t')
fibrates_df = spark.createDataFrame(fibrates)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, fibrates_df, 'fibrates')

# COMMAND ----------

del fibrates, fibrates_df

# COMMAND ----------

fusidic = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/fusidic_acid.txt', dtype=str, sep='\t')
fusidic_df = spark.createDataFrame(fusidic)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, fusidic_df, 'fusidic_acid')


# COMMAND ----------

del fusidic, fusidic_df

# COMMAND ----------

colchicine =  pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/colchicine.txt', dtype=str, sep='\t')
colchicine_df = spark.createDataFrame(colchicine)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, colchicine_df, 'colchicine')

# COMMAND ----------

del colchicine, colchicine_df

# COMMAND ----------

cyclosporin = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/cyclosporin.txt', dtype=str, sep='\t')
cyclosporin_df = spark.createDataFrame(cyclosporin)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, cyclosporin_df, 'cyclosporin')


# COMMAND ----------

del cyclosporin, cyclosporin_df

# COMMAND ----------

protease_inhibitors = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/hiv_hcv_protease_inhibitors.txt', dtype=str, sep='\t')
protease_inhibitors_df = spark.createDataFrame(protease_inhibitors)
final_dataset = add_binary_column_concomitant_medication_efficient(final_dataset, di_df, protease_inhibitors_df, 'hiv_hcv_protease_inhibitors')


# COMMAND ----------

final_dataset.count()

# COMMAND ----------

del protease_inhibitors, protease_inhibitors_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Add other covariates
# MAGIC - Ethnicity
# MAGIC - Smoker/non-smoker
# MAGIC - Alcohol/no alcohol
# MAGIC

# COMMAND ----------

## calculating patient age

# we define the age *at* statin exposure

# for every patient, if we have their mob, then we'll assume their birthday is on the 15th of that month
# if we don't have their mob, then we'll assume their birthday is on the 31st July

final_dataset = final_dataset.withColumn(
    'birthday',
    F.when(
        final_dataset['mob'].isNotNull(),
        F.last_day(F.concat_ws('-', final_dataset['yob'], final_dataset['mob'], F.lit(1)))
    ).otherwise(
        F.concat_ws('-', final_dataset['yob'], F.lit(7), F.lit(31))
    )
)

# Calculate age and drop birthday column
final_dataset = final_dataset.withColumn('age_at_statin_exposure', F.floor(F.datediff(final_dataset['index_date'], F.to_date(final_dataset['birthday'])) / 365.25))

# COMMAND ----------

final_dataset.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### combine comorbidity, drugs and lifestyle

# COMMAND ----------

final_dataset = final_dataset.join(final_df, on='patid', how='inner')
display(final_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ### add ethnicity

# COMMAND ----------

ethnic_data = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/data/cprd_statin_data/Ethnicity.csv")
ethnic_data = ethnic_data.withColumnRenamed('ethnic_6', 'Ethnic').drop('ethnic_6')
final_dataset = final_dataset.join(ethnic_data, on='patid', how='inner')
display(final_dataset)

# COMMAND ----------

final_dataset.count()

# COMMAND ----------

#drop unwanted columns
columns_to_drop = ['obsdate', 'mob', 'yob', 'index_date', 'study_entry_date', 'outcome_date', 'study_exit_date', 'birthday']
final_dataset = final_dataset.drop(*columns_to_drop)

# COMMAND ----------

final_dataset.count()

# COMMAND ----------

from pyspark.sql.functions import when, col

final_dataset = final_dataset.withColumn("statins", 
                   when(col("drugsubstancename") == "Simvastatin", 1)
                   .when(col("drugsubstancename") == "Atorvastatin calcium trihydrate", 2)
                   .when(col("drugsubstancename") == "Pravastatin sodium", 3)
                   .when(col("drugsubstancename") == "Rosuvastatin calcium", 4)
                   .when(col("drugsubstancename") == "Fluvastatin sodium", 5)
                   .otherwise(None))

final_dataset = final_dataset.drop("drugsubstancename")


# COMMAND ----------

display(final_dataset)

# COMMAND ----------

final_dataset = final_dataset.filter(final_dataset.gender != 3)

# COMMAND ----------

import os
print(os.path.isdir('/dbfs/mnt/data/cprd_statin_data/final_dataset.csv'))


# COMMAND ----------

final_dataset.toPandas().to_csv('/dbfs/mnt/data/cprd_statin_data/final_dataset_output.csv', index=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Some quick data visualisation

# COMMAND ----------

visualise = False

# COMMAND ----------


if (visualise):
    # Convert Spark DataFrame to Pandas DataFrame for plotting
    final_dataset_pd = final_dataset.toPandas()

    # Plot histogram of 'age_at_statin_exposure'
    plt.figure(figsize=(10, 6))
    plt.hist(final_dataset_pd['age_at_statin_exposure'], bins=30, edgecolor='k', alpha=0.7)
    plt.title('Histogram of Age at Statin Exposure')
    plt.xlabel('Age at Statin Exposure')
    plt.ylabel('Frequency')
    plt.grid(True)
    #plt.savefig('visualisation/age_hist.png', dpi = 100)
    plt.show()

    # Plot pie chart of 'gender'
    gender_counts = final_dataset_pd['gender'].value_counts()
    gender_labels = ['male' if gender == '1' else 'female' if gender == '2' else 'unknown' for gender in gender_counts.index]
    plt.figure(figsize=(8, 8))
    plt.pie(gender_counts, labels=gender_labels, autopct='%1.1f%%', startangle=140)
    plt.title('Pie Chart of Gender')
    #plt.savefig('visualisation/gender_pie.png', dpi = 100)
    plt.show()

    # Plot bar chart of 'Outcome' with log scale
    outcome_counts = final_dataset_pd['Outcome'].value_counts()
    plt.figure(figsize=(10, 6))
    bars = plt.bar(outcome_counts.index, outcome_counts.values, color='skyblue')
    plt.yscale('log')
    plt.title('Bar Chart of Outcome')
    plt.xlabel('Outcome')
    plt.ylabel('Count (log scale)')
    plt.grid(True, which="both", ls="-")

    # Add annotations
    for bar in bars:
        height = bar.get_height()
        plt.annotate(f'{height:,.0f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')
    #plt.savefig('visualisation/outcome_bar.png', dpi = 100)

    plt.show()