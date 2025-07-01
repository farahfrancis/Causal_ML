# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is for conversion of comorbidities to binary outcome
# MAGIC - no disease = 0 and with disease = 1
# MAGIC - make sure the diseases are present during baseline, pateints diagnosed with new comorbidities afer baseline are not considered to have the disease

# COMMAND ----------

import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("binarising").getOrCreate()

# COMMAND ----------

def load_codelists():
    """
    Loads multiple comorbidity codelists from specified file paths into Spark DataFrames.
    Processes the DataFrames by selecting specific columns and renaming them as needed.

    Returns:
        dict: A dictionary where keys are comorbidity names and values are their corresponding DataFrames.
    """
    import os
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Define file paths for each comorbidity
    codelist_paths = {
        'hypothyroid': '/mnt/data/cprd_statin_data/Codelists/hypothyroid.txt',
        'hyperthyroid': '/mnt/data/cprd_statin_data/Codelists/hyperthyroid.txt',
        'hypertension': '/mnt/data/cprd_statin_data/Codelists/hypertension_data.txt',
        'asthma': '/mnt/data/cprd_statin_data/Codelists/asthma.txt',
        'atrial_fibrillation': '/mnt/data/cprd_statin_data/Codelists/atrial_fibrillation.txt',
        'copd': '/mnt/data/cprd_statin_data/Codelists/copd.txt',
        'diabetes': '/mnt/data/cprd_statin_data/Codelists/diabetes.txt',
        'atherosclerotic': '/mnt/data/cprd_statin_data/Codelists/m_atherosclerotic_stroke_aurum.txt',
        'smoke': '/mnt/data/cprd_statin_data/Codelists/m_smoker_aurum.txt',
        'alcohol': '/mnt/data/cprd_statin_data/Codelists/alcohol.txt',
        'bmi': '/mnt/data/cprd_statin_data/Codelists/bmi.txt'
    }

    # Load and process each codelist
    codelists = {}
    for name, path in codelist_paths.items():
        df = spark.read.csv(path, header=True, sep='\t')
        if 'MedCodeId' in df.columns:
            df = df.withColumnRenamed('MedCodeId', 'medcodeid')
        if 'Term' in df.columns:
            df = df[['medcodeid', 'Term']]
        codelists[name] = df

    return codelists


# COMMAND ----------

# Usage example: load all codelists into a dictionary
codelists = load_codelists()
hypothyroid_df = codelists['hypothyroid']
hyperthyroid_df = codelists['hyperthyroid']
hypertension_df = codelists['hypertension']
asthma_df = codelists['asthma']
atrial_fibrillation_df = codelists['atrial_fibrillation']
copd_df = codelists['copd']
diabetes_df = codelists['diabetes']
atherosclerotic_df = codelists['atherosclerotic']
smoke_df = codelists['smoke']
alcohol_df = codelists['alcohol']
bmi_df = codelists['bmi']

# COMMAND ----------

# read in statins codelist
codelist_path = '/mnt/data/cprd_statin_data/Codelists/statins.txt'
statin_df = spark.read.csv(codelist_path, header=True, sep='\t')
statin_df = statin_df.drop('DMDCode', 'TermfromEMIS', 'formulation', 'routeofadministration', 'bnfcode', 'DrugIssues')
broadcasted_statin_codelist = F.broadcast(statin_df)

# COMMAND ----------

# read the observation and patient file

obs_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Observation/Observation.parquet/')
obs_df = obs_df.select('patid', 'obsdate', 'medcodeid', 'enterdate')
patient_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet/')
drug_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/DrugIssue/DrugIssue.parquet/')

# COMMAND ----------

patient_df.select("patid").count()

# COMMAND ----------

# MAGIC %run /Workspace/Users/farah.francis1@mhra.gov.uk/CPRD_Statin/src/add_condition_flag_with_date

# COMMAND ----------

# Generate the hypothyroid DataFrame
hypothyroid_df = add_condition_flag_with_date(obs_df, hypothyroid_df, "hypothyroid")

# Generate the hyperthyroid DataFrame
hyperthyroid_df = add_condition_flag_with_date(obs_df, hyperthyroid_df, "hyperthyroid")

# Generate the hypertension DataFrame
hypertension_df = add_condition_flag_with_date(obs_df, hypertension_df, "hypertension")

# Generate asthma DataFrame
asthma_df = add_condition_flag_with_date(obs_df, asthma_df, "asthma")

# Generate diabetes DataFrame
diabetes_df = add_condition_flag_with_date(obs_df, diabetes_df, "diabetes")

# generate copd DataFrame
copd_df = add_condition_flag_with_date(obs_df, copd_df, "copd")

# Generate atrial fibrillation DataFrame
atrial_fibrillation_df = add_condition_flag_with_date(obs_df, atrial_fibrillation_df, "atrial_fibrillation")

# Generate atherosclerotic DataFrame
atherosclerotic_df = add_condition_flag_with_date(obs_df, atherosclerotic_df, "atherosclerotic")

# Generate smoke DataFrame
smoke_df = add_condition_flag_with_date(obs_df, smoke_df, "smoke")

# Generate alcohol DataFrame
alcohol_df = add_condition_flag_with_date(obs_df, alcohol_df, "alcohol")



# COMMAND ----------

# MAGIC %run /Workspace/Users/farah.francis1@mhra.gov.uk/CPRD_Statin/src/filter_earliest_entry_by_condition_with_enterdate

# COMMAND ----------

## select only the earliest date for each condition
# Apply the function for hypertension
hypertension_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=hypertension_df,
    patid_col="patid",
    obsdate_col="obsdate_hypertension",
    enterdate_col="enterdate",
    condition_flag_col="hypertension"
)

# Apply the function for hypothyroid
hypothyroid_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=hypothyroid_df,
    patid_col="patid",
    obsdate_col="obsdate_hypothyroid",
    enterdate_col="enterdate",
    condition_flag_col="hypothyroid"
)

# Apply the function for hyperthyroid
hyperthyroid_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=hyperthyroid_df,
    patid_col="patid",
    obsdate_col="obsdate_hyperthyroid",
    enterdate_col="enterdate",
    condition_flag_col="hyperthyroid"
)

# Apply the function for asthma
asthma_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=asthma_df,
    patid_col="patid",
    obsdate_col="obsdate_asthma",
    enterdate_col="enterdate",
    condition_flag_col="asthma"
)

# Apply the function for diabetes
diabetes_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=diabetes_df,
    patid_col="patid",
    obsdate_col="obsdate_diabetes",
    enterdate_col="enterdate",
    condition_flag_col="diabetes"
)

# Apply the function for copd
copd_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=copd_df,
    patid_col="patid",
    obsdate_col="obsdate_copd",
    enterdate_col="enterdate",
    condition_flag_col="copd"
)

# Apply the function for atrial fibrillation
atrial_fibrillation_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=atrial_fibrillation_df,
    patid_col="patid",
    obsdate_col="obsdate_atrial_fibrillation",
    enterdate_col="enterdate",
    condition_flag_col="atrial_fibrillation"
)

# Apply the function for atherosclerotic
atherosclerotic_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=atherosclerotic_df,
    patid_col="patid",
    obsdate_col="obsdate_atherosclerotic",
    enterdate_col="enterdate",
    condition_flag_col="atherosclerotic"
)

# Apply the function for smoke
smoke_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=smoke_df,
    patid_col="patid",
    obsdate_col="obsdate_smoke",
    enterdate_col="enterdate",
    condition_flag_col="smoke"
)

# Apply the function for alcohol
alcohol_df_filtered = filter_earliest_entry_by_condition_with_enterdate(
    df=alcohol_df,
    patid_col="patid",
    obsdate_col="obsdate_alcohol",
    enterdate_col="enterdate",
    condition_flag_col="alcohol"
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Need to combine all comorbidities in one table
# MAGIC #### need to modify it base on baseline

# COMMAND ----------

def combine_disease_dataframes(filtered_dfs):
    """
    Combines multiple filtered disease DataFrames into one DataFrame by 'patid', keeping all columns.

    Parameters:
        filtered_dfs (list): A list of filtered disease DataFrames to combine.

    Returns:
        DataFrame: A combined DataFrame with all columns.
    """
    from functools import reduce

    combined_df = reduce(lambda left, right: left.join(right, on="patid", how="outer"), filtered_dfs)
    return combined_df


# COMMAND ----------

# Combine disease DataFrames into one DataFrame by 'patid', keeping all columns
filtered_dfs = [
    hypertension_df_filtered,
    hypothyroid_df_filtered,
    hyperthyroid_df_filtered,
    asthma_df_filtered,
    diabetes_df_filtered,
    copd_df_filtered,
    atrial_fibrillation_df_filtered,
    atherosclerotic_df_filtered,
    smoke_df_filtered,
    alcohol_df_filtered
]

combined_disease_df = combine_disease_dataframes(filtered_dfs)


# COMMAND ----------

# Get the shape of the DataFrame
row_count = combined_disease_df.count()
column_count = len(combined_disease_df.columns)
(row_count, column_count)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate BMI df with values

# COMMAND ----------

# read lookup data
unit_lookup = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/202409_Lookups_CPRDAurum/NumUnit.txt', sep='\t', header=0)
unit_df = spark.createDataFrame(unit_lookup)

# COMMAND ----------

#reload fresh obs_df and retain the value column
obs_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Observation/Observation.parquet/')
obs_df = obs_df.select('patid', 'obsdate', 'medcodeid', 'enterdate', 'value', 'numunitid')


#combine bmi_df with obs_df
new_bmi_df = obs_df.join(bmi_df, on="medcodeid", how="left_outer")


# COMMAND ----------

from pyspark.sql.functions import coalesce, col, row_number
from pyspark.sql.window import Window

# Combine the obs_df with unit_df to get the unit for BMI
bmi_unit_df = new_bmi_df.join(unit_df, on="numunitid", how="left_outer")

# Fill missing obsdate with enterdate
bmi_unit_df = bmi_unit_df.withColumn("obsdate", coalesce(col("obsdate"), col("enterdate")))

# Filter rows with the correct unit description (kg/m2)
bmi_unit_df = bmi_unit_df.filter(col("description") == "kg/m2")

# Drop unnecessary columns
bmi_unit_df = bmi_unit_df.drop("numunitid", "medcodeid", "term", "enterdate", "Description")

# Filter BMI values within a reasonable range (6.7 to 100)
bmi_unit_df = bmi_unit_df.filter((col("value") >= 6.7) & (col("value") <= 70))

# Rename columns for clarity
bmi_unit_df = bmi_unit_df.withColumnRenamed("obsdate", "obsdate_bmi")
bmi_unit_df = bmi_unit_df.withColumnRenamed("value", "BMI")

# Define a window partitioned by 'patid' and ordered by 'obsdate_bmi' in descending order
windowSpec = Window.partitionBy("patid").orderBy(col("obsdate_bmi").desc())

# Assign a row number within each partition
bmi_unit_df = bmi_unit_df.withColumn("row_num", row_number().over(windowSpec))

# Filter to keep only the most recent record (row_num = 1) for each patid
bmi_unit_df = bmi_unit_df.filter(col("row_num") == 1)

# Drop the row_num column as it is no longer needed
bmi_unit_df = bmi_unit_df.drop("row_num")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### combine the bmi with other comorbidiities

# COMMAND ----------

combined_bmi_patient_df = patient_df.join(bmi_unit_df, on="patid", how="outer")


# COMMAND ----------

def merge_and_clean_data(combined_bmi_patient_df, combined_disease_df):
    """
    Merges the patient DataFrame with the combined disease DataFrame,
    replaces nulls with 0 for specified columns.

    Parameters:
        patient_df (DataFrame): Annotated patient DataFrame.
        combined_disease_df (DataFrame): Combined disease DataFrame to merge.

    Returns:
        DataFrame: Cleaned and merged DataFrame.
    """
    # Merge the DataFrames
    merged_df = combined_bmi_patient_df.join(combined_disease_df, on='patid', how='left')

    # Replace null values with 0 for specific columns
    merged_df = merged_df.fillna(0, subset=['hypertension', 'hypothyroid', 'hyperthyroid', 'asthma', 'diabetes', 'copd', 'atrial_fibrillation', 'atherosclerotic', 'smoke', 'alcohol'])

    return merged_df

# COMMAND ----------

# Assuming pat_df_annotated and combined_disease_df are already loaded as DataFrames
cleaned_merged_df = merge_and_clean_data(combined_bmi_patient_df, combined_disease_df)

# Display or process the resulting DataFrame
print(cleaned_merged_df)


# COMMAND ----------

# MAGIC %md
# MAGIC - the table showed the time to first statin is in days
# MAGIC - some obs have no obsdate
# MAGIC - no obs date and no disease means the patient does not have the disease during baseline
# MAGIC - should we removed unwanted columns like the dates

# COMMAND ----------

# MAGIC %run /Workspace/Users/farah.francis1@mhra.gov.uk/CPRD_Statin/src/validate_disease_dates

# COMMAND ----------

# Apply the function
# Usage example
diseases_list = [
    "BMI",
    "hypertension",
    "hypothyroid",
    "hyperthyroid",
    "asthma",
    "diabetes",
    "copd",
    "atrial_fibrillation",
    "atherosclerotic",
    "smoke",
    "alcohol"
]

updated_df = validate_disease_dates(cleaned_merged_df, diseases_list, "time_to_first_statin")


# COMMAND ----------

columns_to_drop = [
    "gender", "yob", "mob", "emis_ddate", "regstartdate", "regenddate", 
    "acceptable", "first_statin_date", "time_to_first_statin", 
    "study_entry_date", "outcome_date", "Outcome"
]

# Drop columns that have 'obsdate' in their name
obsdate_columns = [col for col in updated_df.columns if 'obsdate' in col]
columns_to_drop.extend(obsdate_columns)

# Drop the specified columns
final_df = updated_df.drop(*columns_to_drop)

#display(final_df)