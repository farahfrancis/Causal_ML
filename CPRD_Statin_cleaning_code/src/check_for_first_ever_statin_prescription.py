# Databricks notebook source
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Check for statin prescription").getOrCreate()

# COMMAND ----------

def add_earliest_statin_prescription(statin_df, di_df, patient_df):

    """
    Function to calculate the earliest statin prescription for each patient.

    Args:
    - statin_df (DataFrame): SparkDataFrame containing statin codelist.
    - di_df (DataFrame): SparkDataFrame containing drug issue data.
    - patient_df (DataFrame): SparkDataFrame containing patient data.

    Returns:
    - DataFrame: SparkDataFrame containing patient data, including date of first statin prescription for each patient and time between registration date and first statin date.

    Example:
    patient_df_annotated = add_earliest_statin_prescription(statin_df, di_df, patient_df)

    Detailed Steps:
    1. Check Columns: Ensure the required columns exist in the input DataFrames.
    2. Filter Drug Issues: Filter the drug issue DataFrame to include only statin codes.
    3. Convert Dates: Convert the 'issuedate' column to date format.
    4. Determine Earliest Prescription: Use a window specification to find the earliest statin prescription date for each patient.
    5. Annotate Patient Data: Add the earliest statin prescription date to the patient DataFrame.
    6. Calculate Time Difference: Calculate the time between the registration date and the first statin prescription date.

    """

    # Check required columns in a single loop
    required_columns = {
        'di_df': ['prodcodeid', 'patid', 'issuedate'],
        'patient_df': ['patid'],
        'statin_df': ['ProdCodeId', 'drugsubstancename']
    }
    
    for df_name, cols in required_columns.items():
        df = locals()[df_name]
        missing_cols = [col for col in cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"{df_name} must contain columns: {', '.join(missing_cols)}")

    # Filter and join dataframes, selecting only necessary columns
    di_df_filt = di_df.select('prodcodeid', 'patid', 'issuedate') \
                      .join(statin_df.select('ProdCodeId', 'drugsubstancename'), di_df.prodcodeid == statin_df.ProdCodeId, how='inner')

    # Convert issuedate column to date format
    di_df_filt = di_df_filt.withColumn('issuedate', F.to_date(F.col('issuedate'), 'dd/MM/yyyy'))

    # Use a window specification to find the earliest statin prescription date for each patient
    window_spec = Window.partitionBy('patid').orderBy('issuedate')
    di_df_final = di_df_filt.withColumn("row_num", F.row_number().over(window_spec)) \
                            .filter(F.col("row_num") == 1) \
                            .drop("row_num")

    # Join with patient_df and calculate the time difference
    patient_df_annotated = patient_df.join(
        di_df_final.select("patid", "issuedate", "drugsubstancename").alias("earliest_date"),
        on='patid',
        how='left'
    ).withColumnRenamed('issuedate', 'first_statin_date') \
     .withColumn('regstartdate', F.to_date(F.col('regstartdate'), 'dd/MM/yyyy')) \
     .withColumn('time_to_first_statin', F.datediff(F.col('first_statin_date'), F.col('regstartdate')))

    return patient_df_annotated