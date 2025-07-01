# Databricks notebook source
# The purpose of this notebook is to work out the study end date for every patient in the study

import pandas as pd
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StructType, StructField, DateType, StringType

import datetime 

# COMMAND ----------

def add_study_end_date(pat_df, di_df, statin_df):
    """
    Function to calculate the study end date for each patient.

    Args:
    - pat_df (DataFrame): Spark DataFrame containing patient data.
    - di_df (DataFrame): Spark DataFrame containing drug issue data.
    - statin_df (DataFrame): Spark DataFrame containing statin codelist.

    Returns:
    - DataFrame: Spark DataFrame containing patient data with the study end date.

    Detailed Steps:
    1. Add a censor date defined as 120 days after the last statin pill.
    2. Ensure all relevant date columns are in date format.
    3. Calculate the minimum of the relevant dates as the study end date.

    We calculate the "last statin pill" by taking the latest issuedate for statins for each patid, and then adding "duration" days to it. If we have no duration quantity, then we define the censor date as the last issuedate for each patient + 200 days
    """

    # Step 1: Add a censor date
    # Filter drug issue records to include only statin information
    di_df_filt = di_df.join(statin_df, di_df.prodcodeid == statin_df.ProdCodeId, how='inner')

    # Convert 'issuedate' column to date format if not already a date object
    if not isinstance(di_df_filt.schema['issuedate'].dataType, DateType):
        di_df_filt = di_df_filt.withColumn('issuedate', F.to_date(F.col('issuedate'), 'dd/MM/yyyy'))

    # Get the last statin issuedate for each patient
    window_spec = Window.partitionBy('patid').orderBy(F.col('issuedate').desc())
    di_df_latest = di_df_filt.withColumn('rank', F.row_number().over(window_spec)).filter(F.col('rank') == 1).drop('rank')

    # Calculate censor date
    di_df_latest = di_df_latest.withColumn('duration_int', F.col('duration').cast('int'))
    di_df_latest = di_df_latest.withColumn(
        'censor_date',
        F.when(F.col('duration_int').isNotNull(), F.expr("date_add(issuedate, duration_int + 120)"))
         .otherwise(F.expr("date_add(issuedate, 180)"))
    ).drop('duration_int')

    # Join censor date with patient data
    pat_df_temp = pat_df.join(di_df_latest.select('patid', 'censor_date'), on='patid', how='left')

    # Step 2: Ensure all relevant date columns are in date format
    date_columns = ['emis_ddate', 'regenddate', 'outcome_date', 'censor_date']
    for col_name in date_columns:
        if not isinstance(pat_df_temp.schema[col_name].dataType, DateType):
            pat_df_temp = pat_df_temp.withColumn(col_name, F.to_date(F.col(col_name), 'dd/MM/yyyy'))

    # Step 3: Calculate the study end date
    pat_df_temp = pat_df_temp.withColumn(
        'patient_leaves_study_date',
        F.least(
            F.col('emis_ddate'), F.col('regenddate'),
            F.lit(datetime.date(2020, 3, 31)), F.col('outcome_date'),
            F.col('censor_date')
        )
    )

    return pat_df_temp