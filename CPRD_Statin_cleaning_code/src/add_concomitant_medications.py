# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

def add_binary_column_concomitant_medication_efficient(pat_df, di_df, codelist_df, new_column_label):
    """
    Adds a binary column oral_steroids to the patient dataframe (pat_df) 
    indicating whether each patient had any occurrences of medication codes 
    from a given codelist (codelist_df) during their baseline period.
    
    The baseline period is defined as the time between a patient's study_entry_date 
    and their first_statin_date.

    Parameters:
    ----------
    pat_df : DataFrame
        The patient dataframe containing patient identifiers (patid), 
        study_entry_date, and index_date.
    
    di_df : DataFrame
        The drug issues dataframe containing records of medication issues 
        with fields such as patid, prodcodeid, and issue_date.
    
    codelist_df : DataFrame
        A dataframe containing a list of medication codes (e.g., ProdCodeId) 
        used to filter drug issues relevant to the binary indicator.

    new_column_label : Str
        A string which should be the name of the new column to be added to the patient dataframe.

    Returns:
    -------
    DataFrame
        The modified pat_df with an additional binary column oral_steroids. 
        This column will contain:
        - 1 if the patient had any qualifying drug issues during their baseline period.
        - 0 otherwise.

    This should do the same as the "inefficient" function, but has been optimised by copilot to use increased chaining

    """

    # Check if the new column already exists
    if new_column_label in pat_df.columns:
        raise Exception('Column of that name already exists')

    # Step 1 & 2: Join the codelist with drug issues and merge with the patient dataframe in a single step
    codelist_df_bc = F.broadcast(codelist_df)
    merged_df = (
        di_df.join(codelist_df_bc, di_df['prodcodeid'] == codelist_df_bc['ProdCodeId'], 'inner')
        .join(pat_df, 'patid', 'inner')
        .withColumn('effective_date', F.coalesce(
            F.to_date(F.col('issuedate'), 'dd/MM/yyyy'), 
            F.to_date(F.col('enterdate'), 'dd/MM/yyyy')
        ))
        .filter(
            (F.col('effective_date') >= F.col('study_entry_date')) & 
            (F.col('effective_date') <= F.col('index_date'))
        )
        .select('patid')
        .distinct()
        .withColumn(new_column_label, F.lit(1))
    )

    # Step 3: Add the binary column to the patient dataframe
    pat_df = (
        pat_df.join(merged_df, 'patid', 'left')
        .withColumn(new_column_label, F.coalesce(F.col(new_column_label), F.lit(0)))
    )

    return pat_df

# COMMAND ----------

def add_binary_column_concomitant_medication(pat_df, di_df, codelist_df, new_column_label):
    """
    Adds a binary column oral_steroids to the patient dataframe (pat_df) 
    indicating whether each patient had any occurrences of medication codes 
    from a given codelist (codelist_df) during their baseline period.
    
    The baseline period is defined as the time between a patient's study_entry_date 
    and their first_statin_date.

    Parameters:
    ----------
    pat_df : DataFrame
        The patient dataframe containing patient identifiers (patid), 
        study_entry_date, and index_date.
    
    di_df : DataFrame
        The drug issues dataframe containing records of medication issues 
        with fields such as patid, prodcodeid, and issue_date.
    
    codelist_df : DataFrame
        A dataframe containing a list of medication codes (e.g., ProdCodeId) 
        used to filter drug issues relevant to the binary indicator.

    new_column_label : Str
        A string which should be the name of the new column to be added to the patient dataframe.

    Returns:
    -------
    DataFrame
        The modified pat_df with an additional binary column oral_steroids. 
        This column will contain:
        - 1 if the patient had any qualifying drug issues during their baseline period.
        - 0 otherwise.
    """

    
    # Check if the new column already exists
    if new_column_label in pat_df.columns:
        raise Exception('Column of that name already exists')


    # Broadcast codelist_df

    codelist_df_bc = F.broadcast(codelist_df)

    # Step 1: Filter the drug issues based on the medication codelist
    di_filt = di_df.join(
        codelist_df_bc, 
        codelist_df_bc['ProdCodeId'] == di_df['prodcodeid'],
        how='inner'
    )

    # Step 2: Join the filtered drug issues with the patient dataframe
    merged_df = di_filt.join(
        pat_df, 
        on='patid',
        how='inner'
    )

    # Step 3: Convert date columns to proper date format if they are not already date objects
    if dict(merged_df.dtypes)['enterdate'] != 'date':
        merged_df = merged_df.withColumn('enterdate', F.to_date(F.col('enterdate'), 'dd/MM/yyyy'))
    if dict(merged_df.dtypes)['issuedate'] != 'date':
        merged_df = merged_df.withColumn('issuedate', F.to_date(F.col('issuedate'), 'dd/MM/yyyy'))

    # Handle cases where issue_date is null by using enter_date as a fallback.
    merged_df = merged_df.withColumn(
        "effective_date", 
        F.coalesce(F.col("issuedate"), F.col("enterdate"))
    )

    # Step 4: Filter for drug issues within the baseline period
    result_df = merged_df.filter(
        (F.col("effective_date") >= F.col("study_entry_date")) & 
        (F.col("effective_date") <= F.col("index_date"))
    )
    
    # Step 5: Deduplicate and mark patients with relevant drug issues
    result_df = result_df.select("patid").distinct()
    result_df = result_df.withColumn(new_column_label, F.lit(1))

    # Step 6: Merge the results back into the patient dataframe
    pat_df = pat_df.join(
        result_df, 
        on='patid', 
        how='left'
    )

    # Fill missing values in the oral_steroids column with 0 for patients without qualifying drug issues.
    pat_df = pat_df.withColumn(
        new_column_label, 
        F.coalesce(pat_df[new_column_label], F.lit(0))
    )

    # Return the modified patient dataframe with the new binary column.
    return pat_df