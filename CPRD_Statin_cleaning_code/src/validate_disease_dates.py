# Databricks notebook source
from pyspark.sql.functions import when, col

def validate_disease_dates(df, diseases, reference_date_col):
    """
    Validates disease columns by checking if their observation date is before or the same as a reference date.
    If not, replaces the disease value with '0'.

    Parameters:
    df (DataFrame): The input Spark DataFrame.
    diseases (list): List of disease column names.
    reference_date_col (str): Column name for the reference date (e.g., 'earliest_statin_date').

    Returns:
    DataFrame: Updated Spark DataFrame with validated disease columns.
    """
    for disease in diseases:
        obsdate_col = f"obsdate_{disease}"
        df = df.withColumn(
            disease,
            when(
                (df[disease] == 1) & 
                ((df[obsdate_col] > df[reference_date_col]) | df[obsdate_col].isNull()),
                0
            ).otherwise(df[disease])
        )
    return df

