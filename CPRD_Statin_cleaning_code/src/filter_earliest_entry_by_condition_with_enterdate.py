# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def filter_earliest_entry_by_condition_with_enterdate(
    df: DataFrame,
    patid_col: str,
    obsdate_col: str,
    enterdate_col: str,
    condition_flag_col: str
) -> DataFrame:
    """
    Filters the input DataFrame to retain only the earliest entry for a specified condition
    based on the observation date column or enter date if observation date is null.
    If both observation date and enter date are null, leaves the observation date null.

    Parameters:
    - df (DataFrame): The input DataFrame to filter.
    - patid_col (str): The column name representing patient IDs.
    - obsdate_col (str): The column name representing observation dates.
    - enterdate_col (str): The column name representing enter dates.
    - condition_flag_col (str): The column name representing the condition flag (1 for condition present, 0 otherwise).

    Returns:
    - DataFrame: A filtered DataFrame retaining only the earliest entry for the specified condition.
    """
    # Replace `obsdate_col` with `enterdate_col` if `obsdate_col` is null
    df = df.withColumn(
        obsdate_col,
        F.when(F.col(obsdate_col).isNotNull(), F.col(obsdate_col))
         .otherwise(F.col(enterdate_col))
    )

    # Define the window specification based on the observation date
    window_spec = Window.partitionBy(patid_col).orderBy(F.col(obsdate_col).asc())

    # Add row number to identify the earliest entry for the condition
    df_with_row_num = df.withColumn(
        f"row_num_{condition_flag_col}", F.row_number().over(window_spec)
    )

    # Filter to retain only rows with the earliest entry for the condition
    filtered_df = df_with_row_num.filter(
        (F.col(condition_flag_col) == 1) & (F.col(f"row_num_{condition_flag_col}") == 1)
    )

    # Drop the helper column and the `enterdate` column
    filtered_df = filtered_df.drop(f"row_num_{condition_flag_col}", enterdate_col)

    return filtered_df
