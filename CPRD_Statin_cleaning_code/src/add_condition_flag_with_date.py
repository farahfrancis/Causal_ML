# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This function merge the obs_df with the codelist, rename the medcodeid into the obsname name found in the codelist. Its also rename the obsdate into obsdate_disease to reduce confusion when combining the df with other diseases' df

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def add_condition_flag_with_date(
    obs_df: DataFrame,
    condition_df: DataFrame,
    condition_name: str
) -> DataFrame:
    """
    Adds a condition flag column and renames the observation date column for a specific condition.
    Drops the 'Term' column if it exists in the condition_df.

    Parameters:
    obs_df (DataFrame): The main observation DataFrame.
    condition_df (DataFrame): The condition DataFrame containing `medcodeid` entries.
    condition_name (str): The name of the condition, used to name the output columns.

    Returns:
    DataFrame: The updated DataFrame with the condition flag and renamed observation date.
    """
    # Drop the 'Term' column from condition_df if it exists
    if "Term" in condition_df.columns:
        condition_df = condition_df.drop("Term")
    
    # Perform a left join between obs_df and condition_df
    joined_df = obs_df.join(
        condition_df.withColumn(condition_name, col("medcodeid")),
        on="medcodeid",
        how="left_outer"
    )

    # Add the condition flag column
    updated_df = joined_df.withColumn(
        condition_name,
        when(col(condition_name).isNotNull(), 1).otherwise(0)
    )

    # Drop the medcodeid column
    updated_df = updated_df.drop("medcodeid")

    # Rename the obsdate column
    updated_df = updated_df.withColumnRenamed("obsdate", f"obsdate_{condition_name}")

    return updated_df
