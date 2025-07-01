# Databricks notebook source
import pandas as pd 
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

def cklevel_filter(obs_df, patient_df, path_to_ck_codelist='/dbfs/mnt/data/cprd_statin_data/Codelists/cklevel.txt'):

    """
    Function to filter the observation DataFrame to include only CK level codes and join it with patient gender information.

    Args:
    - obs_df (SparkDataFrame): The DataFrame containing the observation data.
    - patient_df (SparkDataFrame): The DataFrame containing the patient data.
    - path_to_ck_codelist (str, optional): The file path to the list of CK level codes. Default is '/dbfs/mnt/data/cprd_statin_data/Codelists/cklevel.txt'.

    Returns:
    - SparkDataFrame: The filtered observation DataFrame containing only CK-level records, with gender information added.

    Example:
    filtered_df = cklevel_filter(obs_df, patient_df)

    Detailed Steps:
    1. Read CK Codes: The function reads a list of CK codes from a specified file path.
    2. Broadcast CK Codes: The CK codes are broadcasted to optimize the join operation.
    3. Alias DataFrames: Aliases are created for the observation and CK DataFrames to avoid column name ambiguity.
    4. Filter Observations: The observation DataFrame is filtered to include only records with CK-level codes.
    5. Select Relevant Columns: Extra columns are removed, retaining only the original observation data.
    6. Join with Patient Data: The filtered observation data is joined with the patient DataFrame to add gender information.
    """
    
    # Read in the list of creatine kinase codes
    ck_list = pd.read_csv(path_to_ck_codelist, sep='\t', header=0, dtype=str)
    ck_df = spark.createDataFrame(ck_list)
    ck_df_bc = F.broadcast(ck_df)

    # Alias the dataframes to avoid ambiguity (otherwise multiple columns may have the same name)
    obs_df_alias = obs_df.alias("obs")
    ck_df_alias = ck_df_bc.alias("ck")

    # Filter the observation dataframe to only include CK-level records
    obs_df_filt = obs_df_alias.join(
        ck_df_alias,
        obs_df_alias.medcodeid == ck_df_alias.MedCodeId,
        how='inner'
    )

    # Remove extra columns and retain only the original observation data
    obs_df_filt = obs_df_filt.select([f"obs.{col}" for col in obs_df.columns])

    # Add gender information to the CK-level observation data by joining with the patient dataframe
    obs_df_filt = obs_df_filt.join(patient_df.select('patid', 'gender'), on='patid', how='left')

    return obs_df_filt


# COMMAND ----------

def filter_by_acceptable_units(obs_df_filt, acceptable_numunitid):

    """
    Filters the observation DataFrame to retain rows with acceptable units.
    
    Args:
    - obs_df_filt (SparkDataFrame): The DataFrame containing the filtered observation data.
    - acceptable_numunitid (List[int]): List of numunitid codes with correct units.

    Returns:
    - SparkDataFrame: Filtered DataFrame containing rows with acceptable units.

    Example:
    filtered_df = filter_by_acceptable_units(obs_df_filt, acceptable_numunitid)

    Detailed Steps:
    1. Filter Rows: The function filters the observation DataFrame to retain only rows with acceptable numunitid values.
    2. Mark Unacceptable Rows: Rows in the original DataFrame that are not in the filtered DataFrame are marked with CK_level = 3. This basically means "undefined". The patient has a measurement for CK level, but we are unsure what the measurement means, so we should probably not use that patient's record in the final study
    """

    # Filter rows with acceptable numunitid values
    obs_df_temp = obs_df_filt.filter(obs_df_filt['numunitid'].isin(acceptable_numunitid))
    
    # Mark rows in obs_df_filt that are not in obs_df_temp with CK_level = 3
    obs_df_final = obs_df_filt.join(
        obs_df_temp.select('obsid').withColumn('in_temp', F.lit(True)),
        on='obsid',
        how='left'
    ).withColumn(
        'CK_level',
        F.when(F.col('in_temp').isNull(), 3).otherwise(None)
    ).drop('in_temp')

    return obs_df_final

# COMMAND ----------

def add_CK_code(obs_df_filt):

    """
    Takes in an observation DataFrame and adds a column with "myopathy_indication" which is:
    - For males (gender == 1):
      - 0 if CKLevel <= 800
      - 1 if CKLevel > 800 and <= 2000
      - 2 if CKLevel > 2000
    - For females (gender == 2):
      - 0 if CKLevel <= 400
      - 1 if CKLevel > 400 and <= 1000
      - 2 if CKLevel > 1000
    - None if the value column is Null or gender is missing.

    Args:
    - obs_df_filt (SparkDataFrame): The DataFrame containing the observation data.

    Returns:
    - SparkDataFrame: The DataFrame with a new column "CK_level".

    Example:
    updated_df = add_CK_code(obs_df_filt)
    """

    # Add a "CK_level" column based on gender-specific thresholds if CK_level is not already set
    obs_df_filt = obs_df_filt.withColumn(
        "CK_level",
        F.when(
            F.col("CK_level").isNull(),
            F.when(
                F.col("gender") == 1,  # Male thresholds ULN is 320 IU/L
                F.when(F.col("value") <= 1280.0, 0)  # No myopathy
                .when(((F.col("value") > 1280.0) & (F.col("value") <= 3200.0)), 1)  # Myopathy
                .when(F.col("value") > 3200.0, 2)  # Severe myopathy
                .otherwise(None)
            ).when(
                F.col("gender") == 2,  # Female thresholds ULN is 200 IU/L
                F.when(F.col("value") <= 800.0, 0)  # No myopathy
                .when(((F.col("value") > 800.0) & (F.col("value") <= 2000.0)), 1)  # Myopathy (4 x ULN)
                .when(F.col("value") > 2000.0, 2)  # Severe myopathy (10 x ULN)
                .otherwise(None)
            ).otherwise(None)  # No value or missing gender
        ).otherwise(F.col("CK_level"))
    )

    return obs_df_filt

# COMMAND ----------

def add_outcome_column(pat_df, obs_df, 
                       path_to_ck_codelist='/dbfs/mnt/data/cprd_statin_data/Codelists/cklevel.txt', 
                       path_to_myopathy_codelist='/dbfs/mnt/data/cprd_statin_data/Codelists/outcome_myopathy.txt',
                       acceptable_num_unit_codes=[276, 154, 49, 852, 1307, 151, 2698, 999, 153, 13350, 4548],
                       convert_from_per_ml_unit_codes=[155, 277],
                       explore_codes=False,
                       path_to_cprd_numunit_lookup='/dbfs/mnt/data/cprd_statin_data/202409_Lookups_CPRDAurum/NumUnit.txt'):
    
    """
    Adds an outcome column to the patient DataFrame based on CK levels and myopathy diagnosis.

    Args:
    - pat_df (SparkDataFrame): The DataFrame containing the patient data.
    - obs_df (SparkDataFrame): The DataFrame containing the observation data.
    - path_to_ck_codelist (str, optional): The file path to the list of CK level codes. Default is '/dbfs/mnt/data/cprd_statin_data/Codelists/cklevel.txt'.
    - path_to_myopathy_codelist (str, optional): The file path to the list of myopathy codes. Default is '/dbfs/mnt/data/cprd_statin_data/Codelists/outcome_myopathy.txt'.
    - acceptable_num_unit_codes (List[int], optional): List of acceptable numunitid codes. Default is [276, 154, 49, 852, 1307, 151, 2698, 999, 153, 13350, 4548].
    - convert_from_per_ml_unit_codes (List[int], optional): List of numunitid codes to convert from IU/ml to IU/L. Default is [155, 277].
    - explore_codes (bool, optional): Flag to explore unit codes. Default is False.
    - path_to_cprd_numunit_lookup (str, optional): The file path to the CPRD numunit lookup. Default is '/dbfs/mnt/data/cprd_statin_data/202409_Lookups_CPRDAurum/NumUnit.txt'.

    Returns:
    - SparkDataFrame: The patient DataFrame with an added outcome column.

    Example:
    pat_df_with_outcome = add_outcome_column(pat_df, obs_df)

    Detailed Steps:
    1. Handle CK Level Codes:
       - Filter the observation DataFrame to include only CK level codes and add patient gender information.
       - Optionally explore unit codes if `explore_codes` is True.
       - Convert units from IU/ml to IU/L for specified codes.
       - Filter observations to retain rows with acceptable units.
       - Add CK level codes based on gender-specific thresholds.
       - Rename CK level column to myopathy code.
    2. Filter for Myopathy Diagnosis:
       - Read myopathy and rhabdomyolysis codes.
       - Filter observations for myopathy and rhabdomyolysis diagnoses.
       - Add myopathy outcome codes to the filtered observations.
    3. Combine Outcomes:
       - Combine CK level, myopathy, and rhabdomyolysis observations.
       - Convert date columns and determine the outcome date.
       - Filter to keep the earliest outcome for each patient.
    4. Add Outcome to Patient Data:
       - Join the earliest outcome data with the patient DataFrame.
       - Add an outcome column indicating the presence of myopathy.
    """
    
    ### 1) first, we handle the CK Level codes
    
    # filter the obs_df to only contain CK level codes (and add patient gender column from patient list)
    obs_df_ck = cklevel_filter(obs_df, pat_df)

    ##################################################################################################################################
    # what are the different codes that are used for the unit IDs? This will only run if explore_codes=True 
    if (explore_codes):

        # need to extract what the different values and numunitids actually are 
        numunitid_counts = obs_df_ck.groupBy('numunitid').count()

        # read lookup data
        unit_lookup = pd.read_csv(path_to_cprd_numunit_lookup, sep='\t', header=0)
        unit_df = spark.createDataFrame(unit_lookup)

        numunitid_counts = numunitid_counts.join(unit_df, on='numunitid', how='left')
        numunitid_counts = numunitid_counts.orderBy('count', ascending=False)
        
        # if explore_codes is used, then this will return the dataframe of all the different unit codes so that it can be checked
        return numunitid_counts
    ##################################################################################################################################

    # if we are not exploring codes, then we will work with the list of codes that we have already been given 
    if (len(acceptable_num_unit_codes) == 0):
        raise Exception('Acceptable codes cannot be empty!')

    if (len(convert_from_per_ml_unit_codes) != 0): 
        # convert the codes that are given in IU/ml
        obs_df_ck = obs_df_ck.withColumn('value', 
                                            F.when(obs_df_ck['numunitid'].isin(convert_from_per_ml_unit_codes), obs_df_ck['value'].cast('float') * 1000) # when the value in the numunitid column is in the list of codes to convert, multiply the value by 1000 (iu/ml --> iu/L)
                                            .otherwise(obs_df_ck['value'].cast('float')) 
                                            )
        obs_df_ck = obs_df_ck.withColumn('numunitid', 
                                            F.when(obs_df_ck['numunitid'].isin(convert_from_per_ml_unit_codes), 276) # change the value in the numunitid column 276 after this transformation (now an acceptable code)
                                            .otherwise(obs_df_ck['numunitid'])
                                            )
        
    # keep only the rows with acceptable units. For the rows with unacceptable units, we give them CK_level=3 (undefined)
    obs_df_ck_final = filter_by_acceptable_units(obs_df_ck, acceptable_num_unit_codes)
    
    # for each row, add the appropriate CK_level code (0 = no myopathy based on CK level, 1 = myopathy based on CK level, 2=severe myopathy based on CK level, 3=CK level units undefined)
    obs_df_ck_labelled = add_CK_code(obs_df_ck_final)
    
    # rename CK level to myopathy code so we can merge with later dataframe 
    obs_df_ck_labelled = obs_df_ck_labelled.withColumnRenamed('CK_level', 'myopathy_code')

    del obs_df_ck, obs_df_ck_final # delete intermediary arrays for memory management

    ##############################################################################################################################################
    ### 2) Now we filter based on whether there is a diagnosis of myopathy/severe myopathy for the patient 

    myopathy_codelist = pd.read_csv(path_to_myopathy_codelist, sep='\t', header=0, dtype=str) 

    # get codes for rhabdomyolysis
    rhabdo_codelist = myopathy_codelist[myopathy_codelist['Term'].str.lower().str.contains('rhabdo')]
    rhabdo_df = spark.createDataFrame(rhabdo_codelist)
    rhabdo_df = rhabdo_df.withColumnRenamed('MedCodeId', 'rhabdo_medcodeid')
    rhabdo_df_bc = F.broadcast(rhabdo_df)

    # get codes for myopathy
    myo_codelist = myopathy_codelist[~myopathy_codelist['MedCodeId'].isin(rhabdo_codelist['MedCodeId'])]
    myo_df = spark.createDataFrame(myo_codelist)
    myo_df = myo_df.withColumnRenamed('MedCodeId', 'myo_medcodeid')
    myo_df_bc = F.broadcast(myo_df)

    # filter out observation from obs_df that are not in the rhabdo codelist
    obs_df_rhabdo = obs_df.join(rhabdo_df_bc.select('rhabdo_medcodeid'), rhabdo_df_bc['rhabdo_medcodeid']==obs_df['medcodeid'], how='inner').drop('rhabdo_medcodeid')

    # add a myopathy outcome column to obs_df_rhabdo 
    obs_df_rhabdo = obs_df_rhabdo.withColumn('myopathy_code', F.lit(2))

    # do the same for the myopathy codelist
    obs_df_myo = obs_df.join(myo_df_bc.select('myo_medcodeid'), myo_df_bc['myo_medcodeid']==obs_df['medcodeid'], how='inner').drop('myo_medcodeid')

    # add a myopathy outcome column to obs_df_myo 
    obs_df_myo = obs_df_myo.withColumn('myopathy_code', F.lit(1))

    del rhabdo_df, rhabdo_df_bc, myo_df, myo_df_bc

    ###################################################################################################################################################
    ### 3) Accounting for either outcome

    # rearranging columns of the different dataframes so they are consistent
    obs_df_ck_labelled = obs_df_ck_labelled.select('obsid', 'patid', 'pracid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid', 'myopathy_code')
    obs_df_rhabdo = obs_df_rhabdo.select('obsid', 'patid', 'pracid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid', 'myopathy_code')
    obs_df_myo = obs_df_myo.select('obsid', 'patid', 'pracid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid', 'myopathy_code')

    # combine all the different relevant observations
    outcome_df = obs_df_myo.union(obs_df_rhabdo).union(obs_df_ck_labelled)

    # Convert 'obsdate' and 'enterdate' to date objects
    outcome_df = outcome_df.withColumn('obsdate', F.to_date('obsdate', 'dd/MM/yyyy'))
    outcome_df = outcome_df.withColumn('enterdate', F.to_date('enterdate', 'dd/MM/yyyy'))

    # significant number of rows (practice dependent) where there is no obsdate but there is an enterdate. Let's use the obsdate, but if it is null, then use the enterdate
    outcome_df = outcome_df.withColumn('outcome_date', F.coalesce('obsdate', 'enterdate'))

    
    ## filter this outcome_df to only include the first record for each patient of an outcome that is > 0 (e.g. myopathy, severe myopathy or a CK level measurement that we are unsure about)

    # Define a window specification to partition by 'patid' and order by 'obsdate' and 'enterdate'
    window_spec = Window.partitionBy('patid').orderBy('outcome_date')

    # Add a row number to each row within the partition
    outcome_df = outcome_df.withColumn('row_num', F.row_number().over(window_spec))

    # Filter to keep only the first row for each 'patid' with myopathy_code > 0
    earliest_outcome_df = outcome_df.filter((F.col('row_num') == 1) & (F.col('myopathy_code') > 0)).drop('row_num')

    del obs_df_ck_labelled, obs_df_rhabdo, obs_df_myo, outcome_df

    #########################################################################################################################################################
    ### 4) use the earliest_outcome_df to add the outcome for each patient to our patient list

    # uses the myopathy_code if the patient appears in earliest_outcome_df
    # if the patient does not, we assume they have no outcome
    # also merges on the obsdate if the patient appears in that list, or the enterdate if obsdate is null
    # if outcome_date is null, then the patient did not experience the outcome

    pat_df_with_out = pat_df.join(
        earliest_outcome_df.select('patid', 'myopathy_code', 'outcome_date'),
        on = 'patid',
        how = 'left').withColumn('outcome', F.coalesce(F.col('myopathy_code'), F.lit(0))).drop('myopathy_code')

    return pat_df_with_out
