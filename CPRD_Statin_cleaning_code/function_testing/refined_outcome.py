# Databricks notebook source
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Outcome Check").getOrCreate()

# COMMAND ----------

# MAGIC %run /Workspace/Users/griffin.farrow@mhra.gov.uk/CPRD_Statin/src/add_outcome_information

# COMMAND ----------

# read in observation files 
obs_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Observation/Observation.parquet/')

# read in patient list 
pat_df = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/clean_final_datasets/Patient/Patient.parquet/')

# COMMAND ----------

# MAGIC %md
# MAGIC Rhabdomyolysis:
# MAGIC - either a code for rhabdomyolysis 
# MAGIC - or a CK level 10x normal AND rhabdomyolysis within 30 days of it (but this would be covered by looking for the rhabdomyolysis code)
# MAGIC   
# MAGIC Myopathy (not severe):
# MAGIC - 

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Manually carrying out every step in the filtering for outcome process

# COMMAND ----------

# MAGIC %md
# MAGIC ### CKLevel Checking
# MAGIC The next few cells check the CKLevel and then add a column to the Observation dataframe which includes this information

# COMMAND ----------

obs_df_ck = cklevel_filter(obs_df, pat_df)

# COMMAND ----------

# need to extract what the different values and numunitids actually are 
numunitid_counts = obs_df_ck.groupBy('numunitid').count()

# read lookup data
unit_lookup = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/202409_Lookups_CPRDAurum/NumUnit.txt', sep='\t', header=0)
unit_df = spark.createDataFrame(unit_lookup)

numunitid_counts = numunitid_counts.join(unit_df, on='numunitid', how='left')
numunitid_counts = numunitid_counts.orderBy('count', ascending=False)


# COMMAND ----------

 # take all the iu/L codes as is
acceptable_codes = [276, 154, 49, 852, 1307, 151, 2698, 999, 153, 13350, 4548] # these all define iu/L definitely

# needs to be converted
convert_code = [155, 277] # iu/ML, so values should be multiplied by 1000

# uncertainty (these are "Unknown" or the units are possibly correct, but we can't tell)- maybe drop them. Better be safe
uncertain_codes = [153, 553, 405, 1155, 923]

# all other codes are in units that are hard to convert

# convert those rows whose units we know
obs_df_ck = obs_df_ck.withColumn('value', 
                                     F.when(obs_df_ck['numunitid'].isin(convert_code), obs_df_ck['value'].cast('float') * 1000) # when the value in the numunitid column is in convert_code, multiply the value by 1000 (iu/ml --> iu/L)
                                     .otherwise(obs_df_ck['value'].cast('float')) 
                                    )
obs_df_ck = obs_df_ck.withColumn('numunitid', 
                                      F.when(obs_df_ck['numunitid'].isin(convert_code), 276) # change the value in the numunitid column from 155 to 276 after this transformation
                                      .otherwise(obs_df_ck['numunitid'])
                                    )

# COMMAND ----------

# convert acceptable rows to add a ratio value to the column
obs_df_ck_final = filter_by_acceptable_units(obs_df_ck, acceptable_codes)

# COMMAND ----------

obs_df_ck_labelled = add_CK_code(obs_df_ck_final)

# COMMAND ----------

obs_df_ck_labelled = obs_df_ck_labelled.withColumnRenamed('CK_level', 'myopathy_code')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Myopathy Code Checking
# MAGIC These next few cells check for myopathy and severe myopathy by code (rather than CKLevel)

# COMMAND ----------

myopathy_codelist = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/outcome_myopathy.txt', sep='\t', header=0, dtype=str) 

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

# COMMAND ----------

rhabdo_codelist

# COMMAND ----------

myo_codelist

# COMMAND ----------

# filter out observation from obs_df that are not in the rhabdo codelist
obs_df_rhabdo = obs_df.join(rhabdo_df_bc.select('rhabdo_medcodeid'), rhabdo_df_bc['rhabdo_medcodeid']==obs_df['medcodeid'], how='inner').drop('rhabdo_medcodeid')

# add a myopathy outcome column to obs_df_rhabdo 
obs_df_rhabdo = obs_df_rhabdo.withColumn('myopathy_code', F.lit(2))

# COMMAND ----------

# do the same for the myopathy codelist
obs_df_myo = obs_df.join(myo_df_bc.select('myo_medcodeid'), myo_df_bc['myo_medcodeid']==obs_df['medcodeid'], how='inner').drop('myo_medcodeid')

# add a myopathy outcome column to obs_df_myo 
obs_df_myo = obs_df_myo.withColumn('myopathy_code', F.lit(1))

###the is no '0'

# COMMAND ----------

obs_df_ck_labelled = obs_df_ck_labelled.select('obsid', 'patid', 'pracid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid', 'myopathy_code')

# COMMAND ----------

# put columns in the same order as obs_df_ck_labelled
obs_df_rhabdo = obs_df_rhabdo.select('obsid', 'patid', 'pracid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid', 'myopathy_code')
obs_df_myo = obs_df_myo.select('obsid', 'patid', 'pracid', 'obsdate', 'enterdate', 'medcodeid', 'value', 'numunitid', 'myopathy_code')

# COMMAND ----------

# Combine myopathy and rhabdomyolysis observations
rhabdo_myo_df = obs_df_myo.union(obs_df_rhabdo)

# combine this with the CK level observations
outcome_df = rhabdo_myo_df.union(obs_df_ck_labelled)

# COMMAND ----------

# Convert 'obsdate' and 'enterdate' to date objects
outcome_df = outcome_df.withColumn('obsdate', F.to_date('obsdate', 'dd/MM/yyyy'))
outcome_df = outcome_df.withColumn('enterdate', F.to_date('enterdate', 'dd/MM/yyyy'))

# COMMAND ----------

outcome_df = outcome_df.withColumn('outcome_date', F.coalesce('obsdate', 'enterdate'))

# COMMAND ----------

# Define a window specification to partition by 'patid' and order by 'obsdate' and 'enterdate'
window_spec = Window.partitionBy('patid').orderBy('outcome_date')

# Add a row number to each row within the partition
outcome_df = outcome_df.withColumn('row_num', F.row_number().over(window_spec))

# Filter to keep only the first row for each 'patid' with myopathy_code > 0
earliest_outcome_df = outcome_df.filter((F.col('row_num') == 1) & (F.col('myopathy_code') > 0)).drop('row_num')

# COMMAND ----------

# form a final judgment of the different patient lists

# uses the myopathy_code if the patient appears in earliest_outcome_df
# if the patient does not, we assume they have no outcome
# also merges on the obsdate if the patient appears in that list, or the enterdate if obsdate is null
# if outcome_date is null, then the patient did not experience the outcome
pat_df_with_out = pat_df.join(
    earliest_outcome_df.select('patid', 'myopathy_code', 'outcome_date'),
    on = 'patid',
    how = 'left').withColumn('Outcome', F.coalesce(F.col('myopathy_code'), F.lit(0))).drop('myopathy_code')

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Compare to just using the function we've written

# COMMAND ----------

pat_outcome = add_outcome_column(pat_df, obs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3) Compare the output dataframes
# MAGIC

# COMMAND ----------

diff = pat_outcome.exceptAll(pat_df_with_out)
diff.show()

# if empty, this means the two are identical