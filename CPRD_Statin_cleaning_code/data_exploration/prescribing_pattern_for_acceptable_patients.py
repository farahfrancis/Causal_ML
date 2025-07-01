# Databricks notebook source
# MAGIC %md
# MAGIC The purpose of this notebook is to filter the patients who have enough baseline data for us to use them  
# MAGIC - For each patient in the patient list, we want to extract their first ever statin prescription date  
# MAGIC - If that date is not *at least* a year after their registration, then we need to filter out those patients

# COMMAND ----------

import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StructType, StructField, DateType, StringType

import matplotlib.pyplot as plt 


spark = SparkSession.builder.appName("Custom Baselining").getOrCreate()

# COMMAND ----------

# read in statins codelist
codelist_path = '/mnt/data/cprd_statin_data/Codelists/statins.txt'
statin_df = spark.read.csv(codelist_path, header=True, sep='\t')
statin_df = statin_df.drop('DMDCode', 'TermfromEMIS', 'formulation', 'routeofadministration', 'bnfcode', 'DrugIssues')
broadcasted_statin_codelist = F.broadcast(statin_df)

# COMMAND ----------

di_df4 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List4/DrugIssue/DrugIssue.parquet')
patient_df4 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List4/Patient/Patient.parquet')

# COMMAND ----------

di_df5 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List5/DrugIssue/DrugIssue.parquet')
patient_df5 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List5/Patient/Patient.parquet')

# COMMAND ----------

di_df6 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List6/DrugIssue/DrugIssue.parquet')
patient_df6 = spark.read.format('parquet').load('dbfs:/mnt/data/cprd_statin_data/List6/Patient/Patient.parquet')

# COMMAND ----------

di_df7 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List7/DrugIssue/data/')
patient_df7 = spark.read.format('delta').load('dbfs:/mnt/data/cprd_statin_data/List7/Patient/data/')

# COMMAND ----------

# merge all dataframes
di_df = di_df4.union(di_df5).union(di_df6).union(di_df7)
patient_df = patient_df4.union(patient_df5).union(patient_df6).union(patient_df7)

# COMMAND ----------

# filter drug issue dataframe to have only statin relevant codes
di_df_filt = di_df.join(broadcasted_statin_codelist, di_df.prodcodeid == broadcasted_statin_codelist.ProdCodeId, how='inner')

# convert issuedate to date object
di_df_filt = di_df_filt.withColumn("issuedate", F.to_date(F.col("issuedate"), "dd/MM/yyyy"))

# COMMAND ----------

# filter list to only get one statin prescription per patient
# add issuedate to each patient record as their first statin prescription 

# Use a window specification to partition the date by 'patid' and order by 'issuedate'
window_spec = Window.partitionBy('patid').orderBy('issuedate')

# add row number based on window spec
di_df_final = di_df_filt.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

# add a column to patient_id dataframe based on what the earliest date is 
pat_only_one = patient_df.join(
    di_df_final.select("patid", "issuedate").alias("earliest_date"),
    on='patid',
    how='left'
)

# COMMAND ----------

# filter out all patients whose first statin prescription is before January 1st 2010
pat_suitable = pat_only_one.filter(F.col("issuedate") >= "2010-01-01")

# COMMAND ----------

# convert registration date to correct format
pat_suitable = pat_suitable.withColumn("regstartdate", F.to_date(F.col("regstartdate"), "dd/MM/yyyy"))

# COMMAND ----------

# add a column with difference between registration date and issuedate
pat_suitable = pat_suitable.withColumn(
                        'days_between', 
                        F.datediff(F.col('issuedate'), F.col('regstartdate'))
                    )

# COMMAND ----------

# filter out all patients whose first statin prescription is not at least a year after their registration date 
pat_suitable = pat_suitable.filter(F.col("days_between") >= 365)

# COMMAND ----------

# filter out all patients without an acceptable patient flag 
pat_suitable = pat_suitable.filter(F.col('acceptable') == "1")

# COMMAND ----------

## how many patients are acceptable in the whole patient list
patient_total = patient_df.count()
patient_suitable = pat_suitable.count()

print(patient_suitable/patient_total)

# COMMAND ----------

di_df_final = di_df_filt.join(pat_suitable.select('patid'), on='patid', how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC ## what is the prescribing patterns of these patients 

# COMMAND ----------

# for each patid, identify the first statin that they were prescribed 
di_df_first_statin = di_df_final.withColumn("row_num", F.row_number().over(Window.partitionBy("patid").orderBy("issuedate"))).filter(F.col("row_num") == 1).drop("row_num")

# COMMAND ----------

df_first_statin_counts = di_df_first_statin.groupBy('drugsubstancename').agg(F.count('drugsubstancename').alias('count'))
df_first_statin_counts_df = df_first_statin_counts.toPandas()

# COMMAND ----------

df_first_statin_counts_df['drugsubstancename'] = df_first_statin_counts_df['drugsubstancename'].apply(lambda x: x.split()[0])
df_first_statin_counts_df = df_first_statin_counts_df.sort_values('count')

# COMMAND ----------

total = df_first_statin_counts_df['count'].sum() # total number of counts

# COMMAND ----------

fig, ax = plt.subplots()
bars = ax.bar(df_first_statin_counts_df['drugsubstancename'], 100*df_first_statin_counts_df['count']/total)
ax.tick_params(axis='x', rotation=90)
ax.set_ylabel(r"% of total counts")
ax.set_title(r"Fraction of patients who were prescribed this as their first statin")
ax.set_ylim(0, 65)

for bar in bars:
    height = bar.get_height()
    ax.annotate(f'{height:.2f}%', xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3), textcoords="offset points", ha='center', va='bottom')
    

# COMMAND ----------

window_spec = Window.partitionBy("patid", "drugsubstancename").orderBy("issuedate")

all_unique_statins_df = di_df_final.withColumn("row_number", F.row_number().over(window_spec)) \
                   .filter(F.col("row_number") == 1) \
                   .drop("row_number") # dataframe showing all unique statin prescriptions for each patid (taking the first appearance of each of these unique statins as well)

# COMMAND ----------

unique_values_df = all_unique_statins_df.groupBy("patid").agg(F.countDistinct("drugsubstancename").alias("number_unique_values")) # number of unique drugsubstancenames 

# COMMAND ----------

# count how often each value appears
df_counts = unique_values_df.groupBy('number_unique_values').count()
df_counts_pd = df_counts.toPandas().sort_values(by='count')
total = df_counts_pd['count'].sum()

# COMMAND ----------

fig, ax = plt.subplots()
bars = ax.bar(df_counts_pd['number_unique_values'], 100*df_counts_pd['count']/total)
ax.set_ylabel(r"% of total counts")
ax.set_title(r"% of the patient list who has been prescribed this number of statins")
ax.set_ylim(0, 80)

for bar in bars:
    height = bar.get_height()
    ax.annotate(f'{height:.2f}%', xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3), textcoords="offset points", ha='center', va='bottom')


# COMMAND ----------

# Define the window specification
windowSpec = Window.partitionBy('patid').orderBy('issuedate')

# Group by 'patid' and collect list of tuples
result_df = di_df_final.groupBy('patid').agg(
    F.collect_list(F.struct('issuedate', 'drugsubstancename')).alias('drug_list')
)

# COMMAND ----------

# Define the function to process 'drug_list'
def filter_drug_list(drug_list):
    if not drug_list:
        return []
    drug_list = sorted(drug_list, key=lambda x: x['issuedate'])

    filtered_list = [drug_list[0]]  # Always keep the first tuple
    
    for i in range(1, len(drug_list) - 1):
        if drug_list[i]['drugsubstancename'] != drug_list[i - 1]['drugsubstancename']:
            filtered_list.append(drug_list[i])
    
    if len(drug_list) > 1:
        filtered_list.append(drug_list[-1])  # Always keep the last tuple
    
    return filtered_list

# COMMAND ----------

# Register the function as a UDF
schema = ArrayType(StructType([
    StructField("issuedate", DateType(), True),
    StructField("drugsubstancename", StringType(), True)
]))

# COMMAND ----------

filter_drug_list_udf = F.udf(filter_drug_list, schema)

# COMMAND ----------

# Apply the UDF to create the 'prescribing_pattern' column
result_df = result_df.withColumn('prescribing_pattern', filter_drug_list_udf(result_df['drug_list']))

# add another column which is the length of the filtered_drug_list
result_df = result_df.withColumn('len(prescribing_pattern)', F.size(result_df['prescribing_pattern']))

# COMMAND ----------

result_df = result_df.withColumn('drugsubstancename_list', F.expr("transform(prescribing_pattern, x -> x.drugsubstancename)"))

# COMMAND ----------

result_df = result_df.withColumn('drugsubstancename_list', F.expr("transform(drugsubstancename_list, x -> lower(substring(x, 1, 1)) )"))
result_df.show()

# COMMAND ----------

result_2 = result_df.groupBy('drugsubstancename_list').count()
result_2.show()

# COMMAND ----------

# convert this to pandas
drug_lists = result_2.toPandas()

# COMMAND ----------

drug_lists = drug_lists.sort_values(by='count', ascending=False)

# COMMAND ----------

drug_lists['list_length'] = drug_lists['drugsubstancename_list'].apply(len)

# COMMAND ----------

drug_lists.sort_values('list_length', ascending=False)

# COMMAND ----------

top_10_drug_counts = drug_lists.head(20).copy()
top_10_drug_counts.columns = ['Drug_Pattern', 'Count', 'list_length']
top_10_drug_counts.loc[:, 'Drug_Pattern'] = top_10_drug_counts.loc[:, 'Drug_Pattern'].apply(str)

plt.figure(figsize=(10, 6))
plt.bar(top_10_drug_counts['Drug_Pattern'], top_10_drug_counts['Count'], color='skyblue')
plt.xlabel('Drug Pattern')
plt.ylabel('Count')
plt.title('Top 20 Drug Patterns')
plt.xticks(rotation=90)
plt.show()