# Databricks notebook source
# MAGIC %md
# MAGIC # Initial Data Processing  
# MAGIC   
# MAGIC The purpose of this notebook is to read in the individual list files (e.g. all of the different `DrugIssue` files), merge them into one super file and save that file as a parquet file  
# MAGIC   
# MAGIC The only thing you should need to change is the cell which specifies which list we are interested in./

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import pandas as pd

import xml.etree.ElementTree as ET

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("ReadMergeListFiles").getOrCreate()
dbutils = DBUtils(spark)

# COMMAND ----------

# MAGIC %run /Workspace/Users/griffin.farrow@mhra.gov.uk/CPRD_Statin/src/add_outcome_information

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change List to the one that you are interested in

# COMMAND ----------

# overall path 

#########
# Change this part to the list that you are actually using!
#########
listnum = 'List3/'

path = 'dbfs:/mnt/data/cprd_statin_data/' + listnum

# COMMAND ----------

# read the log schema

# this is useful for determining if we have the correct number of records

def parse_log_xml_to_dataframe(path):
    # Parse the XML
    tree = ET.parse(path)
    root = tree.getroot()
    
    # List to hold parsed data
    data = []
    
    # Extract data from each section
    for section in root.findall(".//section"):
        section_title = section.find("title").text
        
        for item in section.findall("item"):
            # Try to extract timestamp and text; some items may lack timestamp
            timestamp = item.find("timestamp").text if item.find("timestamp") is not None else None
            text = item.find("text").text if item.find("text") is not None else None
            
            # Append to data list as dictionary
            data.append({
                "section_title": section_title,
                "timestamp": timestamp,
                "text": text
            })

    # Convert list to DataFrame
    df = pd.DataFrame(data)
    
    return df

def get_records_from_log(log_df, request_string):
    # get the row counts for DrugIssue and Observation
    proc = log_df[log_df['section_title'] == request_string]
    # find the records retrieved row
    proc2 = proc[proc['text'].str.contains('records retrieved')]
    # get the number from the records retrieved row 
    proc2_split = proc2['text'].str.split('records retrieved').str[0]
    num_records = int(proc2_split.str.extract('(\d+)')[0].iloc[0])
    return num_records

# COMMAND ----------

# Usage
file_list = dbutils.fs.ls(path)
xml_files = [file.path for file in file_list if file.path.endswith(".xml")]

# Check if there’s exactly one XML file
if len(xml_files) == 1:
    log_xml_path = xml_files[0]
    print(f"XML file found: {log_xml_path}")
else:
    print("Error: No XML file found or multiple XML files found in the directory.")

log_xml_path_split = log_xml_path.split(':')[-1]
log_xml_path_clean = '/dbfs/'+log_xml_path_split
log_df = parse_log_xml_to_dataframe(log_xml_path_clean)

# Display the DataFrame
print(log_df)


# COMMAND ----------

num_drug_issue = get_records_from_log(log_df, 'DrugIssue data request')
num_obs = get_records_from_log(log_df, 'Observation data request')
num_patients = get_records_from_log(log_df, 'Patient data request')
num_practices = get_records_from_log(log_df, 'Practice data request')

# COMMAND ----------

num_drug_issue

# COMMAND ----------

# MAGIC %md
# MAGIC ## DrugIssue  
# MAGIC   
# MAGIC First, deal with the `DrugIssue` files

# COMMAND ----------

#make sure the files are unzipped
drug_issue_merge = merger(path, 'DrugIssue') # sets up the file paths and checks there are actually data files for us to work with 

# COMMAND ----------

drug_issue_merge.read_schema() # reads the .xml schema file from the directory

# COMMAND ----------

drug_issue_merge.read_all_files() # reads all .txt files and merges into a single dataframe

# COMMAND ----------

display(drug_issue_merge.df) # displays the merged data frame. Does it look sensible? Are the columns as expected?

# COMMAND ----------

drug_issue_merge.check_count(num_drug_issue) # counts the rows of the dataframe and checks that is corresponds to the expected number

# COMMAND ----------

drug_issue_merge.save_data(True) # if all else is fine, saves the data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observation Data
# MAGIC Perform the same merging operation using the Observation data

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/data/cprd_statin_data/List3/")


# COMMAND ----------

obs_merge = merger(path, 'Observation') # sets up the file paths and checks there are actually data files for us to work with 

# COMMAND ----------

obs_merge.read_schema() # reads the .xml schema file from the directory

# COMMAND ----------

obs_merge.read_all_files() # reads all .txt files and merges into a single dataframe

# COMMAND ----------

display(obs_merge.df) # displays the merged data frame. Does it look sensible? Are the columns as expected?

# COMMAND ----------

obs_merge.check_count(num_obs) # counts the rows of the dataframe and checks that is corresponds to the expected number

# COMMAND ----------

obs_merge.save_data(True) # if all else is fine, saves the data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patient List 

# COMMAND ----------

patient_merge = merger(path, 'Patient') # sets up the file paths and checks there are actually data files for us to work with 

# COMMAND ----------

patient_merge.read_schema() # reads the .xml schema file from the directory

# COMMAND ----------

patient_merge.read_all_files() # reads all .txt files and merges into a single dataframe

# COMMAND ----------

display(patient_merge.df) # displays the merged data frame. Does it look sensible? Are the columns as expected?

# COMMAND ----------

patient_merge.check_count(num_patients) # counts the rows of the dataframe and checks that is corresponds to the expected number

# COMMAND ----------

patient_merge.save_data(True) # if all else is fine, saves the data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice List

# COMMAND ----------

practice_merge = merger(path, 'Practice') # sets up the file paths and checks there are actually data files for us to work with 

# COMMAND ----------

practice_merge.read_schema() # reads the .xml schema file from the directory

# COMMAND ----------

practice_merge.read_all_files() # reads all .txt files and merges into a single dataframe

# COMMAND ----------

display(practice_merge.df) # displays the merged data frame. Does it look sensible? Are the columns as expected?

# COMMAND ----------

practice_merge.check_count(num_practices) # counts the rows of the dataframe and checks that is corresponds to the expected number

# COMMAND ----------

practice_merge.save_data(True) # if all else is fine, saves the data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Double check that parquet files look sensible
# MAGIC - Almost definitely unnecessary
# MAGIC - Read back in Parquet files
# MAGIC - Check that the number of rows matches the expected number from the data spec
# MAGIC - Check that the table looks sensible
# MAGIC - Check that the first few rows of the actual data look similar (in structure) 

# COMMAND ----------

# read DrugIssue parquet
df_di = spark.read.parquet(path +'DrugIssue/DrugIssue.parquet')

# read Obs parquet 
df_obs = spark.read.parquet(path +'Observation/Observation.parquet')

# read patient parquet 
df_patient = spark.read.parquet(path +'Patients/Patients.parquet')

# read practice parquet 
df_practice = spark.read.parquet(path +'Practice/Practice.parquet')


# COMMAND ----------

# row checks
di_count = df_di.count()
obs_count = df_obs.count()
patient_count = df_patient.count()
practice_count = df_practice.count()

print(f"Expected {num_drug_issue}, found {di_count} drugs")
print(f"Expected {num_obs}, found {obs_count} observations")
print(f"Expected {num_patients}, found {patient_count} patients")
print(f"Expected {num_practices}, found {practice_count} practices")

# COMMAND ----------

# checking that the data looks sensible
display(df_di)

# COMMAND ----------

# read first line of df_patient file to see how this looks 
test_path = (drug_issue_merge.files[1].path).split(':')[1]
test_path2 = '/dbfs'+test_path

with open(test_path2, 'r') as file:
   line = file.readline()
   line2 = file.readline()
print(line) 
print(line2)

a = line2.split()

print(a)

# COMMAND ----------

df_di_filt = df_di.filter(((df_di.patid == a[0]) & (df_di.pracid == a[1]) & (df_di.drugrecid == a[2]) & (df_di.prodcodeid == a[5]) & (df_di.issuedate == a[3])))
display(df_di_filt)

# does this value match the first line of the data up above?

# COMMAND ----------

display(df_obs)

# COMMAND ----------

# read first line of df_patient file to see how this looks 
test_path = (obs_merge.files[1].path).split(':')[1]
test_path2 = '/dbfs'+test_path

with open(test_path2, 'r') as file:
   line = file.readline()
   line2 = file.readline()
print(line) 
print(line2)

a = line2.split()

print(a)

# COMMAND ----------

df_obs_filt = df_obs.filter(((df_obs.patid == a[0]) & (df_obs.pracid == a[1]) & (df_obs.obsdate == a[3]) & (df_obs.obsid == a[2])))
display(df_obs_filt)

# COMMAND ----------

# read first line of df_patient file to see how this looks 
test_path = (patient_merge.files[0].path).split(':')[1]
test_path2 = '/dbfs'+test_path

with open(test_path2, 'r') as file:
   line = file.readline()
   line2 = file.readline()
print(line) 
print(line2)

a = line2.split()

# COMMAND ----------

df_patient_filt = df_patient.filter(df_patient.patid == a[0])
display(df_patient_filt)

# COMMAND ----------

display(df_practice)

# COMMAND ----------

# read first line of df_patient file to see how this looks 
test_path = (practice_merge.files[0].path).split(':')[1]
test_path2 = '/dbfs'+test_path

with open(test_path2, 'r') as file:
   line = file.readline()
   line2 = file.readline()
print(line) 
print(line2)

a = line2.split()
print(a)

# COMMAND ----------

df_practice_filtered = df_practice.filter(df_practice.pracid == a[0])
display(df_practice_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC If you are happy with all this, then you can finally delete the .txt files  
# MAGIC Probably keep a version on your Virtual Machine if you need to upload them later