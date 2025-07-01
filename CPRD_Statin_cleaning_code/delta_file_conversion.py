# Databricks notebook source
# MAGIC %md
# MAGIC # Turn files into DELTA files
# MAGIC This notebook will take all the data and convert to Delta file type (and optimised); should expect this to run queries a bit quicker

# COMMAND ----------

# MAGIC %md
# MAGIC First, we need to convert our individual patient lists to delta files 
# MAGIC This will make the code run a lot quicker

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List1/DrugIssue/DrugIssue.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List1/Observation/Observation.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List1/Practice/Practice.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List1/Patients/Patients.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List2/DrugIssue/DrugIssue.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List2/Observation/Observation.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List2/Practice/Practice.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List2/Patient/Patient.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List3/DrugIssue/DrugIssue.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List3/Observation/Observation.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List3/Practice/Practice.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List3/Patient/Patient.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List4/DrugIssue/DrugIssue.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List4/Observation/Observation.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List4/Practice/Practice.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List4/Patient/Patient.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List5/DrugIssue/DrugIssue.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List5/Observation/Observation.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List5/Practice/Practice.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List5/Patient/Patient.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List6/DrugIssue/DrugIssue.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List6/Observation/Observation.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List6/Practice/Practice.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List6/Patient/Patient.parquet`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List7/DrugIssue/data/DrugIssue.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List7/Observation/data/Observation.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List7/Practice/data/Practice.parquet`;
# MAGIC CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd_statin_data/List7/Patient/data/Patient.parquet`;

# COMMAND ----------

# MAGIC %md
# MAGIC Now optimise the different Delta files

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List1/DrugIssue/DrugIssue.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List1/Observation/Observation.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List1/Practice/Practice.parquet`;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List1/Patients/Patients.parquet` ZORDER BY patid;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List2/DrugIssue/DrugIssue.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List2/Observation/Observation.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List2/Practice/Practice.parquet`;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List2/Patient/Patient.parquet` ZORDER BY patid;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List3/DrugIssue/DrugIssue.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List3/Observation/Observation.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List3/Practice/Practice.parquet`;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List3/Patient/Patient.parquet` ZORDER BY patid;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List4/DrugIssue/DrugIssue.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List4/Observation/Observation.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List4/Practice/Practice.parquet`;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List4/Patient/Patient.parquet` ZORDER BY patid;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List5/DrugIssue/DrugIssue.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List5/Observation/Observation.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List5/Practice/Practice.parquet`;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List5/Patient/Patient.parquet` ZORDER BY patid;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List6/DrugIssue/DrugIssue.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List6/Observation/Observation.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List6/Practice/Practice.parquet`;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List6/Patient/Patient.parquet` ZORDER BY patid;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List7/DrugIssue/data/DrugIssue.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List7/Observation/data/Observation.parquet` ZORDER BY patid;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List7/Practice/data/Practice.parquet`;
# MAGIC OPTIMIZE delta.`dbfs:/mnt/data/cprd_statin_data/List7/Patient/data/Patient.parquet` ZORDER BY patid;