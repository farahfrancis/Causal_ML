# Databricks notebook source
# MAGIC %md
# MAGIC The code sets up text input widgets in a Databricks notebook to allow user input for various parameters, including biological codes, medical codes, product codes, and baseline parameters.
# MAGIC
# MAGIC - biopath stores biological values (e.g., systolic blood pressure, height, weight).
# MAGIC - medpath retrieves discrete medical codes that indicate the presence or absence of conditions.
# MAGIC - prodpath retrieves discrete product codes indicating drug presence.
# MAGIC
# MAGIC An addon column method (part IV) is suggested to enhance performance by facilitating quicker processing of discrete-valued/categorical -valued medcodes, e.g. ethniity, as each patient can have different ethnicity values. 

# COMMAND ----------

dbutils.widgets.text("biomedcodeIds", "","")
biopath<- dbutils.widgets.get("biomedcodeIds") 
dbutils.widgets.text("list_medcodelist", "","")
medpath<- dbutils.widgets.get("list_medcodelist")
dbutils.widgets.text("list_prodcodelist", "","")
prodpath<- dbutils.widgets.get("list_prodcodelist")
dbutils.widgets.text("baseline", "","")
baselines<- dbutils.widgets.get("baseline") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### CPRD Aurum roll-up clinical dataset generation

# COMMAND ----------

library(SparkR)
sparkR.session()

# COMMAND ----------

# MAGIC %md
# MAGIC Function for deleting temp files during the process.

# COMMAND ----------

## logical switch to basically ensure this code is never run 
do_not_run = TRUE
if (do_not_run) {
  stop("This code block is not meant to be executed.")
}

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import scala.util.{Try, Success, Failure}
# MAGIC
# MAGIC def delete(p: String): Unit = {
# MAGIC   dbutils.fs.ls(p).map(_.path).toDF.foreach { file =>
# MAGIC     dbutils.fs.rm(file(0).toString, true)
# MAGIC     println(s"deleted file: $file")
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC final def walkDelete(root: String)(level: Int): Unit = {
# MAGIC   dbutils.fs.ls(root).map(_.path).foreach { p =>
# MAGIC     println(s"Deleting: $p, on level: ${level}")
# MAGIC     val deleting = Try {
# MAGIC       if(level == 0) delete(p)
# MAGIC       else if(p endsWith "/") walkDelete(p)(level-1)
# MAGIC       //
# MAGIC       // Set only n levels of recursion, so it won't be a problem
# MAGIC       //
# MAGIC       else delete(p)
# MAGIC     }
# MAGIC     deleting match {
# MAGIC       case Success(v) => {
# MAGIC         println(s"Successfully deleted $p")
# MAGIC         dbutils.fs.rm(p, true)
# MAGIC       }
# MAGIC       case Failure(e) => println(e.getMessage)
# MAGIC     }
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC The first step is to convert the original Parquet files, specifically the obs, drug, and patient tables. This code only needs to be run once. Comment them out once used.

# COMMAND ----------

# MAGIC %sql
# MAGIC --CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd/AurumRelease/Parquet/observation/`
# MAGIC --CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd/AurumRelease/Parquet/drugissue/`;
# MAGIC --CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd/AurumRelease/Parquet/patient/`;
# MAGIC --CONVERT TO DELTA parquet.`dbfs:/mnt/data/cprd/AurumRelease/Parquet/patientAccept/`;

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize the delta table to support fast query. Again, comment them out once used.

# COMMAND ----------

# MAGIC %sql
# MAGIC --optimize delta.`dbfs:/mnt/data/cprd/AurumRelease/Parquet/observation/` ZORDER BY (CodeId)
# MAGIC --optimize delta.`dbfs:/mnt/data/cprd/AurumRelease/Parquet/drugissue/` ZORDER BY (CodeId);  
# MAGIC -- optimize delta.`dbfs:/mnt/data/cprd/AurumRelease/Parquet/patient/`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Metadata setup
# MAGIC

# COMMAND ----------

codeIdcol<-"CodeId"
effectDate<-"EffectiveDate"
patIdCol<-"PatientIdentifier"
`%notin%` <- Negate(`%in%`)
mcodefiles<-dbutils.fs.ls(medpath)
pcodefiles<-dbutils.fs.ls(prodpath)
gt_observation<-"dbfs:/mnt/data/cprd/AurumRelease/Parquet/observation"
gt_drug<-"dbfs:/mnt/data/cprd/AurumRelease/Parquet/drugissue"
gt_aurumpatients<-"dbfs:/mnt/data/cprd/AurumRelease/Parquet/patients"
gt_accept<-"dbfs:/mnt/data/cprd/AurumRelease/Parquet/patientAccept"

#PART I
gt_p_path<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_p"
gt_m_path<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_m"
gt_fullpat_path_m<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_m/"
gt_fullpat_path_m_full<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_m_full/"
gt_fullpat_path_p_full<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_p_full/"
gt_fullpat_path_p<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_p/"
gt_fullpat_path<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat"
gt_fullaccptpat_path<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullaccptpat"

#PART II
gt_mbio_path<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_mbio"
gt_fullpat_path_mbio<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_mbio/"
gt_fullpat_path_mbio_full<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_mbio_full/"
gt_fullpatbio_path<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_bio"
gt_fullbioaccptpat_path<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullbioaccptpat"

#PART III
gt_full_patients_addons<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_full_patients_addons"
gt_full_patients<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_full_patients"
gt_full<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_full"
gt_csv<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_csv/gt.csv"
gt_obs_csv<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/obs_csv/obs.csv"
gt_drugs_csv<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/drugs_csv/drugs.csv"
gt_patients_csv<-"dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/pats_csv/pats.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dataset roll-up on variables from medcode/prodcode list (PART I)
# MAGIC Load medcodes and prodcodes from a list of files, with each file representing a covariable/drug/adr, e.g., the presence of a comorbidity.

# COMMAND ----------

mcodes<-read.df(mcodefiles[[1]]$path, source="csv", sep="\t", header=TRUE, inferScehma=TRUE)
mcodes<-select(mcodes, mcodes[[1]])
for(file in mcodefiles){
  mlist<-read.df(file$path, source="csv", sep="\t", header=TRUE, inferScehma=TRUE)
  mcodes<-union(mcodes,select(mlist, mlist[[1]]))
}
mcodes<-distinct(mcodes)

pcodes<-read.df(pcodefiles[[1]]$path, source="csv", sep="\t", header=TRUE, inferScehma=TRUE)
pcodes<-select(pcodes, pcodes[[1]])
for(file in pcodefiles){
  plist<-read.df(file$path, source="csv", sep="\t", header=TRUE, inferScehma=TRUE)
  pcodes<-union(pcodes,select(plist, plist[[1]]))
}
pcodes<-distinct(pcodes)

# COMMAND ----------

# MAGIC %md
# MAGIC This is collecting medical and product codes from previously defined datasets, processing baseline values, and retrieving all observations and drug-related data from specified Delta tables using SQL queries. We are now quering on Delta tables suggests that this code is utilizing Apache Spark for big data processing

# COMMAND ----------

m_list<-collect(mcodes)[,1]
p_list<-collect(pcodes)[,1]
baselines<-unlist(strsplit(baselines, ","))

query<-paste("select * from delta.`",gt_observation,"`",sep="")
obs <- sql(query)
query<-paste("select * from delta.`",gt_drug,"`",sep="")
drugs <- sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Filter the obs dataset to retain only the relevant observations based on medical codes and broad date ranges, while also filtering the drugs dataset for relevant drug records using product codes and adjusted date ranges. Both datasets are then refined to include only distinct records, ensuring unique combinations of patient ID, code ID, and effect date. This structured approach prepares the data for further analysis or processing.

# COMMAND ----------

obs<-where(obs, obs[[codeIdcol]] %in% m_list & obs[[effectDate]]>= baselines[1] & obs[[effectDate]]<= baselines[2])
obs<-distinct(select(obs, c(patIdCol,codeIdcol,effectDate)))
drugs<-where(drugs, drugs[[codeIdcol]] %in% p_list & drugs[[effectDate]]>= (as.Date(baselines[1])-90) & drugs[[effectDate]]<= (as.Date(baselines[2])-90)) 
drugs<-distinct(select(drugs, c(patIdCol,codeIdcol,effectDate)))

# COMMAND ----------

# MAGIC %md
# MAGIC Filters the obs and drugs datasets based on individual baselines defined by the specified medical and product codes. The use of SQL queries to impose these filters leverages the distributed processing capabilities of Spark.

# COMMAND ----------

## Use only when needed

# Assuming obs and drugs DataFrames have already been created and filtered

# Define parameters
specific_mcode <- "your_specific_mcode"      # Replace with your specific medical code
specific_pcode <- "your_specific_pcode"      # Replace with your specific product code
days_before_medcode <- 30                    # Days before for the medical code baseline
days_before_prodcode <- 30                     # Days before for the product code baseline 

# Create a temporary view for obs
createOrReplaceTempView(obs, "obs_view")

# Create a temporary view for drugs
createOrReplaceTempView(drugs, "drugs_view")

# Use SQL to filter observations for the specific medical code and baseline
query_obs <- paste0("
  SELECT *
  FROM obs_view
  WHERE ", codeIdcol, " = '", specific_mcode, "' 
    AND effectDate BETWEEN DATE_ADD(effectDate, -", days_before_medcode, ") AND effectDate
")

obs <- sql(query_obs)

# Use SQL to filter drugs for the specific product code and baseline
query_drugs <- paste0("
  SELECT *
  FROM drugs_view
  WHERE ", codeIdcol, " = '", specific_pcode, "' 
    AND effectDate BETWEEN DATE_ADD(effectDate, -", days_before_prodcode, ") AND effectDate
")

drugs <- sql(query_drugs)

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare the rolling up process by first storing the filtered drugs and obs.

# COMMAND ----------

all_pat_mlist<-list()
mlistcount<-0
all_pat_plist<-list()
plistcount<-0
results1_full<-NULL

# COMMAND ----------

write.df(drugs,path=gt_p_path,source="delta",header=TRUE,mode="overwrite")
write.df(obs,path=gt_m_path,source="delta",header=TRUE,mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC Read in for processing.

# COMMAND ----------

obs<-read.df(path=gt_m_path, source="delta", header=TRUE) 
drugs<-read.df(path=gt_p_path, source="delta", header=TRUE) 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This code performs aggregation of the `obs` (observations) and `drugs` tables for each patient, organizing and summarizing relevant data.
# MAGIC
# MAGIC #### 1. Initialization
# MAGIC - A distinct list of patient IDs is created from the `obs` DataFrame (`fullpat_m`). This serves as a reference for identifying all patients in the dataset.
# MAGIC
# MAGIC #### 2. Loop for Medical Codes
# MAGIC - The first loop iterates over a list of files containing medical codes (`mcodefiles`). For each file:
# MAGIC   - **Reading Code File**: Medical codes are read from the specified CSV file.
# MAGIC   - **Creating Code List**: The first column of the read DataFrame is collected into a list of codes (`codelist`).
# MAGIC   - **Filtering Observations**: The `obs` DataFrame is filtered to retain records where `codeIdcol` matches any code in `codelist` (`results1_a`).
# MAGIC   - **Identifying Patients with Codes**: Distinct patient IDs and effect dates are selected from the filtered observations.
# MAGIC   - **Finding Patients Without Codes**: Patients without any matching medical codes are identified by taking the difference between `fullpat_m` and the patients found in `results1_pat_a_p` (`results1_pat_b`).
# MAGIC
# MAGIC #### 3. Window Function Application
# MAGIC - A window specification is created (`ws`) to order observations by effect date for each patient.
# MAGIC - Dense ranking is applied to create a new DataFrame (`extext2`), which ranks observations based on the effect date for each patient.
# MAGIC
# MAGIC #### 4. Aggregation of Results
# MAGIC - Results are aggregated by patient ID, capturing the maximum and minimum effect dates while adding timestamp information.
# MAGIC - New columns are created with descriptive names based on the file name, formatted appropriately using string manipulation.
# MAGIC
# MAGIC #### 5. Handling Patients Without Codes
# MAGIC - Similar transformations are applied to patients without matching medical codes, ensuring they have appropriate columns for timestamps and date ranges filled with `NULL` values.
# MAGIC
# MAGIC #### 6. Combining Results
# MAGIC - Results from patients with and without matching medical codes are combined into a single DataFrame (`results1_full`) for each file, stored in a list (`all_pat_mlist`).
# MAGIC
# MAGIC #### 7. Loop for Drug Codes
# MAGIC - A similar process is repeated for drug codes (`pcodefiles`):
# MAGIC   - The steps for reading drug codes, filtering the `drugs` DataFrame, and aggregating results mirror those used for the medical codes, resulting in a distinct list of drug-associated patients and their relevant timestamps.
# MAGIC
# MAGIC #### Key Outputs
# MAGIC - Two lists, `all_pat_mlist` and `all_pat_plist`, are created to store results for patients associated with medical codes and drug codes, respectively.
# MAGIC - Each entry in these lists contains a DataFrame with aggregated information about timestamps and effect dates for patients, organized by patient ID.
# MAGIC
# MAGIC This code effectively combines and aggregates data from observation and drug tables while maintaining a clear structure that enables easy identification of patient associations with specific medical and drug codes.
# MAGIC

# COMMAND ----------

fullpat_m<-distinct(select(obs,obs[[patIdCol]]))
for(file in mcodefiles){
  mlistcount<-mlistcount+1
  code<-read.df(file$path, source="csv", sep="\t", header=TRUE, inferScehma=TRUE)
  codelist<- collect(select(code, code[[1]]))[,1]
  results1_a<-where(obs, obs[[codeIdcol]] %in% codelist) 
  results1_pat_a<-distinct(select(results1_a,  results1_a[[patIdCol]], results1_a[[effectDate]]))
  results1_pat_a_p<-distinct(select(results1_a,  results1_a[[patIdCol]]))
  results1_pat_b<-except(fullpat_m,results1_pat_a_p)
  ws <- orderBy(windowPartitionBy(patIdCol), effectDate) 
  extext2<-select(results1_pat_a, over(dense_rank(), ws), results1_pat_a[[effectDate]], results1_pat_a[[patIdCol]])   
  extext3<-withColumn(extext2,paste("timestamps@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep=""),over(collect_list(extext2[[2]]),ws))
  
  agg_results_a<-agg(groupBy(extext3, patIdCol),  
                            alias(max(extext3[[4]]),paste("timestamps@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep="")),
                     alias(max(extext3[[2]]),paste("latest@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep="")),
                    alias(min(extext3[[2]]),paste("earliest@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep="")))
  agg_results_a<-withColumn(agg_results_a,paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), lit(1))
 
  agg_results_b<-withColumn(results1_pat_b,paste("timestamps@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep=""), lit(NULL))
   agg_results_b<-withColumn(agg_results_b,paste("latest@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep=""), lit(NULL))
    agg_results_b<-withColumn(agg_results_b,paste("earliest@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep=""), lit(NULL))
  agg_results_b<-withColumn(agg_results_b,paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), lit(NULL))

  results1_full<-union(agg_results_a,agg_results_b) 
  all_pat_mlist[[mlistcount]]<-arrange(results1_full, results1_full[[patIdCol]])
}   

fullpat_p<-distinct(select(drugs,drugs[[patIdCol]]))
for(file in pcodefiles){
  plistcount<-plistcount+1
  code<-read.df(file$path, source="csv", sep="\t", header=TRUE, inferScehma=TRUE)
  codelist<- collect(select(code, code[[1]]))[,1]
  results1_a<-where(drugs, drugs[[codeIdcol]] %in% codelist) 
  results1_pat_a<-distinct(select(results1_a,  results1_a[[patIdCol]], results1_a[[effectDate]]))
  results1_pat_a_p<-distinct(select(results1_a,  results1_a[[patIdCol]]))
  results1_pat_b<-except(fullpat_m,results1_pat_a_p)
  ws <- orderBy(windowPartitionBy(patIdCol), effectDate) 
  extext2<-select(results1_pat_a, over(dense_rank(), ws), results1_pat_a[[effectDate]], results1_pat_a[[patIdCol]])   
  extext3<-withColumn(extext2,paste("timestamps@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep=""),over(collect_list(extext2[[2]]),ws))
  
  agg_results_a<-agg(groupBy(extext3, patIdCol),  
                            alias(max(extext3[[4]]),paste("timestamps@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep="")),
                     alias(max(extext3[[2]]),paste("latest@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep="")),
                    alias(min(extext3[[2]]),paste("earliest@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep="")))
  agg_results_a<-withColumn(agg_results_a,paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), lit(1))
 
  agg_results_b<-withColumn(results1_pat_b,paste("timestamps@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep=""), lit(NULL))
   agg_results_b<-withColumn(agg_results_b,paste("latest@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep=""), lit(NULL))
  agg_results_b<-withColumn(agg_results_b,paste("earliest@",paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), sep=""), lit(NULL))
  agg_results_b<-withColumn(agg_results_b,paste("[",gsub(pattern= "[ ]",replacement = "_",gsub(pattern= "[.]",replacement = "_", file$name)),"]", sep=""), lit(NULL))

  results1_full<-union(agg_results_a,agg_results_b) 
  all_pat_plist[[plistcount]]<-arrange(results1_full, results1_full[[patIdCol]])
} 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Writing Medical Code Patient Data
# MAGIC - The first loop iterates over the `all_pat_mlist`, which contains DataFrames for each patient's data associated with medical codes.
# MAGIC   
# MAGIC   - **File Writing Process**:
# MAGIC     - For each index `i`, the path for saving the DataFrame is constructed by concatenating the base path (`gt_fullpat_path_m`) with the index.
# MAGIC     - The `write.df` function is used to save the corresponding DataFrame to the specified path in Delta format, overwriting any existing data.
# MAGIC
# MAGIC #### 2. Writing Drug Code Patient Data
# MAGIC - The second loop performs a similar operation for the `all_pat_plist`, which holds DataFrames related to drug codes.
# MAGIC   
# MAGIC   - **File Writing Process**:
# MAGIC     - Again, for each index `i`, the path for saving the DataFrame is generated by combining the base path (`gt_fullpat_path_p`) with the index.
# MAGIC     - The `write.df` function saves the current DataFrame to the designated path in Delta format, ensuring that previous data is overwritten.
# MAGIC
# MAGIC

# COMMAND ----------


for (i in 1:length(all_pat_mlist)){ 
  paths<-paste(gt_fullpat_path_m,i,sep="")
  write.df(all_pat_mlist[[i]],path=paths,source="delta",header=TRUE,mode="overwrite")
 }
 
for (i in 1:length(all_pat_plist)){ 
  paths<-paste(gt_fullpat_path_p,i,sep="")
  write.df(all_pat_plist[[i]],path=paths,source="delta",header=TRUE,mode="overwrite")
 }

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Merging Medical Code Patient Data
# MAGIC - **File Listing**:
# MAGIC   - The `dbutils.fs.ls` function lists all files in the specified Delta directory (`gt_fullpat_path_m`) and stores the result in `mfiles`.
# MAGIC
# MAGIC - **Initial DataFrame Creation**:
# MAGIC   - The first Delta file is read into a DataFrame (`fulls_m`) using `read.df`.
# MAGIC
# MAGIC - **Merging Loop**:
# MAGIC   - A loop iterates over the files in `mfiles` to merge the subsequent DataFrames with the existing `fulls_m`.
# MAGIC   - **Merge Process**:
# MAGIC     - For each file index `i`, if it's not the last file:
# MAGIC       - The next file is read into a temporary DataFrame (`fulls_TEMP`).
# MAGIC       - The `join` operation merges `fulls_m` with `fulls_TEMP` using a left outer join on `PatientIdentifier`.
# MAGIC       - The joined `PatientIdentifier` column from `fulls_TEMP` is dropped to avoid duplication.
# MAGIC   
# MAGIC   - **Writing to Parquet**:
# MAGIC     - The merged DataFrame is written to Parquet format, with the output path constructed by concatenating `gt_fullpat_path_m_full` with the current index `i`.
# MAGIC     - The merged DataFrame is then re-read from the newly created Parquet file to update `fulls_m` for the next iteration.
# MAGIC
# MAGIC #### 2. Merging Drug Code Patient Data
# MAGIC - **File Listing**:
# MAGIC   - Similarly, the `dbutils.fs.ls` function lists all files in the Delta directory for drug codes (`gt_fullpat_path_p`) and stores the result in `pfiles`.
# MAGIC
# MAGIC - **Initial DataFrame Creation**:
# MAGIC   - The first Delta file is read into a DataFrame (`fulls_p`).
# MAGIC
# MAGIC - **Merging Loop**:
# MAGIC   - A loop iterates over the files in `pfiles` to merge the subsequent DataFrames with the existing `fulls_p`.
# MAGIC   - **Merge Process**:
# MAGIC     - For each index `i`, if it's not the last file:
# MAGIC       - The next file is read into a temporary DataFrame (`fulls_TEMP`).
# MAGIC       - The `join` operation merges `fulls_p` with `fulls_TEMP` using a left outer join on `PatientIdentifier`.
# MAGIC       - The joined `PatientIdentifier` column from `fulls_TEMP` is dropped to avoid duplication.
# MAGIC   
# MAGIC   - **Writing to Parquet**:
# MAGIC     - The merged DataFrame is written to Parquet format, with the output path constructed by concatenating `gt_fullpat_path_p_full` with the current index `i`.
# MAGIC     - The merged DataFrame is then re-read from the newly created Parquet file to update `fulls_p` for the next iteration.
# MAGIC

# COMMAND ----------

mfiles<-dbutils.fs.ls(gt_fullpat_path_m)
fulls_m<- read.df(path=mfiles[[1]]$path, source="delta",header=TRUE, inferSchema=TRUE)  
for (i in 1:length(mfiles)){
   if(i<length(mfiles)){
         fulls_TEMP<- read.df(path=mfiles[[(i+1)]]$path, source="delta",header=TRUE, inferSchema=TRUE) 
         fulls_m<-drop(join(fulls_m, fulls_TEMP, fulls_m$PatientIdentifier == fulls_TEMP$PatientIdentifier,"left_outer"), fulls_TEMP$PatientIdentifier)
         paths<- paste(gt_fullpat_path_m_full,i,".parquet",sep="")
         write.df(fulls_m,path=paths,souce="parquet",header=TRUE, mode="overwrite")
         fulls_m<-read.df(path=paths, source="parquet",header=TRUE, inferSchema=TRUE)
   }
 }
pfiles<-dbutils.fs.ls(gt_fullpat_path_p)
fulls_p<- read.df(path=pfiles[[1]]$path, source="delta",header=TRUE, inferSchema=TRUE)  
for (i in 1:length(pfiles)){
   if(i<length(pfiles)){
         fulls_TEMP<- read.df(path=pfiles[[(i+1)]]$path, source="delta",header=TRUE, inferSchema=TRUE) 
         fulls_p<-drop(join(fulls_p, fulls_TEMP, fulls_p$PatientIdentifier == fulls_TEMP$PatientIdentifier,"left_outer"), fulls_TEMP$PatientIdentifier)  
         paths<- paste(gt_fullpat_path_p_full,i,".parquet",sep="")
         write.df(fulls_p,path=paths,souce="parquet",header=TRUE, mode="overwrite")
         fulls_p<-read.df(path=paths, source="parquet",header=TRUE, inferSchema=TRUE) 
       }
 }

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Data Preparation
# MAGIC - **Assigning DataFrames**:
# MAGIC   - The existing DataFrames `fulls_m` and `fulls_p` are assigned to `m` and `p`, respectively.
# MAGIC
# MAGIC - **Creating Temporary Views**:
# MAGIC   - Two temporary views are created for SQL queries:
# MAGIC     - `m_table` for medical data
# MAGIC     - `p_table` for drug data
# MAGIC
# MAGIC #### 2. Merging Data
# MAGIC - **Left Join**:
# MAGIC   - A left outer join is performed between the two tables using `PatientIdentifier`:
# MAGIC     ```sql
# MAGIC     SELECT * FROM m_table
# MAGIC     ```
# MAGIC   - The result is stored in `fulls_m`, dropping the duplicate `PatientIdentifier` from the right table.
# MAGIC
# MAGIC - **Right Join**:
# MAGIC   - A right outer join is performed, and the result is stored in `fulls_p`, also dropping the duplicate `PatientIdentifier` from the left table.
# MAGIC   - The columns of `fulls_p` are then limited to the names present in `fulls_m`.
# MAGIC
# MAGIC - **Union**:
# MAGIC   - The two merged DataFrames (`fulls_m` and `fulls_p`) are combined into a single DataFrame (`fulls_union`) using a union operation.
# MAGIC
# MAGIC - **Removing Duplicates**:
# MAGIC   - Duplicates in `fulls_union` are dropped based on the `PatientIdentifier` to create `fulls_common`.
# MAGIC
# MAGIC #### 3. Writing to Delta Format
# MAGIC - The final consolidated DataFrame `fulls_common` is written to a Delta table specified by `gt_fullpat_path`, with the following options:
# MAGIC   - **Source**: Delta
# MAGIC   - **Header**: True
# MAGIC   - **Mode**: Overwrite

# COMMAND ----------

m<-fulls_m
p<-fulls_p
#printSchema(m)
createOrReplaceTempView(m, "m_table")
createOrReplaceTempView(p, "p_table") 

left <- sql("SELECT * FROM m_table")
right <- sql("SELECT * FROM p_table")
fulls_m<-drop(join(left, right, left$PatientIdentifier == right$PatientIdentifier,"left_outer"), right$PatientIdentifier)
fulls_p<-drop(join(left, right, left$PatientIdentifier == right$PatientIdentifier,"right_outer"), left$PatientIdentifier)
fulls_p<-fulls_p[,names(fulls_m)]
fulls_union<-union(fulls_m, fulls_p)
fulls_common<-dropDuplicates(fulls_union, "PatientIdentifier")
write.df(fulls_common,path=gt_fullpat_path,source="delta",header=TRUE,mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Data Loading
# MAGIC - **Load Full Patients Data**:
# MAGIC   - The complete patient dataset is loaded from a Delta table:
# MAGIC     ```r
# MAGIC     allpatients <- read.df(path=gt_fullpat_path, source="delta", header=TRUE)
# MAGIC     ```
# MAGIC
# MAGIC - **Load Acceptable Patients Data**:
# MAGIC   - The dataset containing acceptable patients is also loaded:
# MAGIC     ```r
# MAGIC     aurumpatients <- read.df(path=gt_aurumpatients, source="delta", header=TRUE)
# MAGIC     ```
# MAGIC
# MAGIC #### 2. Querying Acceptable Patients
# MAGIC - **SQL Query**:
# MAGIC   - A SQL query is constructed to select all records from `allpatients` that join with the `gt_accept` dataset on `PatientIdentifier`, where the `IsAcceptable` flag is set to 1:
# MAGIC     ```sql
# MAGIC     SELECT a.* 
# MAGIC     FROM delta.`<gt_fullpat_path>` a 
# MAGIC     JOIN delta.`<gt_accept>` b 
# MAGIC     ON a.PatientIdentifier = b.PatientIdentifier 
# MAGIC     AND b.IsAcceptable = 1
# MAGIC     ```
# MAGIC
# MAGIC - **Execute Query**:
# MAGIC   - The query is executed, and the results are stored in `acceptable_fulls`:
# MAGIC     ```r
# MAGIC     acceptable_fulls <- sql(query)
# MAGIC     ```
# MAGIC
# MAGIC #### 3. Writing Results
# MAGIC - The filtered DataFrame `acceptable_fulls` is written to a new Delta table specified by `gt_fullaccptpat_path`, with the following options:
# MAGIC   - **Source**: Delta
# MAGIC   - **Header**: True
# MAGIC   - **Mode**: Overwrite

# COMMAND ----------

allpatients<-read.df(path=gt_fullpat_path, source="delta", header=TRUE)
aurumpatients<-read.df(path=gt_aurumpatients, source="delta", header=TRUE)
query<-paste("select a.* from delta.`",gt_fullpat_path,"` a join delta.`",gt_accept,"` b on a.PatientIdentifier=b.PatientIdentifier and b.IsAcceptable=1", sep="")
acceptable_fulls<-sql(query)
write.df(acceptable_fulls,path=gt_fullaccptpat_path,source="delta",header=TRUE,mode="overwrite")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Dataset roll-up on variables from biological/value-measure medcodes (PART II)
# MAGIC
# MAGIC Congrats! Welcome to Part II, we first load medcodes with measured values.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Splitting Input Strings**:
# MAGIC    - The variable `biopath` is split into a vector `b`, where each element corresponds to a code ID. This is done using `strsplit`, which separates the string by commas.
# MAGIC    - Similarly, `baselines` is created by splitting the `baselines` string into two date components, representing the start and end of a date range.
# MAGIC
# MAGIC 2. **Querying the Observation Table**:
# MAGIC    - A SQL query is constructed to select all records from the Delta table identified by `gt_observation`. The query is executed using the `sql` function, which retrieves the data into the `obs` DataFrame.
# MAGIC
# MAGIC 3. **Filtering Observations**:
# MAGIC    - The `obs` DataFrame is then filtered to retain only those records where the `codeIdcol` column matches any of the values in vector `b` and where the `effectDate` falls within the specified date range (from the first to the second baseline date).
# MAGIC    - The `where` function is used to apply these filtering conditions.
# MAGIC
# MAGIC 4. **Saving Filtered Data**:
# MAGIC    - Finally, the filtered observations are written back to a Delta table at the specified path `gt_mbio_path` using the `write.df` function. The data is saved with headers included and in overwrite mode to replace any existing data at that path.
# MAGIC

# COMMAND ----------

b<-unlist(strsplit(biopath, ",")) 
baselines<-unlist(strsplit(baselines, ","))
query<-paste("select * from delta.`",gt_observation,"`",sep="")
obs <- sql(query)
obs<-where(obs, obs[[codeIdcol]] %in% c(b) & obs[[effectDate]]>= baselines[1] & obs[[effectDate]]<= baselines[2])


# COMMAND ----------

# MAGIC %md
# MAGIC Filter observations from a dataset based on a specific medical code and a defined baseline period.  

# COMMAND ----------

# Define parameters
specific_mcode <- "your_specific_mcode"      # Replace with your specific medical code 
days_before_medcode <- 30                      # Days before for the medical code baseline
days_before_prodcode <- 30                     # Days before for the product code baseline 

# Create a temporary view for obs
createOrReplaceTempView(obs, "obs_view")
 

# Use SQL to filter observations for the specific medical code and baseline
query_obs <- paste0("
  SELECT *
  FROM obs_view
  WHERE ", codeIdcol, " = '", specific_mcode, "' 
    AND effectDate BETWEEN DATE_ADD(effectDate, -", days_before_medcode, ") AND effectDate
")

# Execute the SQL query
obs_filtered <- sql(query_obs)

# Write the filtered observations to a specified path
write.df(obs_filtered, path=gt_mbio_path, source="delta", header=TRUE, mode="overwrite")

# COMMAND ----------

obs<-read.df(path=gt_mbio_path, source="delta", header=TRUE) 

# COMMAND ----------

testdf1<-where(obs, obs$CodeId %in% b)
#pre rollup phase
newdf1<-testdf1
for(j in 1:length(b)){ 
   newdf1<-withColumn(newdf1, as.character(b[j]),ifelse(newdf1$CodeId==b[j],ifelse(isNull(newdf1$Value),1,newdf1$Value),NA))
  }

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialization
# MAGIC
# MAGIC 1. **allsections**: 
# MAGIC    - This variable is initialized as an empty list to store the aggregated results from each iteration of the loop.
# MAGIC   
# MAGIC 2. **count**: 
# MAGIC    - This counter variable is set to zero and will be incremented with each iteration of the loop to track the number of processed sections.
# MAGIC
# MAGIC #### Loop Through Each Medical Code
# MAGIC
# MAGIC - The loop iterates over the list `b`, which contains specific medical codes. The purpose of the loop is to perform aggregation on the dataset for each medical code.
# MAGIC
# MAGIC #### Inside the Loop
# MAGIC
# MAGIC 1. **Incrementing the Counter**: 
# MAGIC    - `count <- count + 1` increments the counter for each medical code being processed.
# MAGIC
# MAGIC 2. **Define `cname`**: 
# MAGIC    - This variable holds the current medical code from the list `b`, converting it to a character string.
# MAGIC
# MAGIC 3. **Define Window Specification**: 
# MAGIC    - `ws <- orderBy(windowPartitionBy(patIdCol), effectDate)` creates a window specification that partitions the data by patient ID (`patIdCol`) and orders it by `effectDate`. This allows for operations to be performed within each patient's data chronologically.
# MAGIC
# MAGIC 4. **Select Relevant Columns**: 
# MAGIC    - `extext3 <- select(newdf1, over(dense_rank(), ws), newdf1[[effectDate]], newdf1[[patIdCol]], newdf1[[cname]])` creates a new DataFrame that includes a dense rank (a sequential number) for each observation within the window, alongside the effect date, patient ID, and the value associated with the current medical code.
# MAGIC
# MAGIC 5. **Concatenate Values**: 
# MAGIC    - `extext4 <- withColumn(extext3, paste("values@", cname), concat(extext3[[4]], lit('@'), extext3[[2]]))` creates a new column in `extext3` that concatenates the value associated with the current medical code and the effect date, separated by an '@' symbol.
# MAGIC
# MAGIC 6. **Collect List of Values**: 
# MAGIC    - `extext5 <- withColumn(extext4, paste("values@", cname, sep=""), over(collect_list(extext4[[5]]), ws))` collects all the concatenated values into a list for each patient within the window.
# MAGIC
# MAGIC #### Aggregation of Results
# MAGIC
# MAGIC - The aggregation process is performed with `agg()` on the grouped data:
# MAGIC   - **Group By Patient ID**: `groupBy(extext5, patIdCol)` groups the data by patient ID.
# MAGIC   - **Calculate Aggregates**: 
# MAGIC     - `alias(avg(extext5[[4]]), paste("mean@", cname, sep=""))` calculates the average of the values associated with the current medical code and assigns a name.
# MAGIC     - Similar calculations are performed for the maximum (`max`), minimum (`min`), and the maximum timestamp (`max`), with appropriate naming for each result.
# MAGIC   
# MAGIC 7. **Store Aggregated Results**: 
# MAGIC    - `allsections[[count]] <- agg_results` stores the aggregated results for the current medical code into the `allsections` list, indexed by the counter.
# MAGIC

# COMMAND ----------

#rollup processing starts
allsections<-list()
count<-0
for(j in 1:length(b))
{
  count<-count+1
  cname<-as.character(b[j]) 
  #ws <- orderBy(windowPartitionBy("patid"), "obsdate")
  ws <- orderBy(windowPartitionBy(patIdCol), effectDate) 
  #df1 <- select(newdf1, over(rank(), ws),newdf1[["patid"]],newdf1[["value"]])
  #extext3<-select(newdf1, over(dense_rank(), ws), newdf1[["obsdate"]], newdf1[["patid"]],newdf1[[cname]]) 
  extext3<-select(newdf1, over(dense_rank(), ws), newdf1[[effectDate]], newdf1[[patIdCol]],newdf1[[cname]])  
  # simiple decision tree here, if biological value, we do average otherwise flag as 1 
  extext4<-withColumn(extext3,paste("values@",cname), concat(extext3[[4]],lit('@'),extext3[[2]])) 
  extext5<-withColumn(extext4,paste("values@",cname, sep=""),over(collect_list(extext4[[5]]),ws))
  agg_results<-agg(groupBy(extext5, patIdCol), 
                            alias(avg(extext5[[4]]),paste("mean@",cname, sep="")), 
                            alias(max(extext5[[4]]),paste("max@",cname, sep="")), 
                            alias(min(extext5[[4]]),paste("min@",cname, sep="")),  
                            alias(max(extext5[[6]]),paste("timestamps@",cname, sep="")))#, stddev(extext3[[2]]))
  allsections[[count]]<- agg_results
  
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Saving Aggregated Results
# MAGIC
# MAGIC 1. **Loop Through Aggregated Sections**: 
# MAGIC    - `for (i in 1:length(allsections)) { ... }` iterates over each element in the `allsections` list, which contains the aggregated results for each medical code.
# MAGIC
# MAGIC 2. **Define File Paths**: 
# MAGIC    - `paths <- paste(gt_fullpat_path_mbio, i, sep="")` constructs a file path for each iteration, using `gt_fullpat_path_mbio` as the base path and appending the index `i`.
# MAGIC
# MAGIC 3. **Write Aggregated Data**: 
# MAGIC    - `write.df(allsections[[i]], path=paths, source="delta", header=TRUE, mode="overwrite")` saves each aggregated DataFrame from `allsections` to the specified path in Delta format, overwriting any existing files.
# MAGIC
# MAGIC #### Listing Saved Files
# MAGIC
# MAGIC 4. **List Files in Directory**: 
# MAGIC    - `mbiofiles <- dbutils.fs.ls(gt_fullpat_path_mbio)` retrieves a list of all files saved in the `gt_fullpat_path_mbio` directory, storing the results in `mbiofiles`.
# MAGIC
# MAGIC #### Merging Saved Files
# MAGIC
# MAGIC 5. **Read First File**: 
# MAGIC    - `fulls_mbio <- read.df(path=mbiofiles[[1]]$path, source="delta", header=TRUE, inferSchema=TRUE)` reads the first file from the list of saved files into a DataFrame called `fulls_mbio`.
# MAGIC
# MAGIC 6. **Loop Through Remaining Files**: 
# MAGIC    - `for (i in 1:length(mbiofiles)) { ... }` iterates through each file in the `mbiofiles` list.
# MAGIC
# MAGIC 7. **Conditional Check**: 
# MAGIC    - `if (i < length(mbiofiles)) { ... }` ensures that the merging process only occurs for files that have subsequent entries in the list.
# MAGIC
# MAGIC 8. **Read Temporary File**: 
# MAGIC    - `fulls_TEMP <- read.df(path=mbiofiles[[(i + 1)]]$path, source="delta", header=TRUE, inferSchema=TRUE)` reads the next file in the list into a temporary DataFrame `fulls_TEMP`.
# MAGIC
# MAGIC 9. **Merge DataFrames**: 
# MAGIC    - `fulls_mbio <- drop(join(fulls_mbio, fulls_TEMP, fulls_mbio$PatientIdentifier == fulls_TEMP$PatientIdentifier, "left_outer"), fulls_TEMP$PatientIdentifier)` performs a left outer join on `fulls_mbio` and `fulls_TEMP` based on the `PatientIdentifier` column, effectively merging the two DataFrames. The `drop` function is used to remove the `PatientIdentifier` column from `fulls_TEMP` after the join to avoid redundancy.
# MAGIC
# MAGIC 10. **Define New File Paths**: 
# MAGIC     - `paths <- paste(gt_fullpat_path_mbio_full, i, ".parquet", sep="")` constructs a new file path for each iteration, where the merged results will be saved in Parquet format.
# MAGIC
# MAGIC 11. **Write Merged Data**: 
# MAGIC     - `write.df(fulls_mbio, path=paths, source="parquet", header=TRUE, mode="overwrite")` saves the merged DataFrame into the specified path in Parquet format, overwriting any existing files.
# MAGIC
# MAGIC 12. **Read Back Merged Data**: 
# MAGIC     - `fulls_mbio <- read.df(path=paths, source="parquet", header=TRUE, inferSchema=TRUE)` reads the newly saved merged DataFrame back into `fulls_mbio` for potential further processing.
# MAGIC

# COMMAND ----------

for (i in 1:length(allsections)){ 
  paths<-paste(gt_fullpat_path_mbio,i,sep="")
  write.df(allsections[[i]],path=paths,source="delta",header=TRUE,mode="overwrite")
 }

mbiofiles<-dbutils.fs.ls(gt_fullpat_path_mbio)
fulls_mbio<- read.df(path=mbiofiles[[1]]$path, source="delta",header=TRUE, inferSchema=TRUE)  
for (i in 1:length(mbiofiles)){
   if(i<length(mbiofiles)){
         fulls_TEMP<- read.df(path=mbiofiles[[(i+1)]]$path, source="delta",header=TRUE, inferSchema=TRUE) 
         fulls_mbio<-drop(join(fulls_mbio, fulls_TEMP, fulls_mbio$PatientIdentifier == fulls_TEMP$PatientIdentifier,"left_outer"), fulls_TEMP$PatientIdentifier)
         paths<- paste(gt_fullpat_path_mbio_full,i,".parquet",sep="")
         write.df(fulls_mbio,path=paths,souce="parquet",header=TRUE, mode="overwrite")
         fulls_mbio<-read.df(path=paths, source="parquet",header=TRUE, inferSchema=TRUE)
   }
 }
# fulls<-select(allsections[[1]], patIdCol)
# #fulls<-select(allsections[[1]], "patid")
# for (section in allsections){
#   fulls<-merge(fulls,section)
#   #remove _y columns and rename _x column back to original
#   fulls<-drop(fulls,grep("_y", names(fulls), value = TRUE))
#   names(fulls)<- gsub(pattern= "_x",replacement = "", names(fulls))
# }

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning Column Names
# MAGIC
# MAGIC 1. **Remove Opening Parentheses**: 
# MAGIC    - `names(fulls_mbio) <- gsub(pattern = "[(]", replacement = "", names(fulls_mbio))` uses the `gsub` function to remove any opening parentheses `(` from the column names of the `fulls_mbio` DataFrame. This is important for ensuring that column names do not contain characters that might interfere with subsequent operations.
# MAGIC
# MAGIC 2. **Remove Closing Parentheses**: 
# MAGIC    - `names(fulls_mbio) <- gsub(pattern = "[)]", replacement = "", names(fulls_mbio))` follows a similar approach to remove any closing parentheses `)` from the column names, further cleaning the names for better usability.
# MAGIC
# MAGIC #### Saving the Cleaned DataFrame
# MAGIC
# MAGIC 3. **Write DataFrame to Delta**: 
# MAGIC    - `write.df(fulls_mbio, path=gt_fullpatbio_path, source="delta", mode="overwrite")` saves the cleaned DataFrame `fulls_mbio` to the specified path (`gt_fullpatbio_path`) in Delta format. The `mode = "overwrite"` parameter indicates that any existing data at that path will be replaced with the new data.
# MAGIC
# MAGIC #### Filtering Acceptable Records
# MAGIC
# MAGIC 4. **Construct SQL Query**: 
# MAGIC    - `query <- paste("select a.* from delta.`", gt_fullpatbio_path, "` a join delta.`", gt_accept, "` b on a.PatientIdentifier=b.PatientIdentifier and b.IsAcceptable=1", sep="")` creates a SQL query string that selects all columns from the `fulls_mbio` DataFrame while joining it with another dataset `gt_accept` on the `PatientIdentifier` column. The join condition ensures that only records where `b.IsAcceptable` equals 1 are included in the results. This step is crucial for filtering the dataset to retain only acceptable records based on defined criteria.
# MAGIC
# MAGIC 5. **Execute SQL Query**: 
# MAGIC    - `acceptable_fullbios <- sql(query)` executes the SQL query constructed in the previous step, retrieving the filtered records and storing them in a new DataFrame called `acceptable_fullbios`.
# MAGIC
# MAGIC #### Saving the Filtered Data
# MAGIC
# MAGIC 6. **Write Filtered DataFrame to Delta**: 
# MAGIC    - `write.df(acceptable_fullbios, path=gt_fullbioaccptpat_path, source="delta", header=TRUE, mode="overwrite")` saves the filtered DataFrame `acceptable_fullbios` to the specified path (`gt_fullbioaccptpat_path`) in Delta format. Similar to the previous save operation, it uses `mode = "overwrite"` to replace any existing data at that location.
# MAGIC

# COMMAND ----------

names(fulls_mbio)<- gsub(pattern= "[(]",replacement = "", names(fulls_mbio))
names(fulls_mbio)<- gsub(pattern= "[)]",replacement = "", names(fulls_mbio))

# COMMAND ----------

 write.df(fulls_mbio, path=gt_fullpatbio_path,source="delta", mode="overwrite") 

# COMMAND ----------

query<-paste("select a.* from delta.`",gt_fullpatbio_path,"` a join delta.`",gt_accept,"` b on a.PatientIdentifier=b.PatientIdentifier and b.IsAcceptable=1", sep="")
acceptable_fullbios<-sql(query)
write.df(acceptable_fullbios,path=gt_fullbioaccptpat_path,source="delta",header=TRUE,mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combine Part I and Part II (Part III)
# MAGIC
# MAGIC Output list:
# MAGIC * a cross-sectional dataset of all medcodes and prodcodes as columns, one patient per line
# MAGIC * a observation table of all relevant patients 
# MAGIC * a drug record table of all relevant patients
# MAGIC * a patient table

# COMMAND ----------

left <- sql(paste("SELECT * FROM delta.`",gt_fullaccptpat_path,"`",sep=""))
right <- sql(paste("SELECT * FROM delta.`",gt_fullbioaccptpat_path,"`",sep=""))
fulls_left<-drop(join(left, right, left$PatientIdentifier == right$PatientIdentifier,"left_outer"), right$PatientIdentifier)
fulls_right<-drop(join(left, right, left$PatientIdentifier == right$PatientIdentifier,"right_outer"), left$PatientIdentifier) 
fulls_right<-fulls_right[,names(fulls_left)]
fulls_union<-union(fulls_left, fulls_right)
fulls_common<-dropDuplicates(fulls_union, "PatientIdentifier")
#write.df(fulls_common,path=gt_full,source="delta",header=TRUE,mode="overwrite")
query<-paste("select b.*, a.`Sex`, a.`YearOfBirth`, a.`DateOfDeath` from delta.`",gt_aurumpatients,"` a join delta.`",gt_full,"` b on CONCAT(a.PatientId,a.CPRDPracticeId)=b.PatientIdentifier",sep="")
patients <- sql(query) 
patientsf<-dropDuplicates(patients, "PatientIdentifier")  
write.df(patientsf,path=gt_full_patients,source="delta",header=TRUE,mode="overwrite")

# COMMAND ----------

df<-read.df(path=gt_full_patients, source="delta")
count(df)

# COMMAND ----------

#cross-sectional dataset output
full<-read.df(path=gt_full_patients, source="delta", header=TRUE)
#drop timestamp columns
fullcolumns<-names(full)
timestamp_colnames<-fullcolumns[grepl("timestamps@", fullcolumns)]
for(timecolname in timestamp_colnames){
    full<-withColumn(full,timecolname, concat_ws(",",full[[timecolname]])) 
}
#csvDF_output<-drop(full,c(timestamp_colnames))
full_csv <- coalesce(full, 1L)
write.df(full_csv,path=gt_csv,source="csv", sep="\t",header=TRUE,mode="overwrite") 

# COMMAND ----------

#observation dataset output
m_list<-collect(mcodes)[,1] 
baselines<-unlist(strsplit(baselines, ",")) 
query<-paste("select a.* from delta.`",gt_observation,"` a join delta.`",gt_m_path,"` b on a.PatientIdentifier=b.PatientIdentifier and a.CodeId=b.CodeId and a.EffectiveDate>='",baselines[1],"' and a.EffectiveDate<='",baselines[2],"'",sep="")
obs <- sql(query) 
obs_csv<-coalesce(dropDuplicates(union(obs, sql(paste("select * from delta.`",gt_mbio_path,"`",sep="")))), 1L)
write.df(obs_csv,path=gt_obs_csv,source="csv", sep="\t",header=TRUE,mode="overwrite") 

# COMMAND ----------

#drug dataset output
p_list<-collect(pcodes)[,1]
baselines<-unlist(strsplit(baselines, ","))
query<-paste("select a.* from delta.`",gt_drug,"` a join delta.`",gt_p_path,"` b on a.PatientIdentifier=b.PatientIdentifier and a.CodeId=b.CodeId and a.EffectiveDate>='",baselines[1],"' and a.EffectiveDate<='",baselines[2],"'",sep="")
drugs <- sql(query) 
#drugs<-where(drugs, drugs[[codeIdcol]] %in% p_list & drugs[[effectDate]]>= baselines[1] & drugs[[effectDate]]<= baselines[2]) 
#display(drugs)
full_csv <- coalesce(drugs, 1L)
#write.df(drugs,path=gt_p_path,source="delta",header=TRUE,mode="overwrite")
write.df(full_csv,path=gt_drugs_csv,source="csv", sep="\t",header=TRUE,mode="overwrite") 

# COMMAND ----------

#patient dataset output
query<-paste("select a.* from delta.`",gt_aurumpatients,"` a join delta.`",gt_full_patients,"` b on CONCAT(a.PatientId,a.CPRDPracticeId)=b.PatientIdentifier",sep="")
patients <- sql(query) 
#display(patients)
full_csv <- coalesce(patients, 1L)
write.df(full_csv,path=gt_patients_csv,source="csv", sep="\t",header=TRUE,mode="overwrite") 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Addon columns (Part IV)
# MAGIC
# MAGIC Add extra columns that each medcode can represent a value itself, usually categorical values, e.g. medcodes representing ethnicity. These columns can be added in an ad-hoc manner when co-hort is defined from previous parts.

# COMMAND ----------

gt <- sql(paste("SELECT * FROM delta.`",gt_full,"`",sep=""))
#display(gt)

# COMMAND ----------

#check codelist on ethnicity using 2011 census
query<-paste("select  * from delta.`dbfs:/mnt/data/cprd/AurumRelease/DataFlows/medcodelist` " ,sep="")
codes <- sql(query)  
census<-filter(codes, like(codes[[2]],"%2001 census"))
census_codes<-collect(select(census, census[[1]]))
display(census)

# COMMAND ----------

gt_observation<-"dbfs:/mnt/data/cprd/AurumRelease/Parquet/observation"
gt<-sql("select * from delta.`dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_full_patients`")


#check codelist on ethnicity using 2011 census
query<-paste("select  * from delta.`dbfs:/mnt/data/cprd/AurumRelease/DataFlows/medcodelist` " ,sep="")
codes <- sql(query)  
census<-filter(codes, like(codes[[2]],"%2001 census"))
census_codes<-collect(select(census, census[[1]]))
query<-paste("select distinct a.PatientIdentifier, a.CodeId, a.EffectiveDate, b.term  from delta.`",gt_observation,"` a left join delta.`dbfs:/mnt/data/cprd/AurumRelease/DataFlows/medcodelist` b on a.CodeId=b.medcodeid",sep="")
obs_alltime <- sql(query)
#obs_alltime<-withColumnRenamed(obs_alltime, "CodeId", "Ethnicity2001")
#display(obs_alltime)
obs_census<-filter(obs_alltime, obs_alltime[["CodeId"]] %in% census_codes[,1])
#printSchema(obs_census)
ws <- orderBy(windowPartitionBy("PatientIdentifier"),"EffectiveDate") 
# printSchema(obs_census)
extext2<-select(obs_census, over(dense_rank(), ws), obs_census[[1]],obs_census[[2]],obs_census[[3]],obs_census[[4]])   
#extext3<-withColumn(extext2,paste("values@2001census",sep=""), concat(extext2[[3]],lit('@'),extext2[[4]])) 
extext4<-withColumn(extext2,paste("timestamps@2001census", sep=""),over(max(extext2[[4]]),ws))
agg_results<-agg(groupBy(extext4, "PatientIdentifier"), 
                            alias(max(extext4[[5]]),paste("timestamps@2001census", sep="")))
 
fulls_add_census<-drop(join(gt, agg_results, gt$PatientIdentifier == agg_results$PatientIdentifier,"left_outer"), agg_results$PatientIdentifier)

#social occupation
query<-paste("select  * from delta.`dbfs:/mnt/data/cprd/AurumRelease/DataFlows/medcodelist` where emiscodecatid=17" ,sep="")
codes <- sql(query)  
census_codes<-collect(select(codes, codes[[1]]))
query<-paste("select distinct a.PatientIdentifier, a.CodeId, a.Value, a.EffectiveDate, b.term  from delta.`",gt_observation,"` a left join delta.`dbfs:/mnt/data/cprd/AurumRelease/DataFlows/medcodelist` b on a.CodeId=b.medcodeid",sep="")
obs_alltime <- sql(query)
#obs_alltime<-withColumnRenamed(obs_alltime, "CodeId", "Ethnicity2001")
#display(obs_alltime)
obs_census<-filter(obs_alltime, obs_alltime[["CodeId"]] %in% census_codes[,1])
#printSchema(obs_census)
ws <- orderBy(windowPartitionBy("PatientIdentifier"),"EffectiveDate") 
# printSchema(obs_census)
extext2<-select(obs_census, over(dense_rank(), ws), obs_census[[1]],obs_census[[2]],obs_census[[3]],obs_census[[4]],obs_census[[5]])   
extext3<-withColumn(extext2,paste("values@2001census",sep=""), concat(extext2[[6]],lit('@'),ifelse(isNull(extext2[[4]]),"",extext2[[4]]))) 
extext4<-withColumn(extext3,paste("timestamps@socialstatus", sep=""),over(collect_set(extext3[[7]]),ws))
agg_results<-agg(groupBy(extext4, "PatientIdentifier"), 
                            alias(max(extext4[[8]]),paste("timestamps@socialstatus", sep="")))
 
fulls_add_census<-drop(join(fulls_add_census, agg_results, gt$PatientIdentifier == agg_results$PatientIdentifier,"left_outer"), agg_results$PatientIdentifier)
write.df(fulls_add_census,path=gt_full_patients_addons,source="delta",header=TRUE,mode="overwrite")

# COMMAND ----------

display(fulls_add_census)

# COMMAND ----------

# MAGIC %md
# MAGIC #### drop fast query delta tables

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // #PART I
# MAGIC val gt_p_path="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_p"
# MAGIC val gt_m_path="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_m"
# MAGIC val gt_fullpat_path="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat"
# MAGIC val gt_fullaccptpat_path="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullaccptpat"
# MAGIC
# MAGIC // #PART II
# MAGIC val gt_mbio_path="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_mbio"
# MAGIC val gt_fullpatbio_path="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_bio"
# MAGIC val gt_fullbioaccptpat_path="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullbioaccptpat"
# MAGIC
# MAGIC  
# MAGIC walkDelete(gt_p_path)(0) 
# MAGIC walkDelete(gt_m_path)(0) 
# MAGIC walkDelete(gt_fullpat_path)(0)
# MAGIC walkDelete(gt_fullaccptpat_path)(0) 
# MAGIC walkDelete(gt_mbio_path)(0) 
# MAGIC walkDelete(gt_fullpatbio_path)(0)
# MAGIC walkDelete(gt_fullbioaccptpat_path)(0)

# COMMAND ----------

# MAGIC %scala 
# MAGIC val gt_fullpat_path_m_full="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_m_full/"
# MAGIC val gt_fullpat_path_p_full="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_p_full/"
# MAGIC val gt_fullpat_path_m="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_m/"
# MAGIC val gt_fullpat_path_p="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_p/"
# MAGIC walkDelete(gt_fullpat_path_m)(0) 
# MAGIC walkDelete(gt_fullpat_path_p)(0) 
# MAGIC walkDelete(gt_fullpat_path_m_full)(0) 
# MAGIC walkDelete(gt_fullpat_path_p_full)(0) 

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val gt_fullpat_path_mbio="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_mbio/"
# MAGIC val gt_fullpat_path_mbio_full="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_fullpat_mbio_full/"
# MAGIC
# MAGIC walkDelete(gt_fullpat_path_mbio)(0) 
# MAGIC walkDelete(gt_fullpat_path_mbio_full)(0) 

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // val gt_full="dbfs:/mnt/data/cprd/AurumRelease/DataFlows/covidgt/gt_full"
# MAGIC
# MAGIC // walkDelete(gt_full)(0)   

# COMMAND ----------

#cdata.df[,grepl("avg",names(cdata.df))]<-replace(cdata.df[,grepl("avg",names(cdata.df))], cdata.df[,grepl("avg",names(cdata.df))] == 0, NA)
#cdata.df[,grepl("avg",names(cdata.df))]

# COMMAND ----------

 #replace(cdata.df[,grepl("std",names(cdata.df))],cdata.df[,grepl("std",names(cdata.df))]=="NaN", 0)