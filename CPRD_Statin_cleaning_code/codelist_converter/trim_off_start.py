# Databricks notebook source
# MAGIC %md
# MAGIC This is a notebook that attempts to convert unreadable codelists to a format where we can read them.  
# MAGIC For some reason, a lot of the codelists that we have been sent are in a format where they use a space as both the delimiter and as a normal character in the file. This means you can't read them in with `pd.read_csv()`.  
# MAGIC   
# MAGIC The solution to this problem is to take only the `MedCodeID`/`prodcodeid` from the file and drop all the other data and then read the codelist into the Code browser. This can then be used to make a codelist that is delimited a bit better. This notebook is a function for doing that conversion

# COMMAND ----------

import numpy as np 
import pandas as pd 

# COMMAND ----------

def reformat_into_readable_df(path):
    data = []
    
    # Open and process the file
    with open(path, 'r') as file:
        for line in file:

            # Remove the leading quote and split the line (each line starts with a ' character)
            parts = line[1:].strip().split(maxsplit=2)  # Strip and split by whitespace

            # Ensure there are at least two strings (the files we can't read are of format ['CPRD MedCodeID Term']), so we only want to keep the first two columns of this
            if len(parts) >= 2:
                data.append((parts[0], parts[1]))  # Append the first two strings as a tuple
                
    df = pd.DataFrame(data, columns=['Group', 'prodcodeid']) # convert this into a dataframe
    
    print(df)
    
    return df

def save_dataframe(df, path_to_save):
    df.to_csv(path_to_save, index=False)


# COMMAND ----------

bb_path = '/dbfs/mnt/data/cprd_statin_data/Codelists/p_beta_blockers_aurum.txt'

# COMMAND ----------

bb_df_reformat = reformat_into_readable_df(bb_path)

# COMMAND ----------

save_dataframe(bb_df_reformat, '/dbfs/mnt/data/cprd_statin_data/Codelists/beta_blockers_proc.txt')

# COMMAND ----------

df = pd.read_csv('/dbfs/mnt/data/cprd_statin_data/Codelists/beta_blockers_proc.txt')
print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC