# Introduction 
A set of notebooks for processing CPRD data using Pyspark. The parts of the process that are handled are:
1) Reading in a set of DrugIssue, Patient, Practice and Observation files from CPRD and merging them to produce a single `.parquet` file for each.  
  Generally the data comes as many DrugIssue files, each with the filesize limit of ~1.1GB. There is a script for merging these, checking the quality of the data and converting to `parquet` for some scalability
2) Convert `parquet` files to `delta` and optimising
3) Filtering the patient list to apply the different exclusion criteria 
4) Add a "patient outcome" column that gives the outcome the patient experienced and when.

# Getting Started
If you start out with individual, unmerged DrugIssue and Observation `.txt` files: the first step is to run `read_merge_individual_lists`  
Next, you should be checking that this data merging has actually worked: do some quality checking on the data to check that it is suitable  
  
Once this has been done, then you can do data exploration: 
- use `data_exploration/statin_prescribing_patterns` to explore what the statin prescribing patterns actually look like 
- use `data_exploration/patient_filtering_flow_diagram` to visualise how filtering the patients might look like (and produce a flow chart)
- use `function_testing/explore_outcomes` to explore what the different outcomes in the dataset are 

Once that is done, you can run `generating_acceptable_patient_list` to produce a final set of dataframes with only suitable patients for the study 

# Outcomes
The `outcome` for each patient is: 
- 0 if there is no record associated with myopathy, severe myopathy or a CK level measurement for them in the Observations data. We assume no outcome record means that the patient did not have any outcome.
- 1 if there is a CK level between $4$ and $10 \times ULN$, or a code for "myopathy" in the observation data for that patient 
- 2 if there is a CK level above $10 \times ULN$ for that patient, or a code for "rhabdomyolysis" in the Observation data for that patient
- 3 if the outcome is undefined. There is a recorded measurement for "CK level" for the patient, but the units are unconvertable, so we don't know whether it is abnormal or not. 
  
For each patient, we only keep the first recorded outcome for them.

# Still to be done/known issues
- Some patients have a measurement of CK level, but the units are unconvertable. We have given these patients `myopathy_code=3`, meaning undefine. They may need to be dropped, as it's hard to know for certain what the intended outcome for these patients was. Alternatively, they could be kept as `outcome=0`
- What about patients who experience multiple outcomes? GP records them as having both myopathy and rhabdomyolysis?