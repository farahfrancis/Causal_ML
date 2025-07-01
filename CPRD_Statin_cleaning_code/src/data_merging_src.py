# Databricks notebook source
import xml.etree.ElementTree as ET
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import warnings 

# COMMAND ----------

def parse_schema(path):
    
    """
    Parses the schema of CPRD data files from an XML file.

    Args:
    - path (str): Absolute path to the schema file.

    Returns:
    - list of dict: CPRD schema extracted from the XML file, where each dictionary contains:
        - 'displayname' (str): The display name of the column.
        - 'header' (str): The header name of the column.
        - 'columnposition' (int): The position of the column in the file.

    Example:
    schema = parse_schema('/path/to/schema.xml')
    """

    # Parse the XML
    tree = ET.parse(path)
    root = tree.getroot()

    # Extract and display schema details
    schema = []
    for column in root.findall(".//datacolumn"):
        column_info = {
            "displayname": column.find("displayname").text,
            "header": column.find("header").text,
            "columnposition": int(column.find("columnposition").text)
        }
        schema.append(column_info)

    return schema

# COMMAND ----------

class merger:
    """
    Helper class to manage and merge CPRD data files.

    Initialization parameters:
    - input_path (str): Absolute path to the directory containing the CPRD data files.
    - subsection_name (str): Name of the subsection of the CPRD data files (e.g., DrugIssue, Observation, Patient, Practice).

    Attributes:
    - path (str): Combined path of input_path and subsection_name.
    - files (list of str): List of filenames in the directory defined by path.
    - schema_path (str): Path to the schema file.
    - schema (list of dict): Schema of the CPRD data files.
    - struct_fields (list of StructField): StructType schema for Spark.
    - custom_schema (StructType): StructType schema for Spark.
    - df (DataFrame): Spark DataFrame containing the merged CPRD data files.
    - data_count (int): Number of records in the merged CPRD data files.
    - save_data_bool (bool): Flag to indicate whether the data should be saved as a parquet file.

    Methods:
    - read_schema: Reads the schema of the CPRD data files.
    - read_all_files: Reads all the CPRD data files into a single DataFrame.
    - check_count: Checks the number of records in the merged DataFrame and compares it to an expected count.
    - save_data: Saves the merged DataFrame as a parquet file.

    Example usage:
    listnum = 'List6/'
    path = 'dbfs:/mnt/data/cprd_statin_data/' + listnum
    drug_issue_merge = merger(path, 'DrugIssue')  # Sets up the file paths and checks for data files.
    drug_issue_merge.read_schema()  # Reads the .xml schema file from the directory.
    drug_issue_merge.read_all_files()  # Reads all .txt files and merges into a single DataFrame.
    display(drug_issue_merge.df)  # Displays the merged DataFrame.
    drug_issue_merge.check_count(num_drug_issue)  # Checks the row count of the DataFrame.
    drug_issue_merge.save_data(False)  # Saves the data if everything is fine.
    """

    def __init__(self, input_path, subsection_name):
        """
        Initializes the merger class with the given input path and subsection name.
        """
        self.input_path = input_path 
        self.subsection_name = subsection_name
        self.path = self.input_path + '/' + self.subsection_name + '/'
        self.files = dbutils.fs.ls(self.path)
        self.schema_path = '/dbfs'+(([file.path for file in self.files if file.path.endswith('.xml')][0]).split(':'))[-1]
        print("SCHEMA PATH = ", self.schema_path)
        print("NUMBER OF FILENAMES = ", len(self.files))

    def read_schema(self):
        """
        Reads and parses the schema file for the CPRD data files.
        """
        try:
            self.schema = parse_schema(self.schema_path)
            self.struct_fields = [StructField(col["header"], StringType(), True) for col in self.schema]
            self.custom_schema = StructType(self.struct_fields)
        except:
            self.schema = None
            self.custom_schema = None
            warnings.warn("Schema file not found. Code will continue but may get the schema of the data incorrect")

    def read_all_files(self):
        """
        Reads all CPRD data files into a single DataFrame.
        """
        if self.custom_schema is not None: 
            self.df = spark.read.csv(self.path+'*.txt', schema=self.custom_schema, header=True, sep='\t')
        else:
            warnings.warn("Inferring schema because schema not found")
            self.df = spark.read.csv(self.path+'*.txt', header=True, sep='\t')

    def check_count(self, expected_count):
        """
        Checks the number of records in the merged DataFrame and compares it to the expected count.

        Args:
        - expected_count (int): The expected number of records.
        """
        self.data_count = self.df.count()
        print(self.data_count, "records")
        if self.data_count != expected_count:
            raise Exception(f"Expected {expected_count} records, found {self.data_count} records")

    def save_data(self, save_data_bool=False):
        """
        Saves the merged DataFrame as a parquet file if save_data_bool is True.

        Args:
        - save_data_bool (bool): Flag to indicate whether to save the data.
        """
        if save_data_bool:
            print("Data is being saved as a parquet file")
            save_path = self.path + self.subsection_name + '.parquet'
            self.df.write.parquet(save_path, mode='overwrite')
        else:
            print("Logical switch is set to False. Set to True if the data is clean and you know it is okay to proceed")
