# Databricks notebook source
!pip install tableone

# COMMAND ----------

# Start Spark session
spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## code for automated table 1
# MAGIC

# COMMAND ----------

#from tableone import TableOne
import pandas as pd

#table one analysis
data = spark.read.csv("dbfs:/mnt/data/cprd_statin_data/final_dataset_output.csv", header=True, inferSchema=True)

#convert into pandas dataframe
data_pandas = data.toPandas()

data.columns

#drop missing values
data_pandas = data_pandas.dropna()

# COMMAND ----------

display(data_pandas)

# COMMAND ----------

import pandas as pd
import numpy as np

# Assume your dataset is already loaded into the DataFrame 'data_pandas'
# and that an identifier column (e.g., 'patid') is dropped if not needed.
df = data_pandas.drop(['patid'], axis=1)

# Define the grouping variable (e.g., "statins") which has two categories: 1 and 2.
group_var = 'statins'
group_pairs = [(1, 2)]  # Only one comparison: group 1 vs group 2

def compute_numeric_smd(data, group1, group2, col):
    """
    Compute the standardized mean difference (SMD) for a continuous variable.
    
    Parameters:
        data (DataFrame): The dataset.
        group1 (int or str): The value for group 1.
        group2 (int or str): The value for group 2.
        col (str): The column name for the continuous variable.
    
    Returns:
        float: The SMD for the variable between group1 and group2.
    """
    # Select data for each group
    x1 = data.loc[data[group_var] == group1, col]
    x2 = data.loc[data[group_var] == group2, col]
    
    # Compute means and standard deviations for both groups
    mean1 = x1.mean()
    mean2 = x2.mean()
    sd1 = x1.std()
    sd2 = x2.std()
    
    # Calculate the pooled standard deviation
    pooled_sd = np.sqrt((sd1**2 + sd2**2) / 2)
    
    # Return SMD (if pooled_sd is 0, return NaN to avoid division by zero)
    return np.abs(mean1 - mean2) / pooled_sd if pooled_sd != 0 else np.nan

# Identify numeric columns (excluding the grouping variable)
numeric_cols = df.select_dtypes(include=np.number).columns.tolist()

if group_var in numeric_cols:
    numeric_cols.remove(group_var)

# List to store results
results = []

# Loop over the group pair and numeric columns
for g1, g2 in group_pairs:
    for col in numeric_cols:
        smd = compute_numeric_smd(df, g1, g2, col)
        results.append({
            'Variable': col,
            'Group_Comparison': f"{g1} vs {g2}",
            'SMD': smd
        })

# Create a summary DataFrame from the results
results_df = pd.DataFrame(results)

# Display the results
print(results_df)

# Save the summary table to a CSV file
results_df.to_csv('numeric_SMD.csv', index=False)


# COMMAND ----------

import pandas as pd
import numpy as np

# Assume your dataset is already loaded into the DataFrame 'data_pandas'
df = data_pandas.drop(['patid'], axis=1)


# Set the grouping variable ("statins") and assume it has two categories: 1 and 2.
group_var = 'statins'
group_pairs = [(1, 2)]  # Only one comparison: 1 vs 2

# Function to check if a series is binary (only 0 and 1 present)
def is_binary(series):
    vals = series.dropna().unique()
    return set(vals) == {0, 1} or set(vals) == {0.0, 1.0}

# Function to compute SMD for binary variables for a specific level.
def compute_binary_smd_level(data, group1, group2, col, level):
    """
    For a binary variable, compute the SMD for the given level.
    
    The proportion for the level is computed as the mean of the indicator (1 if value equals level).
    """
    p1 = (data.loc[data[group_var] == group1, col] == level).mean()
    p2 = (data.loc[data[group_var] == group2, col] == level).mean()
    
    # Calculate pooled variance for proportions:
    var_pooled = (p1 * (1 - p1) + p2 * (1 - p2)) / 2
    
    return np.abs(p1 - p2) / np.sqrt(var_pooled) if var_pooled != 0 else np.nan

# Function to compute SMD for continuous numeric variables.
def compute_numeric_smd(data, group1, group2, col):
    x1 = data.loc[data[group_var] == group1, col]
    x2 = data.loc[data[group_var] == group2, col]
    mean1 = x1.mean()
    mean2 = x2.mean()
    sd1 = x1.std()
    sd2 = x2.std()
    pooled_sd = np.sqrt((sd1**2 + sd2**2) / 2)
    return np.abs(mean1 - mean2) / pooled_sd if pooled_sd != 0 else np.nan

# Function to compute SMD for categorical variables (non-binary).
def compute_categorical_smd(data, group1, group2, col, level):
    p1 = (data.loc[data[group_var] == group1, col] == level).mean()
    p2 = (data.loc[data[group_var] == group2, col] == level).mean()
    var_pooled = (p1 * (1 - p1) + p2 * (1 - p2)) / 2
    return np.abs(p1 - p2) / np.sqrt(var_pooled) if var_pooled != 0 else np.nan

# Identify numeric and categorical columns (excluding the grouping variable)
numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()

if group_var in numeric_cols:
    numeric_cols.remove(group_var)
if group_var in categorical_cols:
    categorical_cols.remove(group_var)

results = []

# Loop over the group pair and calculate the SMDs for each variable
for g1, g2 in group_pairs:
    # Process numeric columns
    for col in numeric_cols:
        if is_binary(df[col]):
            # For a binary variable, calculate for both levels
            for level in [0, 1]:
                smd = compute_binary_smd_level(df, g1, g2, col, level)
                results.append({
                    'Variable': col,
                    'Level': level,
                    'Group_Comparison': f"{g1} vs {g2}",
                    'SMD': smd
                })
        else:
            # For continuous numeric variables, use the usual calculation.
            smd = compute_numeric_smd(df, g1, g2, col)
            results.append({
                'Variable': col,
                'Level': None,
                'Group_Comparison': f"{g1} vs {g2}",
                'SMD': smd
            })
    
    # Process categorical columns
    for col in categorical_cols:
        levels = df[col].dropna().unique()
        # If the variable is binary, include both levels
        if set(levels) == {0, 1} or set(levels) == {0.0, 1.0}:
            for level in [0, 1]:
                smd = compute_binary_smd_level(df, g1, g2, col, level)
                results.append({
                    'Variable': col,
                    'Level': level,
                    'Group_Comparison': f"{g1} vs {g2}",
                    'SMD': smd
                })
        else:
            # For non-binary categorical variables, calculate SMD for each level.
            for lev in levels:
                smd = compute_categorical_smd(df, g1, g2, col, lev)
                results.append({
                    'Variable': col,
                    'Level': lev,
                    'Group_Comparison': f"{g1} vs {g2}",
                    'SMD': smd
                })

# Create a summary DataFrame with the results
results_df = pd.DataFrame(results)

# Display the resulting DataFrame
print(results_df)

# Save the summary table to a CSV file
results_df.to_csv('SMD.csv', index=False)


# COMMAND ----------

import pandas as pd
import numpy as np

# Assume your dataset is already loaded into the DataFrame 'data_pandas'
# Here we drop an identifier column, if needed.
df = data_pandas.drop(['patid'], axis=1)
df['ethic'] = df['Ethnic'].astype('category')
df['gender'] = df['gender'].astype('category')
df['Outcome'] = df['Outcome'].astype('category')


# Set the grouping variable ("statins") and assume it has two categories: 1 and 2.
group_var = 'statins'
group_pairs = [(1, 2)]  # Only one comparison: 1 vs 2

# Function to check if a series is binary (only 0 and 1 present)
def is_binary(series):
    vals = series.dropna().unique()
    return set(vals) == {0, 1} or set(vals) == {0.0, 1.0}

# Function to compute SMD for binary variables for a specific level.
def compute_binary_smd_level(data, group1, group2, col, level):
    """
    For a binary variable, compute the SMD for the given level.
    The proportion for the level is computed as the mean of the indicator (1 if value equals level).
    """
    p1 = (data.loc[data[group_var] == group1, col] == level).mean()
    p2 = (data.loc[data[group_var] == group2, col] == level).mean()
    # Calculate pooled variance for proportions:
    var_pooled = (p1 * (1 - p1) + p2 * (1 - p2)) / 2
    return np.abs(p1 - p2) / np.sqrt(var_pooled) if var_pooled != 0 else np.nan

# Function to compute SMD for continuous numeric variables.
def compute_numeric_smd(data, group1, group2, col):
    x1 = data.loc[data[group_var] == group1, col]
    x2 = data.loc[data[group_var] == group2, col]
    mean1 = x1.mean()
    mean2 = x2.mean()
    sd1 = x1.std()
    sd2 = x2.std()
    pooled_sd = np.sqrt((sd1**2 + sd2**2) / 2)
    return np.abs(mean1 - mean2) / pooled_sd if pooled_sd != 0 else np.nan

# Function to compute SMD for categorical variables (non-binary).
def compute_categorical_smd(data, group1, group2, col, level):
    """
    For a categorical (non-binary) variable,
    compute the SMD for the given level by comparing the proportions of that level.
    """
    p1 = (data.loc[data[group_var] == group1, col] == level).mean()
    p2 = (data.loc[data[group_var] == group2, col] == level).mean()
    var_pooled = (p1 * (1 - p1) + p2 * (1 - p2)) / 2
    return np.abs(p1 - p2) / np.sqrt(var_pooled) if var_pooled != 0 else np.nan

# Identify numeric and categorical columns (excluding the grouping variable)
numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()

if group_var in numeric_cols:
    numeric_cols.remove(group_var)
if group_var in categorical_cols:
    categorical_cols.remove(group_var)

results = []

# Loop over the group pairs and calculate the SMDs for each predictor variable
for g1, g2 in group_pairs:
    # Process numeric columns
    for col in numeric_cols:
        if is_binary(df[col]):
            # For a binary numeric variable, calculate for both levels (0 and 1)
            for level in [0, 1]:
                smd = compute_binary_smd_level(df, g1, g2, col, level)
                results.append({
                    'Variable': col,
                    'Level': level,
                    'Group_Comparison': f"{g1} vs {g2}",
                    'SMD': smd
                })
        else:
            # For continuous numeric variables, use the usual calculation.
            smd = compute_numeric_smd(df, g1, g2, col)
            results.append({
                'Variable': col,
                'Level': None,
                'Group_Comparison': f"{g1} vs {g2}",
                'SMD': smd
            })
    
    # Process categorical columns
    for col in categorical_cols:
        levels = df[col].dropna().unique()
        # Debug: print the variable and its levels to check what values are present.
        print(f"Processing categorical variable '{col}' with levels: {levels}")
        if set(levels) == {0, 1} or set(levels) == {0.0, 1.0}:
            # If the variable is binary, compute for both levels.
            for level in [0, 1]:
                smd = compute_binary_smd_level(df, g1, g2, col, level)
                results.append({
                    'Variable': col,
                    'Level': level,
                    'Group_Comparison': f"{g1} vs {g2}",
                    'SMD': smd
                })
        else:
            # For non-binary categorical variables (e.g., gender, ethnic), compute SMD for each level.
            for lev in levels:
                smd = compute_categorical_smd(df, g1, g2, col, lev)
                results.append({
                    'Variable': col,
                    'Level': lev,
                    'Group_Comparison': f"{g1} vs {g2}",
                    'SMD': smd
                })

# Create a summary DataFrame with the results
results_df = pd.DataFrame(results)

# Display the resulting DataFrame
print(results_df)

# Save the summary table to a CSV file
results_df.to_csv('SMD.csv', index=False)


# COMMAND ----------

print(results_df)


# COMMAND ----------

columns = ['patid',
 'gender',
 'Outcome',
 'oral_steroids',
 'diuretics',
 'angiotension_receptor_blockers',
 'ace_inhibitors',
 'calcium_channel_blockers',
 'beta_blockers',
 'fibrates',
 'fusidic_acid',
 'colchicine',
 'cyclosporin',
 'hiv_hcv_protease_inhibitors',
 'age_at_statin_exposure',
 'BMI',
 'hypertension',
 'hypothyroid',
 'hyperthyroid',
 'asthma',
 'diabetes',
 'copd',
 'atrial_fibrillation',
 'atherosclerotic',
 'smoke',
 'alcohol',
 'Ethnic',
 'statins']

categorical = [
 'hypertension',
 'hypothyroid',
 'hyperthyroid',
 'asthma',
 'diabetes',
 'copd',
 'atrial_fibrillation',
 'atherosclerotic',
 'smoke',
 'alcohol']

continuous = ['BMI']

groupby = 'statins'

mytable = TableOne(data_pandas, columns, categorical, continuous, groupby, pval=True)

print(mytable.tabulate(tablefmt="github"))
mytable.to_csv('mytable_by_statin_risk.csv')

# COMMAND ----------

from pyspark.sql.functions import col

# Group by both 'outcome' and 'statins' to get subgroup counts
outcome_statins_freq = data.groupBy("outcome", "statins").count()

# Calculate the total count per 'outcome' category
outcome_totals = data.groupBy("outcome").count().withColumnRenamed("count", "total")

# Join subgroup counts with the outcome totals on 'outcome'
joined_df = outcome_statins_freq.join(outcome_totals, on="outcome")

# Calculate the percentage of each statin group within each outcome category
result = joined_df.withColumn("percentage", (col("count") / col("total")) * 100)

display(result)


# COMMAND ----------

statins_5_freq = data.filter((col("statins") == 5) & col("Outcome").isin(0, 1, 2)).groupBy("Outcome").count()
total_statins_5 = data.filter((col("statins") == 5) & col("Outcome").isin(0, 1, 2)).count()
#statins_5_freq = statins_5_freq.withColumn("percentage", (col("count") / total_statins_5) * 100)
#display(statins_5_freq)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, sum as Fsum
from pyspark.ml.stat import ChiSquareTest
from pyspark.ml.feature import StringIndexer, VectorAssembler
from scipy.stats import ttest_ind
import matplotlib.pyplot as plt
import itertools


class DataAnalysis:
    """
    A class for performing exploratory data analysis on a dataset using PySpark.
    
    This class provides functionality for loading, preprocessing, and analyzing data,
    including statistical tests and visualization.
    
    Attributes:
        spark (SparkSession): The Spark session used for data processing.
        file_path (str): Path to the dataset file.
        df (DataFrame): The original raw dataset.
        final_df (DataFrame): The preprocessed dataset used for analysis.
    """
    
    def __init__(self, file_path):
        """
        Initialize the DataAnalysis class with a file path.
        
        Args:
            file_path (str): Path to the CSV dataset file.
        
        Returns:
            None
        """
        self.spark = SparkSession.builder.appName("EDA").getOrCreate()
        self.file_path = file_path
        self.df = self.load_data()
        self.final_df = None

    def load_data(self):
        """
        Load the dataset from the file path specified during initialization.
        
        Reads a CSV file with headers and creates a Spark DataFrame.
        
        Args:
            None
        
        Returns:
            DataFrame: The loaded Spark DataFrame or None if loading fails.
        
        Raises:
            Exception: Prints error message if dataset loading fails.
        """
        print("\nLoading dataset...")
        try:
            df = self.spark.read.format("csv").option("header", "true").load(self.file_path)
            print(f"Dataset loaded successfully with {df.count()} rows and {len(df.columns)} columns.")
            return df
        except Exception as e:
            print(f"Error loading dataset: {e}")
            return None

    def preprocess_data(self):
        """
        Clean and prepare the data for analysis.
        
        Performs the following operations:
        1. Removes rows with missing values
        2. Converts categorical variables to numerical using StringIndexer
        3. Casts numerical variables to appropriate types
        4. Renames columns for clarity
        
        Args:
            None
        
        Returns:
            None: Updates self.final_df with the preprocessed data.
        """
        print("\nPreprocessing data...")
        
        # Drop rows with missing values
        self.final_df = self.df.dropna(how='any')
        print(f"Final dataset after removing missing values: {self.final_df.count()} rows.")

        # Convert categorical variables to integers using StringIndexer
        categorical_columns = [c for c in self.final_df.columns if self.final_df.select(c).distinct().count() < 20 and c not in ["age_at_statin_exposure", "BMI", "Outcome"]]

        for category_col in categorical_columns:
            indexer = StringIndexer(inputCol=category_col, outputCol=f"{category_col}_index", handleInvalid="keep")
            self.final_df = indexer.fit(self.final_df).transform(self.final_df).drop(category_col).withColumnRenamed(f"{category_col}_index", category_col)

        # Convert all numerical variables to appropriate types
        self.final_df = self.final_df.select(
            *(col(c).cast("double").alias(c) if c in ["age_at_statin_exposure", "BMI"]
              else col(c).cast("integer").alias(c) for c in self.final_df.columns)
        )

        # Rename columns
        self.final_df = self.final_df.withColumnRenamed("age_at_statin_exposure", "age")

    def plot_histograms(self):
        """
        Generate and display histograms for BMI and Age variables.
        
        Creates a figure with two histograms side by side:
        1. Distribution of BMI values
        2. Distribution of Age values
        
        Args:
            None
        
        Returns:
            None: Displays the histogram plots.
        """
        print("\nGenerating histograms...")
        df_pandas = self.final_df.select('BMI', 'age').dropna().toPandas()

        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        axes[0].hist(df_pandas['BMI'], edgecolor='black', bins=100)
        axes[0].set_title('Histogram of BMI')
        axes[0].set_xlabel('BMI')
        axes[0].set_ylabel('Frequency')

        axes[1].hist(df_pandas['age'], edgecolor='black', bins=100)
        axes[1].set_title('Histogram of Age')
        axes[1].set_xlabel('Age')
        axes[1].set_ylabel('Frequency')

        plt.tight_layout()
        plt.show()

    def plot_boxplots(self):
        """
        Generate and display boxplots for BMI and Age variables by Outcome group.
        
        Creates a figure with two boxplots:
        1. BMI distribution across different Outcome groups
        2. Age distribution across different Outcome groups
        
        Args:
            None
        
        Returns:
            None: Displays the boxplot figures.
        """
        print("\nGenerating boxplots...")
        df_pandas = self.final_df.select('BMI', 'age', 'Outcome').dropna().toPandas()

        fig, axes = plt.subplots(2, 1, figsize=(12, 10))
        sns.boxplot(x='Outcome', y='BMI', data=df_pandas, ax=axes[0])
        axes[0].set_title('Box Plot of BMI by Outcome Group')

        sns.boxplot(x='Outcome', y='age', data=df_pandas, ax=axes[1])
        axes[1].set_title('Box Plot of Age by Outcome Group')

        plt.tight_layout()
        plt.show()

    def plot_categorical_counts(self):
        """
        Generate and display bar charts for all categorical variables in the dataset.
        
        Creates a multi-panel figure with bar charts showing the frequency distribution
        of each categorical variable. Variables are arranged in a grid with 3 columns.
        
        Args:
            None
        
        Returns:
            None: Displays the bar chart figure.
        """
        print("\nGenerating bar charts for categorical variables...")
        columns_to_plot = [col for col in self.final_df.columns if col not in ['BMI', 'age', 'patid', 'Ethnic', 'Outcome', 'statins']]
        num_plots = len(columns_to_plot)
        num_cols = 3
        num_rows = (num_plots + num_cols - 1) // num_cols

        plt.figure(figsize=(15, 30))
        for i, column in enumerate(columns_to_plot, 1):
            value_counts_df = self.final_df.groupBy(column).count().orderBy('count', ascending=False)
            value_counts_pandas = value_counts_df.toPandas()

            if value_counts_pandas.empty:
                print(f"Skipping {column}, as it has no data.")
                continue  # Skip plotting if there's no data

            plt.subplot(num_rows, num_cols, i)
            bars = plt.bar(value_counts_pandas[column].astype(str), value_counts_pandas['count'])
            plt.title(f'Bar Chart of {column}')
            plt.ylabel('Count')

            # Ensure only integer tick labels are displayed correctly for y-axis
            y_max = value_counts_pandas['count'].max() if not value_counts_pandas.empty else 1
            y_step = y_max // 10 if y_max >= 10 else 1  # Ensure step size is at least 1
            plt.yticks(np.arange(0, y_max + 1, step=y_step))

            # Rotate x-axis labels to avoid overlap
            plt.xticks(rotation=90)

        plt.tight_layout()
        plt.show()

    def chi_square_tests(self):
        """
        Perform Chi-Square tests for all categorical variables against outcome pairs.
        
        For each categorical variable and each possible pair of outcome values,
        runs a Chi-Square test of independence and collects the results.
        
        Args:
            None
        
        Returns:
            None: Saves results to a CSV file named 'chi_square_results.csv'.
        """
        print("\nPerforming Chi-Square tests...")
        categorical_columns = [c for c in self.final_df.columns if c not in ['patid', 'BMI', 'age', 'Outcome']]
        indexed_columns = {}

        for col_name in categorical_columns:
            index_col_name = f"{col_name}_index"
            if index_col_name not in self.final_df.columns:
                indexer = StringIndexer(inputCol=col_name, outputCol=index_col_name, handleInvalid="keep")
                self.final_df = indexer.fit(self.final_df).transform(self.final_df)
            indexed_columns[col_name] = index_col_name

        results = []
        outcome_pairs = [(0, 1), (0, 2), (1, 2)]
        for category_col in categorical_columns:
            for outcome1, outcome2 in outcome_pairs:
                self.chi_square_test(category_col, indexed_columns[category_col], outcome1, outcome2, results)

        results_df = pd.DataFrame(results, columns=["Category", "Outcome1", "Outcome2", "Chi-Square Statistic", "p-value"])
        results_df.to_csv("chi_square_results.csv", index=False)
        print("Chi-Square test results saved to chi_square_results.csv")

    def chi_square_test(self, category_col, category_index, outcome1, outcome2, results):
        """
        Helper function to run a Chi-Square test for a specific categorical variable
        and a pair of outcome values.
        
        Args:
            category_col (str): Name of the original categorical column.
            category_index (str): Name of the indexed version of the categorical column.
            outcome1 (int): First outcome value to compare.
            outcome2 (int): Second outcome value to compare.
            results (list): List to append the test results to.
        
        Returns:
            None: Appends results to the input results list.
        """
        filtered_df = self.final_df.filter(col("Outcome").isin([outcome1, outcome2]))

        if filtered_df.count() < 2:
            return

        assembler = VectorAssembler(inputCols=[category_index], outputCol="features")
        transformed_df = assembler.transform(filtered_df)
        chi_sq_test = ChiSquareTest.test(transformed_df, "features", "Outcome").collect()[0]

        results.append([category_col, outcome1, outcome2, chi_sq_test[0], chi_sq_test[1]])

    def perform_t_tests(self):
        """
        Perform pairwise t-tests for age and BMI across different outcome groups.
        
        For each pair of outcome groups, runs a Welch's t-test (unequal variance)
        to compare the means of age and BMI.
        
        Args:
            None
        
        Returns:
            None: 
                - Prints the test results to console
                - Saves results to a CSV file named 't_test_results.csv'
        """
        print("\nPerforming T-tests...")
        pdf = self.final_df.toPandas()

        def pairwise_t_test(df, variable, group_col):
            """
            Helper function to perform pairwise t-tests between groups.
            
            Args:
                df (DataFrame): Pandas DataFrame containing the data.
                variable (str): Name of the numerical variable to test.
                group_col (str): Name of the column containing group labels.
            
            Returns:
                DataFrame: Results of the pairwise t-tests.
            """
            groups = df[group_col].unique()
            results = []
            for i in range(len(groups)):
                for j in range(i + 1, len(groups)):
                    group1 = df[df[group_col] == groups[i]][variable]
                    group2 = df[df[group_col] == groups[j]][variable]
                    t_stat, p_val = ttest_ind(group1, group2, equal_var=False)
                    results.append({'Variable': variable, 'Group1': groups[i], 'Group2': groups[j], 'T-Statistic': t_stat, 'P-Value': p_val})
            return pd.DataFrame(results)

        # Perform T-tests for age and BMI
        age_results = pairwise_t_test(pdf, 'age', 'Outcome')
        bmi_results = pairwise_t_test(pdf, 'BMI', 'Outcome')

        # Combine results into a single DataFrame
        t_test_results = pd.concat([age_results, bmi_results], ignore_index=True)

        # Save to CSV file
        t_test_results.to_csv("t_test_results.csv", index=False)
        print("T-test results saved to 't_test_results.csv'")

        # Print results
        print(age_results)
        print(bmi_results)

    def pairwise_outcome_chi_square_by_ethnic(self, output_csv_path="pairwise_outcome_chi_square_by_ethnic.csv"):
        """
        Perform Chi-Square tests for outcome distribution within each ethnic group.
        
        For each distinct ethnic group, filters the dataset to that group, then for every 
        pair of outcome values present within that ethnic group, performs a chi-square test
        to assess whether the distribution of outcomes differs significantly.
        
        Args:
            output_csv_path (str, optional): Path to save the results CSV file. 
                                           Defaults to "pairwise_outcome_chi_square_by_ethnic.csv".
        
        Returns:
            DataFrame: Pandas DataFrame containing test results with columns:
                       - Ethnic: The ethnic group
                       - Outcome1: First outcome in the pair
                       - Outcome2: Second outcome in the pair
                       - ChiSquareStatistic: The chi-square test statistic
                       - DegreesOfFreedom: Degrees of freedom for the test
                       - pValue: p-value for the test
                       
            None: If self.final_df is None (preprocessing has not been run).
            
        Side effects:
            Saves the results to a CSV file at the specified path.
        """
        if self.final_df is None:
            print("final_df is None. Please run preprocess_data() first.")
            return None
        
        # Ensure Outcome is cast to integer
        df = self.final_df.withColumn("Outcome", col("Outcome").cast("integer"))
        
        # Get distinct ethnic groups (assuming column name is "ethnic")
        ethnic_groups = [row["ethnic"] for row in df.select("ethnic").distinct().collect()]
        print("Distinct Ethnic groups:", ethnic_groups)
        
        results = []
        for ethnic in ethnic_groups:
            print(f"\nProcessing ethnic group: {ethnic}")
            df_ethnic = df.filter(col("ethnic") == ethnic)
            
            # Get distinct outcomes for this ethnic group
            outcome_list = [row["Outcome"] for row in df_ethnic.select("Outcome").distinct().collect()]
            print(f"  Distinct outcomes in {ethnic}: {outcome_list}")
            
            # Generate pairwise combinations of outcomes
            outcome_pairs = list(itertools.combinations(outcome_list, 2))
            
            for outcome1, outcome2 in outcome_pairs:
                print(f"  Testing Outcome pair: {outcome1} vs {outcome2}")
                df_subset = df_ethnic.filter(col("Outcome").isin(outcome1, outcome2))
                if df_subset.count() < 2:
                    print("    Not enough data for this outcome pair, skipping.")
                    continue
                
                df_subset = df_subset.withColumn("is_outcome1", when(col("Outcome") == outcome1, 1.0).otherwise(0.0))
                assembler = VectorAssembler(inputCols=["is_outcome1"], outputCol="features")
                df_subset = assembler.transform(df_subset)
                
                try:
                    chi_result = ChiSquareTest.test(df_subset, "features", "is_outcome1").head()
                    chi_stat = chi_result["statistics"][0]
                    dof = chi_result["degreesOfFreedom"][0]
                    p_value = chi_result["pValues"][0]
                    
                    results.append((ethnic, outcome1, outcome2, chi_stat, dof, p_value))
                except Exception as e:
                    print(f"    Error while testing outcome pair {outcome1} vs {outcome2}: {e}")
        
        results_df = pd.DataFrame(results, columns=["Ethnic", "Outcome1", "Outcome2", 
                                                    "ChiSquareStatistic", "DegreesOfFreedom", "pValue"])
        results_df.to_csv(output_csv_path, index=False)
        print(f"\nPairwise outcome chi-square test results saved to CSV file: {output_csv_path}")
        
        return results_df

# Run Analysis
file_path = "dbfs:/mnt/data/cprd_statin_data/final_dataset_output.csv"
analysis = DataAnalysis(file_path)
analysis.preprocess_data()
analysis.plot_histograms()
analysis.plot_boxplots()
analysis.plot_categorical_counts()
analysis.chi_square_tests()
analysis.perform_t_tests()
analysis.pairwise_outcome_chi_square_by_ethnic()