# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Data Analysis

# COMMAND ----------

# Start and Initialize SparkR Session
library(SparkR)

sparkR.session(appName = "EDA")


# COMMAND ----------

# Load CSV File
data <- read.df("/mnt/data/cprd_statin_data/final_dataset.csv", 
                source = "csv", 
                header = "true", 
                inferSchema = "true")

# Show data
head(data)

# Check schema
printSchema(data)

# COMMAND ----------

# Ensure data is a dataframe
data <- as.data.frame(data)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling missing data

# COMMAND ----------

# run summary of dataset to check missing data
summary(data)

# COMMAND ----------

# MAGIC %md
# MAGIC based on the summary, only BMI has missing data

# COMMAND ----------

# Calculate the percentage of missingness for BMI
missing_percentage_BMI <- sum(is.na(data$BMI)) / nrow(data) * 100
print(paste('% missing BMI:', missing_percentage_BMI))

# COMMAND ----------

# MAGIC %md
# MAGIC since the missing data is less than 30% but more than 5%, we will drop the NA rows

# COMMAND ----------

# Drop rows with NA values in the BMI column
data <- data[!is.na(data$BMI), ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## inspect distribution of each covariates

# COMMAND ----------

# Plot histograms for BMI and age_at_statin_exposure in subplots with reduced size
par(mfrow=c(1,2), mar=c(4,4,2,1), cex=0.8)  # Set up a 1x2 plotting space with reduced margins and text size

# Plot histogram for BMI
hist(data$BMI, main="Histogram of BMI", xlab="BMI", col="blue", border="black")

# Plot histogram for age_at_statin_exposure
hist(data$age_at_statin_exposure, main="Histogram of Age at Statin Exposure", xlab="Age at Statin Exposure", col="green", border="black")

# Reset plotting space to default
par(mfrow=c(1,1), mar=c(5,4,4,2) + 0.1, cex=1)

# COMMAND ----------

# MAGIC %md
# MAGIC The plots indicated that both variables are not normally distributed

# COMMAND ----------



# Get the list of column names excluding 'BMI' and 'age_at_statin_exposure'
columns_to_plot <- setdiff(names(data), c("BMI", "age_at_statin_exposure"))

# Set up plotting space for interactive visualization
par(mfrow=c(ceiling(length(columns_to_plot)/2), 2), mar=c(4,4,2,1), cex=0.8)

# Loop through each column and plot bar plots
for (col in columns_to_plot) {
  # Check if column is categorical
  if (is.factor(data[[col]]) || is.character(data[[col]])) {
    # Handle missing values
    col_data <- table(data[[col]], useNA="ifany")
    
    # Plot bar chart and display interactively
    print(barplot(col_data, 
                  main=paste("Bar Plot of", col), 
                  xlab=col, 
                  col="lightblue", 
                  border="black"))
    
    # Save the plot as PNG in the specified path
    png(filename = paste0(save_path, col, "_barplot.png"))
    barplot(col_data, 
            main=paste("Bar Plot of", col), 
            xlab=col, 
            col="lightblue", 
            border="black")
    dev.off()
  }
}

# Reset plotting space to default
par(mfrow=c(1,1), mar=c(5,4,4,2) + 0.1, cex=1)


# COMMAND ----------

file.exists(save_path)
