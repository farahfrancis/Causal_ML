# Databricks notebook source
# Load required libraries
library(SparkR)
library(tmle)
library(data.table)

# Initialize Spark session
sparkR.session(master = "local[*]", appName = "TMLE_Two_statins")


# COMMAND ----------

# Load your data into Spark DataFrame (replace with actual data)
data <- read.df("path/to/your/data.csv", source = "csv", header = TRUE, inferSchema = TRUE)

# Collect data to R for TMLE processing (TMLE operates in-memory)
data_r <- as.data.frame(collect(data))

#drop patid


# COMMAND ----------


# Assuming the dataset has the following columns:
# - W: Covariates (e.g., baseline characteristics)
# - A: Treatment (binary, e.g., 0 and 1 for two treatments)
# - Y: Outcome variable

# Define the columns
W <- data_r[, c("W1", "W2", "W3")]  # Replace W1, W2, W3 with your covariate column names
A <- as.numeric(data_r$A)  # Treatment variable
Y <- as.numeric(data_r$Y)  # Outcome variable

# TMLE estimation
# Fit initial estimators for outcome regression (Q) and propensity score (g)
tmle_fit <- tmle(Y = Y, A = A, W = W, family = "binomial")

# Summary of results
summary(tmle_fit)

# Output causal risk difference, relative risk, and odds ratio
cat("Causal Risk Difference:", tmle_fit$estimates$ATE$psi, "\n")
cat("Relative Risk:", tmle_fit$estimates$RR$psi, "\n")
cat("Odds Ratio:", tmle_fit$estimates$OR$psi, "\n")

# Stop Spark session
sparkR.session.stop()
