# Databricks notebook source
file_path = dbutils.fs.ls('/FileStore/tables/CreditRisk/credit_risk_data(cleaned).csv/')[-1][0]
file_path

# COMMAND ----------

#importing cleaned dataset:
df = spark.read.csv(file_path,header=True,inferSchema=True)
display(df)

# COMMAND ----------

#checking the number of rows:
df.count()

# COMMAND ----------

# DBTITLE 1,Creating the ML Pipeline
#importing the libraries
from pyspark.ml.feature import StandardScaler,RFormula
from pyspark.ml import Pipeline

#separatng numeric columns:
num_cols = [i for (i,j) in  df.dtypes if i != 'loan_status' and j != 'string']
print(num_cols)

# COMMAND ----------

#creating standard scaler and rformula object


# COMMAND ----------


