# Databricks notebook source
#importing the datasets:
df = spark.read.csv("/FileStore/tables/CreditRisk/credit_risk_data.csv",header=True,inferSchema=True)
display(df)

# COMMAND ----------

#summarizing the dataset:
dbutils.data.summarize(df)

# COMMAND ----------

#checking the datatypes of columns:
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Cleaning the data:
#removing not required columns:
# list of columns to be removed: ('member_id','funded_amnt_inv','batch_enrolled','pymnt_plan','desc','title','zip_code','mths_since_last_delinq','mths_since_last_record','mths_since_last_major_derog','verification_status_joint')

new_df = df.drop('member_id','funded_amnt_inv','batch_enrolled','pymnt_plan','desc','title','zip_code','mths_since_last_delinq','mths_since_last_record','mths_since_last_major_derog','emp_title','verification_status_joint')
display(new_df)

# COMMAND ----------

display(new_df.select('term').value_counts())

# COMMAND ----------


