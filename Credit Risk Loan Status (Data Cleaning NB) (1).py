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

#checking the number of rows:
df.count()

# COMMAND ----------

# DBTITLE 1,Cleaning the data:
#removing not required columns:
# list of columns to be removed: ('member_id','funded_amnt_inv','batch_enrolled','pymnt_plan','desc','title','zip_code','mths_since_last_delinq','mths_since_last_record','mths_since_last_major_derog','verification_status_joint')

new_df = df.drop('member_id','funded_amnt_inv','batch_enrolled','pymnt_plan','desc','title','zip_code','mths_since_last_delinq','mths_since_last_record','mths_since_last_major_derog','emp_title','verification_status_joint')
display(new_df)

# COMMAND ----------

#checking for the duplicates:
new_df.dropDuplicates().count()

# COMMAND ----------

#checking for missing values:
new_df.na.drop().count()

# COMMAND ----------

#dropping the missing values:
new_df = new_df.na.drop()
display(new_df)

# COMMAND ----------

#checking the values in emp_length column:
display(new_df.groupBy('emp_length').count().orderBy('count',ascending=False))

# COMMAND ----------

#checking the values in purpose column:
display(new_df.groupBy('purpose').count().orderBy('count',ascending=False))

# COMMAND ----------

#cleaning the purpose column:
from pyspark.sql.functions import when, col
new_df = new_df.withColumn('purpose',when(col('purpose').contains(' '),'other').otherwise(col('purpose')))
display(new_df.groupBy('purpose').count().orderBy('count',ascending=False))

# COMMAND ----------

display(new_df)

# COMMAND ----------

#checking the values in addr_state column:
display(new_df.groupBy('addr_state').count().orderBy('count',ascending=False))

# COMMAND ----------

#cleaning the addr_state column:
new_df = new_df.withColumn('addr_state',when(col('addr_state').contains(' '),'other').otherwise(col('addr_state')))
display(new_df.groupBy('addr_state').count().orderBy('count',ascending=False))

# COMMAND ----------

#cleaning dti:
print(new_df.select('dti'))
display(new_df.select('dti').distinct())

# COMMAND ----------

#changing the dtype to float:
new_df = new_df.filter("dti >= 0")
new_df = new_df.withColumn('dti',col('dti').cast('double'))
display(new_df)

# COMMAND ----------

#cleaning delinq_2yrs:
print(new_df.select('delinq_2yrs'))
display(new_df.select('delinq_2yrs').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('delinq_2yrs',col('delinq_2yrs').cast('int'))
display(new_df)

# COMMAND ----------

#cleaning inq_last_6mths:
print(new_df.select('inq_last_6mths'))
display(new_df.select('inq_last_6mths').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('inq_last_6mths',col('inq_last_6mths').cast('int'))
display(new_df)

# COMMAND ----------

#cleaning open_acc:
print(new_df.select('open_acc'))
display(new_df.select('open_acc').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('open_acc',col('open_acc').cast('int'))
display(new_df)

# COMMAND ----------

#cleaning pub_rec:
print(new_df.select('pub_rec'))
display(new_df.select('pub_rec').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('pub_rec',col('pub_rec').cast('int'))
display(new_df)

# COMMAND ----------

#cleaning revol_bal:
print(new_df.select('revol_bal'))
display(new_df.select('revol_bal').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('revol_bal',col('revol_bal').cast('int'))
display(new_df)

# COMMAND ----------

#cleaning revol_util:
print(new_df.select('revol_util'))
display(new_df.select('revol_util').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('revol_util',col('revol_util').cast('int'))
display(new_df)

# COMMAND ----------

#cleaning total_acc:
print(new_df.select('total_acc'))
display(new_df.select('total_acc').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('total_acc',col('total_acc').cast('int'))
display(new_df)

# COMMAND ----------

#cleaning initial_list_status:
print(new_df.select('initial_list_status'))
display(new_df.select('initial_list_status').distinct())

# COMMAND ----------

#cleaning total_rec_int:
print(new_df.select('total_rec_int'))
display(new_df.select('total_rec_int').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('total_rec_int',col('total_rec_int').cast('float'))
display(new_df)

# COMMAND ----------

#cleaning total_rec_late_fee:
print(new_df.select('total_rec_late_fee'))
display(new_df.select('total_rec_late_fee').distinct())

# COMMAND ----------

#changing the dtype to float:
new_df = new_df.withColumn('total_rec_late_fee',col('total_rec_late_fee').cast('float'))
display(new_df)

# COMMAND ----------

#cleaning recoveries:
print(new_df.select('recoveries'))
display(new_df.select('recoveries').distinct())

# COMMAND ----------

#changing the dtype to float:
new_df = new_df.withColumn('recoveries',col('recoveries').cast('float'))
display(new_df)

# COMMAND ----------

#cleaning collection_recovery_fee:
print(new_df.select('collection_recovery_fee'))
display(new_df.select('collection_recovery_fee').distinct())

# COMMAND ----------

#changing the dtype to float:
new_df = new_df.withColumn('collection_recovery_fee',col('collection_recovery_fee').cast('float'))
display(new_df)

# COMMAND ----------

#cleaning collections_12_mths_ex_med:
print(new_df.select('collections_12_mths_ex_med'))
display(new_df.select('collections_12_mths_ex_med').distinct())

# COMMAND ----------

#changing the dtype to int:
new_df = new_df.withColumn('collections_12_mths_ex_med',col('collections_12_mths_ex_med').cast('int'))
display(new_df)

# COMMAND ----------

#cleaning application_type:
print(new_df.select('application_type'))
display(new_df.select('application_type').distinct())

# COMMAND ----------

#cleaning last_week_pay:
print(new_df.select('last_week_pay'))
display(new_df.select('last_week_pay').distinct())

# COMMAND ----------

#cleaning application_type:
print(new_df.select('acc_now_delinq'))
display(new_df.select('acc_now_delinq').distinct())

# COMMAND ----------


