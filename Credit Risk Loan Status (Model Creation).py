# Databricks notebook source
#importing cleaned dataset:
df = spark.table('credit_risk_cleaned')
display(df)

# COMMAND ----------

#checking the number of rows:
df.count()

# COMMAND ----------

# DBTITLE 1,Creating the ML Pipeline
#importing the libraries
from pyspark.ml.feature import StandardScaler,RFormula
from pyspark.ml import Pipeline

#separatng numeric and categorical columns columns:
num_cols = [i for (i,j) in  df.dtypes if i != 'loan_status' and j != 'string']
print(num_cols)
cat_cols = [i for (i,j) in  df.dtypes if i != 'loan_status' and j == 'string']
print(cat_cols)


# COMMAND ----------

#creating standard scaler and rformula object
rform = RFormula(formula="loan_status ~ .",
                 handleInvalid='skip',
                 featuresCol='enc_features',
                 labelCol='loan_status')

sc = StandardScaler(inputCol='enc_feautures',
                    outputCol='sc_features')
                     

# COMMAND ----------

#creating the pipeline
from pyspark.ml.classification import LogisticRegression
algo = LogisticRegression()

stages = [rform,sc,algo]
pipeline = Pipeline(stages=stages)



# COMMAND ----------

#data spliting:
train,val,test = df.randomSplit([0.6,0.2,0.2])


# COMMAND ----------


