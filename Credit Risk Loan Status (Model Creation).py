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

sc = StandardScaler(inputCol='enc_features',
                    outputCol='sc_features')
                     

# COMMAND ----------

#applying rformula on train data:
rform_model = rform.fit(train)
enc_df_train = rform_model.transform(train)
enc_df_train.display()


# COMMAND ----------

#applying rform on test data
enc_df_test = rform_model.transform(test)
enc_df_test.display()

# COMMAND ----------

#applying standard scaler:
sc_model = sc.fit(enc_df_train)
sc_train = sc_model.transform(enc_df_train)
sc_train.display()

# COMMAND ----------

#applying standard scaler on test data:
sc_test = sc_model.transform(enc_df_test)
sc_test.display()

# COMMAND ----------

#data spliting:
train,val,test = df.randomSplit([0.6,0.2,0.2])

# COMMAND ----------

#creating the pipeline
from pyspark.ml.classification import LogisticRegression
algo = LogisticRegression(featuresCol='sc_features',
                            labelCol='loan_status',
                            )

stages = [rform,sc,algo]
pipeline = Pipeline(stages=stages)


# COMMAND ----------

#training the model with default hyperparameters:

pipe_model = pipeline.fit(train)
preds = pipe_model.transform(test)
display(preds)

# COMMAND ----------

#evaluating the base model
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(metricName='f1',labelCol='loan_status')
score = evaluator.evaluate(preds)
print(score)

# COMMAND ----------


