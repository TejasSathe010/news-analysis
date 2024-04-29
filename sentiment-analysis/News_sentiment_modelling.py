# Databricks notebook source
# MAGIC %md
# MAGIC # Multilayer perceptron classifier for news sentiment

# COMMAND ----------

# MAGIC %md
# MAGIC The financial news sentiment dataset has about 4846 records of financial news and corresponinding sentiment. The sentiments include 'positive', 'neutral' and 'negative'. 
# MAGIC The below code will load, precess data and use the mulilayer perceptron .

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a spark session and load the Financial News Data set

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('FinancialNewsDL').getOrCreate()

# COMMAND ----------

#spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

file_location = "/FileStore/finalproject/FinancialNewsSentiment.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "false"
delimiter = ","

df_news = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)\

#end loading

# COMMAND ----------

df_news = df_news.withColumnRenamed("_c0","label")
df_news = df_news.withColumnRenamed("_c1","news")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data processing

# COMMAND ----------

df_news = df_news.dropna()

# COMMAND ----------

# Create a 70-30 train test split

train_data,test_data=df_news.randomSplit([0.7,0.3])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building the MultiLayerPerceptron model

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

# Index label column as it is category variable - positive, negative, neutral
from pyspark.ml.feature import StringIndexer
labelIndexer = StringIndexer(inputCol="label",outputCol="indexedLabel")
#labelIndexer = labelIndexer.fit(df_news)

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF, Tokenizer

tokenizer = Tokenizer(inputCol="news", outputCol="words")

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures= 4000)

idf = IDF(inputCol="rawFeatures", outputCol="features")

# COMMAND ----------

# Import the required libraries
from pyspark.ml.classification import MultilayerPerceptronClassifier

# COMMAND ----------

# Create an object for the MultilayerPerception model

layers = [4000,128,64,3]
mlpModel = MultilayerPerceptronClassifier(maxIter=50, layers= layers, seed=1984, blockSize=128, featuresCol="features", labelCol= "indexedLabel")

# COMMAND ----------

# Pipeline is used to pass the data through indexer, tokenizer, TF-IDF simultaneously. Also, it helps to pre-rocess the test data in the same way as that of the train data. It also 
from pyspark.ml import Pipeline

pipe = Pipeline(stages=[labelIndexer,tokenizer,hashingTF,idf,mlpModel])


# COMMAND ----------

fit_model=pipe.fit(train_data)

# COMMAND ----------

# Store the results in a dataframe

result = fit_model.transform(test_data)

# COMMAND ----------

result.select("indexedLabel","prediction").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluating the model

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Accuracy

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
pred_and_actual = result.select("prediction","indexedLabel").withColumnRenamed("indexedLabel","label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("The accuracy of the model is {}".format(evaluator.evaluate(pred_and_actual)))
#print(str(evaluator.evaluate(pred_and_actual)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Confusion Matrix

# COMMAND ----------

from pyspark.sql.types import FloatType
from pyspark.mllib.evaluation import MulticlassMetrics
#important: need to cast to float type, and order by prediction, else it won't work
pred_and_actual = result.select(['prediction','indexedLabel']).withColumn('label', f.col('indexedLabel').cast(FloatType())).orderBy('prediction')

#select only prediction and label columns
pred_and_actual = pred_and_actual.select(['prediction','label'])

metrics = MulticlassMetrics(pred_and_actual.rdd.map(tuple))

#print(metrics.confusionMatrix().toArray())
print("Below is the confusion matrix \n {}".format(metrics.confusionMatrix().toArray()))

# COMMAND ----------

# MAGIC %md
# MAGIC #####  Area under the ROC

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
AUC_evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='indexedLabel',metricName='areaUnderROC')
AUC = AUC_evaluator.evaluate(result)
print("The area under the curve is {}".format(AUC))

# COMMAND ----------

# MAGIC %md
# MAGIC A roughly 75% area under ROC denotes the model has performed reasonably well in predicting the sentiment of financial news

# COMMAND ----------

# MAGIC %md
# MAGIC #####  Area under the PR

# COMMAND ----------

PR_evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='indexedLabel',metricName='areaUnderPR')
PR = PR_evaluator.evaluate(result)
print("The area under the PR curve is {}".format(PR))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Model

# COMMAND ----------

#set / load basePath
basePath = "/FileStore/finalproject"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Save the pipeline

# COMMAND ----------

pipe.write().overwrite().save(basePath + "/pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Save trained model (pipeline)

# COMMAND ----------

fit_model.write().overwrite().save(basePath + "/model")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Model for new prediction

# COMMAND ----------

#Load / set basePath
basePath = "/FileStore/finalproject"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load the pipeline

# COMMAND ----------

from pyspark.ml import Pipeline
pipe_new = Pipeline.load(basePath + "/pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load the trained model (pipeline)

# COMMAND ----------

from pyspark.ml import PipelineModel
load_fit_model = PipelineModel.load(basePath + "/model")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Test Loading trained model. To be removed in the product

# COMMAND ----------

new_result = load_fit_model.transform(test_data)

# COMMAND ----------

pred_and_actual = new_result.select("prediction","indexedLabel").withColumnRenamed("indexedLabel","label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("The accuracy of the model is {}".format(evaluator.evaluate(pred_and_actual)))

# COMMAND ----------

pred_and_actual = new_result.select(['prediction','indexedLabel']).withColumn('label', f.col('indexedLabel').cast(FloatType())).orderBy('prediction')

#select only prediction and label columns
pred_and_actual = pred_and_actual.select(['prediction','label'])

metrics = MulticlassMetrics(pred_and_actual.rdd.map(tuple))

#print(metrics.confusionMatrix().toArray())
print("Below is the confusion matrix \n {}".format(metrics.confusionMatrix().toArray()))
