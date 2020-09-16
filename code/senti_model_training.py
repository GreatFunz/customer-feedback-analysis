# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 09:26:52 2020

@author: haruna
"""

import findspark
findspark.init()
findspark.find()

###Spark session
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn

spark = (SparkSession.builder
         .appName("ModelTraining")
         #.config("spark.executor.memory", "4g")
         .getOrCreate()
         )

##import libraries and modules
import html

from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover, IDF
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

###### replace with your system user profile name in the below paths #######
timestampFormat = "EEE MMM dd HH:mm:ss zzz YYYY "
schema = '_c0 INT, _c1 LONG, _c2 STRING, _c3 STRING, _c4 STRING, _c5 STRING' 
train_data_path = "C:\\Users\\haruna\\Twitter_Project\\training_data\\training.1600000.processed.noemoticon.csv"
train_data_partioned_path = "C:\\Users\\haruna\\Twitter_Project\\training_data_part\\"
model_path = "C:\\Users\\haruna\\Twitter_Project\\senti_trained_model\\"

### Clean data function ###
@udf
def unescape_html(string_type: str):
    if isinstance(string_type, str):
        return  html.unescape(string_type)

def clean_tweetsdata(DataFrame_instance):
    # create regex definition
    regexp_url = r"(((https?):((//)|(\\\\))|(www.))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)"
    regexp_email = r"\"?([-\w.`?{}]+@(\w+\.)?\w+\.\w+)\"?"
    regexp_username = r"\b?\@\w{1,15}[:'.\s]?\b"
    regexp_hashtag = r"#"
    regexp_num = r"\b\d+\b"
    regexp_doublespace = r"[\\b]?\s\s[\\b]?"
  
    #create a new column text and clean using regexp  and timestamp definitions 
    cleaned_data = (DataFrame_instance
                  .withColumn("text", fn.regexp_replace(fn.col("text"), regexp_url, ""))
                  .withColumn("text", fn.regexp_replace(fn.col("text"), regexp_email, ""))
                  .withColumn("text", fn.regexp_replace(fn.col("text"), regexp_username, ""))
                  .withColumn("text", fn.regexp_replace(fn.col("text"), regexp_hashtag, ""))
                  .withColumn("text", fn.regexp_replace(fn.col("text"), regexp_num, ""))
                  .withColumn("text", fn.regexp_replace(fn.col("text"), regexp_doublespace, ""))
                  .withColumn("text", fn.trim(fn.col("text")) )
                  .withColumn("text", unescape_html(fn.col("text")))
                  .dropna(how = "any", subset = "text")
                 )
   
    return cleaned_data

###Load the model training raw data
train_raw_data = spark.read.schema(schema).load(train_data_path, format ='csv')

train_raw_data = train_raw_data.select(
                            fn.col('_c0').alias('label'),
                            fn.col('_c1').alias('id'),
                            fn.col('_c2').alias('timestamp'),
                            fn.col('_c3').alias('query'),
                            fn.col('_c4').alias('user'),
                            fn.col('_c5').alias('text')
                            )
# #### Data summary ####
#train_raw_data.groupBy("label").count().toPandas()
# train_raw_data.summary().toPandas()

# ###Data Visualization ########
# sn.distplot(train_raw_data.select('label').toPandas(), kde=True, rug=True)

# ####Repartition and save #######
# train_raw.repartition(20).write.partitionBy('label').csv(train_data_partioned_path, mode="overwrite")
                                
###Data cleaning and drop empty columns
train_raw_data = train_raw_data.select("label", "text")
clean_train_data = (clean_tweetsdata(train_raw_data)).na.drop().dropDuplicates()
#clean_train_data.groupBy("label").count().toPandas()

######  Data split #############
train_data, validation_data, test_data = clean_train_data.randomSplit([0.98,0.01,0.01], seed=12345)

##Define pipeline
tokenizer = Tokenizer(inputCol="text", outputCol="words")

stopwordsremover = (StopWordsRemover()
                    .setInputCol(tokenizer.getOutputCol())
                    .setStopWords(StopWordsRemover().loadDefaultStopWords("english"))
                    .setOutputCol("stopwords"))

hashingTF = HashingTF(inputCol = stopwordsremover.getOutputCol(), outputCol = "rawFeatures1")

inv_doc_freq = IDF(minDocFreq=5, inputCol = hashingTF.getOutputCol(), outputCol="features")

lr = LogisticRegression()

para_grid = (ParamGridBuilder()
        #.addGrid(hashingTF.numFeatures, [100])#features vector dimension, default value 2^20
        .addGrid(lr.maxIter, [100])
        .addGrid(lr.regParam, [0.01, 0.1]).build())
 
###pipeline as the estimator for the cross validation     
pipeline = Pipeline(stages=[tokenizer, stopwordsremover, hashingTF, inv_doc_freq, lr])

#pipe_model_train =  pipeline.fit(train_data) #######Test this model tomorrow it has been saved

cros_val = (CrossValidator(estimator = pipeline,
                           estimatorParamMaps = para_grid,
                           evaluator = BinaryClassificationEvaluator(),
                           numFolds = 5))

###Train the pipeline model
cros_val_model = cros_val.fit(train_data)

###Save the best trained model #####
pipeline_train_model = cros_val_model.bestModel
pipeline_train_model.write().overwrite().save(model_path)

####Load the model ####
cros_val_model_load = PipelineModel.load(model_path)

###Make a prediction
make_predict = cros_val_model_load.transform(test_data)
make_predict.select( "probability", "prediction", 'label').show()

### Evaluate the performane of the prediction ###
train_model_eval = MulticlassClassificationEvaluator(predictionCol = "prediction", 
                                             labelCol = "label", 
                                             metricName = "accuracy"
                                             )

train_model_eval.evaluate(make_predict) 











