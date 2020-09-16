# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 10:04:51 2020

@author: haruna
"""

import numpy
import matplotlib.pyplot as plt
import html
import geopandas as gpd

## Initialize pyspark
import findspark
findspark.init()
findspark.find()

from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql import functions as fn
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType


##Craete a SparkSession
spark = (SparkSession.builder
         .appName("FeedbackAnalysis")
         #.config("spark.executor.memory", "4g")
         .getOrCreate()
         )


##File path
income_tweets_path = "C:\\Users\\haruna\\Twitter_Project/haruna_small\\"
city_data_path = "C:/Users/haruna/Twitter_Project/city_data/worldcities.csv"
empty_df_path = "C:/Users/haruna/Twitter_Project/empty_df/empty_df_load.csv"
trained_model_path = "C:/Users/haruna/Twitter_Project/tweets_trained_model/"
timestampFormat="EEE MMM dd HH:mm:ss zzzz yyyy"
 
empty_df_schema = StructType([
   StructField("timestamp", StringType(), True),
   StructField("screen_name", StringType(), True),
   StructField("location", StringType(), True),
   StructField("text", StringType(), True),
   StructField("city", StringType(), True),
   StructField("long", DoubleType(), True),
   StructField("lat", DoubleType(), True),
   StructField("country", StringType(), True)])
             
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

### Datframe filter and merge function
def df_filt_merge(tweets_df: DataFrame, empty_df: DataFrame, world_cities_df: DataFrame):
     ###Convert long column to a list
    world_city_data = world_cities_df.select('city')
    world_city_data = world_city_data.toPandas()
    city_array = numpy.array(world_city_data)
    city_array_conc = numpy.concatenate(city_array).ravel()  
    city_list = city_array_conc.tolist()
    
    ###Convert long column to a list
    world_long_data = world_cities_df.select('long')
    world_long_data = world_long_data.toPandas()
    long_array = numpy.array(world_long_data)
    long_array_conc = numpy.concatenate(long_array).ravel()  
    long_list = long_array_conc.tolist()
    
    ###Convert lat column to a list
    world_lat_data = world_cities_df.select('lat')
    world_lat_data = world_lat_data.toPandas()
    lat_array = numpy.array(world_lat_data)
    lat_array_conc = numpy.concatenate(lat_array).ravel()  
    lat_list = lat_array_conc.tolist()
    
    city_range = range(len(city_list))

    ##Script start here ######
    for i in city_range:
        if i <= len(city_list)-1:
            print(f"Working on number {i}/{len(city_list)}: {city_list[i]}")
            tweets_df_filt = tweets_df.filter(fn.col('location').contains(f"{city_list[i]}")).distinct()
            city_df_filt = world_cities_df.filter((fn.col('city')==(f"{city_list[i]}")) & (fn.col('long')==(f"{long_list[i]}")) & (fn.col('lat')==(f"{lat_list[i]}")))
            icount2 = tweets_df_filt.count()
            
            if icount2 <= 0:
                print("No city found in the search")
               
            elif icount2 == 1:
                try:
                    print("I found one city")
                    tweets_df_filt_join = (tweets_df_filt.join(city_df_filt, tweets_df_filt.location.contains(city_df_filt.city)))
                    print("perforing union of the dataframe")
                    empty_df =  empty_df.union(tweets_df_filt_join).distinct()
                    # rows = empty_df.count()
                    # print(f"I got: {rows} rows dataframe")
                    print("Found row added to the dataframe")
                except Exception as e:
                    print(e)
                    print("Error additing this row to the dataframe")
                
            else:
                try:
                    print("I found morethan one cities, working on it")
                    city_to_array = fn.udf(lambda city : [city] * int(icount2), ArrayType(StringType()))
                    city_df_filt_duplicate = city_df_filt.withColumn('city', city_to_array(city_df_filt.city))
                    city_df_filt_duplicate = city_df_filt_duplicate.withColumn('city', fn.explode(city_df_filt_duplicate.city))
                    tweets_df_filt_join = (tweets_df_filt.join(city_df_filt_duplicate, tweets_df_filt.location.contains(city_df_filt_duplicate.city)))
                    print("perforing union of the dataframe")
                    empty_df =  empty_df.union(tweets_df_filt_join).distinct()
                    # rows = empty_df.count()
                    # print(f"I got: {rows} rows dataframe")
                    print("Found rows added to the dataframe")
                except Exception as e:
                    print(e)
                    print("Error additing these rows to the dataframe")
                    
            i = i+1   
            
    return empty_df


###world cities data downloaded from the internet to enable me get the list of cities in the world and their longitude and latitude
world_city_raw_data = ((spark.read.load(city_data_path, format='csv', inferSchema=True, header=True))
                       .select('city', fn.col('lng').alias('long'), 'lat', 'country')
                       .dropDuplicates().persist(StorageLevel.DISK_ONLY))

## Load empty dataframe with the defined schema ######
empty_df_load = (spark.read
                       .load(empty_df_path, format='csv', schema = empty_df_schema ).na.drop())

#read schema from the raw tweets data
#use this schema structure to define your own schema if necessary
get_schema = spark.read.json(income_tweets_path).limit(5).schema

# Stream the raw tweets data, select the require columns
raw_data_view=(spark.readStream
          .schema(get_schema)
          .json(income_tweets_path)
          .select(fn.col("created_at").alias('timestamp'),
                  fn.col("user.screen_name").alias("screen_name"),
                  fn.col("user.location").alias("location"),"text")
          #.persist(StorageLevel.DISK_ONLY) # persist to disk if you don't have adequate of memory
              )

##Call the clean function to clean the data
clean_data = clean_tweetsdata(raw_data_view)

# ###Aggregation action
# clean_data_agg = clean_data.groupBy('location').count()

##query from the atreming datatable
query = (clean_data.writeStream
         .format('memory')
         .queryName('tweets_data')
         .trigger(processingTime='20 seconds')
         #.outputMode('complete') # to use this, you must perform aggregation action ie as above
         .outputMode('append')
         ).start()

# ###### Check for if streaming  ###
# raw_data_view.isStreaming      
# query.isActive 
  
##query cleaned tweets dataframe from the streaming table
stream_clean_df = ((spark.sql(" select * from tweets_data"))
                  .na.drop(how='any', subset='location')
                  .dropDuplicates())

##### Filter and merge the cleaned tweets and world dataframe
filt_merge_df = ((df_filt_merge(stream_clean_df, empty_df_load, world_city_raw_data))
                 .select("screen_name", "city", "long", "lat", "country", "text"))

####Load the trained sentimental analsis model
trained_model_load = PipelineModel.load(trained_model_path)
make_predict = ((trained_model_load.transform(filt_merge_df))
                .select("screen_name", "city", "long", "lat", "country", "prediction", "text"))

negative_senti = make_predict.filter(fn.col("prediction") ==0)
positive_senti = make_predict.filter(fn.col("prediction") ==4)

## Convert to pandas and Geopandas Dataframe
negative_senti_topandas = negative_senti.toPandas()
positive_senti_topandas = positive_senti.toPandas()

negative_senti_togpd = (gpd.GeoDataFrame
                        (negative_senti_topandas, geometry=gpd.points_from_xy
                         (negative_senti_topandas['long'],negative_senti_topandas['lat'])))

positive_senti_togpd = (gpd.GeoDataFrame
                        (positive_senti_topandas, geometry=gpd.points_from_xy
                         (positive_senti_topandas['long'],positive_senti_topandas['lat'])))

negative_senti_togpd.plot(figsize=(20,10))
positive_senti_togpd.plot(figsize=(20,10))

###Plot worldmap
world=gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
ax=world.plot(figsize=(20,10))
#ax.axis('off')

###Overlap the worldmap  plot
myplot1, ax=plt.subplots(figsize=(20,10)) #myplot1 is the figure name. axis as ax to define myplot1 area, plt is a module and the subplots is a method 
negative_senti_togpd.plot(cmap='jet', ax=ax) #cmap is colormap to compare variance in the plot, you can put None as well
positive_senti_togpd.plot(ax=ax)
world.geometry.boundary.plot( edgecolor=None,linewidth=1,ax=ax)












































