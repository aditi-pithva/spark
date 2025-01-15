# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 17:20:22 2025

@author: Aditi Pithva
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.\
    builder.\
        appName("sparkstream").\
            getOrCreate()
            
lines = (spark.readStream.format('socket')\
          .option('host', 'localhost')\
              .option('port', 9999)\
                  .load())
    
words = lines.select(split(col('value'), '\\s').alias("word"))

counts = words.groupBy('word').count()

checkpointDir = "C:\\checkpoint\\"

streamingQuery = (counts
                  .writeStream
                  .format('console')
                  .outputMode('complete')
                  .trigger(processingTime='1 second')
                  .option('checkpointLocation', checkpointDir)
                  .start())

streamingQuery.awaitTermination()
