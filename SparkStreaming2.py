# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 20:38:37 2025

@author: Aditi Pithva
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *

inputDirectory = "C:/Learning/input"
outputDirectory = "C:/Learning/output"
checkpointDirectory = "C:/Learning/checkpoint"

spark = SparkSession.builder.appName("sparkstream").getOrCreate()

fileSchema = (StructType([
    StructField('userID', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('friends', IntegerType(), True),
    ]))

inputDf = (spark.readStream
           .format('csv')
           .schema(fileSchema)
           .load(inputDirectory))

resultDf = inputDf.select('name', 'friends').where(inputDf.age < 30)

streamingQuery = (resultDf.writeStream
                  .format('csv')
                  .option('path', outputDirectory)
                  .option('checkpointLocation', checkpointDirectory)
                  .start())
