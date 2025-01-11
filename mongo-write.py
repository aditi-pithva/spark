# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 20:52:53 2025

@author: Aditi
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields = line.split('|')
    return Row(user_id= int(fields[0]), 
               age = int(fields[1]),
               gender = fields[2],
               occupation = fields[3],
               zip = fields[4])

if __name__ = "__main__":
    spark = SparkSession.\
        builder.appName("MongoDBIntegration").\
            getOrCreate()
            
    lines = spark.sparkContext.textFile("hdfs:///yser/maria_dev/mongodb/movies.user")
    
    users - lines.map(parseInput)
    usersDataset = spark.createDataFrame(users)
    
    usersDataset.write\
        .format('com.mongodb.spark.sql.DefaultSource')\
        .option("uri", "mongodb://127.0.0.1/moviesdata.users")\
        .mode('append')\
        .save()