# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 22:46:50 2025

@author: Aditi
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

if __name == "__main__":
    
    spark = SparkSession.\
        builder.appName("MongoDBIntegration").\
        .getOrCreate()
        
    readUsers = spark.read\
        .format('com.mongodb.spark.sql.DefaultSaurce')\
        .option('uri', 'mongodb://127.0.0.1/moviesdata.users')\
        .load()
        
    readUsers.createOrReplaceTempView("users")
    
    readUsers.printSchema()
    
    sqlDF = spark.sql("""
                      SELECT occupation, count(user_id) as cnt_usr 
                      FROM users
                      GROUP BY occupation
                      ORDER BY cnt_usr DESC
                      """)
    sqlDF.show()