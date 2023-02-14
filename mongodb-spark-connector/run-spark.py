import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

spark = (SparkSession
    .builder
    .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") 
    .config("spark.mongodb.connection.uri", "mongodb://mongo1:30001,mongo2:30002,mongo3:30003/?replicaSet=my-replica-set")
    .config("spark.mongodb.change.stream.publish.full.document.only","true")
    .appName("debug")
    .getOrCreate())

mongo_events = (spark
    .readStream
    .format("mongodb")
    .option("database", "spark")
    .option("collection", "test_collection")
    .load())

write_console = (mongo_events
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start())

write_console.awaitTermination()
