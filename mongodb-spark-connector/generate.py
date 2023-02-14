import pymongo
import os

client = pymongo.MongoClient("mongodb://mongo1:30001,mongo2:30002,mongo3:30003/?replicaSet=my-replica-set")
db = client["spark"]
collection = db["test_collection"]

for i in range(100):
    collection.insert_one({
        "version": 1
    })
