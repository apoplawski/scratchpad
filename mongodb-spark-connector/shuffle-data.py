import random
import string
import os
import time

from pymongo import MongoClient

client = MongoClient("mongodb://mongo1:30001,mongo2:30002,mongo3:30003/?replicaSet=my-replica-set")
db = client['spark']
documents = db["test_collection"]

docs = list(documents.find({}).limit(1))

for doc in docs:
    print(f"modifying {doc}")

    result = documents.update_one({ '_id': doc['_id'] }, { '$set':  { "version": doc["version"] + 1 } } )

    time.sleep(2)

    result = documents.update_one({ '_id': doc['_id'] }, { '$set': { "version": doc["version"] + 2 } } )

client.close()
