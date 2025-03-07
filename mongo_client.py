from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time

def main():
    try:
        client = MongoClient('localhost',27017)
        db = client.temp_data
        print("Connection established at port 27017.")
    except:  
        print("Connection failed.")
        return    
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    consumer.subscribe(["F1","NFL","MLB","Football","Soccer"])

    for msg in consumer:
        name = msg.topic
        counts = int(msg.value)      
        timeStamp = int(time.time())
        try:
            toBeInserted = {
                "name" : name,
                "count": counts,
                "time": timeStamp

            }
            id = db.hashtags.insert_one(toBeInserted)
            print("Data: {} inserted with id:".format(toBeInserted, id))
        except:
            print("Mongo insertion failed.")

def batchProcessing(initialTimeStamp, finalTimeStamp):
    MongoClient = MongoClient('localhost',27017)
    db = MongoClient.temp_data
    query= None
    if type(initialTimeStamp) == int and type(finalTimeStamp) == int:
        query = {"time": {"$gte": initialTimeStamp, "$lte": finalTimeStamp}}
    else:
        query = {"time": {"$gte": initialTimeStamp, "$lte": finalTimeStamp}}
    queryOutput = db.hashtags.find(query)
    resultDic = {}
    for i in queryOutput:
        if i["name"] in resultDic:
            resultDic[i["name"]] += i["count"]
        else:
            resultDic[i["name"]] = i["count"]
    return resultDic

if __name__ == "__main__":
    main()
