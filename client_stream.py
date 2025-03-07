from base64 import encode
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.sql import Row
from json import dumps
from kafka import KafkaProducer

sc = SparkContext(appName="spark2kafka")
spark = SparkSession(sc)

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: 
                         dumps(x).encode('utf-8'), key_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

def countAndPush(rdd):
    spark=SparkSession \
            .builder \
            .config(conf=rdd.context.getConf()) \
            .getOrCreate()

    rowRdd = rdd.map(lambda x: Row(word=x))
    
    if not rowRdd.isEmpty():
        wordsDF = spark.createDataFrame(rowRdd)

        wordsDF.createOrReplaceTempView("words")
        AggregatedWordCountsDF = spark.sql("select word, count(*) as total from words group by word order by 2 desc")
        AggregatedWordCountsDF.show()
        queryResults = AggregatedWordCountsDF.select("word","total").rdd.flatMap(lambda x: x).collect()
        print(queryResults)
        i=0
        while i <len(queryResults):
            hashtag = queryResults[i]
            count = queryResults[i+1]
            i+=2
            if type(hashtag) == bytes:
                hashtag = hashtag.decode('utf-8')
            print("word: ", hashtag)
            print("count: ", count)
            producer.send(hashtag[1:], count)
            producer.flush()

ssc = StreamingContext(sc,10)
socket_stream = ssc.socketTextStream('localhost', 9008)
lines=socket_stream.window(10)
df=lines.flatMap(lambda x:x.split(" ")).filter(lambda x:x.startswith("#")).filter(lambda x: x.upper() in ['#F1','#NFL','#MLB','#Football','#Soccer'])

df.foreachRDD(countAndPush)
ssc.start()             
ssc.awaitTermination()  