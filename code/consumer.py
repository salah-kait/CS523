#!/usr/bin/python3

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.streaming.kafka import KafkaUtils
import unicodedata
import json

def handle_rdd(rdd):
    if not rdd.isEmpty():
        global ss
        print(rdd)
        df = ss.createDataFrame(rdd, schema=['text', 'words', 'length'])
        df.show()
        df.write.mode("append").insertInto('default.zozo')
        ss.sql("SELECT * FROM default.zozo ORDER BY words desc limit 1").show()


sc = SparkContext(appName="BDT523")
ssc = StreamingContext(sc, 5)

ss = SparkSession.builder \
    .appName("BDT523") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("hive.metastore.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

ss.sparkContext.setLogLevel('WARN')

ss.sql("CREATE TABLE IF NOT EXISTS  default.zozo ( text STRING, words INT, length INT )")

ks = KafkaUtils.createStream(ssc, 'zookeeper:2181', 'Spark-Streaming', {'TWITTER-TWEETS-CS523': 10})
lines = ks.map(lambda v: json.loads(v[1]))
transform = lines.map(lambda tweet: (unicodedata.normalize('NFD', tweet["text"]).encode('ASCII', 'ignore'), int(len(tweet["text"].split())), int(len(tweet["text"]))))
transform.foreachRDD(handle_rdd)

ssc.start()
ssc.awaitTermination()