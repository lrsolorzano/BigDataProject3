from pyspark import SparkConf, SparkContext
from operator import add
import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'

try:
    import json
except ImportError:
    import simplejson as json

from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

def read_credentials():
    file_name = "/usr/local/credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load credentials.json")
        return None

def read_tweets():

    sc = SparkContext(appName="sentimentProducer")
    ssc = StreamingContext(sc,600)  # Test 60 segundos
    brokers = "localhost:9092"
    kvs = KafkaUtils.createDirectStream(ssc, ["test"], {"metadata.broker.list": brokers})
    kvs.foreachRDD(create_format)
    producer.flush()
    ssc.start()
    ssc.awaitTermination()

def create_format(messages):
    
    stream = twitter_stream.statuses.filter(language='en', track='MAGA,DICTATOR,IMPEACH,DRAIN,SWAMP,COMEY')
    count=0
    for tweet in stream:
        producer.send('sentiment', bytes(json.dumps(tweet, indent=5), "ascii"))
        count+=1
        if(count==15000):
           break

if __name__ == "__main__":

    print("Starting collect tweets ... ")
    credentials = read_credentials()
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    read_tweets()
