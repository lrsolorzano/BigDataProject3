import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Word2Vec
from pyspark.ml.classification import LogisticRegression
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import sys
import csv

from pyspark.sql import Row, SparkSession
try:
    import json
except ImportError:
    import simplejson as json


#def getSparkSessionInstance(sparkConf):
#    if ('sparkSessionSingletonInstance' not in globals()):
#        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
#    return globals()['sparkSessionSingletonInstance']

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']
    
def Sentiment():
    
    #
    # Fase 3 Lectura de Tweet's
    #
    context = StreamingContext(sc, 120)    
    dStream = KafkaUtils.createDirectStream(context, ["sentiment"], {"metadata.broker.list": "localhost:9092"})
    
    dStream.foreachRDD(p1)
        
    context.start()
    context.awaitTermination()

def p1(time,rdd):
    
    rdd=rdd.map(lambda x: json.loads(x[1])).map(lambda x: x['text']).map(lambda x: x.upper())
    #rdd=rdd.map(lambda x:x.upper()).filter(lambda tweet:tweet!="HTTP" and tweet!="/" and tweet!="RT" and tweet!="@")
    
    rdd_MAGA     = rdd.filter(lambda x: "MAGA" in x).map(lambda x: [x, "MAGA"])
    rdd_DICTATOR = rdd.filter(lambda x: "DICTATOR" in x).map(lambda x: [x, "DICTATOR"])
    rdd_IMPEACH  = rdd.filter(lambda x: "IMPEACH" in x).map(lambda x: [x, "IMPEACH"])
    rdd_DRAIN    = rdd.filter(lambda x: "DRAIN" in x).map(lambda x: [x, "DRAIN"])
    rdd_SWAMP    = rdd.filter(lambda x: "SWAMP" in x).map(lambda x: [x, "SWAMP"])
    rdd_COMEY    = rdd.filter(lambda x: "COMEY" in x).map(lambda x: [x, "COMEY"])
    
    rdd1 = sc.union([rdd_MAGA, rdd_DICTATOR, rdd_IMPEACH, rdd_DRAIN, rdd_SWAMP, rdd_COMEY])
    
    parts = rdd1.map(lambda x: Row(sentence=x[0], label=x[1], date=time))
    spark = getSparkSessionInstance(rdd.context.getConf())
    partsDF = spark.createDataFrame(parts)
    #partsDF.show(truncate=False)
    
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    tokenized = tokenizer.transform(partsDF)
    #tokenized.show(truncate=False)
    
    remover = StopWordsRemover(inputCol="words", outputCol="base_words")
    base_words = remover.transform(tokenized)
    #base_words.show(truncate=False)
    
    train_data_raw = base_words.select("base_words", "label", "date")
    #train_data_raw.show(truncate=False)
    
    base_words = train_data_raw.select("base_words")
    #base_words.show(truncate=False)
    
    #Vectorize
    word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")
    model = word2Vec.fit(train_data_raw)
    final_train_data3 = model.transform(train_data_raw)
    #final_train_data3.show()
    final_train_data3 = final_train_data3.select("label", "features", "date")
    #final_train_data3.show(truncate=False)

    final_model = lrModel.transform(final_train_data3)
    final_model.show()       
        
    sentimentDataFrame = final_model.select("label", "date", "prediction")
    sentimentDataFrame.createOrReplaceTempView("sentimental")
    sentimentDataFrame = spark.sql("select label, date, prediction, count(*) as total_label from sentimental group by label, date, prediction order by label")
    sentimentDataFrame.show()
    sentimentDataFrame.write.mode("append").saveAsTable("sentiment1")
    #ds=spark.sql("select keyword, sum(total) as suma from trumpkeyword group by keyword order by suma desc limit 10")
    #ds.show()
    

    #context.start()
    #context.awaitTermination()

if __name__ == "__main__":

    sc = SparkContext(appName="Sentiment")
   
    #
    # FASE I - Training
    #

    rdd = sc.textFile("/tmp/train1.csv")
    
    spark = getSparkSessionInstance(rdd.context.getConf())
    
    r = rdd.mapPartitions(lambda x : csv.reader(x))
    #r = r.map(lambda x:x.upper()).filter(lambda tweet:tweet!="HTTP" and tweet!="/" and tweet!="RT" and tweet!="@")
        
    parts = r.map(lambda x: Row(sentence=str.strip(x[3]), label=int(x[1])))
    partsDF = spark.createDataFrame(parts)
    partsDF.show(truncate=False)
    
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    tokenized = tokenizer.transform(partsDF)
    tokenized.show(truncate=False)
    
    remover = StopWordsRemover(inputCol="words", outputCol="base_words")
    base_words = remover.transform(tokenized)
    base_words.show(truncate=False)
    
    train_data_raw = base_words.select("base_words", "label")
    train_data_raw.show(truncate=False)
    
    base_words = train_data_raw.select("base_words")
    base_words.show(truncate=False)
    
    #Vectorize
    word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")
    model = word2Vec.fit(train_data_raw)
    final_train_data = model.transform(train_data_raw)
    final_train_data.show()
    final_train_data = final_train_data.select("label", "features")
    final_train_data.show(truncate=False)

    #Logistic Regression
    lr = LogisticRegression(maxIter=1000, regParam=0.001, elasticNetParam=0.0001)
    lrModel = lr.fit(final_train_data)
    lrModel.transform(final_train_data).show()
 
    #
    # Fase II Validacion
    #
    rdd = sc.textFile("/tmp/val.csv")
    
    spark = getSparkSessionInstance(rdd.context.getConf())
    
    r = rdd.mapPartitions(lambda x : csv.reader(x))
    #r = r.map(lambda x:x.upper()).filter(lambda tweet:tweet!="HTTP" and tweet!="/" and tweet!="RT" and tweet!="@")
        
    parts = r.map(lambda x: Row(sentence=str.strip(x[3]), label=int(x[1])))
    partsDF = spark.createDataFrame(parts)
    partsDF.show(truncate=False)
    
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    tokenized = tokenizer.transform(partsDF)
    tokenized.show(truncate=False)
    
    remover = StopWordsRemover(inputCol="words", outputCol="base_words")
    base_words = remover.transform(tokenized)
    base_words.show(truncate=False)
    
    train_data_raw = base_words.select("base_words", "label")
    train_data_raw.show(truncate=False)
    
    base_words = train_data_raw.select("base_words")
    base_words.show(truncate=False)
    
    #Vectorize
    word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="base_words", outputCol="features")
    model = word2Vec.fit(train_data_raw)
    final_train_data2 = model.transform(train_data_raw)
    final_train_data2.show()
    final_train_data2 = final_train_data2.select("label", "features")
    final_train_data2.show(truncate=False)

    lrModel.transform(final_train_data2).show()

    #
    # Fase III Leer Tweets
    #    
    
    Sentiment()
