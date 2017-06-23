from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import sys

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionInstance' not in globals()):
        globals()['sparkSessionInstance'] = SparkSession.builder.config(conf=sparkConf) \
                                            .enableHiveSupport().getOrCreate()
    return globals()['sparkSessionInstance']

def visualization():
    spark = getSparkSessionInstance(sc.getConf())
    date, time = sys.argv[1:]
    datetime=date+" "+time
    query="select prediction, sum(total_label) as suma from sentiment1 where \
            date between cast('{}' as timestamp)- INTERVAL 1 HOUR and cast('{}' as timestamp) and label='IMPEACH' \
            group by prediction".format(datetime, datetime)
    ds=spark.sql(query)    
    df = ds.toPandas()
    pie=plt.pie( df['suma'],labels=df['prediction'],shadow=False, startangle=90,autopct='%1.1f%%')
    df['legend']=df.prediction.astype(str).str.cat(df.suma.astype(str), sep=':     ')
    plt.legend(labels=df['legend'], loc="upper right")
    plt.axis('equal')
    plt.suptitle("Sentiment IMPEACH", fontsize = 20)
    plt.tight_layout()
    plt.show()      
    #actualFigure = plt.figure(figsize = (16,10))


if __name__ == "__main__":
    sc = SparkContext(appName="Sentiment IMPEACH")
    visualization()
