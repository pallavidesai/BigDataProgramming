import sys
import os

os.environ["SPARK_HOME"] = "D:\\spark-2.4.3-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 15)

Eachwords = lines.flatMap(lambda line: line.split(" "))
KeyPair = Eachwords.map(lambda word: (word, 1))
ResultCount = KeyPair.reduceByKey(lambda x, y: x + y)
ResultCount.pprint()

ssc.start()
ssc.awaitTermination()