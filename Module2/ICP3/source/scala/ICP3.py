
from pyspark.sql import SparkSession
from operator import add
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql import HiveContext
import numpy as np
import pandas as pd
import os
os.environ["SPARK_HOME"] = "C:\softwares\Spark\spark-2.4.0-bin-hadoop2.7\spark-2.4.0-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="c:\\winutil\\"

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

dataFrame = spark.read.csv("F:\\MS\\Summer\\2ICP3\\Source Code\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\survey.csv",header=True);

sc = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .master("local[*]")\
    .config("spark.sql.warehouse.dir", "something") \
    .enableHiveSupport() \
    .getOrCreate()

dataFrame.dropDuplicates().show()


dataFrame1 = dataFrame.limit(10)
dataFrame2 = dataFrame.limit(10)
unionDf = dataFrame1.unionAll(dataFrame2)
unionDf.orderBy('Country').coalesce(1).write.format('csv').save("output", header='true')

print(dataFrame.groupBy("treatment"))

joined = dataFrame1.join(dataFrame2, dataFrame1.state == dataFrame2.state)
joined.show()

dataFrame.groupby('Country').agg({'Age': 'mean'}).show()

dataFrame_13=dataFrame.take(13)
print(dataFrame_13[-1])
