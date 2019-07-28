from pyspark.sql import SparkSession
from pyspark.sql import *
import os
from operator import add
from pyspark import SparkContext
from pyspark.sql.types import DoubleType

os.environ["SPARK_HOME"] = "D:\\spark-2.4.3-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\winutils"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv("WorldCupMatches.csv", header=True);
df.show()
print(df.schema)

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile("WorldCups.csv", 1)
    header = lines.first()
    content = lines.filter(lambda line: line != header)
    rdd = content.map(lambda line: (line.split(","))).collect()
    rdd_len = content.map(lambda line: len(line.split(","))).distinct().collect()

    from pyspark.sql.types import StructType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StringType, IntegerType

    schema = StructType([StructField('Year', StringType(), True),
                         StructField('Country', StringType(), True),
                         StructField('Winner', StringType(), True),
                         StructField('Runners-Up', StringType(), True),
                         StructField('Third', StringType(), True),
                         StructField('Fourth', StringType(), True),
                         StructField('GoalsScored', StringType(), True),
                         StructField('QualifiedTeams', StringType(), True),
                         StructField('MatchesPlayed', StringType(), True),
                         StructField('Attendance', StringType(), True)])
    df = spark.createDataFrame(rdd, schema)
    df = df.withColumn('GoalsScored', df['GoalsScored'].cast(IntegerType()))
    df = df.withColumnRenamed('Runners-Up', 'Runnersup')


    print("**********************Each year with its highest goal scored***************************")
    # Using RDD
    rdd1 = (content.filter(lambda line: line.split(",")[6] != "NULL")
            .map(lambda line: (line.split(",")[0], int(line.split(",")[6])))
            .takeOrdered(10, lambda x: -x[1]))
    print(rdd1)
    # Using Dataframe
    df.select("Year", "GoalsScored").orderBy("GoalsScored", ascending=False).show(20, truncate=False)

    print("********************Display of the matche playedis equal to highest match played********************")
    # Using RDD
    print((content.filter(lambda line: line.split(",")[8] == "64")
     .map(lambda line: (line.split(","))).collect()))
    # using Dataframe
    df = df.withColumn('MatchesPlayed', df['MatchesPlayed'].cast(IntegerType()))
    df.filter(df.MatchesPlayed == 64).show()


    print("************************Matches that are played in England*********************************")
    # using RDD
    print((content.filter(lambda line: line.split(",")[1] == "England")
     .map(lambda line: (line.split(","))).collect()))
    # using Dataframe
    df.filter(df.Country == "England").show()

    print("-------------- Details of matches held in England, USA and France ---------------")
    # using RDD
    country = ["England", "USA", "France"]
    print((content.filter(lambda line: line.split(",")[1] in country)
     .map(lambda line: (line.split(",")[0], line.split(",")[2], line.split(",")[3])).collect()))
    # using Dataframe
    df.select("Year", "Winner", "Runnersup").filter(df.Country.isin(country)).show()

    print("**********************details of Runnersup country *************************")
    # using RDD
    print((content.filter(lambda line: line.split(",")[1] == line.split(",")[3])
     .map(lambda line: (line.split(",")[0], line.split(",")[1], line.split(",")[2]))
     .collect()))
    # using Dataframe
    df.select("Year", "Country", "Runnersup").filter(df["Country"] == df["Runnersup"]).show()