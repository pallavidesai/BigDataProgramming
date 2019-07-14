import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Coalesce


object SparkDataFrame1 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.json("F:\\MS\\Summer\\2ICP3\\Source Code\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\survey.csv")
    df.dropDuplicates()

    val Union = df.limit(10).union(df.limit(10))

    Union.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("F:\\MS\\Summer\\2ICP3\\Source Code\\SparkDataframe1\\SparkDataframe\\src\\main\\scala\\myFile.csv")


  }
}
