package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Example17 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("json tuple").master("local[*]").getOrCreate()



  val df1 = spark.read.format("csv").option("escape","\"").option("header",true).option("path","src/main/resources/dummy1.csv").load()

  df1.show(false)
  df1.printSchema()
  // solution 1 -> using json_tuple takes two arguments json_tuple(<json-string>,<key-in-json-string-to-fetch-json-string-for-that-key>)
  val df2 = df1.select(col("*"),json_tuple(col("request"),"Response").as("response"))
    .select(col("*"),json_tuple(col("response"),"MessageId","Latitude","longitude").as(Array("MessageId","Latitude","longitude")))
    .drop("request","response")

  df2.show(false)

}
