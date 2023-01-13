package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Example20 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("select all columns from json struct type").master("local[*]").getOrCreate()


  val df1 = spark.read.format("json").option("multiline", true).option("path", "src/main/resources/ambigious_columns.json").load()
  val df2 = df1.select("*","Delivery.*").drop("Delivery")


}
