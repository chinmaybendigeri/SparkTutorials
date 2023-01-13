package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Example3 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("multi delimiter").master("local[*]").getOrCreate()

  val rawDf = spark.read.format("csv").option("header",true).option("delimiter","~|").option("path","src/main/resources/multi_delimited.csv").load()

  rawDf.show(false)
}
