package scenarios

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession


object Example39 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Assignment 1").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  val rdd1 = sc.textFile("src/main/resources/input_real_estate.csv")

  val header = rdd1.filter(x => x.startsWith("Property_ID"))

  val rdd2 = rdd1.filter(x => x != rdd1.first())

  val headerRdd = header.map(x => x.split("\\|")(0) + "|" + x.split("\\|")(1) + "|" + "Final_Price")
 // headerRdd.foreach(println)
  val rdd3 = rdd2.map(x => x.split("\\|")).map(x => x(0) +"|"+x(1)+"|"+ x(5).toDouble * x(6).toDouble)
  //rdd3.foreach(println)

  val rdd4 = rdd3.union(headerRdd)

  rdd4.coalesce(1).saveAsTextFile("output/log_bcast_c.txt")
}
