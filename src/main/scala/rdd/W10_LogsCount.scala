package rdd

import org.apache.spark.SparkContext

object W10_LogsCount extends App {

  val sc = new SparkContext("local[*]", "Logs Count")
  sc.setLogLevel("ERROR")

  val logsData = List("WARN: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0410",
    "ERROR: Tuesday 4 September 0410",
    "ERROR: Tuesday 4 September 0410",
    "ERROR: Tuesday 4 September 0410",
    "ERROR: Tuesday 4 September 0410",
    "ERROR: Tuesday 4 September 0410")

  val logsRdd = sc.parallelize(logsData)

  val pairRdd = logsRdd.map(x => (x.split(":")(0), 1))
  //pairRdd.foreach(x => println()x._1)
  //pairRdd.reduceByKey(_ + _).collect().foreach(println)
}
