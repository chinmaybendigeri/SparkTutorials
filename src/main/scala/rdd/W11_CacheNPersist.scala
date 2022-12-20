package rdd

import org.apache.spark.{SparkConf, SparkContext}

object W11_CacheNPersist extends App{

  val sc = new SparkContext("local[*]","persistNCache")
  sc.setLogLevel("ERROR")
  val rawLogs = sc.textFile("/user/itv000285/datasets/textdata/bigLog.txt")

  val logsFiltered = rawLogs.map(x => (x.split(":")(0),x.split(":")(1),x.split(":")(3)))
  val processedLogs = logsFiltered.map(x => (x._1,x._2.split(" ")(2),x._3.split(" ")(2)))

  processedLogs.cache()

  val logsCount = processedLogs.map(x => (x._1,1))
  val logsCountByKey = logsCount.reduceByKey((x,y) => x+y)

  val yearlyErrorCount = processedLogs.filter(x => x._1 == "ERROR").map(x => (x._3,x._1)).groupByKey().map(x => (x._1,x._2.size))
  yearlyErrorCount.sortBy(x => x._2,false).take(1).foreach(println)

  val montlyErrorCount = processedLogs.filter(x => x._1 == "ERROR").map(x => (x._2 +"-"+x._3,x._1)).groupByKey().map(x => (x._1,x._2.size))
  montlyErrorCount.sortBy(x => x._2,true).take(1).foreach(println)

}
