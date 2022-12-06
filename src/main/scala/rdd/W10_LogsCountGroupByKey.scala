package rdd

import org.apache.spark.SparkContext

object W10_LogsCountGroupByKey extends App {

  val sc = new SparkContext("local[*]","Group By Word Count")
  sc.setLogLevel("ERROR")

  val inputRdd = sc.textFile("src/main/resources/bigLog.txt")
  val wordCountRdd = inputRdd.map(x => (x.split(":")(0),1))
  wordCountRdd.groupByKey().collect().foreach(x=> System.out.println(x._1,x._2.size))

  System.in.read()

}
