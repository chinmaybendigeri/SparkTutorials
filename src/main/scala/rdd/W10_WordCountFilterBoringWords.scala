package rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object W10_WordCountFilterBoringWords extends App {

  val sparkConf = new SparkConf().set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext("local[*]", "myFirstApp", sparkConf)
  sc.setLogLevel("ERROR")

  val inputRdd = sc.textFile("src/main/resources/bigdatacampaigndata-201014-183159.csv")
  val boringWords = sc.broadcast(loadBoringWords)
  val requiredRdd = inputRdd.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  val words = requiredRdd.flatMapValues(x => x.split(" "))
  val finalWords = words.map(x => (x._2, x._1)).filter(x => !boringWords.value(x._1))
  val result = finalWords.reduceByKey((x, y) => x + y)
  val sortedResult = result.sortBy(x => x._2, false).collect
  sortedResult.foreach(println)

  def loadBoringWords(): Set[String] = {
    var boringSet: Set[String] = Set()
    val lines = Source.fromFile("src/main/resources/boringwords-201014-183159.txt").getLines()
    for (line <- lines) {
      boringSet += line
    }
    boringSet
  }
}
