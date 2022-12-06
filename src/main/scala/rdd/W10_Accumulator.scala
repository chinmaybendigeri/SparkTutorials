package rdd

import org.apache.spark.SparkContext

object W10_Accumulator extends App {

  val sc = new SparkContext("local[*]", "my accumulator")
  sc.setLogLevel("ERROR")
  val myRdd = sc.textFile("src\\main\\resources\\samplefile-201014-183159.txt")
  val myaccum = sc.longAccumulator("blank lines accumulator")

  myRdd.foreach(x => if (x == "") myaccum.add(1))

  println(myaccum.value)

}
