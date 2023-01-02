package structuredapi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object W13_BroadCastJoin_Rdd extends App{
  val spark = SparkSession.builder().appName("BroadCast-1").master("local[*]").getOrCreate()
  val sc =spark.sparkContext
  val logsRdd = sc.textFile("/user/itv000285/datasets/textdata/bigLogNew.txt")

  val pairRdd = logsRdd.map(x => (x.split(": ")(0),x.split(": ")(1)))
  val a = Array(
    ("ERROR",0),
    ("WARN",1)
  )

  val smallRdd = sc.parallelize(a)
  pairRdd.join(smallRdd).saveAsTextFile("output/log_join_c")

  //storing in broadcast variable for small rdd
  val keyMap = a.toMap
  val bcast = sc.broadcast(keyMap)
  val bcastRdd = pairRdd.map(x => (x._1,x._2,bcast.value(x._1)))
  bcastRdd.saveAsTextFile("output/log_bcast_c")

}
