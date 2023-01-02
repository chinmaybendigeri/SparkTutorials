package structuredapi

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object W13_BroadCastJoin_Df extends App{
  val sparkConf = new SparkConf().set("spark.sql.autoBroadcastJoinThreshold", "-1")
  val spark = SparkSession.builder().config(sparkConf).appName("BroadCastJoin-DF").master("yarn").getOrCreate()

  val customersDf = spark.read.option("path", "datasets/tpch/customer/7ec4e2b9-a50b-44b8-9e1a-13d05b453060.parquet").load()
  val ordersDf = spark.read.option("path", "datasets/tpch/orders/d8bd37eb-9efb-469b-9da5-5fdc65564ad8.parquet").load()

  val joinedDf = customersDf.join(ordersDf,ordersDf("o_custkey") === customersDf("c_custkey"),"inner")

  joinedDf.write.parquet("output21")
}
