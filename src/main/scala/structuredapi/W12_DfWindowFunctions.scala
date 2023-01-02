package structuredapi

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

object W12_DfWindowFunctions extends App{
  val spark = SparkSession.builder().appName("Window Functions on Dataframes").master("yarn").getOrCreate()

  val windowDf = spark.read.format("csv").option("inferSchema",true).option("path","/user/itv000285/datasets/textdata/windowdata.csv").load().toDF("country","weeknum","numofinvoices","totalquantity","invoicevalue")

  windowDf.withColumn("running_total",functions.sum(col("invoicevalue")).over(Window.partitionBy("country").orderBy("weeknum").rowsBetween(Window.unboundedPreceding,Window.currentRow))).show()
}
