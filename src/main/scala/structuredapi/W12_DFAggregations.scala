package structuredapi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object W12_DFAggregations extends App{
  val spark = SparkSession.builder().appName("Df aggregations").master("yarn").getOrCreate()
  val inputDf = spark.read.format("csv").option("header", true).option("inferSchema", true).option("path", "/user/itv000285/datasets/retail-data/all/online-retail-dataset.csv").load()
  inputDf.show(false)

  // Simple Aggregations using column,string and spark sql
  inputDf.select(count(col("*")).as("total_records"), countDistinct(col("CustomerID")).as("total_customers"),
    sum(col("Quantity")).as("total_quantity"), avg(col("UnitPrice")).as("average_price_per_unit"), sum(col("UnitPrice") * col("Quantity")).as("total_price"))
    .show()
  // using column object notations for simple aggreagations

  inputDf.selectExpr("count(*) as total_records", "count(distinct CustomerID) as total_customers", "sum(Quantity) as total_quantity",
    "avg(UnitPrice) as average_price_per_unit", "sum(UnitPrice * Quantity) as total_price").show()
  // using string expression for simple aggregations
  // use selectExpr method instead of select methd for string expression notations

  val sqlDf = inputDf.createOrReplaceTempView("sales")
  spark.sql("select count(1) as total_records,count(distinct CustomerID),sum(Quantity) as total_quantity,avg(UnitPrice) as average_price_per_unit,sum(UnitPrice * Quantity) as total_price from sales").show()
  //using spoark sql notations for simple aggregations

  //group aggregations
  inputDf.groupBy(col("InvoiceNo"),col("Country")).agg(count(col("*")).as("num_of_stockcode")).show()

}
