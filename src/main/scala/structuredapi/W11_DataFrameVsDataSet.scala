package structuredapi


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, expm1}

case class OrdersData(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)

object W11_DataFrameVsDataSet extends App{

  Logger.getLogger(this.getClass).setLevel(Level.ERROR)

  val spark =  SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  val ordersDf = spark.read.option("header",true).option("inferSchema",true).csv("src/main/resources/orders-201019-002101.csv")
  ordersDf.select("order_id","order_customer_id").groupBy("order_customer_id")
    .count().orderBy(desc("count")).show()
  // equivalent sql query -> select order_customer_id,count(*) as num_of_orders from orders table group by order_customer_id order by num_of_orders desc

  // Datasets Vs DataFrames
  // data sets provide compile type safety where as dataframes doesnt provide compile type safety.In dataframes , errors are caught at run time.
  // Even though Datasets provide type safety , dataframes are preferred more in industry because there is overhead involved for converting Dataframe to Dataset

  //How to convert Dataframe to a Dataset?
  // Create a case class for your dataframe
  // import spark.implicits._
  // case your dataframe using case class to get DataSet
  import spark.implicits._
  val ordersDs = ordersDf.as[OrdersData]
  ordersDs.filter(x=> x.order_id > 10)

 // ordersDs.foreach(x=> x.order_ids > 10) // This will give compile time error

  ordersDf.filter("order_ids > 10") // This will give run time error
}
