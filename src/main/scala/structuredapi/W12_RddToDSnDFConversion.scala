package structuredapi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col

case class Orders(orderid : Int, orderdate : String, customerid: Int, status : String) //explicit Schema creation for Dataframes and Datasets
object W12_RddToDSnDFConversion extends App{

  Logger.getLogger(this.getClass).setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("rdd to Dataframe conversion").getOrCreate()

  // create reg expr for parsing the input row
  val myRegex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  def parseLine(line: String) : Orders = {
        line match {
          case myRegex(order_id,order_date,customer_id,status) => Orders(order_id.toInt,order_date,customer_id.toInt,status)
        }
  }
  //read the unstructured format into a raw rdd to process the file using regular expression and to attach the required schema for the same
  val rawRdd = spark.sparkContext.textFile("src/main/resources/orders_new-201019-002101.csv")

  //converting rdd to Dataframe using toDF()
  import spark.implicits._ // import is required to use toDF() and toDS() methods ,if not imported this will throw error
  val parsedDF = rawRdd.map(parseLine).toDF()
  parsedDF.select("orderid")

  parsedDF.show(false)
  parsedDF.printSchema()

  parsedDF.groupBy("orderid").count().show()
  //parsedDF.filter(x => x.status == "COMPLETE").show() // cant use this because dataframes doesnt provide compile time safety

  //converting rdd to Dataset using toDS()
  val parsedDS = rawRdd.map(parseLine).toDS()
  parsedDS.show(false)
  parsedDS.printSchema()

  parsedDS.groupBy(col("orderid"),col("status")).count().show()

  parsedDS.filter(x => x.status == "COMPLETE").show()

  // converting Df to Dataset
  val test = parsedDF.as[Orders].toDF().as[Orders]

}
