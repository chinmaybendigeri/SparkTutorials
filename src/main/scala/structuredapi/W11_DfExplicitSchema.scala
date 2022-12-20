package structuredapi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object W11_DfExplicitSchema extends App{

    val spark = SparkSession.builder().appName("Explicit Schema read").master("local[*]").getOrCreate()
    // Schema can be attached manually in two ways explicitly

    // 1. Programmatic Approach
    val ordersSchema = StructType(List(StructField("orderid",IntegerType),
    StructField("orderdate",TimestampType),
    StructField("customerid",IntegerType),
    StructField("status",StringType)))

    val ordersDf = spark.read.format("csv").schema(ordersSchema).option("header",true).option("path","src/main/resources/orders-201019-002101.csv").load()

    ordersDf.printSchema()
    ordersDf.show()

    // 2. Using DDL Statements
    val ordersDDLschema = "orderid int, orderdate string,cusid int,status string"

    val ordersDfDDL = spark.read.format("csv").schema(ordersDDLschema).option("header",true).option("path","src/main/resources/orders-201019-002101.csv").load()

    ordersDfDDL.printSchema()
    ordersDfDDL.show()
}
