package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, to_date}

object Example6 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Expiry date calculation").master("local[*]").getOrCreate()

  val df1 = spark.read.format("csv").option("header",true).option("inferSchema",true).option("path","src/main/resources/recharge_date.csv").load()

  df1.show(false)

  df1.printSchema()

  val df2 = df1.withColumn("ExpiryDate",to_date(col("RechargeDate").cast("string"),"yyyyMMdd"))

  df2.withColumn("ExpiryDate",expr("date_add(ExpiryDate,RemainingDays)")).show(false)

}
