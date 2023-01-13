package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Example32{

  def main(args : Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("transactions data pivot type").master("local[*]").getOrCreate()

    val df1 = spark.read.option("header", value = true).option("inferSchema", true).csv("src/main/resources/transactions.csv")

    df1.show(false)

    // solution 1 using group by and agg
    val df2 = df1
      .withColumn("Amount",when(col("Transaction Type") === "debit",col("Amount") * -1)
      .otherwise(col("Amount")))


    df2.show(false)

    df2.groupBy("Customer_No").sum("Amount")
      .withColumnRenamed("sum(Amount)","Total_Balance")
      .orderBy("Customer_No").show(false)

    // solution using pivot and agg
    df1.groupBy("Customer_No").pivot("Transaction Type").sum("Amount")
      .select(col("Customer_No"),(col("credit") - col("debit")).as("Total_Balance"))
      .show(false)

  }

}
