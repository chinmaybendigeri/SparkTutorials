package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Example13 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("explode and posexplode").master("local[*]").getOrCreate()

  val df1 = spark.read.format("csv")
    .option("delimiter","|")
    .option("inferSchema",true)
    .option("header",true)
    .option("path","src/main/resources/array_elements.csv")
    .load()

  val df2 = df1.withColumn("Education",split(col("Education"),","))

  val df3 = df2.select(col("*"),posexplode_outer(col("Education")))
    .withColumnRenamed("pos","Position")
    .withColumnRenamed("col","Qualification")
    .drop("Education")

  df3.show(false)

  df3.printSchema()

}
