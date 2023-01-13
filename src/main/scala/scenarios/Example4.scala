package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, column, lit}

import scala.Predef.Set

object Example4 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("Merge two files").master("local[*]").getOrCreate()


  val rawDF1 = spark.read.format("csv").option("header",true).option("delimiter","~|").option("path","src/main/resources/multi_delimited.csv").load()
  val rawDF2 = spark.read.format("csv").option("header",true).option("delimiter","~|").option("path","src/main/resources/multi_delimited_with_gender.csv").load()

  // Solution 1
  rawDF1.withColumn("Gender",lit(null))  // use lit function to add a new column with default value
    .union(rawDF2)

  // Solution 2
  val rawDF1_2 = rawDF1.withColumn("Gender", lit(null)) // use lit function to add a new column with default value

  val t1 = rawDF1_2
  val t2 = rawDF2

  t1.show(false)
  t2.show(false)

  val joinedDF = t1
    .join(t2, t1("Name") === t2("Name") && t1("Age") === t2("Age") , "left")

   val new_joinedDf =  joinedDF.select(coalesce(t1("Name"),t2("Name")).as("full_name"),coalesce(t1("Age"),t2("Age")).as("new_age"),coalesce(t1("Gender"),t2("Gender")).as("new_gender"))

  new_joinedDf.show(false)

}
