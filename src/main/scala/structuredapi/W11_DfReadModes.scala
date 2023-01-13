package structuredapi

import org.apache.spark.sql.SparkSession

object W11_DfReadModes extends App{

  // create a sparkSession
  val spark = SparkSession.builder().appName("Read Modes in Spark").master("local[*]").getOrCreate()

  val ordersDf = spark.read.format("json").option("mode","FAILFAST").option("path","src/main/resources/players-201019-002101.json").load()
  // There are 3 types of read mode in spark incase of corrupted date in source
  // 1. PERMISSIVE - default mode if nothing is specified. This will place null values in all the columns and created a new column named _corrupted_record column where the corrupted data is placed
  // 2. DROPMALFORMED - doesnt consider the corrupted record in the dataframe
  // 3. FAILFAST - thorws exception whenever spark encounters corrupted record

  ordersDf.show(false)
}
