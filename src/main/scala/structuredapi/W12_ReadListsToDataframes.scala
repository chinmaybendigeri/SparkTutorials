package structuredapi

import org.apache.spark.sql.SparkSession

object W12_ReadListsToDataframes extends App{

  val spark = SparkSession.builder().appName("create dataframe from list").master("local[*]").getOrCreate()


  val myList = List(
    ("Spain",49,1,67,174.72),
    ("Germany",48,11,1795,3309.75),
    ("Lithuania",48,3,622,1598.06),
    ("Germany",49,12,1852,4521.39)
  )

  // create Dataframe from lcoal variable without schema
  import spark.implicits._
  val sampleDF = spark.createDataFrame(myList)
  sampleDF.show()
  sampleDF.printSchema()

  // create Dataset from local variable without schema
  val sampleDS = spark.createDataset(myList)
  sampleDS.show()
  sampleDS.printSchema()

}
