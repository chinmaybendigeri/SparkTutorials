package structuredapi

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession
object W11_DfWriteModes extends App {
  //set Log level to error
  Logger.getLogger(this.getClass).setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Df Write Modes Example").master("local[*]").getOrCreate()
  // reader api
  val ordersDf = spark.read.format("csv").option("header",true).option("inferSchema",true).option("path","src/main/resources/orders-201019-002101.csv").load()
  ordersDf.show()

  // There are 4 modes in which you can write the data
  // Overwrite
  // Append
  // ErrorIfExists
  // Ignore

  //writer api
  ordersDf.write.format("csv").mode("Overwrite").option("path","src/main/resources/output").save()
  // the number of files generated in the output folder is equal to number of partitions of the Dataframe
}
