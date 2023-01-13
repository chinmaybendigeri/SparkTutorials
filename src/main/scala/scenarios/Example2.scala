package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Example2 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("Read Modes").master("local[*]").getOrCreate()

  val schema = "emp_no int,emp_name string,department_name string"

  val rawDf = spark.read.format("csv").option("mode","DROPMALFORMED").schema(schema).option("header",true).option("path","src/main/resources/invalid.csv").load()

  rawDf.show(false)

  rawDf.printSchema()
}
