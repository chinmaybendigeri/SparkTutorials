package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}

import scala.collection.mutable.ListBuffer

object NullColumnsDrop extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("drop null values").master("local").getOrCreate()

  val inputDf = spark.read.format("csv")
    .option("delimiter","|").option("header",true).option("inferSchema",true).option("path","src/main/resources/null_columns.csv").load()

  inputDf.show(false)

  // drop columns which have null values

  val header = inputDf.columns

  val countDf = inputDf.select(header.indices.map(i => (count(col(header(i))).as(header(i))).cast("int")):_*)

  countDf.show(false)
  var listNonNull = ListBuffer[String]()
  for (i <- countDf.columns) {
      if (countDf.first().getAs(i).asInstanceOf[Int] > 0)
        listNonNull += i
  }


  println(listNonNull)
  //val finalDf = inputDf.drop().show(false)
  inputDf.select(listNonNull.toList.map(x => col(x)):_*).show(false)


   inputDf.na.drop(1).show(false)

   inputDf.dropDuplicates().show(false)


}
