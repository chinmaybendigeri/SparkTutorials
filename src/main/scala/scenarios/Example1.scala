package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, regexp_replace, split}


object Example1 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
 // name | education | yearOfExpr | Tech | Mobnum |
  val spark = SparkSession.builder().appName("apply line break for every 5th occurance of pipe").master("local[*]").getOrCreate()

  val rawDF = spark.read.format("csv").option("path","src/main/resources/candidate.csv").load()

  rawDF.show(false)

  val header = Array("Name", "Education", "YearsOfExpr", "Tech", "MobileNum")

  val parsedDf = rawDF.withColumn("candidate_info",regexp_replace(col("_c0"),"(.*?\\|){5}","$0-")).drop("_c0")

  val intermediateDf = parsedDf.withColumn("candidate_info",explode(split(col("candidate_info"),"\\|-")))
     .withColumn("candidate_info",split(col("candidate_info"),"\\|"))

 //intermediateDf.printSchema()

 intermediateDf.show(false)

// intermediateDf.select( col("candidate_info")(0).as("Name"),col("candidate_info")(1).as("Tech")).show(false) // this logic is more generalized as below

 val finalDF = intermediateDf.select((0 until header.length).map(i => col("candidate_info")(i).as(header(i))):_*)

 finalDF.show(false)

}
