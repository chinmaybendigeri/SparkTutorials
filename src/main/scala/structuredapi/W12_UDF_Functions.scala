package structuredapi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object W12_UDF_Functions extends App{

  def ageCheck(age: Int) : String = {
    if(age > 18 ) "Y" else "N"
  }

  val spark = SparkSession.builder().appName("UDF generation").master("local[*]").getOrCreate()

  val dataFrame = spark.read.format("csv").option("mode","DROPMALFORMED").option("inferSchema",true).option("path","src/main/resources/dataset1").load()

  val inputDf = dataFrame.toDF("name","age","city")


  val myAgeCheckUDF = udf(ageCheck(_:Int):String)
  spark.catalog.listFunctions().filter(x => x.name == "MyAgeCheckUDF").show(false)
  // the above way of udf function declaration doesnt show in spark catalog list functions and hence we wont we able to use this for spark sql

  val processedDf = inputDf.withColumn("adult",myAgeCheckUDF(col("age")))
  processedDf.show()

  // the below way of registering udf function in spark catalog will list the same function in spark catalog list functions and hence we can use this for spark sql as shown below
  spark.udf.register("MyAgeCheckUDF",ageCheck(_:Int):String)
  spark.catalog.listFunctions().filter(x => x.name == "MyAgeCheckUDF").show(false)

  inputDf.createOrReplaceTempView("person")
  spark.sql("select *,MyAgeCheckUDF(age) as adult from person").show(false)

  scala.io.StdIn.readLine()

}
