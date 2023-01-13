package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Example31 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("mask data").master("local[*]").getOrCreate()

  val df1 = spark.read.option("header",value = true).option("inferSchema",true).csv("src/main/resources/data_mask.csv")

  df1.show(false)

  def mask_email(input :String) : String = {
    var masked_email = ""
    val inputCharArray = input.split("@")(0).toCharArray
    for (i <- 0 until inputCharArray.length){
      if(i == 0) masked_email = masked_email + inputCharArray(i)
      else if(i == inputCharArray.length-1) masked_email = masked_email + inputCharArray(i)
      else masked_email = masked_email + "*"
    }
    masked_email = masked_email + "@" + input.split("@")(1)
    masked_email
  }

  spark.udf.register("MaskEmail",mask_email(_:String):String)

  val maskEmail = udf(mask_email(_:String):String)

  val df2 = df1.withColumn("masked_email",maskEmail(col("email")))

  df2.show(false)

  df1.withColumn("masked_email",maskEmail(col("email"))).explain(true)
  //println(mask_email("chinmay@gmail.com"))

  df1.createOrReplaceTempView("customers")

  spark.sql("select *,MaskEmail(email) as masked_email from customers").show(false)

}
