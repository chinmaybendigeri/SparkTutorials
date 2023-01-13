package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Example18 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("window functions").master("local[*]").getOrCreate()


  val df1 = spark.read.format("csv").option("inferSchema", true).option("header", true).option("path", "src/main/resources/education_duplicate_records.csv").load()

  df1.show(false)
  df1.printSchema()

  val df2 = df1.withColumn("rank",row_number().over(Window.partitionBy("Name","Age","Education","Year").orderBy("Name")))
    .where(col("rank") > 1)
    .drop("rank")
    .dropDuplicates()

  df2.show(false)

  val df3 = df1.groupBy("Name","Age","Education","Year").count().where("count > 1").drop("count")

  df3.show(false)
}
