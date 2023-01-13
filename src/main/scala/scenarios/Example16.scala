package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, split}

object Example16 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("merge two schemas").master("local[*]").getOrCreate()

  val df1 = spark.read.format("json")
    .option("path", "src/main/resources/input1.json")
    .load()

  val df2 = df1.withColumn("Education",split(concat_ws(",",col("Education.Qualification"),col("Education.year"),col("Education.Age")),","))

  val df3 = spark.read.format("json")
    .option("path", "src/main/resources/input2.json")
    .load()


  val df4 = df3.withColumn("Education",split(concat_ws(",",col("Education.Qualification"),col("Education.year")),","))

  df2.show(false)
  df2.printSchema()

  df4.show(false)
  df4.printSchema()

  val df5 = df2.union(df4)

  df5.show(false)

}
