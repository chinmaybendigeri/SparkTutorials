package scenarios


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Example23 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("select N lines from 1st partition").master("local[*]").getOrCreate()

  val rdd1 = spark.sparkContext.textFile( "src/main/resources/unstructed_data.csv")
  import spark.implicits._
  //rdd1.toDF().show(100,false)

 // println(rdd1.getNumPartitions)
  val rdd2 = rdd1.mapPartitionsWithIndex((index,iter) =>  if(index==0) iter.drop(8)  else iter)

  import spark.implicits._
  val header = Array("Page","Date","Pageviews","Unique_Pageviews","Sessions")
 // println(schema)
  val df1 = rdd2.toDF()

   val df2 = df1.withColumn("value",split(col("value"),","))
    .select(header.indices.map(i => col("value")(i).as(header(i))):_*)

  // checking data count in each partition (data skewness)
  df1.repartition(4).withColumn("partition_id",spark_partition_id()).groupBy("partition_id").count().show(false)
}
