package scenarios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Example15 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("pivot solution").master("local[*]").getOrCreate()

  val df1 = spark.read.format("csv")
    .option("inferSchema", true)
    .option("header", true)
    .option("path", "src/main/resources/pivot_data.csv")
    .load()

  //Solution 1
  val header = Array("roll_no","English","Physics","Maths","Science","History")

  val df2 = df1.withColumn("English",when(col("subject") === "English",col("marks")))
    .withColumn("Physics",when(col("subject") === "Physics",col("marks")))
    .withColumn("Maths",when(col("subject") === "Maths",col("marks")))
    .withColumn("Science",when(col("subject") === "Science",col("marks")))
    .withColumn("History",when(col("subject") === "History",col("marks")))
    .drop("subject","marks")

  val df3 = df2.groupBy("roll_no").max("English","Physics","Maths","Science","History")
  val df4 = df3.select((0 until header.length).map(i => if(i==0) col(header(i)).as(header(i)) else col("max("+header(i)+")").as(header(i))):_*)
  df4.show(true)


  //Solution 2
  val df5 = df1.groupBy("roll_no").pivot("subject").max("marks")

  df5.show(false)

  df5.printSchema()
}
