package rdd

import org.apache.spark.SparkContext

object W11_MovieRatingsExample extends App {
  val sc = new SparkContext("local[*]","Avg Movies greater than 4.5 ratings")
  sc.setLogLevel("ERROR")

  val ratingsRdd = sc.textFile("src/main/resources/ratings-201019-002101.dat")
  val processedRdd = ratingsRdd.map(x => (x.split("::")(1),x.split("::")(2)))
  val countRatings = processedRdd.map(x => (x._1,(x._2.toFloat,1.0)))
  val avgMovieRatings = countRatings.reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))

  val topRatedMovies = avgMovieRatings.filter(x => x._2._2 > 100 ).map(x => (x._1,x._2._1/x._2._2)).filter(x => x._2 > 4.5)
  //topRatedMovies.foreach(println)

  val moviesRdd = sc.textFile("src/main/resources/movies-201019-002101.dat")
  val processedMovieRdd = moviesRdd.map(x => (x.split("::")(0),x.split("::")(1)))
  //rdd join will happen only on key-value pair
  val topMovies = processedMovieRdd.join(topRatedMovies).map(x => (x._2._2.toFloat,x._2._1))
  val myMovies = topMovies.coalesce(1).sortByKey(false).foreach(println)
  System.out.println();

}
