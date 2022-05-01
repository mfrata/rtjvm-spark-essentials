package part2dataframes

import org.apache.spark.sql.functions._

import utils.Session.spark
import utils.Read


object Expressions extends App {
  /**
    * Exercises
    *
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  /**
    * How to run
    *
    *   sbt "runMain part2dataframes.Expressions"
    */

  val moviesDF = Read.json("movies")

  val moviesTitleDirectorDateDF = moviesDF.select("Title", "Director", "Release_Date")
  moviesTitleDirectorDateDF.printSchema()

  val moviesProfitDF = moviesDF.withColumn(
    "Total_Profit",
    col("US_Gross") + col("Worldwide_Gross")
  ).select("Title", "Total_Profit")
  moviesProfitDF.show()

  val goodComedyMoviesDF = moviesDF.filter(
    col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.0
  ).select("Title")

  goodComedyMoviesDF.show()

  spark.stop()
}
