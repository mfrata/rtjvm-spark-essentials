package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Aggregations extends App {
  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    */

  /**
    * How to run
    *
    *   sbt "runMain part2dataframes.Aggregations"
    */

  val spark = SparkSession.builder()
    .master("local")
    .appName("Expressions")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val us_gross = col("US_Gross")

  val sumProfits = moviesDF.select(sum(us_gross))
  sumProfits.show()

  val distinctDirectors = moviesDF.select("Director").distinct.count()
  println(s" distinct directors $distinctDirectors")

  val avgStdDF = moviesDF.select(mean(us_gross), stddev(us_gross))
  avgStdDF.show()

  val perDirectorDF = moviesDF
    .groupBy("Director")
    .agg(
      mean(col("IMDB_Rating")).as("rating"),
      mean(us_gross).as("revenue")
    )

    val bestDirector = perDirectorDF
      .orderBy(col("rating").desc_nulls_last)
    bestDirector.show(3)

    val mostProfitableDirector = perDirectorDF
      .orderBy(col("revenue").desc_nulls_last)
    mostProfitableDirector.show(3)

  spark.stop()
}
