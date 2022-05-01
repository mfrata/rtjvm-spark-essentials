package part2dataframes

import org.apache.spark.sql.functions._

import utils.Session.spark
import utils.Read


object DataSources extends App {
  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table "public.movies" in the Postgres DB
    */

  /**
    * How to run
    *
    *   docker-compose up
    *   sbt "runMain part2dataframes.DataSources"
    *   docker-compose down
    */

  val moviesDF = Read.json("movies")

  moviesDF.write
    .mode("overwrite")
    .option("sep", ",")
    .csv("/tmp/data/movies.csv")

  moviesDF.write
    .mode("overwrite")
    .parquet("/tmp/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .mode("overwrite")
    .options(Read.databaseConnectionConfig)
    .option("dbtable", "public.movies")
    .save()

  spark.stop()
}
