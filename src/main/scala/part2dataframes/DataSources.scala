package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


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

  val spark = SparkSession.builder()
    .master("local")
    .appName("DataSources")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode("overwrite")
    .option("sep", ",")
    .csv("/tmp/data/movies.csv")

  moviesDF.write
    .mode("overwrite")
    .parquet("/tmp/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .options(
      Map(
        "driver" -> "org.postgresql.Driver",
        "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
        "user" -> "docker",
        "password" -> "docker",
        "dbtable" -> "public.movies"
      )
    ).save()

  spark.stop()
}
