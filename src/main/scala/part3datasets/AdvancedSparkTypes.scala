package part3datasetes

import org.apache.spark.sql.functions._

import utils.Session.spark
import utils.Read


object AdvancedSparkTypes extends App {
  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse the dates
    */


  /**
    * How to run
    *
    *   sbt "runMain part3datasetes.AdvancedSparkTypes"
    */

  val stocksDF = Read.csv("stocks")
  stocksDF.select(
    col("symbol"),
    to_date(col("date"), "MMM d yyyy").as("date"),
    col("price")
  ).show()

  spark.stop()
}
