package part3datasetes

import org.apache.spark.sql.functions._

import utils.Session.spark
import utils.Read


object SparkTypes extends App {
  /**
    * Exercise
    *
    * Filter the cars DF by a list of car names obtained by an API call
    * Versions:
    *   - contains
    *   - regexes
    */

  /**
    * How to run
    *
    *   sbt "runMain part3datasetes.SparkTypes"
    */

  val carsDF = Read.json("cars")
  carsDF.show()

  val fordCarsDF = carsDF.select("*").where(col("Name").contains("ford"))
  fordCarsDF.show()


  val carsList = List("Torino", "Pinto")
  val regexString = carsList.map(_.toLowerCase()).mkString("|")

  val torinoPintoDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("regex")
    )
    .where(col("regex") =!= "")
    .drop("regex")

  torinoPintoDF.show()

  spark.stop()
}
