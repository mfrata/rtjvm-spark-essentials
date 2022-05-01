package part2dataframes

import org.apache.spark.sql.functions._

import utils.Session.spark
import utils.Read


object DataFrames extends App {
  /**
    * Exercise:
    * 1) Create a manual DF describing smartphones
    *   - make
    *   - model
    *   - screen dimension
    *   - camera megapixels
    *
    * 2) Read another file from the data/ folder, e.g. movies.json
    *   - print its schema
    *   - count the number of rows, call count()
    */

  /**
    * How to run
    *
    *   sbt "runMain part2dataframes.DataFrames"
    */

  // 1)
  import spark.implicits._
  val df1 = Seq(
      ("apple", "iphone13", 6.2, 14),
      ("samsung", "galaxy", 6.0, 24),
      ("motorola", "motog5", 4.0, 2)
    ).toDF("make", "model", "screen_dimension", "camera_megapixels")


  df1.show()

  // 2)
  val moviesDF = Read.json("movies")

  moviesDF.printSchema()

  println(s"movies count ${moviesDF.count()}")

  spark.stop()
}
