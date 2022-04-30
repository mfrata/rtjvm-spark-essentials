package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DataFrameBasics extends App {
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

  val spark = SparkSession.builder()
    .appName("DataFrameBasics")
    .master("local")
    .getOrCreate()


  // 1)
  import spark.implicits._
  val df1 = Seq(
      ("apple", "iphone13", 6.2, 14),
      ("samsung", "galaxy", 6.0, 24),
      ("motorola", "motog5", 4.0, 2)
    ).toDF("make", "model", "screen_dimension", "camera_megapixels")


  df1.show()


  // 2)
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  println(s"movies count ${moviesDF.count()}")

  spark.stop()
}
