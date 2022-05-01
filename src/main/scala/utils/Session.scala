package utils

import org.apache.spark.sql.SparkSession


object Session {
  val spark = SparkSession.builder()
    .master("local")
    .appName("mfrata-rtjvm-spark-essential")
    .getOrCreate()
}
