package utils

import org.apache.spark.sql.DataFrame

import Session.spark


object Read {
  val databaseConnectionConfig = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker"
  )

  val basePath = "src/main/resources/data"

  def json(name: String): DataFrame = spark.read
    .option("inferSchema", "true")
    .json(s"$basePath/$name.json")
}
