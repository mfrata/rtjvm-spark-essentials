package utils

import org.apache.spark.sql.DataFrame

import Session.spark


object Read {
  // Class tighly coupled with this project! Not the best absctraction
  val databaseConnectionConfig = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker"
  )

  val baseDataPath = "src/main/resources/data"

  def json(name: String): DataFrame = spark.read
    .option("inferSchema", "true")
    .json(s"$baseDataPath/$name.json")

  def table(name: String): DataFrame = spark.read
    .format("jdbc")
    .options(databaseConnectionConfig)
    .option("dbtable", s"public.$name")
    .load()
}
