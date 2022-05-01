package part2dataframes

import org.apache.spark.sql.functions._

import utils.Session.spark
import utils.Read


object Joins extends App {
  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  /**
    * How to run
    *
    *   docker-compose up
    *   sbt "runMain part2dataframes.Joins"
    */

  val employeesDF = Read.table("employees")
  val salariesDF = Read.table("salaries")
  val maxSalariesDF = salariesDF.groupBy("emp_no").max("salary")
  maxSalariesDF.join(employeesDF, "emp_no").show()

  val deptManagerDF = Read.table("dept_manager")
  val dptManEmpJoinCondition = employeesDF.col("emp_no") === deptManagerDF.col("emp_no")
  val employeesNeverBeenManagersDF = employeesDF.join(deptManagerDF, dptManEmpJoinCondition, "left_anti")
  employeesNeverBeenManagersDF.show()

  val titlesDF = Read.table("titles")
  val top10salariesTitlesDF = titlesDF.join(maxSalariesDF, "emp_no")
    .orderBy(col("max(salary)").desc)
    .limit(10)
  top10salariesTitlesDF.show()

  spark.stop()
}
