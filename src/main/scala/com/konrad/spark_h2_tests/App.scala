package com.konrad.spark_h2_tests

import com.konrad.spark_h2_tests.database.{DatabaseConnectionSettings, Repository}
import com.konrad.spark_h2_tests.model.{Person, Student}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}


class App(implicit spark: SparkSession) extends LazyLogging {
  def ingest(inputDb: DatabaseConnectionSettings, outputDb: DatabaseConnectionSettings) = {
    import spark.implicits._
    val students: Dataset[Student] = Repository.readTable(inputDb, "(select first_name as firstName, last_name as lastName from student) students").as[Student]
    val persons: Dataset[Person] = students.map(student => Person(student.firstName + " " + student.lastName))
    Repository.save(persons.toDF(), outputDb, "person")
  }
}

object App extends LazyLogging {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("H2 Test App")
      .master("local")
      .getOrCreate()

    //provide your own database configurations
    new App().ingest(DatabaseConnectionSettings("jdbc:sqlserver://localhost:1433;databaseName=input", "admin", ""),
      DatabaseConnectionSettings("jdbc:sqlserver://localhost:1433;databaseName=output", "admin", ""))
  }

}
