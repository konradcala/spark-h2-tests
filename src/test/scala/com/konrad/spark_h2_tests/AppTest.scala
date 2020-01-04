package com.konrad.spark_h2_tests

import com.konrad.spark_h2_tests.database.Repository
import com.konrad.spark_h2_tests.model.Person
import com.konrad.spark_h2_tests.util.H2DatabaseCreator
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

class AppTest extends org.scalatest.FunSuite with BeforeAndAfterAll with Matchers {
  implicit val spark = SparkSession.builder.appName("test").master("local").getOrCreate

  import spark.implicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    H2DatabaseCreator.createDatabase()
  }

  override protected def afterAll(): Unit = {
    H2DatabaseCreator.dropDatabase()
    super.afterAll()
  }

  test("should read students and save persons") {
    //when
    new App().ingest(H2DatabaseCreator.inputDbConf, H2DatabaseCreator.outputDbConf)

    //then
    val persons = Repository.readTable(H2DatabaseCreator.outputDbConf, "person").as[Person].collect()
    persons(0) should be(Person("Konrad Cala"))
    persons(1) should be(Person("John Doe"))
  }

}
