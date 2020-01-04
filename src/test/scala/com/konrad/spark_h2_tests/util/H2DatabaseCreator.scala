package com.konrad.spark_h2_tests.util

import java.nio.charset.StandardCharsets

import com.konrad.spark_h2_tests.database.DatabaseConnectionSettings
import com.typesafe.scalalogging.LazyLogging
import org.h2.tools.RunScript

object H2DatabaseCreator extends LazyLogging {
  private val inputH2Url = "jdbc:h2:mem:inputDb;MODE=MSSQLServer;USER=sa;DB_CLOSE_DELAY=-1"
  private val outputH2Url = "jdbc:h2:mem:outputDb;MODE=MSSQLServer;USER=sa;DB_CLOSE_DELAY=-1"

  val inputDbConf = DatabaseConnectionSettings(H2DatabaseCreator.inputH2Url, "sa", "", "org.h2.Driver")
  val outputDbConf = DatabaseConnectionSettings(H2DatabaseCreator.outputH2Url, "sa", "", "org.h2.Driver")

  def createDatabase(): Unit = {
    logger.info("Creating test databases")

    RunScript.execute(inputH2Url, "sa", "", "classpath:createInputDatabase.sql", StandardCharsets.UTF_8, false)
    RunScript.execute(outputH2Url, "sa", "", "classpath:createOutputDatabase.sql", StandardCharsets.UTF_8, false)
    logger.info("Create scripts run successfully")
  }

  def dropDatabase(): Unit = {
    logger.info("Dropping all tables")
    RunScript.execute(inputH2Url, "sa", "", "classpath:dropInputTables.sql", StandardCharsets.UTF_8, false)
    RunScript.execute(outputH2Url, "sa", "", "classpath:dropOutputTables.sql", StandardCharsets.UTF_8, false)
    logger.info("Tables dropped")
  }
}

