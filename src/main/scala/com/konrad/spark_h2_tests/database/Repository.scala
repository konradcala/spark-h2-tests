package com.konrad.spark_h2_tests.database

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Repository extends LazyLogging{
  def readTable(dbConf: DatabaseConnectionSettings, tableName: String)(implicit spark: SparkSession) = {
    spark.read
      .format("jdbc")
      .option("url", dbConf.connectionString)
      .option("dbtable", tableName)
      .option("user", dbConf.username)
      .option("password", dbConf.password)
      .option("driver", dbConf.driver)
      .load()
  }

  def save(frame: DataFrame, sqlOutputDatabase: DatabaseConnectionSettings, tableName: String) = {
    logger.info(s"Saving data into table $tableName")
    val connectionProperties: Properties = createJdbcProperties(sqlOutputDatabase)
    frame
      .write
      .mode(SaveMode.Append)
      .jdbc(sqlOutputDatabase.connectionString, tableName, connectionProperties)
    logger.info(s"Data saved successfully into table $tableName")
  }

  private def createJdbcProperties(outputDatabase: DatabaseConnectionSettings) = {
    val connectionProperties = new Properties()
    connectionProperties.put("username", outputDatabase.username)
    connectionProperties.put("password", outputDatabase.password)
    connectionProperties.put("driver", outputDatabase.driver)
    connectionProperties
  }

}
