package com.konrad.spark_h2_tests.database

case class DatabaseConnectionSettings(
                                       connectionString: String,
                                       username: String,
                                       password: String,
                                       driver: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver") {

  override def toString = s"DatabaseConnectionSettings($connectionString, $username)"
}
