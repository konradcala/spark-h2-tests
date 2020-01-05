# Spark-h2-tests

This tiny project written in Scala shows how H2 database can be used for unit testing of Apache Spark application.

## Dependencies
pom.xml contains dependencies used in this project
```xml
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <scala.version>2.12.10</scala.version>
        <scala.compat.version>2.12</scala.compat.version>            
    ...
    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.compat.version}</artifactId>
            <version>2.4.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.compat.version}</artifactId>
            <version>2.4.4</version>
        </dependency>
        <dependency>
            <groupId>com.typesafe.scala-logging</groupId>
            <artifactId>scala-logging_${scala.compat.version}</artifactId>
            <version>3.9.2</version>
        </dependency>
        <dependency>
            <groupId>com.microsoft.sqlserver</groupId>
            <artifactId>mssql-jdbc</artifactId>
            <version>7.4.1.jre11</version>
        </dependency>

        <!--TEST-->
        <dependency>
            <groupId>org.scalatest</groupId>
            <artifactId>scalatest_${scala.compat.version}</artifactId>
            <version>3.1.0</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <version>1.4.200</version>
            <scope>test</scope>
        </dependency>                
```
##Application

Simple application reads rows from **student** table from input database. Dataset of students is transformed to dataset of persons which is saved into **person** table in output database.

| Table | Column | Type |
| --- | --- | --- |
| student | ID | int
| student | first_name | nvarchar(128) |
| student | last_name | nvarchar(128) |
| person | name | nvarchar(256) |
```scala
class App(implicit spark: SparkSession) extends LazyLogging {
  def ingest(inputDb: DatabaseConnectionSettings, outputDb: DatabaseConnectionSettings) = {
    import spark.implicits._
    val students: Dataset[Student] = Repository.readTable(inputDb, "(select first_name as firstName, last_name as lastName from student) students").as[Student]
    val persons: Dataset[Person] = students.map(student => Person(student.firstName + " " + student.lastName))
    Repository.save(persons.toDF(), outputDb, "person")
  }
}
```
**DatabaseConnectionSettings** is just a case class with connection settings 
```scala
case class DatabaseConnectionSettings(connectionString: String,
                                      username: String,
                                      password: String,
                                      driver: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver")
```
Spark SQL API is used to fetch DataFrame with students from input database
```scala
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

```
and also for saving persons into output database
```scala
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

```
##Unit tests
**AppTest** overwrites **beforeAll** and **afterAll** methods to prepare H2 test databases
```scala
class AppTest extends org.scalatest.FunSuite with BeforeAndAfterAll with Matchers {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    H2DatabaseCreator.createTables()
  }

  override protected def afterAll(): Unit = {
    H2DatabaseCreator.dropTables()
    super.afterAll()
  }
```
org.h2.tools.RunScript is used for triggering table creation/drop scripts. MSSQLServer driver compatibility mode is used.
```scala
object H2DatabaseCreator extends LazyLogging {
  private val inputH2Url = "jdbc:h2:mem:inputDb;MODE=MSSQLServer;USER=sa;DB_CLOSE_DELAY=-1"
  private val outputH2Url = "jdbc:h2:mem:outputDb;MODE=MSSQLServer;USER=sa;DB_CLOSE_DELAY=-1"

  val inputDbConf = DatabaseConnectionSettings(H2DatabaseCreator.inputH2Url, "sa", "", "org.h2.Driver")
  val outputDbConf = DatabaseConnectionSettings(H2DatabaseCreator.outputH2Url, "sa", "", "org.h2.Driver")

  def createTables(): Unit = {
    logger.info("Creating tables in test databases")
    RunScript.execute(inputH2Url, "sa", "", "classpath:createInputDatabase.sql", StandardCharsets.UTF_8, false)
    RunScript.execute(outputH2Url, "sa", "", "classpath:createOutputDatabase.sql", StandardCharsets.UTF_8, false)
    logger.info("Create scripts run successfully")
  }

  def dropTables(): Unit = {
    logger.info("Dropping all tables from test databases")
    RunScript.execute(inputH2Url, "sa", "", "classpath:dropInputTables.sql", StandardCharsets.UTF_8, false)
    RunScript.execute(outputH2Url, "sa", "", "classpath:dropOutputTables.sql", StandardCharsets.UTF_8, false)
    logger.info("Tables dropped")
  }
}
```
**createInputDatabase.sql**
```sql
create table student (id int PRIMARY KEY NOT NULL, first_name nvarchar(128), last_name nvarchar(128));
insert into student values (1, 'Konrad', 'Cala');
insert into student values (2, 'John', 'Doe');
```
**createOutputDatabase.sql**
```sql
create table person (name nvarchar(256));
```
**AppTest** checks whether all students are read from input database and transformed to persons and saved in output database. 
```scala
  test("should read students and save persons") {
    //when
    new App().ingest(H2DatabaseCreator.inputDbConf, H2DatabaseCreator.outputDbConf)

    //then
    val persons = Repository.readTable(H2DatabaseCreator.outputDbConf, "person").as[Person].collect()
    persons(0) should be(Person("Konrad Cala"))
    persons(1) should be(Person("John Doe"))
  }
```