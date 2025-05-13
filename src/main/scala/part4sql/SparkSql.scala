package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part2dataframes.Joins.spark

object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("Spark Sql Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF api
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  println(s"American cars df using spark sql:")
  americanCarsDF.show()

  // this will create a spark-warehouse folder from root of the project if
  // no config was set for spark.sql.warehouse.dir
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databaseDF = spark.sql("show databases")

  println(s"showing rtjvm database")
  spark.sql("show databases")
  //databaseDF.show()

  // transfer tables from a DB to spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  private def readTable(tableName: String) =
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()

  val employeesDF = readTable("employees")
  employeesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")
}
