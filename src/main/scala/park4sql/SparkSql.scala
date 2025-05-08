package park4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
  databaseDF.show()
}
