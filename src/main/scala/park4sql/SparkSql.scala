package park4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("Spark Sql Practice")
    .config("spark.master", "local")
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
}
