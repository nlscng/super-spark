package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  // either do this, or use date format string of 'd-MMM-yy' to help parse '7-Aug-98'
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates

  // convert a string column to a date column
  moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
    .show()
}
