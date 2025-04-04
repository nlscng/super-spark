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
  val moviesWithReleaseDAtesDF = moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

  moviesWithReleaseDAtesDF
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

  // dates that can't be parsed with our selected parser would return null,
  // but the dates can still potentially be there, just a different format.
  moviesWithReleaseDAtesDF.select("*").where(col("Actual_Release").isNull).show()

  /**
   * Exercise
   *  1. How to deal with multiple date format?
   *  2. Read the stocks DF
   */

  // 1 - parse DF multiple times, then union the small DFs

  // 2
  val stocksDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDFWithDatesDF = stocksDF.withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  stocksDFWithDatesDF.show()

  // structures, tuples in tables essentially
  // 1 - with col operators
  val movieWorldWideProfitDF = moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    movieWorldWideProfitDF.show()

  val movieUSProfitDF = movieWorldWideProfitDF
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  movieUSProfitDF.show()

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays

  val movieWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // returns an array of string

  movieWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()
}
