package part3typesdatasets
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain, or literal, value to every single row to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter: Column = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter: Column = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(dramaFilter)
  // + multiple ways of filtering

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // filter on a boolean column
  moviesWithGoodnessFlagsDF.where("good_movie").show() // where(col("good_movie") === "true"

  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie"))).show()

  // Numbers
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating")/10 + col("IMDB_Rating")) / 2)
}
