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

  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating")/10 + col("IMDB_Rating")) / 2)

  // correlation = number between 1 and -1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")/* corr is an Action, which is eager/not lazy */)

  // strings
  val carsDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volskwagen"))

  // regex
  val regexStr = "volkswagen|vw"

  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexStr, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexStr, "People's car").as("regex_replace")
  ).show()


  /**
   * Exercise
   *
   * Filter the cars DF by a list of car name obtained by an API call
   * versions:
   *  - contains
   *  - regex
   */
  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // volkswaven|mercedes-benz|ford

  // version 1 with regex
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")
    .show()

  // version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()
}
