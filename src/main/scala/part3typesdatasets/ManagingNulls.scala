package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select the first non-null value
  val moviesWithFirstNotNullRating = moviesDF.select(
    col("Title"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10), // coalesce is firstNonNull
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating")
  )

  println(s"Movies with first none null ratings:")
  moviesWithFirstNotNullRating.show()

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing nulls
  moviesDF.select(
    "Title",
    "IMDB_Rating"
  ).na.drop() // removes rows containing nulls. na is a special object

  // replace nulls
  val moviesWithRatingNullDefault = moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")) // 0 for the two columns where null shows up

  println(s"Movies with null IMDB or RT rating replaced with 0:")
  moviesWithRatingNullDefault.show()

  // another way to fill with default by providing a map. fill method is
  // heavily overloaded
  println(s"Movies with fill from a map:")
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  )).show()

  // complex operations
  println(s"Complex operations using Expr:")
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same as last line
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else returns first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show()
}
