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
}
