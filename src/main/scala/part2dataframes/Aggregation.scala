package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object Aggregation extends App {
  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  genresCountDF.show() // 2926

  // counting all
  moviesDF.select(count("*")).show() // 3201

  // counting distinct major genres
  moviesDF.select(countDistinct(col("Major_Genre"))).show() // 12

  // aprox version for really big data frames
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) //

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  minRatingDF.show() // 1.4
  moviesDF.selectExpr("min(IMDB_Rating)").show() // the same result but with Expr

  // sum
  moviesDF.select(sum(col("US_Gross"))).show() //
  moviesDF.selectExpr("sum(US_Gross)").show() //

  // avg
  moviesDF.select(avg(col("US_Gross"))).show() //
  moviesDF.selectExpr("avg(US_Gross)").show() //


  // for data science, like mean and standard deviation
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // grouping
  val countByGenreDf = moviesDF.groupBy(col("Major_Genre")) // Notice we don't do 'select'
    .count() // select count(*) from moviesDF group by Major_Genre
  countByGenreDf.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  avgRatingByGenreDF.show()

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("Num_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
  aggregationsByGenreDF.show()

}
