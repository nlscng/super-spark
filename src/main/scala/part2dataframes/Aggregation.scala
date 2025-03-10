package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

/**
 * Aggregations done in this exercise are WIDE transformations,
 * one or more input partitions => one or more partitions
 *
 * Aka there's a lot of shuffling, and shuffling can be costly.
 */
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
    .orderBy(col("Avg_Rating"))
  aggregationsByGenreDF.show()


  /**
   * Exercise:
   * 1. Sum up all profits of all movies
   * 2. Count how many distinct directors we have
   * 3. Show means and std for US gross revenue
   * 4. Compute the average IMDB ratings and the average US gross revenue PER director
   */
  // sum all profits
  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show() // 139 trillion and some

  // Count distinct directors
  moviesDF.select(countDistinct(col("Director"))).show() // 550 distinct directors

  // means and std
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

  // Avg of imdb ratings and avg of US gross PER director
  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()
}
