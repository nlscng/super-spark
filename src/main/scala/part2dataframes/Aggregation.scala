package part2dataframes

import org.apache.spark.sql.SparkSession

object Aggregation extends App {
  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resource/data/movies.json")

  // counting
  val genressCountDF = moviesDF.select(col("Major"))
}
