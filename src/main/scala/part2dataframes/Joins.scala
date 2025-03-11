package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()


  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")


  // joins, on guitar players and bands where
  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitarPlayersBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "inner")
  guitarPlayersBandsDF.show()

}
