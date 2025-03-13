package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max, col}


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

  // outer joins
  // left outer joins
  guitarPlayersDF.join(bandsDF, joinCondition, "left_outer").show()

  // right outer joins
  guitarPlayersDF.join(bandsDF, joinCondition, "right_outer").show()

  // outer joins
  guitarPlayersDF.join(bandsDF, joinCondition, "outer").show()

  // semi joins ???
  // apparently, left-semi is like a left inner, but only shows the left table data after the join
  guitarPlayersDF.join(bandsDF, joinCondition, "left_semi").show()


  // anti joins,
  // left anti joins, like a left joins but only data on the left that doesn't fit the join condition
  guitarPlayersDF.join(bandsDF, joinCondition, "left_anti").show()

  // when joining, things to keep in mind, it's common to have the same column in both sides, like "id"
  // and we need to be explicit if we are to select the duplicate column in the joined DF

  // option 1 - rename the column on which we are joining
  guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitarPlayersBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitarPlayersDF.join(bandsModDF, guitarPlayersDF.col("band") === bandsModDF.col("bandId"))

  // option 4 - use complex types, more details in later chapters
  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

}
