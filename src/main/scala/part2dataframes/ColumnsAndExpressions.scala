package part2dataframes

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting
  val carNamesDF = carsDF.select(firstColumn)

  carNamesDF.show()

  // various select methods that do mostly the same thing
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, and why the implicit, auto convert to Column
    $"Horsepower", // fancier interpolated string that returns a Column object
    expr("Origin") // Expression
  )

  // Select with plain strings.
  // Note this cannot be mixed with selecting with column objects
  carsDF.select(
    "Name",
    "Year"
  )

  /**
   * This type of select, where every input partition has exactly one
   * output partition is called a "narrow transformation"
   */

  // Expressions
  val simpleExpr = carsDF.col("Weight_in_lbs")
  val weightInKgExpr = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpr,
    expr("Weight_in_lbs / 2.2")
  )
  carsWithWeightDF.show()

  // selectExpr
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  /**
   * DF processing
   */

  // Adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // escaping to group column name for chars like spaces or hyphens inside expr
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // removing a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val euroCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val euroCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA' ")
  val americanCarsDF2 = carsDF.filter(col("Origin") === "USA")
  // chain filters
  val americanPowerCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americanPowerCarsDF3 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150) // "and" in infix
  val americanPowerCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // union - adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)
  val allCarsDF2 = carsDF union moreCarsDF
  allCarsDF2.show()

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  /**
   * Exercises
   *
   * 1. Read the movies DF and select any two columns
   * 2. Create a new DF with a new column that sums up total profit of the movies,
   *    which is US_Gross + Worldwide_Gross + US_DVD_Sales
   * 3. Select all comedy movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  val moviesDF = spark.read
    .option("inferSchema", "True")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  // 1 select two columns
  private val moviesSelectDF1: DataFrame = moviesDF.select("Title", "Release_Date")
  private val moviesSelectDF2: DataFrame = moviesDF.select(
    col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating")
  )

  private val movieSelectDF3: DataFrame = moviesDF.selectExpr("Title", "Release_Date", "Major_Genre")

  // 2. New DF with new column
  private val moviesWithTotalGrossDF: DataFrame = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )
  moviesWithTotalGrossDF.show()

  private val moviesWithTotalGrossDF2: DataFrame = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  println("Movies with total gross 2:")
  moviesWithTotalGrossDF2.show()

  println("Movies with total gross 3:")
  private val moviesWithTotalGrossDF3: DataFrame = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))
  moviesWithTotalGrossDF3.show()

  // 3. Select comedy above 6
  private val comedyDF1: Dataset[Row] = moviesDF.select("Title", "IMDB_Rating").where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  println("Comedies above six 1:")
  comedyDF1.show()

  private val comedyDF2: Dataset[Row] = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)
  println("Comedies above six 2:")
  comedyDF2.show()

  private val comedyDF3: Dataset[Row] = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  println("comedies above six 3:")
  comedyDF3.show()

}









