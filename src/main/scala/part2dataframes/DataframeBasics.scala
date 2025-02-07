package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object DataframeBasics extends App {
  private val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Dataframe basics")
    .getOrCreate()

  private val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true") // best practice is NOT to use inferSchema, but rather define your own
    .load("src/main/resources/data/cars.json")

  firstDF.show()

  firstDF.printSchema()

  firstDF.take(10).foreach(println)

  val longType = LongType

  // define schema
  val carSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", IntegerType),
    StructField("Cylinders", IntegerType),
    StructField("Displacement", IntegerType),
    StructField("Horsepower", IntegerType),
    StructField("Weight_in_lbs", IntegerType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // or use what spark has inferred
  val carsDFSchema = firstDF.schema


  // read a DF using defined schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // creating a row by hand
  val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")

  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )

  // creating DF with a seq of tuples
  val manualCarsDF = spark.createDataFrame(cars)

  // IMPORTANT: DFs have schemas, rows do not.


  // creating DFs with implicit
  import spark.implicits._  // notice the lower case spark, which is our own created obj
  val manualCarsDFWithImplicit = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  // print two schemas
  println("Printing manual and implicitly converted schemas")
  manualCarsDF.printSchema()
  manualCarsDFWithImplicit.printSchema()

  /**
   * Exercise:
   * 1) create a manual DF describing smartphones
   *  - make
   *  - model
   *  - screen dimension
   *  - camera mega pixels
   * 2) Read another file from the data folder, movies.json
   */
  val smartphones = Seq(
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "iPhone", "iOS", 13),
    ("Nokia", "3310", "something old", 0)
  )
  val smartPhonesDF = smartphones.toDF("Make", "Model", "Platform", "CameraMegaPixels")
  println("Smartphone DF:")
  smartPhonesDF.show()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  println(s"Movies DF schema:")
  moviesDF.printSchema()
  println(s"Movies DF has ${moviesDF.count()} rows")
}
