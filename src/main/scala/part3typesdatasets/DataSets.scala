package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.Date

object DataSets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // with the implicit, we convert Dataframe to Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type, as in, each row containing multiple column, potentially different types
  // By creating a case class, we are providing a schema
  // 1 - define case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Double,
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Long,
                Weight_in_lobs: Long,
                Acceleration: Double,
                Year: Date,
                Origin: String
                )

  // 2 - read the DF from file
  def readDF(filename: String) = {
    val df = spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/$filename")
    df
  }

  // 3 - define encoder (or better, import implicits)
  // Encoder.product takes as its type argument any type that extends the product type,
  // fortunately all case class extends the product type, and this helps spark match
  // which field map to which column in dataframe
  //  implicit val carEncoder = Encoders.product[Car]

  // however, writing one encoder for each type in your data IRL big data project
  // is not feasible, so the spark team wrapped all the implicits into this:

  import spark.implicits._
  val carsDF = readDF("cars.json")
  val carsDS = carsDF.as[Car]


}
