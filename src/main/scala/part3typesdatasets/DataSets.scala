package part3typesdatasets

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import java.sql.Date

object DataSets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
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
                Miles_per_Gallon: Option[Double], // Option to allow null, or make null-able
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: Date,
                Origin: String
                )

  // 2 - read the DF from file
  def readDF(filename: String) = {
    val df = spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$filename")
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
    .withColumn("Year", col("Year").cast(DateType))
  val carsDS = carsDF.as[Car]

  numbersDS.filter(_ < 100).show()

  // map, flatMap, fold, reduce, for comps, functional operators work with dataset
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()


  // Dataframe vs dataset:
  // performance vs type safety
  // use DF when we need it fast
  // use DS when we care about type safety (with the cost of dataset actually contains
  // scala object, spark can't optimize them for performance, so all ops or eval are done
  // in a row by row basis)

  /**
   * Exercise
   * 1 - count how many cars we have
   * 2 - count how many cars have horsepower > 140
   * 3 - average HP for the entire dataset
   */

  // 1
  val carsCount = carsDS.count
  println(s"Total number of cars in this dataset:")
  println(carsCount)

  // 2. the getOrElse needs an explicit type hint to turn AnyVal to a numerical so the greater sign works
  println(s"Number of cars with horsepower greater than 140:")
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140))

  // 3
  println(s"Average HP of the entire dataset of cars:")
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // 3 equivalent
  carsDS.select(avg(col("Horsepower"))).show()

  // Joins
  case class Guitar(id: Int, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Int, name: String, guitars: Seq[Int])
  case class Band(id: Int, name: String, hometown: String, year: Int)

  val guitarDS = readDF("guitars.json").as[Guitar]
  val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandDS = readDF("bands.json").as[Band]

}
