package part2dataframes

//import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.commons.io.FileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import part2dataframes.DataframeBasics.spark

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Source and Formats")
    .config("spark.master", "local")
    .getOrCreate()

//  spark.sparkContext
//    .hadoopConfiguration
//    .setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem)

  val carSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", IntegerType),
    StructField("Cylinders", IntegerType),
    StructField("Displacement", IntegerType),
    StructField("Horsepower", IntegerType),
    StructField("Weight_in_lbs", IntegerType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /**
   * Reading a DF
   * - format
   * - use a defined schema, or set inferSchema = true
   * - zero or more options
   */
  private val carsDF = spark.read
    .format("json")
    .schema(carSchema) // enforce schema with a defined
    .option("mode", "permissive") // mode options: failFast, dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternatively creating a DF with option map, instead of option chaining
  private val carsDFWithOpitonMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))

  /**
   * Writing Dfs
   * - format
   * - save mode = {overwrites, append, ignore, errorIfExists}
   * - zero or more options
   *
   * Side notes, spark on windows machine has an issue, where spark uses hadoop api for file io, and hadoop api
   * doesn't naturally work on windows file system.
   * See: https://stackoverflow.com/questions/74885658/hadoop-bin-directory-does-not-exist
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")


  /**
   * Part 2
   */

  // JSON flags
  spark.read
    .option("dateFormat", "YYYY-MM-dd") // dateFormat only works WITH a schema; if spark fails parsing, will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  private val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))
  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // does data contain header row
    .option("sep", ",")
    .option("nullValue", "") // IMPORTANT as there are no null values in csv, so this says to treat empty as null
    .csv("src/main/resources/data/stocks.csv")

  // Parquet, a compressed binary format, also the default format for spark data frames
  carsDF.write
    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/cars.parquet")
    .save("src/main/resources/data/cars.parquet")  // same as calling parquet since parquet is default format

  // Text files
  spark.read
    .text("src/main/resources/data/sampleTextFile.txt")
    .show()


  // Reading from a remote DB

}
