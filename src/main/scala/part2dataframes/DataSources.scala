package part2dataframes

//import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.commons.io.FileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
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
    StructField("Year", StringType),
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
}
