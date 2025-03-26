package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}
import part2dataframes.DataSources.{spark}


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

  /**
   * Exercise
   * - show all employees and their max salary
   * - show all employees who were never managers
   * - find job titles of the best paid 10 employees
   */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val tableName = "public.employees"
  val password = "docker"

  private def readTable(tableName: String) =
   spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", tableName)
    .load()

  val employeesDF = readTable("employees")
  val salaryDF = readTable("salaries")
  val managerDF = readTable("dept_manager")
  val titleDF = readTable("titles")

  // 1
  val maxSalaryEmployeeDF = salaryDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeeSalaryDF = employeesDF.join(maxSalaryEmployeeDF, "emp_no")
  employeeSalaryDF.show()

  // 2
  val employeeNeverManagerDF = employeesDF.join(
    managerDF, employeesDF.col("emp_no") === managerDF.col("emp_no"), "left_anti")
  employeeNeverManagerDF.show()

  //
  val mostRecentJobTitlesDF = titleDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeeDF = employeeSalaryDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeeDF.join(mostRecentJobTitlesDF, "emp_no")
  bestPaidJobsDF.show()
}
