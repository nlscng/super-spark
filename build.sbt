ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

javacOptions ++= Seq("-source", "11", "-target", "11")  // Use Java 11 for compilation

lazy val root = (project in file("."))
  .settings(
    name := "super-spark",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test"
  )

val sparkVersion = "3.5.1"
val postgresVersion = "42.6.0"
val log4jVersion = "2.24.3"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)