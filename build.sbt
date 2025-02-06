ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"



lazy val root = (project in file("."))
  .settings(
    name := "super-spark",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test"
  )

val sparkVersion = "3.5.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)