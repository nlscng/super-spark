ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.4"



lazy val root = (project in file("."))
  .settings(
    name := "super-spark",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test"
  )