name := "hadoop"

version := "0.1"

scalaVersion := "2.13.5"

val circeVersion = "0.14.0-M4"
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.2.1",
)