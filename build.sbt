name := "project3"

version := "0.1"

scalaVersion := "2.12.10"

updateOptions := updateOptions.value.withGigahorse(false)

//libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.0.0-preview2"

val sparkVersion = "3.0.0-preview2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.12" % sparkVersion
)