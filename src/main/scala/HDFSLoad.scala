

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


object HDFSLoad extends App {

  val inputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\mobile"
  val usageOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\usage"
  val topupOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\topup"
  val usageLogPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\usageLogs"
  val topupLogPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\topupLogs"

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

//  val fs:FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration);
//  fs.delete(new Path(usageOutputPath), true) // true for recursive

  spark.sparkContext.setLogLevel("ERROR")

  case class MobileSchema(Type: String, C2: String, C3: String, C4: String, C5: String, C6: String)

  val userSchema = new StructType().add("Type", "string")
    .add("C2", "string")
    .add("C3", "string")
    .add("C4", "string")
    .add("C5", "string")
    .add("C6", "string")

  val mobileDf = spark
    .readStream
    .format("csv")
    .option("delimiter", " ")
    .option("latestFirst", true)
    .option("maxFilesPerTrigger", "1")
    .schema(userSchema)      // Specify schema of the csv files
    .csv(inputPath)    // Equivalent to format("csv").load("/path/to/directory")

  mobileDf.createOrReplaceTempView("tempDF")
  var usageDF = spark.sql("select * from tempDF where Type = 'USAGE'")
  var topupDF = spark.sql("select * from tempDF where Type = 'TOPUP'")

  usageDF.writeStream
    .format("console")
    .format("csv")
    .option("delimiter", " ")
    .option("path",usageOutputPath)
    .option("checkpointLocation", usageLogPath)
    .outputMode("append")
    .start()

  topupDF.writeStream
    .format("console")
    .format("csv")
    .option("delimiter", " ")
    .option("path",topupOutputPath)
    .option("checkpointLocation", topupLogPath)
    .outputMode("append")
    .start()

  spark.streams.awaitAnyTermination()

//  groupDF.writeStream
//    .format("console")
//    .outputMode("complete")
//    .option("newRows",30)
//    .start()
//    .awaitTermination()
//val groupDF = mobileDf.select("Type")
//    .groupBy("Type").count()
//
//  mobileDf.isStreaming
}
