package sonra.com

import java.io.{File, IOException}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object HDFSLoad extends App {

//  val sourcePath = args(0)
//  val destinationPath = args(1)
//
//  println(sourcePath)

  val inputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\mobile"

  val usageOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\usage"
  val topupOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\topup"
  val usageLogPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\usageLogs"
  val topupLogPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\topupLogs"



  try{
    FileUtils.deleteDirectory(new File(usageOutputPath))
    FileUtils.deleteDirectory(new File(topupOutputPath))
    FileUtils.deleteDirectory(new File(usageLogPath))
    FileUtils.deleteDirectory(new File(topupLogPath))
  }

  catch {
    case ioe: IOException =>
      // log the exception here
      ioe.printStackTrace()
      throw ioe
  }

  val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SonraSolution.com")
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
    .schema(userSchema)
    .csv(inputPath)

  //mobileDf.printSchema()

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

}

