package sonra.com

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession

import scala.util.Try


object HDFSLoad extends App {

//      val sourcePath = args(0)
//      val destinationPath = args(1)
    //
    //  println(sourcePath)




    val inputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\mobile"

    val usageOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\usageMeta"
    val topupOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\topupMeta"

    var th = new Thread(new RenamingThread())
    th.setName("Renaming Thread")
    th.start()

//    try{
//      if(Files.exists(Paths.get(usageOutputPath)) ||  Files.exists(Paths.get(topupOutputPath))){
//        FileUtils.deleteDirectory(new File(usageOutputPath))
//        FileUtils.deleteDirectory(new File(topupOutputPath))
//      }
//
//    }
//
//    catch {
//      case ioe: IOException =>
//        // log the exception here
//        ioe.printStackTrace()
//        throw ioe
//    }
//
//    val spark:SparkSession = SparkSession.builder()
//      .master("local[*]")
//      .appName("sonra.com")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("ERROR")
//
//    case class MobileSchema(Type: String, C2: String, C3: String, C4: String, C5: String, C6: String)
//
//    val userSchema = new StructType().add("Type", "string")
//      .add("C2", "string")
//      .add("C3", "string")
//      .add("C4", "string")
//      .add("C5", "string")
//      .add("C6", "string")
//
//    val mobileDf = spark
//      .readStream
//      .format("csv")
//      .option("delimiter", " ")
//      .option("latestFirst", true)
//      .option("maxFilesPerTrigger", "1")
//      .schema(userSchema)
//      .csv(inputPath)
//
//    //mobileDf.printSchema()
//
//    mobileDf.createOrReplaceTempView("tempDF")
//    var usageDF = spark.sql("select * from tempDF where Type = 'USAGE'")
//    var topupDF = spark.sql("select * from tempDF where Type = 'TOPUP'")
//
//    usageDF.writeStream
//      .format("csv")
//      .option("delimiter", " ")
//      .option("path",usageOutputPath)
//      .option("checkpointLocation", usageOutputPath)
//      .outputMode("append")
//      .start()
//
//    topupDF.writeStream
//      .format("csv")
//      .option("delimiter", " ")
//      .option("path",topupOutputPath)
//      .option("checkpointLocation", topupOutputPath)
//      .outputMode("append")
//      .start()
//
//    Thread.sleep(5000)
//
////    jsonLoader(usageOutputPath)
////    jsonLoader(topupOutputPath)
//
//    spark.streams.awaitAnyTermination()
//
//


}


class RenamingThread extends Runnable
{
    var usageOutputPath = ""
    var topupOutputPath = ""

    def RenamingThread(usageOutputPath: String,topupOutputPath: String): Unit ={
        this.usageOutputPath = usageOutputPath
        this.topupOutputPath = topupOutputPath
    }
    override def run()
    {
        // Displaying the thread that is running
        println("Thread " + Thread.currentThread().getName() +
          " is running.")
            jsonLoader(usageOutputPath)
            jsonLoader(topupOutputPath)
    }

    def mv(oldName: String, newName: String) = {
        Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
    }

    def jsonLoader(jsonLoaderPath:String): Unit ={

        if(Files.exists(Paths.get(jsonLoaderPath+ "\\_spark_metadata"))){
            mv(jsonLoaderPath+ "\\_spark_metadata", jsonLoaderPath+ "\\spark_metadata")
        }

        val spark:SparkSession = SparkSession.builder()
          .master("local[*]")
          .appName("sonra.com")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val metaSourcePath = jsonLoaderPath + "\\sources\\0"
        val metaDestinationPath = jsonLoaderPath + "\\spark_metadata"

        val fileCount = Option(new File(metaSourcePath).list).map(_.length).getOrElse(0)

        var hashMap = scala.collection.mutable.Map("null"->"null")

        var counter = 0
        for (fileNumber <- 0 until fileCount){

            counter += 1
            val metaSourcefilePath = metaSourcePath + "\\" + fileNumber
            val metaDestinationFilePath = metaDestinationPath + "\\" + fileNumber

            val sourcePath = spark.read.json(metaSourcefilePath)
            sourcePath.createOrReplaceTempView("sourceView")
            val destinationPath = spark.read.json(metaDestinationFilePath)
            destinationPath.createOrReplaceTempView("destinationView")


            val sourcePathQuery = spark.sql("SELECT path FROM sourceView ")
            val sourcePathValue = sourcePathQuery.select("path").collect()(1).toString().split("/").last.dropRight(1)
            val destinationPathQuery = spark.sql("SELECT path FROM destinationView ")
            val destinationPathValue = destinationPathQuery.select("path").collect()(1).toString().split("/").last.dropRight(1)

            hashMap += (sourcePathValue -> destinationPathValue)
        }

        println(counter)
        hashMap.keys.foreach{i =>
            mv(jsonLoaderPath + "\\" + hashMap(i),jsonLoaderPath + "\\" + i)
        }
    }
}
