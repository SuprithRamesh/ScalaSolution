package sonra.com

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.util.Try


object HDFSLoad extends App {

//      val sourcePath = args(0)
//      val destinationPath = args(1)
    //
    //  println(sourcePath)

  val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("sonra.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")



    val inputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\mobile"

    val usageOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\usageMeta"
    val topupOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\topupMeta"


  import java.util.concurrent.Executors

  val executor = Executors.newCachedThreadPool

    val fileSplitTask: Runnable = new HDFSLoad.FileSplitter()
    val fileSplitWorker: Thread = new Thread(fileSplitTask)
  fileSplitWorker.start()

  val renameTask: Runnable = new HDFSLoad.JSONRename()
  val renameWorker: Thread = new Thread(renameTask)
  renameWorker.start()


    def FileRenamerMethod(): Unit ={
            try{
              if(Files.exists(Paths.get(usageOutputPath)) ||  Files.exists(Paths.get(topupOutputPath))){
                FileUtils.deleteDirectory(new File(usageOutputPath))
                FileUtils.deleteDirectory(new File(topupOutputPath))
              }

            }

            catch {
              case ioe: IOException =>
                // log the exception here
                ioe.printStackTrace()
                throw ioe
            }

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
              .format("csv")
              .option("delimiter", " ")
              .option("path",usageOutputPath)
              .option("checkpointLocation", usageOutputPath)
              .outputMode("append")
              .start()

            topupDF.writeStream
              .format("csv")
              .option("delimiter", " ")
              .option("path",topupOutputPath)
              .option("checkpointLocation", topupOutputPath)
              .outputMode("append")
              .start()

            spark.streams.awaitAnyTermination()


    }



    def mv(oldName: String, newName: String) = {
        Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
    }

    def jsonLoader(jsonLoaderPath:String): Unit ={

        if(Files.exists(Paths.get(jsonLoaderPath+ "\\_spark_metadata"))){
            mv(jsonLoaderPath+ "\\_spark_metadata", jsonLoaderPath+ "\\spark_metadata")
        }
      else return 0

        val metaSourcePath = jsonLoaderPath + "\\sources\\0"
        val metaDestinationPath = jsonLoaderPath + "\\spark_metadata"

        val metaSourcefileCount = Option(new File(metaSourcePath).list).map(_.length).getOrElse(0)
        val metaDestinationfileCount = Option(new File(metaSourcePath).list).map(_.length).getOrElse(0)

        var hashMap = scala.collection.mutable.Map("null"->"null")

        if(metaDestinationfileCount == metaDestinationfileCount){
          for (fileNumber <- 0 until metaSourcefileCount){

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

        }
        hashMap.keys.foreach{i =>
            mv(jsonLoaderPath + "\\" + hashMap(i),jsonLoaderPath + "\\" + i)
            hashMap.remove(i)
        }
    }

    class JSONRename() extends Runnable {
      private var cancelled = false
        override def run(): Unit = {
          while(!cancelled)
            jsonLoader(usageOutputPath)
            jsonLoader(topupOutputPath)
        }

      def cancel(): Unit = {
        cancelled = true
      }

      def isCancelled: Boolean = cancelled
    }

    class FileSplitter() extends Runnable {
        override def run(): Unit = {
          Thread.sleep(30000)
          FileRenamerMethod()
        }
    }
}


