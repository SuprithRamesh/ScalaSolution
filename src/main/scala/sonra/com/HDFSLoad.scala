package sonra.com

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.util.Try


object HDFSLoad extends App {

  //      val sourcePath = args(0)
  //      val destinationPath = args(1)
  //
  //  println(sourcePath)

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("sonra.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  val inputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\mobile"

  val usageOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\usageMeta"
  val topupOutputPath = "C:\\Users\\Suprith\\Desktop\\TCD\\project3\\topupMeta"

  cleanUp()

  val fileSplitTask: Runnable = new HDFSLoad.FileSplitter()
  val fileSplitWorker: Thread = new Thread(fileSplitTask)
  fileSplitWorker.start()



  val usageRenameTask: Runnable = new HDFSLoad.UsageRename()
  val usageRenameWorker: Thread = new Thread(usageRenameTask)
  usageRenameWorker.start()

  val topupRenameTask: Runnable = new HDFSLoad.TopupRename()
  val topupRenameWorker: Thread = new Thread(topupRenameTask)
  topupRenameWorker.start()

  def reader(metadataInDir: String, metadataOutDir: String): Unit ={
    import org.apache.commons.io.IOUtils
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}

    val hadoopconf = new Configuration()
    val fs = FileSystem.get(hadoopconf)

    val metaSourcefileCount = Option(new File(metadataInDir).list).map(_.length).getOrElse(0)

    for (fileNumber <- 0 until metaSourcefileCount){
      if(Files.exists(Paths.get(metadataInDir + "\\" + fileNumber))){
        //Create input stream from local file
        val inStream = fs.open(new Path(metadataInDir + "\\" + fileNumber))

        //Create output stream to HDFS file
        val outFileStream = fs.create(new Path(metadataOutDir + "\\" + fileNumber))

        IOUtils.copy(inStream, outFileStream)

        //Close both files
        inStream.close()
        outFileStream.close()
      }

    }

  }

  def cleanUp(): Unit = {
    try {
      if (Files.exists(Paths.get(usageOutputPath)) || Files.exists(Paths.get(topupOutputPath))) {
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
  }

  class FileSplitter() extends Runnable {
    override def run(): Unit = {
      FileSplitterMethod()
    }
  }

  class UsageRename() extends Runnable {
    private var cancelled = false

    override def run(): Unit = {
      while (!cancelled)
        jsonLoader(usageOutputPath, "usage")
    }

    def cancel(): Unit = {
      cancelled = true
    }

    def isCancelled: Boolean = cancelled
  }

  class TopupRename() extends Runnable {
    private var cancelled = false

    override def run(): Unit = {
      while (!cancelled)
      jsonLoader(topupOutputPath, "topup")
    }

    def cancel(): Unit = {
      cancelled = true
    }

    def isCancelled: Boolean = cancelled
  }

  def FileSplitterMethod(): Unit = {

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
    val usageDF = spark.sql("select * from tempDF where Type = 'USAGE'")
    val topupDF = spark.sql("select * from tempDF where Type = 'TOPUP'")

    usageDF.writeStream
      .format("csv")
      .option("delimiter", " ")
      .option("path", usageOutputPath)
      .option("checkpointLocation", usageOutputPath)
      .outputMode("append")
      .start()

    topupDF.writeStream
      .format("csv")
      .option("delimiter", " ")
      .option("path", topupOutputPath)
      .option("checkpointLocation", topupOutputPath)
      .outputMode("append")
      .start()

    spark.streams.awaitAnyTermination()

  }

  def jsonLoader(jsonLoaderPath: String,typeOfFile: String): Unit = {

    val metaSourcePath = jsonLoaderPath + "\\sources\\0"
    val metaReadOnlyPath = jsonLoaderPath + "\\_spark_metadata"
    val metaDestinationPath = jsonLoaderPath + "\\spark_metadata"

    val metaSourcefileCount = Option(new File(metaSourcePath).list).map(_.length).getOrElse(0)

    reader(metaReadOnlyPath,metaDestinationPath)

    var hashMap = scala.collection.mutable.Map("null" -> "null")

    for (fileNumber <- 0 until metaSourcefileCount) {

      val metaSourcefilePath = metaSourcePath + "\\" + fileNumber
      val metaDestinationFilePath = metaDestinationPath + "\\" + fileNumber

      if(Files.exists(Paths.get(metaSourcefilePath)) &&
        Files.exists(Paths.get(metaDestinationFilePath))){
        val sourcePath = spark.read.json(metaSourcefilePath)
        sourcePath.createOrReplaceTempView("sourceView")
        val destinationPath = spark.read.json(metaDestinationFilePath)
        destinationPath.createOrReplaceTempView("destinationView")

        val sourcePathQuery = spark.sql("SELECT path FROM sourceView ")
        val sourcePathValue = sourcePathQuery.select("path").collect()(1).toString().split("/").last.dropRight(1)
        val destinationPathQuery = spark.sql("SELECT * FROM destinationView ")
        val destinationPathValue = destinationPathQuery.select("path").collect()(1).toString().split("/").last.dropRight(1)

        hashMap += (sourcePathValue -> destinationPathValue)
      }


    }


    hashMap.keys.foreach { i =>
      if(Files.exists(Paths.get(jsonLoaderPath+ "\\" + hashMap(i)))) {
        renameTSV(jsonLoaderPath + "\\" + hashMap(i), jsonLoaderPath + "\\" + i)
        hashMap.remove(i)
        renameSubString(jsonLoaderPath + "\\" + i,typeOfFile)
      }
    }
  }

  def renameTSV(oldName: String, newName: String) = {
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
  }

  def renameSubString(path: String, subStringName: String): Unit ={
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val s = path.replace("mobile",subStringName)

    fs.rename(new Path(path), new Path(s))

  }
}


