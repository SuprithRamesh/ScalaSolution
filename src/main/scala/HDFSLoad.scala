
package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


object HDFSLoad extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

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
    .schema(userSchema)      // Specify schema of the csv files
    .csv("C:\\Users\\Suprith\\Desktop\\TCD\\project3\\mobile")    // Equivalent to format("csv").load("/path/to/directory")

  val groupDF = mobileDf.select("Type")
    .groupBy("Type").count()

  mobileDf.isStreaming

  groupDF.writeStream
    .format("console")
    .outputMode("complete")
    .option("newRows",30)
    .start()
    .awaitTermination()

//  spark.sparkContext.setLogLevel("ERROR")

//  println("spark read csv files from a directory into RDD")
//  val rddFromFile = spark.sparkContext.textFile("C:/tmp/files/text01.csv")
//  println(rddFromFile.getClass)
//
//  val rdd = rddFromFile.map(f=>{
//    f.split(",")
//  })
//
//  println("Iterate RDD")
//  rdd.foreach(f=>{
//    println("Col1:"+f(0)+",Col2:"+f(1))
//  })
//  println(rdd)
//
//  println("Get data Using collect")
//  rdd.collect().foreach(f=>{
//    println("Col1:"+f(0)+",Col2:"+f(1))
//  })

//  println("read all csv files from a directory to single RDD")
//  val rdd2 = spark.sparkContext.textFile("C:\\Users\\Suprith\\Desktop\\TCD\\project3\\mobile\\*")
//  rdd2.foreach(f=>{
//    println(f)
//  })

//  println("read csv files base on wildcard character")
//  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text*.csv")
//  rdd3.foreach(f=>{
//    println(f)
//  })
//
//  println("read multiple csv files into a RDD")
//  val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.csv,C:/tmp/files/text02.csv")
//  rdd4.foreach(f=>{
//    println(f)
//  })

}
