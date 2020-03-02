import org.apache.spark.sql.{Encoders, SparkSession}
object scalaObject extends App{
println("New")

  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
  case class MobileSchema(Type: String, C2: String, C3: String, C4: String, C5: String, C6: String)

  var mobileSchema = Encoders.product[MobileSchema].schema

  var mobileDf = spark.read.format("csv").     // Use "csv" regardless of TSV or CSV.
    option("header", "false").  // Does the file have a header line?
    option("delimiter", " "). // Set delimiter to tab or comma.
    schema(mobileSchema).        // Schema that was built above.
    load("mobile0.tsv")

  //mobileDf.show(200)

  mobileDf.createOrReplaceTempView("df1")
  var df2 = spark.sql("select * from df1 where Type = 'USAGE'")
  var df3 = spark.sql("select * from df1 where Type = 'TOPUP'")
  println("New")
  //df2.show(200)

  df2.coalesce(1).write.option("delimiter", "\t").csv("C:\\Users\\Suprith\\Desktop\\TCD\\project3\\usage")
  df3.coalesce(1).write.option("delimiter", "\t").csv("C:\\Users\\Suprith\\Desktop\\TCD\\project3\\topup.tsv")
}

