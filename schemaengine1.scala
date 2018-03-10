import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object schemaengine1 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("schemaengine1").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("schemaengine1").setMaster("local[*]")

    val sqlContext = spark.sqlContext
    val data = "C:\\Users\\T S Surendar\\IdeaProjects\\RelusDAaaS\\resources\\SampleCsv\\Sample.csv"
    //val dfschema = spark.read.format("csv").option("inferSchema","true")
   //   .option("header","true").load(data).schema
    //val rdd = dfschema.map("DELIMITER").map(line=> (line,{for (i <-1 to l)})

    //alternative Name,Id,Date,Value
    //val dfschema = new StructType().add("name", "string").add("Id", "string").add("Datecol", "string").add("Value", "string")
    val dfschema = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("data")
  // val rawrdd = spark.sparkContext.textFile(data)
  //  val rdd = rawrdd.filter(x=>x!=rawrdd.first())


    dfschema.show()
    //this is called programmatically schema
   // df.write.parquet("data.parquet")
    //df.coalesce(1)write.parquet("StudentDataPQ.parquet").option("header", "true").save("s3://relus-dataaccelerator/output/sampletable/")
    spark.stop()
  }
}