package com.reluscloud.dna.RelusDAaaS.DAaaSUtil

import org.apache.spark.sql.SparkSession



object SchemaEngine {

  def main(args: Array[String]): Unit = {
    // Current code supports only Eligibility Json data
    //val schemaEngine = new SchemaService("resources/sample.schema")

    val schemaService = new SchemaService("test.schema")
    //schemaService.parseSchemaFile
    //println(SchemaService)

    val spark = SparkSession.builder.appName("SchemaEngine").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val sQLContext = spark.sqlContext
   // val data = schemaService.INPUTFOLDER + schemaService.TABLENAME
    val input = "resources/SampleCsv/Sample.csv"

    val dataschema = spark.read.format("csv").option("inferSchema","true").option("header","true").load(input)
   dataschema.show()
    dataschema.write.format("parquet").save("output.parquet")



  }
}

