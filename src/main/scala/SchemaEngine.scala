package com.reluscloud.dna.RelusDAaaS.DAaaSUtil


object SchemaEngine {

  def main(args: Array[String]): Unit = {
    // Current code supports only Eligibility Json data
    //val schemaEngine = new SchemaService("resources/sample.schema")
    val schemaService = new SchemaService("test.schema")
    schemaService.parseSchemaFile
  }
}