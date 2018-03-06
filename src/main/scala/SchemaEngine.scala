package com.reluscloud.dna.RelusDAaaS.DAaaSUtil


object SchemaEngine {

  def main(args: Array[String]): Unit = {
    // Current code supports only Eligibility Json data
    val schemaEngine = new SchemaService("resources/sample.schema")
    println("Hello World")
  }
}