package com.reluscloud.dna.RelusDAaaS.DAaaSUtil

import scala.io.Source

class SchemaService(val schemaConfigLocation: String) {

  var TABLENAME = ""
  var SOURCEFOLDER = ""
  var COLUMNSCHEMA = List[ColumnSchema]()
  val Pattern = "^\\{.*\\}$".r


  def parseSchemaFile {
    try{
      this.TABLENAME = "Table Name"
      this.SOURCEFOLDER = "Input Folder"

      for (line <- Source.fromFile(schemaConfigLocation).getLines) {
        var trimmedLine = line.trim

        trimmedLine match {
          case Pattern() => {
            println(trimmedLine)
          }
          case _ =>
        }
        //Parse each line here
      }

    }catch{
      case e: Exception => println(e)
    }
  }
}