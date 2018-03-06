package com.reluscloud.dna.RelusDAaaS.DAaaSUtil

class SchemaService(val schemaConfigLocation: String) {

  var TABLENAME = ""
  var INPUTFOLDER = ""
  var COLUMNSCHEMA = List[ColumnSchema]()


  def parseSchemaFile {
    try{
      println(schemaConfigLocation)

      this.TABLENAME = "Table Name"

      this.INPUTFOLDER = "Input Folder"

      //for each row, insert into ColumnSchema
      //bufferedReader.readLine
      //Parse info on that line
      //insert into Column Schema

    }catch{
      case e: Exception => println(e)
    }
  }
}