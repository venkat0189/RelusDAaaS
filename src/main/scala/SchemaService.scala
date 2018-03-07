package com.reluscloud.dna.RelusDAaaS.DAaaSUtil
//import scala.util.control.Breaks._
import scala.io.Source

class SchemaService(val schemaConfigLocation: String) {

  var TABLENAME = ""
  var INPUTFOLDER = ""
  var OUTPUTFOLDER = ""
  var DELIMITER =","
  var COLUMNSCHEMA = List[ColumnSchema]()
  val Pattern = "^([^:.]*):(.*)$".r

  var columnFound = 0

  def parseSchemaFile {
    try{
       for (line <- Source.fromFile(schemaConfigLocation).getLines) {

        println(line)

        var trimmedLine = line.trim

          trimmedLine match {

            case Pattern(skey, svalue) => {
              println("key is " + skey.trim)
              println("value is " + svalue.trim)
              if (columnFound == 0) {
                skey.trim match {
                  case "tableName" => {
                    this.TABLENAME = svalue.trim
                  }
                  case "inputFolder" => {
                    this.INPUTFOLDER = svalue.trim
                  }
                  case "outputFolder" => {
                    this.OUTPUTFOLDER = svalue.trim
                  }
                  case "delimiter" => {
                    this.DELIMITER = svalue.trim
                  }
                  case "columns" => {
                    columnFound = 1

                  }

                  case _ => {
                    println("no match -->" + trimmedLine)
                  }
                }
              }
              if (columnFound == 1) {
                skey.trim match {
                  case _ => {
                    if(skey.trim!='(' || skey.trim!=')'){}
                    //Add objects to COLUMNSCHEMA list
                  }
                }
              }
            }
          }
        //Parse each line here
      }

    }catch{
      case e: Exception => println(e)
    }

    print(COLUMNSCHEMA)
  }


}