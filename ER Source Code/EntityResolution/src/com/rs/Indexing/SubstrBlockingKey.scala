package com.rs.Indexing

import scala.collection.mutable.LinkedHashMap
//remove if not needed
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext

class SubstrBlockingKey {
  def generateBlockingKey(row: String, encodingFunc: LinkedHashMap[Int, Int],colSeparator:String): String = {
    var blockingKey = ""
    for ((key, value) <- encodingFunc) {
      val index = key
      val len = value
      var colLen : Int= -1
      var colVal=""
      if(colSeparator.equals(",")){
        
        colLen = row.split(colSeparator)(index).length        
        colVal = row.split(colSeparator)(index).toLowerCase()
      }
      else
      {
         colLen = row.split(colSeparator)(index - 1 ).length 
         colVal = row.split(colSeparator)(index - 1 ).toLowerCase()
         if(index == 1)
         {
           colVal = colVal.split(",")(1)
         }
        
      }
      
      if (len > colLen) {
        for (i <- 0 until len - colLen) {
          colVal = colVal.concat("x")
        }
        blockingKey = blockingKey.concat(colVal)
        
      } else
        {
        blockingKey = blockingKey.concat(colVal.substring(0, (len)))
        }
    }
    blockingKey
  }
}