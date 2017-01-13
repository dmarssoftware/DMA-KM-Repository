package com.rs.Deduplication

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object UniqueRecordGenerate {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "C:\\winutils");
    
//     val conf = new SparkConf().setAppName("join").setMaster("local")
     val conf = new SparkConf().setAppName("join") 
      // Create a Scala Spark Context.
     val sc = new SparkContext(conf)
     
    val uniqFile = args(0)
    val lookupFile = args(1)
    val outputData = args(2)
    
    val split_Rec = sc.textFile(uniqFile).flatMap ( rec => rec.split("\\s+") ).distinct()  //added
    
    val unique = split_Rec.map ( line => (line.split(",")(0), line.split(",")) )  //added
              
    val lookup = sc.textFile(lookupFile).map ( line => (line.split(",")(0), line.split(",") ) )
      
    val joinSet = unique.join(lookup)
    
    val joinData = joinSet.map(data => (data._2._2).mkString(", "))
    
    joinData.saveAsTextFile(outputData)   
  }
}