package com.rs.Matching

import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.IOException
import collection.mutable.HashMap
import scala.collection.mutable._

object MainObject {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\winutils\\")    // For Local Mode
    
    // For Local Mode
    //val conf = new SparkConf().setAppName("MatchingAlgo").setMaster("local")
    
    // For Spark Cluster
    val conf = new SparkConf().setAppName("MatchingAlgo")
    
    val sc = new SparkContext(conf)
    
    val inputFile = args(0).toString()
    val outputFile = args(1).toString()
    val key = args(2).toString()
    val sep_file = "#"
    val sep_file_column = "\\|"
    val sep_multi_columns = ";"
    val sep_column_algo = ":"
    val sep_data_file = "\\),\\("
    val sep_data_datum = args(3).toString()
    
    val tuple = key.split(sep_file)  // --split("#")
 
    //val chkParam = new HashMap[String,String]()
    var filename = new ArrayBuffer[String]()
    var numKeys = new Array[String](5) 
    
    var colAlgoPair = new ListBuffer[String]()
    var colAlgo : List[String] = List()
    
    var colMatch = new ArrayBuffer[Int]()
    var mAlgo = new ArrayBuffer[String]()
    
   for(v <- 0 to (tuple.length - 1)){
      // in.txt|2:<algo1>;3:<algo2>#in2.txt|3:<algo1>;2:<algo2>
      filename += tuple(v).split(sep_file_column)(0)     // Stores all the filenames --split("\\|")
      val colData = tuple(v).split(sep_file_column)(1)   // Takes the Column Number and the Algorithm Pair for each file --split("\\|")
           
      numKeys = colData.split(sep_multi_columns)             // Stores the matching functions to the respective column numbers  --split(";")
      
      for(y <- 0 until numKeys.length){
        colAlgoPair += colData.split(sep_multi_columns)(y)   //  Takes all the Column number and matching algorithm pairs     --split(":")
      }
      colAlgo = (colAlgoPair.toList)           // Casts the 'colAlgoPair' ListBuffer items to List items.
      
     //chkParam += (filename -> colnum)
    }
    colAlgo.foreach { colMatch += _.split(sep_column_algo)(0).toInt+1  }  // Stores all the column numbers in an Array serially  --split(":")
    colAlgo.foreach { mAlgo += _.split(sep_column_algo)(1) }            // Stores all the matching algorithms in an Array serially  --split(":")
    
      
    /*for ((key,value) <- chkParam){
         println(key,value)
    }*/
  
    var data = sc.textFile(inputFile)
    data.map{ row =>
      //println(row)
     val sp = row.split(sep_data_file)  // --split("\\),\\(")
     var cols = new ArrayBuffer[String]()
     val blockkey1 = sp(0).split(",")(0).replaceAll("[()]", "")
     val blockkey2 = sp(1).split(",")(0).replaceAll("[()]", "")
     
     val filen1 = sp(0).split(",")(1).replaceAll("[()]", "")  //  --split the filename for each set     
     val filen2 = sp(1).split(",")(1).replaceAll("[()]", "")  //  --split the filename for each set
     
     val file_rec1 = sp(0).split(",")(2).replaceAll("[()]", "");  // -- take the filename with record number
     val file_rec2 = sp(1).split(",")(2).replaceAll("[()]", "");  // -- take the filename with record number
     
     var colvalFirst = ""
     var colvalSecond = ""
     var file1_pos = 0
     var file2_pos = 0
     for(n <- 0 until filename.length){
     		if(filen1 == filename(n)){
     			file1_pos = n
     		}
     		if(filen2 == filename(n)){
     			file2_pos = n
     		}
     }
      
     var k = file1_pos * numKeys.length
     var l = file2_pos * numKeys.length
     		
     for(y <- 0 until numKeys.length){
       if(sep_data_datum != "," ){
         if (colMatch(k) == 1){
           val dataSet1 = sp(0).split(",")
           colvalFirst = dataSet1(1).split(sep_data_datum)(0).replaceAll("[()]", "")
              cols += colvalFirst
         }
         else{       
     		    colvalFirst = sp(0).split(sep_data_datum)(colMatch(k)-1).replaceAll("[()]", "")  //  --split the data seprator
     		      cols += colvalFirst
         }
     	   if (colMatch(l) == 1){
     	      val dataSet2 = sp(1).split(",")
     	      colvalSecond = dataSet2(1).split(sep_data_datum)(0).replaceAll("[()]", "")
     	        cols += colvalSecond
     	   }
     	   else{     		
     		    colvalSecond = sp(1).split(sep_data_datum)(colMatch(l)-1).replaceAll("[()]", "")  //  --split the data separtor		
     		      cols += colvalSecond
     	   }		
     		      k = k + 1
     		      l = l + 1
      }
      else{
            colvalFirst = sp(0).split(",")(colMatch(k)).replaceAll("[()]", "")
     				  cols += colvalFirst
     		    colvalSecond = sp(1).split(",")(colMatch(l)).replaceAll("[()]", "")
     				  cols += colvalSecond
     				
     				  k = k + 1
     				  l = l + 1
      }
     }
     //val score = new ArrayBuffer[Double]()
     var score = 0.0
     val comparison = new ArrayBuffer[String]()
     var n = 0
     for(v <- 0 until numKeys.length){
          val matc = MatchFactory(mAlgo(v),cols(n),cols(n+1))
          score = matc.calculateScore()
//          comparison += "{(" + cols(n) + " , " + cols(n+1) + ")" + " => Algorithm :: "+ mAlgo(v) + " , Score :: " + score.toString() +" }"
            comparison +=  cols(n) + " , " + cols(n+1) + " , " + score.toString() + " , "
          n = n + 2
     }
     //(row,comparison)
     var complte_Row = blockkey1 + " , " + file_rec1 + " , " + blockkey2 + " , " + file_rec2 + " , " +comparison.mkString
     var compact_Row = complte_Row.substring(0 , complte_Row.length - 2 ) // Remove the last "," from the string
     
     (compact_Row)
    }.saveAsTextFile(outputFile)
  }
}