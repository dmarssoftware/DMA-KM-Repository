package com.rs.Matching

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.io.LongWritable

class Levenshtein(s1: String, s2: String) extends Matching {

  private var matrix: Array[Array[Int]] = _

  private var calculated: java.lang.Boolean = false
  
  private var compOne: String = _

  private var compTwo: String = _
  
  private var max_len: Double = _
  
    if ((s1.length == 0 || s1.isEmpty) || (s2.length == 0 || s2.isEmpty)){
  	  compOne = s1;
  	  compTwo = s2;
  	}
  	if ((s1.length > 0 || !s1.isEmpty) || (s1.length > 0 || !s2.isEmpty)) {
    	compOne = s1.trim()
    	compTwo = s2.trim()
    	if( compOne.length > compTwo.length ){
    	  max_len = compOne.length
    	}
    	else{
    	  max_len = compTwo.length 
    	}    	  
  	}


  def calculateScore(): Double = {
    if (!calculated) {
      setupMatrix()
    }
    matrix(compOne.length())(compTwo.length) / max_len
  }
  
  def getMatrix(): Array[Array[Int]] = {
    setupMatrix()
    matrix
  }

  private def setupMatrix() {
    matrix = Array.ofDim[Int](compOne.length + 1, compTwo.length + 1)
    var i = 0
    while (i <= compOne.length) {
      matrix(i)(0) = i
      i += 1
    }
    var j = 0
    while (j <= compTwo.length) {
      matrix(0)(j) = j
      j += 1
    }
    for (i <- 1 until matrix.length; j <- 1 until matrix(i).length) {
      if (compOne.charAt(i - 1) == compTwo.charAt(j - 1)) {
        matrix(i)(j) = matrix(i - 1)(j - 1)
      } else {
        var minimum = java.lang.Integer.MAX_VALUE
        if ((matrix(i - 1)(j)) + 1 < minimum) {
          minimum = (matrix(i - 1)(j)) + 1
        }
        if ((matrix(i)(j - 1)) + 1 < minimum) {
          minimum = (matrix(i)(j - 1)) + 1
        }
        if ((matrix(i - 1)(j - 1)) + 1 < minimum) {
          minimum = (matrix(i - 1)(j - 1)) + 1
        }
        matrix(i)(j) = minimum
      }
    }
    calculated = true
   // displayMatrix()
  }

  private def displayMatrix() {
    println("  " + compOne)
    var y = 0
    while (y <= compTwo.length) {
      if (y - 1 < 0) System.out.print(" ") else System.out.print(compTwo.charAt(y - 1))
      var x = 0
      while (x <= compOne.length) {
        System.out.print(matrix(x)(y))
        x += 1
      }
      println()
      y += 1
    }
  }
}