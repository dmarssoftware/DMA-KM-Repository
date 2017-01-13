package com.rs.Preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object DataQualityValidation {
	def main(args: Array[String]): Unit = {
//			System.setProperty("hadoop.home.dir", "C:\\winutils");

//			val conf = new SparkConf().setAppName("join").setMaster("local")
			val conf = new SparkConf().setAppName("DataQuality") 
			// Create a Scala Spark Context.
			val sc = new SparkContext(conf)
			
			val inputFile = args(0)
			val outputFile = args(1)
			val delimitter = args(2)
			
			val check_Junk = sc.textFile(inputFile)
			              .map ( line => line.split(delimitter)
			              .map (_.replaceAll("[^a-zA-Z0-9 `~!@#$%^&*()-_=+{}|;:<>?,.\\/\\\'\\\"\\\\]", "")
			                     .trim())
		                .mkString(","))
		                .saveAsTextFile(outputFile)
	}
}