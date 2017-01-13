package com.rs.Matching

import scala.collection.JavaConversions._
import java.util.Arrays
import java.util.HashSet
import java.util.HashMap
import java.util.Set
import java.util.Map
import java.util.LinkedList
import java.util.Hashtable
import java.lang.String

class cosineSimilarity(s1: String, s2: String) extends Matching {
    private var compOne: String = _

  	private var compTwo: String = _
  	
  	if ((s1.length == 0 || s1.isEmpty) || (s2.length == 0 || s2.isEmpty)){
  	  compOne = s1;
  	  compTwo = s2;
  	}
  	if ((s1.length > 0 || !s1.isEmpty) || (s1.length > 0 || !s2.isEmpty)) {
    	compOne = s1.trim()
    	compTwo = s2.trim()
  	}
  	
  	def getTermFrequencyMap(terms: Array[String]): Map[String, Integer] = {
    val termFrequencyMap = new HashMap[String, Integer]()
    for (term <- terms) {
      var n = termFrequencyMap.get(term)
      n = if ((n == null)) 1 else n+1
      termFrequencyMap.put(term, n)
    }
    termFrequencyMap
  }

  def calculateScore(): Double = {
    
    val a = getTermFrequencyMap(compOne.split(""))
    
    val b = getTermFrequencyMap(compTwo.split(""))
    
    val intersection = new HashSet[String](a.keySet)
    intersection.retainAll(b.keySet)
    
    var dotProduct = 0.0
    var magnitudeA = 0.0
    var magnitudeB = 0.0
    for (item <- intersection) {
      dotProduct += (a.get(item) * b.get(item))
    }
    println(dotProduct)
    for (k <- a.keySet) {
      magnitudeA += Math.pow(a.get(k).toDouble, 2)
    }
    println(magnitudeA)
    for (k <- b.keySet) {
      magnitudeB += Math.pow(b.get(k).toDouble, 2)
    }
    println(magnitudeB)
    dotProduct / Math.sqrt(magnitudeA * magnitudeB)
  }
}