package com.rs.Matching

import java.lang.String

class LongestCommon(s1: String, s2: String) extends Matching {
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
   
   def LCS(compOne: String, compTwo: String): Double = {
     
    if (compOne.length == 0 || compTwo.length == 0) {
      return 0
    }
    val lenA = compOne.length
    val lenB = compTwo.length
    if (compOne.charAt(lenA - 1) == compTwo.charAt(lenB - 1)) {
      1 + 
        LCS(compOne.substring(0, lenA - 1), compTwo.substring(0, lenB - 1))
    } else {
      Math.max(LCS(compOne.substring(0, lenA - 1), compTwo.substring(0, lenB)), LCS(compOne.substring(0, lenA), compTwo.substring(0, 
        lenB - 1)))
    }
  }
   
   def calculateScore(): Double = {
     val score = LCS(s1,s2)
     score
   }
   /*
  def lcs(str1: String, str2: String): String = {
    val l1 = str1.length
    val l2 = str2.length
    val arr = Array.ofDim[Int](l1 + 1, l2 + 1)
    var i = l1 - 1
    while (i >= 0) {
      var j = l2 - 1
      while (j >= 0) {
        arr(i)(j) = if (str1.charAt(i) == str2.charAt(j)) arr(i + 1)(j + 1) + 1 else Math.max(arr(i + 1)(j), 
          arr(i)(j + 1))
        j -= 1
      }
      i -= 1
    }
    i = 0
    var j = 0
    val sb = new StringBuffer()
    while (i < l1 && j < l2) {
      if (str1.charAt(i) == str2.charAt(j)) {
        sb.append(str1.charAt(i))
        i += 1
        j += 1
      } else if (arr(i + 1)(j) >= arr(i)(j + 1)) i += 1 else j += 1
    }
    sb.toString
  }*/
}
