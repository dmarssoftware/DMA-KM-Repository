package com.rs.Matching

import sun.security.util.Length

class JaroWinkler(s1:String, s2:String) extends Matching {

  private var compOne: String = _

  private var compTwo: String = _

  private var theMatchA: String = ""

  private var theMatchB: String = ""

  private var mRange: Int = -1
  
  private var min_length = 0

  if ((s1.length == 0 || s1.isEmpty) || (s2.length == 0 || s2.isEmpty)){
  	  compOne = s1;
  	  compTwo = s2;
  	}
  
  if ((s1.length > 0 || !s1.isEmpty) || (s1.length > 0 || !s2.isEmpty)) {
    if(s1.length > s2.length){
    	compOne = s2
    	compTwo = s1
    	min_length = s2.length()
    }
    else{
      compOne = s1
      compTwo = s2
      min_length = s1.length()
    }
  }

  def calculateScore(): Double = {
    compOne = this.compOne
    compTwo = this.compTwo
    mRange = Math.max(compOne.length, compTwo.length) / 2 - 1
    var res = -1.0
    val m = getMatch
    var t = 0
    if (getMissMatch(compTwo, compOne) > 0) {
      t = (getMissMatch(compOne, compTwo) / getMissMatch(compTwo, compOne))
    }
    val l1 = compOne.length
    val l2 = compTwo.length
    val f = 0.3333
    val mt = (m - t).toDouble / m
    val jw = f * (m.toDouble / l1 + m.toDouble / l2 + mt.toDouble)
    res = jw + 
      getCommonPrefix(compOne, compTwo) * (0.1 * (1.0 - jw))
    res
  }

  private def getMatch(): Int = {
    theMatchA = ""
    theMatchB = ""
    var matches = 0
    for (i <- 0 until compOne.length) {
      var counter = 0
      while (counter <= mRange && i >= 0 && counter <= i) {
        if (compOne.charAt(i) == compTwo.charAt(i - counter)) {
          matches += 1
          theMatchA = theMatchA + compOne.charAt(i)
          theMatchB = theMatchB + compTwo.charAt(i)
        }
        counter += 1
      }
      counter = 1
      while (counter <= mRange && i < compTwo.length && counter + i < compTwo.length) {
        if (compOne.charAt(i) == compTwo.charAt(i + counter)) {
          matches += 1
          theMatchA = theMatchA + compOne.charAt(i)
          theMatchB = theMatchB + compTwo.charAt(i)
        }
        counter += 1
      }
    }
    matches
  }

  private def getMissMatch(s1: String, s2: String): Int = {
    var transPositions = 0
    for (i <- 0 until theMatchA.length) {
      var counter = 0
      while (counter <= mRange && i >= 0 && counter <= i) {
        if (theMatchA.charAt(i) == theMatchB.charAt(i - counter) && 
          counter > 0) {
          transPositions += 1
        }
        counter += 1
      }
      counter = 1
      while (counter <= mRange && i < theMatchB.length && (counter + i) < theMatchB.length) {
        if (theMatchA.charAt(i) == theMatchB.charAt(i + counter) && 
          counter > 0) {
          transPositions += 1
        }
        counter += 1
      }
    }
    transPositions
  }

  private def getCommonPrefix(compOne: String, compTwo: String): Int = {
    var cp = 0
    for (i <- 0 until min_length if compOne.charAt(i) == compTwo.charAt(i)) cp += 1
    cp
  }
}
