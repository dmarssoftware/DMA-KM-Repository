package com.rs.Matching

import java.util.HashSet
import java.util.Set
import java.lang.String

class diceCoefficient(s1: String, s2: String) extends Matching {
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
  	
  	def toSet(s: String): Set[Character] = {
      val ss = new HashSet[Character](s.length)
      for (c <- s.toCharArray()) ss.add(java.lang.Character.valueOf(c))
      ss
    }
  	  
  	def calcDice(s1len: Int, s2len: Int, intsize: Int): Double = {
      (2 * intsize).toDouble / (s1len + s2len).toDouble
    }
  	
  	def calculateScore(): Double = {
  	  val ss1 = toSet(compOne)
      ss1.retainAll(toSet(compTwo))
    //println(ss1.size)
      val dicescore = calcDice(compOne.length, compTwo.length, ss1.size)
      dicescore
  	}
}