package com.rs.Matching

import java.lang.String

class DamerauLevenshtein(a: String, b: String) extends Matching {

  private var compOne: String = _

  private var compTwo: String = _

  private var matrix: Array[Array[Int]] = _

  private var calculated: java.lang.Boolean = false

  if ((a.length == 0 || a.isEmpty) || (b.length == 0 || b.isEmpty)){
  	  compOne = a;
  	  compTwo = b;
  	}
  
  if ((a.length > 0 || !a.isEmpty) || (b.length > 0 || !b.isEmpty)) {
    compOne = a.trim()
    compTwo = b.trim()
  }

  def getMatrix(): Array[Array[Int]] = {
    setupMatrix()
    matrix
  }

  def calculateScore(): Double = {
    val res = -1
    val INF = compOne.length + compTwo.length
    matrix = Array.ofDim[Int](compOne.length + 1, compTwo.length + 1)
    for (i <- 0 until compOne.length) {
      matrix(i + 1)(1) = i
      matrix(i + 1)(0) = INF
    }
    for (i <- 0 until compTwo.length) {
      matrix(1)(i + 1) = i
      matrix(0)(i + 1) = INF
    }
    val DA = Array.ofDim[Int](24)
    for (i <- 0 until 24) {
      DA(i) = 0
    }
    for (i <- 1 until compOne.length) {
      var db = 0
      for (j <- 1 until compTwo.length) {
        val i1 = DA(compTwo.indexOf(compTwo.charAt(j - 1)))
        val j1 = db
        val d = (if ((compOne.charAt(i - 1) == compTwo.charAt(j - 1))) 0 else 1)
        if (d == 0) db = j
        matrix(i + 1)(j + 1) = Math.min(Math.min(matrix(i)(j) + d, matrix(i + 1)(j) + 1), Math.min(matrix(i)(j + 1) + 1, 
          matrix(i1)(j1) + (i - i1 - 1) + 1 + (j - j1 - 1)))
      }
      DA(compOne.indexOf(compOne.charAt(i - 1))) = i
    }
    matrix(compOne.length())(compTwo.length)
  }

  def getDHSimilarity(): Double = {
    if (!calculated) setupMatrix()
    matrix(compOne.length())(compTwo.length)
  }

  private def setupMatrix() {
    var cost = -1
    var del: Int = 0
    var sub: Int = 0
    var ins: Int = 0
    matrix = Array.ofDim[Int](compOne.length + 1, compTwo.length + 1)
    var i = 0
    while (i <= compOne.length) {
      matrix(i)(0) = i
      i += 1
    }
    i = 0
    while (i <= compTwo.length) {
      matrix(0)(i) = i
      i += 1
    }
    i = 1
    while (i <= compOne.length) {
      var j = 1
      while (j <= compTwo.length) {
        cost = if (compOne.charAt(i - 1) == compTwo.charAt(j - 1)) 0 else 1
        del = matrix(i - 1)(j) + 1
        ins = matrix(i)(j - 1) + 1
        sub = matrix(i - 1)(j - 1) + cost
        matrix(i)(j) = minimum(del, ins, sub)
        if ((i > 1) && (j > 1) && (compOne.charAt(i - 1) == compTwo.charAt(j - 2)) && 
          (compOne.charAt(i - 2) == compTwo.charAt(j - 1))) {
          matrix(i)(j) = minimum(matrix(i)(j), matrix(i - 2)(j - 2) + cost)
        }
        j += 1
      }
      i += 1
    }
    calculated = true
    displayMatrix()
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

  private def minimum(d: Int, i: Int, s: Int): Int = {
    var m = java.lang.Integer.MAX_VALUE
    if (d < m) m = d
    if (i < m) m = i
    if (s < m) m = s
    m
  }

  private def minimum(d: Int, t: Int): Int = {
    var m = java.lang.Integer.MAX_VALUE
    if (d < m) m = d
    if (t < m) m = t
    m
  }
}
