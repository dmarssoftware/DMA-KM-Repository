package com.rs.Matching

object MatchFactory {
  def apply(algo:String,col1:String,col2:String)= 
    algo.toUpperCase() match {
    case "LEVENSHTEIN" => new Levenshtein(col1,col2)
    case "JAROWINKLER" => new JaroWinkler(col1,col2)
    case "DAMERAULEVEN" => new DamerauLevenshtein(col1,col2)
    case "LONGESTSUB" => new LongestCommon(col1,col2)    
    case "DICECOEFF" => new diceCoefficient(col1,col2)
    case "COSINESIM" => new cosineSimilarity(col1,col2)
    case default  => new Levenshtein(col1,col2)    
  }
}