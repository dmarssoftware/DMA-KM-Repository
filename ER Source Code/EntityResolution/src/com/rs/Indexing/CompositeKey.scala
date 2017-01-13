package com.rs.Indexing

class CompositeKey {
  var pass =0;
  var blockKey = ""
  var mapno = 0
  var value = 0
  var reducerno =0
  var data = ""
  
  def this(pass:Int, blockKey:String , mapno:Int)
	{
    this()
		this.pass =pass // calls previous auxiliary constructor
		this.blockKey= blockKey
		this.mapno = mapno
	}
  
  def this(pass:Int, mapno:Int , value : Int)
	{
    this()
		this.pass =pass // calls previous auxiliary constructor
		this.value= value
		this.mapno = mapno
	}
  
  def this(pass:Int, reducerno:Int , blockKey:String , data : String)
	{
    this()
		this.pass =pass // calls previous auxiliary constructor
		this.data= data
		this.reducerno = reducerno
		this.blockKey = blockKey
	}
  
  def getReducerNo() : Int =
  {
    return this.reducerno
  }
  
  def getData() : String =
  {
    return this.data
  }
  
  def getPass() : Int =
  {
    return this.pass
  }
  
  def getMapno() : Int =
  {
    return this.mapno
  }
  
  def getValue() : Int =
  {
    return this.value
  }
  
  def getBlockKey() : String =
  {
    return this.blockKey
  }
  
  
}