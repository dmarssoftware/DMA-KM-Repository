package com.rs.Indexing

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._

import collection.mutable.HashMap
import collection.mutable.LinkedHashMap
import collection.mutable.LinkedList
import collection.mutable.ListBuffer
import scala.collection.immutable.ListMap
import scala.collection.mutable.Queue


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader
import java.io.InputStreamReader;
import java.io.FileInputStream;

import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.TaskContext
import org.apache.spark.storage.StorageLevel


case class Key(passNo : Int, boundary:Int,partition:Int,blockingKey:String)


object Key {
	implicit def orderingByPassBoundaryPartitionBK[A <: Key] : Ordering[A] = {
			Ordering.by(fk => (fk.passNo, fk.boundary, fk.partition , fk.blockingKey))
	}
}


object Main {

	var entityIndexes = new HashMap[String,Int] 
			var splitPoint = new ListBuffer[Int]

					def main(args : Array[String]){

		//For running in window machine
//		System.setProperty("hadoop.home.dir", "C:\\winutils");
		val inputFile = args(0)
				val outputFile = args(1)
				val tempFolder = args(2)
				val bkInput = args(3)
				val windowSize =args(4)
				val reduceTasks =args(5).toInt
				val colSeparator = args(6)
				val sparkCl =args(7)



//				     val conf = new SparkConf().setAppName("SNN").setMaster("local")
				val conf = new SparkConf().setAppName("SNN")
				// Create a Scala Spark Context.
				val sc = new SparkContext(conf)

				//Extract columns and substring length for blocking key
				val multipassBkParam =new HashMap[Int,HashMap[String,LinkedHashMap[Int,Int]]]
						var filename = ""
						val passes = bkInput.split("\\/")
						var passno = passes.length
						for (k <- 0 until passes.length){
							val bkParam = new HashMap[String,LinkedHashMap[Int,Int]]()
									val passno = k+1
									val files =passes(k).split("#")
									for(j <- 0 until files.length){
										filename =files(j).split("\\|")(0)
												val bk = files(j).split("\\|")(1)
												val bkmap = new LinkedHashMap[Int,Int]()
												val bkcnt = bk.split(":")
												for(i <- 0 until bkcnt.length){
													val value = bkcnt(i).split(",")

															bkmap += (value(0).toInt -> value(1).toInt)
												}

										bkParam += (filename -> bkmap)
									}
							multipassBkParam += (passno -> bkParam)
						}

		//Extract window size for each pass
		var windowSizeArray = new HashMap[Int,Int]
				val pass_windowsize =windowSize.split("\\/")
				for (s <- 1 to pass_windowsize.length){
					windowSizeArray.put(s, pass_windowsize(s-1).toInt)
				}

		//Generate Blocking key 

		//Append filename to each row
		val fc = classOf[TextInputFormat]
				val kc = classOf[LongWritable]
						val vc = classOf[Text]
								val path :String = inputFile
								val text = sc.newAPIHadoopFile(path, fc ,kc, vc, sc.hadoopConfiguration)
								val linesWithFileNames = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
										.mapPartitionsWithInputSplit((inputSplit, iterator) => {
											val file = inputSplit.asInstanceOf[FileSplit]
													iterator.map(tup => (file.getPath.getName , tup._2))
										}
												)


										linesWithFileNames.map{t => 
										//Extract map number   
										val mapno = TaskContext.get.partitionId()

										var blockingKeys = ""
										var bkmap =  new LinkedHashMap[Int,Int]
												var bkParam = new HashMap[String,LinkedHashMap[Int,Int]]

														//Generate blocking key for each pass 
														for(p <- 1 to passno){
															var blockKey = ""
																	bkParam = multipassBkParam.getOrElse(p,bkParam)
																	bkmap = bkParam.getOrElse(t._1, bkmap)
																	val bkObj = new SubstrBlockingKey()
																	blockKey = bkObj.generateBlockingKey(t.toString(), bkmap,colSeparator)
																	blockingKeys = blockingKeys + "\t" +"Key:"+p+":"+blockKey+":"+mapno;
														}

										(blockingKeys , t.toString())
						}.saveAsSequenceFile(tempFolder+"keyData")
										//    }.saveAsTextFile(tempFolder+"keyData")   



										//Generate KPM---------------------------------------------------------------------------------

						val hadoopconf = new Configuration();
						hadoopconf.set("fs.defaultFS" , sparkCl)
						val fs = FileSystem.get(hadoopconf);

						val outFileStream = fs.create(new Path(tempFolder+"key_cnt"))
								val kpmrdd =sc.sequenceFile[String,String](tempFolder+"keyData").collect()

								kpmrdd.map{t => 

								val passnos=t.toString().split("\\,\\(")(0).split("\t")

								passnos.map{ tt => 
								if(tt != "(")

									outFileStream.writeBytes((tt.split(":")(1).replace("\\(", "").toInt+":"+tt.split(":")(2)+":"+tt.split(":")(3).toInt)+"\n")

								}

						}
						outFileStream.close()
						fs.close()


						val kpmInput = sc.textFile(tempFolder+"key_cnt").persist(StorageLevel.MEMORY_AND_DISK_SER_2)
						val kpcnt=kpmInput.map( kpdata => (kpdata,1) ).persist(StorageLevel.MEMORY_AND_DISK_SER_2).reduceByKey{case (x, y) => x + y}

						kpcnt.persist(StorageLevel.MEMORY_AND_DISK_SER_2).saveAsTextFile(tempFolder+"KPM")
						//---------------------------------kpm matrix done---------------------------------------------

						//Auto Partitioner utilizing KPM
						var KPMlookupText = new HashMap[CompositeKey,Int]

								for (line <- sc.textFile(tempFolder+"KPM/").toLocalIterator.toArray) {      
									val cnt = line.split(",")(1).replaceAll("\\)", "")
											val pass = line.split(",")(0).split(":")(0).replaceAll("\\(", "").toInt
											val bk = line.split(",")(0).split(":")(1)
											val mapno =line.split(",")(0).split(":")(2).toInt
											KPMlookupText.put(new CompositeKey(pass,bk,mapno), cnt.toInt)
								}

						var n=0;
						for((key,value) <- KPMlookupText){
							if(key.getPass() == 1)
								n=n+1
						}

						// Global index of next entity of pass i and block j
						// for this map task. This map contains at most one
						//  entry per (pass; blockKey) pair

						for(i <- 1 to passno){
							var entityIndex =1        
									var BKwithValues = new HashMap[String,CompositeKey]
											var sortkey = new ListBuffer[String]


													//Sort blocking keys for each pass and store in sortedBKWithPassno 
													for((key,value) <- KPMlookupText){
														var kpm_mapno =key.getMapno()
																var kpm_passno =key.getPass()
																var kpm_blockKey=key.getBlockKey()
																if(kpm_passno == i){
																	sortkey += kpm_blockKey
																			var val1 =new CompositeKey(kpm_passno,kpm_mapno,value)
																			BKwithValues.put(kpm_blockKey, val1);               
																}
													}

							//println("Unsorted: "+sortkey.toList)
							//println("Sorted: "+sortkey.toList.sorted)
							// println(BKwithValues.get("blue17").get)

							for(bk <- sortkey.toList.sorted){
								var compkey = BKwithValues.get(bk).get

										var cnt =((i-1)*n)+entityIndex

										entityIndexes.put(i+":"+bk, cnt)

										entityIndex = entityIndex + compkey.getValue()  
//										println(entityIndex)
							}       
						}
//						println()
//						println(entityIndexes)
//						println()
						//Pass-aware and window size-aware reduce task
						//assignment for each entity, targeting an equal
						// number of pairs per reduce task


						var N=0
						for(j <- 1 to passno){
							N = ( n - (windowSizeArray.get(j).get/2) ) * (windowSizeArray.get(j).get - 1 )
						}
						//        println("N: "+N)
						var No = N/reduceTasks;
						//        println("No: "+No)
						var pairsLeft =No;
						var offset =0;

						// 	      println("entities fit: "+ min((n/windowSizeArray.get(1).get)+windowSizeArray.get(1).get/2,n))

						// 	      println(n)
						for(k <- 1 to passno){
							var entitiesLeft = n
									while(entitiesLeft > 0 ){
										var entitiesThatFit = min(((pairsLeft/windowSizeArray.get(k).get)+windowSizeArray.get(k).get/2),entitiesLeft);
//										println("entitiesThatFit: "+entitiesThatFit)

										offset = offset +entitiesLeft;
//										println("offset: "+offset)

										splitPoint += offset
//										println("splitPoint: "+ splitPoint)

										entitiesLeft = entitiesLeft - entitiesThatFit;
//										println("entitiesLeft: "+entitiesLeft)

										var pairsThatFit = (entitiesThatFit - windowSizeArray.get(k).get/2) * (windowSizeArray.get(k).get-1);
										// 			        println(entitiesThatFit)
										// 			        println( windowSizeArray.get(k).get/2)
										// 			        println(windowSizeArray.get(k).get-1)
//										println("pairsThatFit: "+pairsThatFit)

										pairsLeft = pairsLeft - pairsThatFit;
//										println("pairsLeft: "+pairsLeft)
//										println()
										if (pairsLeft <= 0)
											pairsLeft=No;
									}
						}
						splitPoint.toList
//						println("splitPoint "+splitPoint)
						//job2 

						val fs1 = FileSystem.get(hadoopconf)

						//Initialize queues for replicating data
						var QueueList = new HashMap[String,HashMap[String,CompositeKey]]

								val outFileStream1 = fs1.create(new Path(tempFolder+"output_AfterKey")) 


								val lines =sc.sequenceFile[String,String](tempFolder+"keyData").persist(StorageLevel.MEMORY_AND_DISK_SER_2).collect()
								lines.map{ t => 
								var line = t.toString().split(",\\(")(0)
								var keys =line.split("\t")
								var i = 0

								keys.map{ kt =>
								i=i+1
								if(i != 1) {
									var pass = kt.split(":")(1).toInt
											var blockkey = kt.split(":")(2)

											var redu = getPartition(pass,blockkey,reduceTasks)
											outFileStream1.writeBytes((pass+":"+redu+":"+redu+":"+blockkey).toString()+","+t.toString().split(",\\(")(1).replaceAll("\\)", "").toString()+"\n")

											//Identify rows to replicate for P(K)+1
											if(redu < reduceTasks){ 
												var value=t.toString().split(",\\(")(1).replaceAll("\\)", "").toString()
														var q = new HashMap[String,CompositeKey]
																if(QueueList.get(pass+":"+redu).isEmpty){

																	q.put(blockkey, new CompositeKey(pass,redu,blockkey,value))
																	QueueList.put(pass+":"+redu, q)
																}
																else
																{

																	q = QueueList.get(pass+":"+redu).get
																			//if queue length less than the window size 
																			if(q.size < (windowSizeArray.get(pass).get-1)){
																				q.put(blockkey, new CompositeKey(pass,redu,blockkey,value))
																				//sort hashmap on the basis of key low to high

																				QueueList.put(pass+":"+redu, q)

																			}

																			else
																			{
																				var qBk = ListMap(q.toSeq.sortWith(_._1 < _._1):_*).entrySet().iterator().next().getKey
																						if(blockkey > qBk )
																						{
																							q.remove(qBk)
																							q.put(blockkey, new CompositeKey(pass,redu,blockkey,value))
																							QueueList.put(pass+":"+redu, q)
																						}
																			}

																}
											}
								}
								}      

						}

						//Insert  P(K)+1 values 

						for ((k,v) <- QueueList){
							for((bk,comk) <- v){
								outFileStream1.writeBytes(comk.getPass()+":"+(comk.getReducerNo()+1)+":"+comk.getReducerNo()+":"+comk.getBlockKey()+","+comk.getData()+"\n")
							}

						}

						outFileStream1.close()
						fs1.close()
						// partition on the basis of pass number and sort on the basis of entire key

						val input =  sc.textFile(tempFolder+"output_AfterKey").map(line => line.split(",")).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
						val keyValue =input.map { arr => createKeyValueTuple(arr) }.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

						keyValue.repartitionAndSortWithinPartitions(new CustPartitioner(reduceTasks)).persist(StorageLevel.MEMORY_AND_DISK_SER_2).saveAsTextFile(tempFolder+"AfterSecSort")

						// Sliding Window----------------------------------------------------------------------------------
						var lastPass = -1
						var q = new Queue[String]

								val in = sc.wholeTextFiles(tempFolder+"AfterSecSort").persist(StorageLevel.MEMORY_AND_DISK_SER_2)
							
						  var rowData :String =""

								in.map {filedata =>

								val rowdata = filedata.toString().split(",\\(")(1)
								val rows = rowdata.split("\n")
								var matchingcol = ""
								for(row <- 0 until rows.length-1){
									val bk  = rows(row).split("ListBuffer")(0).split("Key")(1).replaceAll("\\(", "").replaceAll("\\)", "").split(",")
											val pass =bk(0).toInt
											val boundary =bk(1).toInt
											val partition = bk(2).toInt
											val block_key = bk(3).toString//ayan
											val value = rows(row).split("ListBuffer")(1).replaceAll("\\(", "").replaceAll("\\)", "")
											if(pass != lastPass){
												q.clear()
												lastPass = pass
											}
									rowData=""
											if(boundary == partition){
												var i =1

														for(e <- q){
															if(i != q.length)
																//                 rowData = rowData.concat(("("+e+"),("+value+")"+"\n"))
																rowData = rowData.concat(("("+e+"),("+block_key+" , "+value+")"+"\n"))//ayan
																else
																	//                 rowData = rowData.concat(("("+e+"),("+value+")"))
																	rowData = rowData.concat(("("+e+"),("+block_key+" , "+value+")")) //ayan

																	i=i+1
														}
											}

									//q.enqueue(value.toString())
									q.enqueue(block_key+" , "+value.toString())
									if(q.size == windowSizeArray.get(pass).get){
										q.dequeue()
									}
									matchingcol =matchingcol.concat(rowData).concat("\n")    
								} 
								q.clear()
								(matchingcol)    

								}.persist(StorageLevel.MEMORY_AND_DISK_SER_2).saveAsTextFile(tempFolder+"WithSpace")

								sc.textFile(tempFolder+"WithSpace").persist(StorageLevel.MEMORY_AND_DISK_SER_2).filter(!_.isEmpty()).saveAsTextFile(outputFile)



								//main end 
	}

	//GetParFunc
	def getPartition(pass:Int,blockKey:String,reduceTasks:Int):Int = {

			var entityIndex1 :Int= entityIndexes.get(pass+":"+blockKey).get;
	entityIndexes.put(pass+":"+blockKey,(entityIndex1 + 1));
	var ret=0
			for (i <- 1 to splitPoint.size()){

				if (entityIndex1 <= (splitPoint.get(i-1))){

					return i;

				}
				ret=i
			}
	return ret;
	}



	//min func
	def min (f : Int, entities: Int) : Int = {
			if (f<entities)
				return f;
			else 
				return entities;
	}

	//for secondary sorting
	def createKeyValueTuple(data: Array[String]) :(Key,ListBuffer[String]) = {
			(createKey(data),listData(data))
	}

	def createKey(data: Array[String]): Key = {
			val keydata = data(0).split(":")
					Key(keydata(0).toInt,keydata(1).toInt,keydata(2).toInt,keydata(3))
	}

	def listData(data: Array[String]): ListBuffer[String] = {
			var row = new ListBuffer[String]

					for(i <- 1 until data.length ){

						row += data(i)

					}
			row

	}

}       