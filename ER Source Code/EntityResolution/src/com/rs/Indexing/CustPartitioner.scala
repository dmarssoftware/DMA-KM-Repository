package com.rs.Indexing
import org.apache.spark.Partitioner
class CustPartitioner (partitions: Int) extends Partitioner {
  
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[Key]
      k.boundary.hashCode() % numPartitions
    }
}  