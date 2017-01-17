package com.radiumone.dw.etl3.poc.kafkastreams

import java.util
import java.util.Random

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

/**
  * Created by broy on 1/11/17.
  */
 class FirstCharPartitioner extends Partitioner {

//  override def AppIdPartitioner()
  val numPartitions = 36

  override def close(): Unit = {
    System.out.println("FirstCharPartitioner, close invoked=" )
    System.out.println("FirstCharPartitioner, close invoked=" )
    System.out.println("FirstCharPartitioner, close invoked=" )
    System.out.println("FirstCharPartitioner, close invoked=" )
    System.out.println("FirstCharPartitioner, close invoked=" )
  }

//  public Int partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster);

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    if (key == null) {
      val partitionId = new Random().nextInt() % numPartitions
      System.out.println("null key, partitionId = " + partitionId)
      partitionId
    } else {
      //val partitionId = key.asInstanceOf[String].charAt(0).getNumericValue - 10
      val partitionId = key.asInstanceOf[String].charAt(0).getNumericValue
      System.out.println("FirstCharPartitioner, key not null, key=" + key)
      System.out.println("FirstCharPartitioner " + key.asInstanceOf[String])
      System.out.println("FirstCharPartitioner " + key.asInstanceOf[String].charAt(0))
      System.out.println("FirstCharPartitioner " + key.asInstanceOf[String].charAt(0).getNumericValue)
      System.out.println("FirstCharPartitioner " + partitionId)
      partitionId
    }

//      Utils.abs(java.util.Arrays.hashCode(key.asInstanceOf[Array[Byte]])) % numPartitions
//      key.hashCode()
//    cluster.pa
//    val partitionCount = cluster.partitionCountForTopic(topic)
//    System.out.println("FirstCharPartitioner, partitionCount=" + partitionCount )
//    0
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    System.out.println("FirstCharPartitioner, configs= " + configs)
    System.out.println("FirstCharPartitioner, configs= " + configs)
//    System.out.println("FirstCharPartitioner, configs= " + configs)
//    System.out.println("FirstCharPartitioner, configs= " + configs)
//    System.out.println("FirstCharPartitioner, configs= " + configs)
//    super.configure(configs)
  }

}
