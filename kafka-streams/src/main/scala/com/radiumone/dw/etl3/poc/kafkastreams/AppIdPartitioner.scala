package com.radiumone.dw.etl3.poc.kafkastreams

import java.util
import java.util.Random

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils

/**
  * Created by broy on 1/11/17.
  */
 class AppIdPartitioner extends Partitioner {

//  override def AppIdPartitioner()

  override def close(): Unit = {

  }

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    if (key == null) {
      val partitionId = new Random().nextInt()
      partitionId
    } else {
        val partitionId = key.asInstanceOf[String].charAt(0).getNumericValue - 10
        System.out.println(key.asInstanceOf[String])
        System.out.println(key.asInstanceOf[String].charAt(0))
        System.out.println(key.asInstanceOf[String].charAt(0).getNumericValue)
        System.out.println(partitionId)
      partitionId
      }
//      Utils.abs(java.util.Arrays.hashCode(key.asInstanceOf[Array[Byte]])) % numPartitions
//      key.hashCode()
//    cluster.pa
  }

  override def configure(configs: util.Map[String, _]): Unit = {

  }

}
