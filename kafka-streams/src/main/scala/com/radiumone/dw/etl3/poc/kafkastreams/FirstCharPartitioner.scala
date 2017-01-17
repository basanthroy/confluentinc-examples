package com.radiumone.dw.etl3.poc.kafkastreams

import java.util
import java.util.Random

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

/**
  * Created by broy on 1/11/17.
  */
class FirstCharPartitioner extends Partitioner {

  val numPartitions = 36

  override def close(): Unit = {

  }

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    if (key == null) {
      val partitionId = new Random().nextInt() % numPartitions
      System.out.println("null key, partitionId = " + partitionId)
      partitionId
    } else {
      val partitionId = key.asInstanceOf[String].charAt(0).getNumericValue
      System.out.println("FirstCharPartitioner, key not null, key=" + key
        + "\n, partitions for this topic = " + cluster.partitionCountForTopic(topic)
        + "\n, numeric value = " + key.asInstanceOf[String].charAt(0).getNumericValue
        + "\n, partitionId = " + partitionId)
      if (partitionId > cluster.partitionCountForTopic(topic)) {
        throw new IllegalStateException("partitionId is > number of partitions for this topic")
      }
      partitionId
    }
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    System.out.println("FirstCharPartitioner, configs= " + configs)
  }

}
