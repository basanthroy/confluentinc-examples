package com.radiumone.dw.etl3.poc.kafkastreams

import java.util.Random

import org.apache.kafka.streams.processor.StreamPartitioner

/**
  * Created by broy on 1/17/17.
  */
class AppIdPartitioner extends StreamPartitioner[Array[Byte], String] {

  override def partition(key: Array[Byte], value: String, numPartitions: Int): Integer = {
    if (value == null) {
      val partitionId = new Random().nextInt() % numPartitions
      System.out.println("null value, partitionId = " + partitionId + ", key = " + key + ", value = " + value)
      partitionId
    } else {
      System.out.println("key = " + key + ", value = " + value)
      val partitionId = value.asInstanceOf[String].charAt(0).getNumericValue
      System.out.println(this.getClass.getName + ", value not null, value=" + value
        + ",\n key = " + key
        + ",\n partitions for this topic = " + numPartitions
        + ",\n numeric value = " + value.asInstanceOf[String].charAt(0).getNumericValue
        + ",\n partitionId = " + partitionId)
      if (partitionId > numPartitions) {
        throw new IllegalStateException("partitionId is > number of partitions for this topic")
      }
      partitionId
    }
  }

}
