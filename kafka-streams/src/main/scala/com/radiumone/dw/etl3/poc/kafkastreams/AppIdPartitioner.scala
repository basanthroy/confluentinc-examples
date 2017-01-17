package com.radiumone.dw.etl3.poc.kafkastreams

import java.util.Random

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
//import com.fasterxml.jackson.module.
//import com.fasterxml.jackson.module.scala
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.streams.processor.StreamPartitioner

/**
  * Created by broy on 1/17/17.
  */
class AppIdPartitioner extends StreamPartitioner[Array[Byte], String] {

  override def partition(key: Array[Byte], value: String, numPartitions: Int): Integer = {
    if (value == null) {
      getDefaultPartitionId(numPartitions)
    } else {

//      System.out.println(this.getClass.getName + ", value not null, value=" + value)

      val objectMapper: ObjectMapper = new ObjectMapper()
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      objectMapper.registerModule(DefaultScalaModule)

//      objectMapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
//      val wajr:WebAnalyticsJsonRecord = null
      val jsonObject:WebAnalyticsJsonRecord = objectMapper
        .readValue(value, classOf[WebAnalyticsJsonRecord])

//      jsonObject.doMethod()

//      System.out.println("\n\njsonObject = " + jsonObject)

      val trackingId = jsonObject.tracking_id

      if (trackingId == null) {
        return getDefaultPartitionId(numPartitions)
      } else {

//      objectMapper.

//      jsonObject

      System.out.println("trackingId = " + trackingId)
      val partitionId = trackingId.asInstanceOf[String].charAt(0).getNumericValue
//      System.out.println(this.getClass.getName + ", value not null, value=" + value
//        + ",\n key = " + key
//        + ",\n partitions for this topic = " + numPartitions
//        + ",\n numeric value = " + trackingId.asInstanceOf[String].charAt(0).getNumericValue
//        + ",\n partitionId = " + partitionId)
      System.out.println("partitionId = " + partitionId)
      if (partitionId > numPartitions) {
        throw new IllegalStateException("partitionId is > number of partitions for this topic")
      }
      partitionId

      }
    }
  }

  def getDefaultPartitionId(numPartitions: Int): Integer = {
    val partitionId = new Random().nextInt() % numPartitions
    System.out.println("Default generated random partitionId = " + partitionId)
    partitionId
  }

}
