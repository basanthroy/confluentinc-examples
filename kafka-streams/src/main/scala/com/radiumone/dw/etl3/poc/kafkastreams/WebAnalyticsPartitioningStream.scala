package com.radiumone.dw.etl3.poc.kafkastreams

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}

/**
  * Created by broy on 1/11/17.
  */
object WebAnalyticsPartitioningStream {

  def main(args: Array[String]): Unit = {
    val builder: KStreamBuilder = new KStreamBuilder

    val streamingConfig = {
      val settings = new Properties()
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "web-analytics-partition-by-appid")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
      settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings
    }

    val stringSerde: Serde[String] = Serdes.String()

    val textLines: KStream[Array[Byte], String] = builder.stream("TextLinesTopic")

    val uppercasedWithMapValues: KStream[Array[Byte], String] = textLines.mapValues(_.toUpperCase())

    uppercasedWithMapValues.to("UppercasedTextR1")

    val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
    stream.start()

  }

}
