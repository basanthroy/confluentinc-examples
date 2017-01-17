// scalastyle:off println
package com.radiumone.dw.etl3.poc.kafkastreams

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer


object KafkaTapMock {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>

        val sampleTrackingId = sampleTrackingIds(scala.util.Random.nextInt(sampleTrackingIds.size))

        val str = requestJsonTemplate format sampleTrackingId
//        val str =  sampleTrackingId

        System.out.println("Producer sampleTrackingId=" + sampleTrackingId
                         + ",\n     Producer topic=" + topic
                         + ",\n     Producer str=" + str.substring(0,Math.min(100, str.length)))

        val message = new ProducerRecord[String, String](topic, null, str)
//        val message = new ProducerRecord[String, String](topic, sampleTrackingId, str)

        val fut = producer.send(message)

        val v1 = fut.get()
        System.out.println("Producer v1=" + v1)
        System.out.println("Producer v1.partition=" + v1.partition())
      }

      Thread.sleep(1000)
    }
  }

  val sampleTrackingIds = List("B80854A8-7B56-4041-8DED-F353890C7976", "C11DC874-7582-4F82-9DAD-94244FAFC999", "AEA4F0B2-D876-4E0E-AF01-A7C8010BBE33",
  "C813F76D-F79F-4F1C-BEE8-CA5CF3A7B71E", "4ED10645-5D2E-476D-9C90-A29B5C12E365")

  val requestJsonTemplate = """{
    "tracking_id": "%s",
    "application_name": null,
    "application_user_id": null,
    "application_version": "11.8.2.2",
    "application_build": "2",
    "conn_type": "WIFI",
    "timezone": "America/Los_Angeles",
    "user_language": "en",
    "sdk_version": "3.3.0",
    "source": "ADVERTISER_SDK",
    "page": {
      "page": {
      "referrer": "http://www.radiumone.com",
      "title": "AdTech Ninjas",
      "url": "www.cnn.com",
      "scroll_position": "30x40"
    }
    },
    "device_info": {
      "device_info": {
      "id_info": {
      "post_cookie": "32charactersneeded",
      "ob_login": "ob_login_value",
      "opt_out": false
    },
      "ip_v4": "92.117.48.48.99.48.92.117.48.48.97.56.92.117.48.48.48.49.92.117.48.48.52.54",
      "user_agent": "Mozilla/5.0 (iPad; CPU OS 10_1_1 like Mac OS X) AppleWebKit/602.2.14 (KHTML, like Gecko) Mobile/14B100",
      "screen": {
      "width": 320,
      "height": 568,
      "density": 2,
      "viewport_size": "768x1024"
    }
    }
    },
    "event_info": {
      "event_info": [
    {
      "event_name": "Key-SearchBegan",
      "key_value": {
      "network": "RealZeit - Android",
      "source": "RealiZeit > RealZeit - Android",
      "id": "123",
      "name": "Shoes",
      "currency": "USD",
      "receipt_status": "no_receipt"
    },
      "lat": null,
      "lon": null,
      "session_id": "D8A7BBF6-DCF2-40AA-9E66-B78C44B816E5",
      "timestamp": 1481329407375,
      "transaction_id": "92.117.48.48.52.102.92.117.48.48.48.51.92.117.48.48.99.51.92.117.48.48.52.56.92.117.48.48.100.55.92.117.48.48.48.56.92.117.48.48.52.54.92.117.48.48.98.57.92.117.48.48.56.50.92.117.48.48.100.54.92.117.48.48.98.54.92.117.48.48.55.50.92.117.48.48.48.97.92.117.48.48.102.97.92.117.48.48.52.57.92.117.48.48.102.102"
    }
      ]
    }
  }"""

}
// scalastyle:on println
