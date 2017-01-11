/**
  * Copyright 2016 Confluent Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
  * in compliance with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License
  * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
  * or implied. See the License for the specific language governing permissions and limitations under
  * the License.
  */

package io.confluent.examples.streams

import java.util.{Collections, Properties}

import io.confluent.examples.streams.avro.WikiFeed
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import io.confluent.examples.streams.utils.SpecificAvroSerde
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig, KafkaAvroSerializer}
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

/**
  * End-to-end integration test that demonstrates how to work on Specific Avro data.
  *
  * See [[GenericAvroScalaIntegrationTest]] for the equivalent Generic Avro integration test.
  */
class SpecificAvroScalaIntegrationTest extends AssertionsForJUnit {

  private val privateCluster: EmbeddedSingleNodeKafkaCluster = new EmbeddedSingleNodeKafkaCluster

  @Rule def CLUSTER = privateCluster

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Before
  def startKafkaCluster() = {
    CLUSTER.createTopic(inputTopic, 2, 1)
    CLUSTER.createTopic(outputTopic)
  }

  @Test
  def shouldRoundTripGenericAvroDataThroughKafka() {
    val f: WikiFeed = WikiFeed.newBuilder.setUser("alice").setIsNew(true).setContent("lorem ipsum").build
    val inputValues: Seq[WikiFeed] = Seq(f)

    //
    // Step 1: Configure and start the processor topology.
    //
    val builder: KStreamBuilder = new KStreamBuilder()

    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "specific-avro-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
      p.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zookeeperConnect())
      p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[SpecificAvroSerde[_ <: SpecificRecord]])
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p
    }

    // Write the input data as-is to the output topic.
    //
    // Normally, because a) we have already configured the correct default serdes for keys and
    // values and b) the types for keys and values are the same for both the input topic and the
    // output topic, we would only need to define:
    //
    //   builder.stream(inputTopic).to(outputTopic);
    //
    // However, in the code below we intentionally override the default serdes in `to()` to
    // demonstrate how you can construct and configure a specific Avro serde manually.
    val stringSerde: Serde[String] = Serdes.String
    val specificAvroSerde: Serde[WikiFeed] = new SpecificAvroSerde[WikiFeed]
    // Note how we must manually call `configure()` on this serde to configure the schema registry
    // url.  This is different from the case of setting default serdes (see `streamsConfiguration`
    // above), which will be auto-configured based on the `StreamsConfiguration` instance.
    val isKeySerde: Boolean = false
    specificAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl), isKeySerde)

    val stream: KStream[String, WikiFeed] = builder.stream(inputTopic)
    stream.to(stringSerde, specificAvroSerde, outputTopic)
    val streams: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
    streams.start()

    //
    // Step 2: Produce some input data to the input topic.
    //
    val producerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl)
      p
    }
    import collection.JavaConverters._
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues.asJava, producerConfig)

    //
    // Step 3: Verify the application's output data.
    //
    val consumerConfig = {
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "specific-avro-scala-integration-test-standard-consumer")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl)
      p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
      p
    }
    val actualValues: java.util.List[WikiFeed] =
      IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig, outputTopic, inputValues.size)
    streams.close()
    assertThat(actualValues).containsExactlyElementsOf(inputValues.asJava)
  }

}