/**
 * Copyright 2016 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import io.confluent.examples.streams.avro.WikiFeed;
import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Computes, for every minute the number of new user feeds from the Wikipedia feed irc stream.
 * Same as {@link WikipediaFeedAvroExample} but uses lambda expressions and thus only works on Java 8+.
 * <p>
 * Note: The specific Avro binding is used for serialization/deserialization, where the {@code WikiFeed}
 * class is auto-generated from its Avro schema by the maven avro plugin. See {@code wikifeed.avsc}
 * under {@code src/main/resources/avro/io/confluent/examples/streams/}.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic WikipediaFeed \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic WikipediaStats \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/examples/tree/master/kafka-streams#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/streams-examples-3.1.1-standalone.jar io.confluent.examples.streams.WikipediaFeedAvroLambdaExample
 * }</pre>
 * 4) Write some input data to the source topics (e.g. via {@link WikipediaFeedAvroExampleDriver}).
 * The already running example application (step 3) will automatically process this input data and
 * write the results to the output topic. The {@link WikipediaFeedAvroExampleDriver} will print the
 * results from the output topic
 * <pre>
 * {@code
 * # Here: Write input data using the example driver.  Once the driver has stopped generating data,
 * # you can terminate it via Ctrl-C.
 * $ java -cp target/streams-examples-3.1.1-standalone.jar io.confluent.examples.streams.WikipediaFeedAvroExampleDriver
 * }</pre>
 */
public class WikipediaFeedAvroLambdaExample {

  public static void main(final String[] args) throws Exception {
    final KafkaStreams streams = buildWikipediaFeed(
      "localhost:9092",
      "localhost:2181",
      "http://localhost:8081",
      "/tmp/kafka-streams");
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  static KafkaStreams buildWikipediaFeed(final String bootstrapServers,
                                         final String zkConnect,
                                         final String schemaRegistryUrl,
                                         final String stateDir) {
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-avro-lambda-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);
    // Where to find the Confluent schema registry instance(s)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
 
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    final KStreamBuilder builder = new KStreamBuilder();

    // read the source stream
    final KStream<String, WikiFeed> feeds = builder.stream(WikipediaFeedAvroExample.WIKIPEDIA_FEED);

    // aggregate the new feed counts of by user
    final KTable<String, Long> aggregated = feeds
      // filter out old feeds
      .filter((dummy, value) -> value.getIsNew())
      // map the user id as key
      .map((key, value) -> new KeyValue<>(value.getUser(), value))
      .groupByKey()
      .count("Counts");

    // write to the result topic, need to override serdes
    aggregated.to(stringSerde, longSerde, WikipediaFeedAvroExample.WIKIPEDIA_STATS);

    return new KafkaStreams(builder, streamsConfiguration);
  }

}
