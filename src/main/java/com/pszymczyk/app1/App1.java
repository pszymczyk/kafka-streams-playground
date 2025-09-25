package com.pszymczyk.app1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

class App1 {

    private static final Logger logger = LoggerFactory.getLogger(App1.class);

    public static void main(String[] args) {
        Topology topology = getTopology();
        StreamsConfig streamsConfig = new StreamsConfig(
            Map.of(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                StreamsConfig.APPLICATION_ID_CONFIG, "app1",
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class
            )
        );
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close, "shutdown-hook-thread"));
        kafkaStreams.start();
    }

    static Topology getTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.<String, String>stream("app1-source")
            .flatMapValues(value -> Arrays.asList(value.split(" ")))
            .peek((k, v) -> logger.info("{},{}", k, v))
            .to("app1-sink");

        return streamsBuilder.build();
    }
}
