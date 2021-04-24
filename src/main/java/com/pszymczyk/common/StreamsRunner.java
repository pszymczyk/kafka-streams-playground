package com.pszymczyk.common;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class StreamsRunner {

    private static final Logger logger = LoggerFactory.getLogger(StreamsRunner.class);

    private KafkaStreams kafkaStreams;

    public KafkaStreams run(String bootstrapServers,
                            String applicationId,
                            StreamsBuilder streamsBuilder,
                            Map<String, Object> customProperties,
                            NewTopic... newTopics) {

        AdminClient adminClient = AdminClient.create(Map.of(
            "bootstrap.servers", bootstrapServers,
            "group.id", "create-topics-admin"));

        adminClient.createTopics(Arrays.asList(newTopics));
        adminClient.close();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // disable caching to see all operations results immediately
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        customProperties.entrySet().forEach(entry ->
            config.put(entry.getKey(), entry.getValue())
        );

        Topology topology = streamsBuilder.build();
        logger.info(topology.describe().toString());
        kafkaStreams = new KafkaStreams(topology, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        return kafkaStreams;
    }
}
