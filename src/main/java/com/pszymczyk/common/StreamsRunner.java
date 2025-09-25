package com.pszymczyk.common;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

public class StreamsRunner {

    private static final Logger logger = LoggerFactory.getLogger(StreamsRunner.class);

    public KafkaStreams run(String bootstrapServers,
                            String applicationId,
                            StreamsBuilder streamsBuilder,
                            Map<String, Object> customProperties,
                            NewTopic... newTopics) {

        createTopics(bootstrapServers, newTopics);

        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, applicationId);
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        config.put(DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        config.put(PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ContinueProductionExceptionHandler.class);
        config.put(PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueProcessingExceptionHandler.class);

        // disable caching to see all operations results immediately
        config.put(STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        config.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_MS_CONFIG), "60000");
        config.putAll(customProperties);

        Topology topology = streamsBuilder.build();
        logger.info(topology.describe().toString());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, config);
        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            logger.error("Handling unexpected exception", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Start closing Kafka Streams...");
            kafkaStreams.close(new KafkaStreams.CloseOptions().leaveGroup(true));
            logger.info("Kafka Streams has been closed.");
        }, "shutdown-hook-thread"));
        return kafkaStreams;
    }

    private static void createTopics(String bootstrapServers, NewTopic[] newTopics) {
        AdminClient adminClient = AdminClient.create(Map.of(
            "bootstrap.servers", bootstrapServers));

        adminClient.createTopics(List.of(newTopics)).all().whenComplete((unused, throwable) -> {
            if (throwable != null) {
                logger.error("Exception has been thrown during topics creation.", throwable);
            } else {
                logger.info("Topics have been created");
            }
        });
        adminClient.close();
    }
}
