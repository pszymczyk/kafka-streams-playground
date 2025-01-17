package com.pszymczyk.app8;

import com.pszymczyk.common.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SetupData {

    private static final Logger logger = LoggerFactory.getLogger(SetupData.class);

    public static void main(String[] args) throws Exception {

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final var kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::close, "shutdown-hook-thread"));

        for (var line : Utils.readLines("warszawa-jerozolimskie20_24-10-2023.csv")) {
            kafkaProducer.send(new ProducerRecord<>(App8.SOURCE_TOPIC, line), (metadata, exception) -> {
                if (metadata != null) {
                    logger.info("Message sent metadata: {}", metadata);
                } else {
                    logger.error("Error ", exception);
                }
            }).get();
        }
    }
}
