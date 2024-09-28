package com.pszymczyk.app6;

import com.pszymczyk.common.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class SetupData {

    private static final Logger logger = LoggerFactory.getLogger(SetupData.class);

    public static void main(String[] args) throws Exception {
        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var kafkaProducer = new KafkaProducer<String, String>(producerProperties)) {
            for (var line : Utils.readLines("app5-users-data.txt")) {
                String[] split = line.split(":");
                String key = split[0];
                String value = split[1];
                kafkaProducer.send(new ProducerRecord<>(App6.APP_6_STATE, key, value), (metadata, exception) -> {
                    if (metadata != null) {
                        logger.info("Message sent metadata: {}", metadata);
                    } else {
                        logger.error("Error ", exception);
                    }
                }).get();
            }
            for (var line : Utils.readLines("app5-messages-data.txt")) {
                String[] split = line.split(":");
                String key = split[0];
                String value = split[1];
                kafkaProducer.send(new ProducerRecord<>(App6.APP_6_SOURCE, key, value), (metadata, exception) -> {
                    if (metadata != null) {
                        logger.info("Message sent metadata: {}", metadata);
                    } else {
                        logger.error("Error ", exception);
                    }
                }).get();
            }
        }
    }
}