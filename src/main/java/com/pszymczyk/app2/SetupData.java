package com.pszymczyk.app2;

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
        try (var kafkaProducer = new KafkaProducer<String, String>(producerProperties);) {
            for (var line : Utils.readLines("app1-data.txt")) {
                kafkaProducer.send(new ProducerRecord<>(App2.APP_2_SOURCE, line), (metadata, exception) -> {
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
