package com.pszymczyk.tx2;

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

    public static void main(String[] args) {
        var topic = "tx2-input";

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final var kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        Utils.readLines("warszawa-krucza49_24-10-2023.csv").forEach(line -> {
            try {
                kafkaProducer.send(new ProducerRecord<>(topic, line), (metadata, exception) -> {
                    if (metadata != null) {
                        logger.info("Record sent, {}.", metadata);
                    } else {
                        logger.error("Record sending failed. ", exception);
                    }
                }).get();
            } catch (Exception exception) {
                logger.error("Record sending failed. ", exception);
                throw new RuntimeException(exception);
            }
        });

        kafkaProducer.close();
    }
}
