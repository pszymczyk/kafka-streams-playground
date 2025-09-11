package com.pszymczyk;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class IntegrationSpec {

    protected static Logger logger = LoggerFactory.getLogger(IntegrationSpec.class);

    protected static KafkaProducer<String, String> kafkaProducer;
    protected static String bootstrapServers;

    protected static Consumer<String, byte[]> kafkaConsumer;

    @BeforeAll
    static void setupSpec() {
        KafkaContainerStarter.start();
        bootstrapServers = KafkaContainerStarter.getBootstrapServers();
        JsonPathConfiguration.configure();
        kafkaProducer = kafkaProducer();
        kafkaConsumer = createKafkaConsumer();
    }

    protected static Consumer<String, byte[]> createKafkaConsumer() {
        return new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "integration-test",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        );
    }

    protected static KafkaProducer<String, String> kafkaProducer() {
        return new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    protected static void produceMessage(String topic, String key, String value) {
        try {
            kafkaProducer.send(new ProducerRecord<>(topic, key, value)).get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Exception while producing message to Kafka", e);
            throw new RuntimeException(e);
        }
    }

    protected static void produceMessage(String topic, String value) {
        produceMessage(topic, null, value);
    }
}
