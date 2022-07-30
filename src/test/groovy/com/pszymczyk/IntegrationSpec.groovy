package com.pszymczyk

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.util.concurrent.TimeUnit

abstract class IntegrationSpec extends Specification {

    protected static Logger logger = LoggerFactory.getLogger(IntegrationSpec.class)
    protected static KafkaProducer<String, String> kafkaProducer
    protected static String bootstrapServers

    protected Consumer<String, String> kafkaConsumer

    def setupSpec() {
        KafkaContainerStarter.start()
        bootstrapServers = KafkaContainerStarter.bootstrapServers
        JsonPathConfiguration.configure()
        kafkaProducer = kafkaProducer()
    }

    def setup() {
        kafkaConsumer = createKafkaConsumer()
    }

    protected Consumer<String, String> createKafkaConsumer(String groupId = this.class.simpleName) {
        new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        )
    }

    protected static KafkaProducer<String, String> kafkaProducer() {
        return new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    protected static void produceMessage(String topic, String key, GString value) {
        produceMessage(topic, key, value.toString())
    }

    protected static void produceMessage(String topic, String key, String value) {
        kafkaProducer.send(new ProducerRecord(topic, key, value.toString())).get(2, TimeUnit.SECONDS)
    }

    protected static void produceMessage(String topic, GString value) {
        produceMessage(topic, value.toString())
    }

    protected static void produceMessage(String topic, String value) {
        kafkaProducer.send(new ProducerRecord(topic, value.toString())).get(2, TimeUnit.SECONDS)
    }
}
