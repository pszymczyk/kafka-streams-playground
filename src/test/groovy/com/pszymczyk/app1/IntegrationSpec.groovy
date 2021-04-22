package com.pszymczyk.app1

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import spock.lang.Specification

abstract class IntegrationSpec extends Specification {

    protected static Logger logger = LoggerFactory.getLogger(CustomersDefaultPaymentMethodsCountAppSpec.class)
    protected static KafkaTemplate<String, String> kafkaTemplate
    protected static String bootstrapServers

    protected Consumer<String, String> kafkaConsumer

    def setupSpec() {
        KafkaContainerStarter.start()
        bootstrapServers = KafkaContainerStarter.bootstrapServers
        JsonPathConfiguration.configure()
        kafkaTemplate = kafkaTemplate()
    }

    def setup() {
        kafkaConsumer = kafkaConsumer()
    }

    protected Consumer<String, String> kafkaConsumer() {
        new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, this.class.simpleName,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        )
    }

    protected static KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
    }
}
