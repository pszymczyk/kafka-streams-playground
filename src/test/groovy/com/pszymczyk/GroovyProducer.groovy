package com.pszymczyk


import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.intellij.lang.annotations.Language
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class GroovyProducer {

    private static final Logger log = LoggerFactory.getLogger(GroovyProducer.class);

    static String TOPIC = "app1"
    static String KEY = null

    @Language("JSON")
    static String VALUE = """
        {
          "some": "json"
        }
    """

    static void main(String[] args) {
        def config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        def kafkaProducer = new KafkaProducer<String, String>(config)
        kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, KEY, VALUE), {
            RecordMetadata metadata, Exception exception -> log.info("Send error: {}", exception)
        }).get()

    }
}
