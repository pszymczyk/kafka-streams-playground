package com.pszymczyk;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class KafkaContainerStarter {

    static KafkaContainer kafkaContainer;

    static void start() {
        if (kafkaContainer == null) {
            kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.0"));
            kafkaContainer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaContainer.close()));
        }
    }

    static String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }
}
