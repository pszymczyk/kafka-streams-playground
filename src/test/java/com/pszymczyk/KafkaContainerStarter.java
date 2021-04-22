package com.pszymczyk;

import org.testcontainers.containers.KafkaContainer;

class KafkaContainerStarter {

    static KafkaContainer kafkaContainer;

    static void start() {
        if (kafkaContainer == null) {
            kafkaContainer = new KafkaContainer();
            kafkaContainer.start();
            System.setProperty("spring.kafka.bootstrap-servers", kafkaContainer.getBootstrapServers());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaContainer.close()));
        }

    }

    static String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }
}
