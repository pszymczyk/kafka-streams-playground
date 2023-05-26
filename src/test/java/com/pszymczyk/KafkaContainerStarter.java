package com.pszymczyk;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class KafkaContainerStarter {

    static KafkaContainer kafkaContainer;

    static void start() {
        if (kafkaContainer == null) {
            kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0")).withKraft();
            kafkaContainer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaContainer.close()));
        }
    }

    static String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }
}
