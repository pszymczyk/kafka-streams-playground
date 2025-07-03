package com.pszymczyk;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

class KafkaContainerStarter {

    static KafkaContainer kafkaContainer;

    static void start() {

    }

    static String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }
}
