package com.pszymczyk.app2;

import com.pszymczyk.IntegrationTest;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.pszymczyk.app2.App2.APP_2_SINK;
import static com.pszymczyk.app2.App2.APP_2_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

class App2IntegrationTest extends IntegrationTest {

    static KafkaStreams kafkaStreams;

    @BeforeAll
    static void setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app2-spec",
                App2.buildKafkaStreamsTopology(),
                Map.of(),
                new NewTopic(APP_2_SOURCE, 1, (short) 1),
                new NewTopic(APP_2_SINK, 1, (short) 1));
    }

    @AfterAll
    static void cleanupSpec() {
        kafkaStreams.close();
    }

    @Test
    void Should_count_messages_sent_to_every_user() {
        kafkaConsumer.assign(List.of(new TopicPartition(APP_2_SINK, 0)));

        produceMessage(APP_2_SOURCE, "123#andrzej123#pszymczyk#Hello! how are you?");
        produceMessage(APP_2_SOURCE, "124#andrzej123#pszymczyk#Hi! what is going on?");
        produceMessage(APP_2_SOURCE, "125#telemarketing#andrzej123#We have a special discount for you!");
        produceMessage(APP_2_SOURCE, "126#telemarketing#pszymczyk#Best wishes in Valentine's day!");

        Map<String, Long> messagesCount = new HashMap<>();

        await().atMost(DEFAULT_AWAIT_TIMEOUT, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
                    logger.info("Received {} events", consumerRecords.count());
                    consumerRecords.forEach(record -> {
                                logger.info("{}:{}", record.key(), record.value());
                                messagesCount.put(record.key(), ByteBuffer.wrap(record.value()).getLong());
                            }
                    );
                    assertEquals(3L, messagesCount.get("pszymczyk"));
                    assertEquals(1L, messagesCount.get("andrzej123"));
                });
    }
}
