package com.pszymczyk.app5;

import com.pszymczyk.IntegrationTest;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.pszymczyk.app5.App5.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

class App5IntegrationTest extends IntegrationTest {

    static KafkaStreams kafkaStreams;

    @BeforeAll
    static void setupSpec() {
        kafkaStreams = new StreamsRunner().run(bootstrapServers, "app5-spec", buildKafkaStreamsTopology(), Map.of(), new NewTopic(APP_5_STATE, 1, (short) 1), new NewTopic(APP_5_SOURCE, 1, (short) 1), new NewTopic(APP_5_SINK, 1, (short) 1));
    }

    @AfterAll
    static void cleanupSpec() {
        kafkaStreams.close();
    }

    @Test
    void Should_enrich_messages_with_full_user_name() {
        kafkaConsumer.assign(List.of(new TopicPartition(APP_5_SINK, 0)));

        produceMessage(APP_5_STATE, "user-id-123", "Pawel#Szymczyk");
        produceMessage(APP_5_STATE, "user-id-456", "Jan#Kowalski");
        produceMessage(APP_5_STATE, "user-id-789", "Anna#Hiacynta");

        produceMessage(APP_5_SOURCE, "1234#telemarketing#user-id-123#Hello <user>, Here is some extra deal for you!");
        produceMessage(APP_5_SOURCE, "1235#telemarketing#user-id-456#Hi <user>, Your order is completed.");
        produceMessage(APP_5_SOURCE, "1236#telemarketing#user-id-789#Alo <user>, All the best in valentine's day.");

        List<String> messages = new ArrayList<>();
        await().atMost(DEFAULT_AWAIT_TIMEOUT, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
                    logger.info("Received {} events", consumerRecords.count());
                    consumerRecords.forEach(record -> {
                        logger.info("Received {}:{}", record.key(), new String(record.value()));
                        messages.add(new String(record.value()));
                    });

                    assertEquals(messages, List.of(
                            "Hello Pawel Szymczyk, Here is some extra deal for you!",
                            "Hi Jan Kowalski, Your order is completed.", "Alo Anna Hiacynta, All the best in valentine's day."));
                });
    }
}
