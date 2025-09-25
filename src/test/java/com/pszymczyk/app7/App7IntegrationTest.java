package com.pszymczyk.app7;

import com.jayway.jsonpath.JsonPath;
import com.pszymczyk.IntegrationTest;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.pszymczyk.app7.App7.APP_7_SINK;
import static com.pszymczyk.app7.App7.APP_7_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

class App7IntegrationTest extends IntegrationTest {

    static KafkaStreams kafkaStreams;

    @BeforeAll
    static void setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app7-top-five-articles-last-five-days-app",
                App7.buildKafkaStreamsTopology(),
                Map.of(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MessageTimeExtractor.class),
                new NewTopic(APP_7_SOURCE, 1, (short) 1),
                new NewTopic(APP_7_SINK, 1, (short) 1));
    }

    @AfterAll
    static void cleanupSpec() {
        kafkaStreams.close();
    }

    @Test
    void Should_aggregate_three_days_long_inbox() {
        var today = Instant.parse("2007-12-15T10:15:30.00Z").toEpochMilli();
        var yesterday = Instant.parse("2007-12-14T10:15:30.00Z").toEpochMilli();
        var dayBeforeYesterday = Instant.parse("2007-12-13T10:15:30.00Z").toEpochMilli();

        produceMessage(APP_7_SOURCE, dayBeforeYesterday + "#andrzej123#pszymczyk#Hello! how are you?");
        produceMessage(APP_7_SOURCE, yesterday + "#romek123#pszymczyk#Hello! how are you?");
        produceMessage(APP_7_SOURCE, today + "#andrzej123#pszymczyk#Hi! what is going on?");
        produceMessage(APP_7_SOURCE, dayBeforeYesterday + "#telemarketing#andrzej123#We have a special discount for you!");
        produceMessage(APP_7_SOURCE, yesterday + "#mango#andrzej123#Best wishes in Valentine's day!");
        produceMessage(APP_7_SOURCE, today + "#telemarketing#andrzej123#Best wishes in Valentine's day!");

        kafkaConsumer.assign(List.of(new TopicPartition(APP_7_SINK, 0)));

        Map<String, String> inboxTable = new HashMap<>();
        await().atMost(DEFAULT_AWAIT_TIMEOUT, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
                    logger.info("Received {} events", consumerRecords.count());
                    consumerRecords.forEach(record -> {
                        logger.info("Received {}:{}", record.key(), new String(record.value()));
                        inboxTable.put(record.key(), new String(record.value()));
                    });

                    var pawelInbox = JsonPath.parse(inboxTable.get("2007-12-13T00:00:00Z-2007-12-16T00:00:00Z-pszymczyk"));
                    assertEquals(3, pawelInbox.read("$.messages", List.class).size());
                    assertEquals(List.of("andrzej123", "romek123", "andrzej123"), pawelInbox.read("$.messages.[0:9].sender", List.class));

                    var andrzejInbox = JsonPath.parse(inboxTable.get("2007-12-13T00:00:00Z-2007-12-16T00:00:00Z-andrzej123"));
                    assertEquals(3, andrzejInbox.read("$.messages", List.class).size());
                    assertEquals(List.of("telemarketing", "mango", "telemarketing"), andrzejInbox.read("$.messages.[0:9].sender", List.class));
                });

    }
}
