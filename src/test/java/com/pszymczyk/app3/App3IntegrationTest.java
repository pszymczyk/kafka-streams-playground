package com.pszymczyk.app3;

import com.jayway.jsonpath.JsonPath;
import com.pszymczyk.IntegrationSpec;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.pszymczyk.app3.App3.APP_3_SINK;
import static com.pszymczyk.app3.App3.APP_3_SOURCE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class App3IntegrationTest extends IntegrationSpec {

    static KafkaStreams kafkaStreams;

    @BeforeAll
    static void setupSpec() {
        kafkaStreams = new StreamsRunner().run(
            bootstrapServers,
            "app3-spec",
            App3.buildKafkaStreamsTopology(),
            Map.of(),
            new NewTopic(APP_3_SOURCE, 1, (short) 1),
            new NewTopic(APP_3_SINK, 1, (short) 1));
    }

    @AfterAll
    static void cleanupSpec() {
        kafkaStreams.close();
    }

    @Test
    void Should_aggregate_messages() {
        produceMessage(APP_3_SOURCE, "1234#andrzej123#pszymczyk#Hello! how are you?");
        produceMessage(APP_3_SOURCE, "1235#andrzej123#pszymczyk#Hi! what is going on?");
        produceMessage(APP_3_SOURCE, "1236#telemarketing#andrzej123#We have a special discount for you!");
        produceMessage(APP_3_SOURCE, "1237#telemarketing#pszymczyk#Best wishes in Valentine's day!");

        kafkaConsumer.assign(List.of(new TopicPartition(APP_3_SINK, 0)));

        Map<String, String> inbox = new HashMap<>();

        await().atMost(50, TimeUnit.SECONDS)
            .ignoreExceptions()
            .untilAsserted(() -> {
                var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
                logger.info("Received {} events", consumerRecords.count());
                consumerRecords.forEach(record -> inbox.put(record.key(), new String(record.value())));

                assertEquals("Hello! how are you?", JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[0].message"));
                assertNotNull(JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[0].senderTime"));
                assertNotNull(JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[0].inboxTime"));
                assertEquals("Hi! what is going on?", JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[1].message"));
                assertNotNull(JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[1].senderTime"));
                assertNotNull(JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[1].inboxTime"));
                assertEquals("Best wishes in Valentine's day!", JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[2].message"));
                assertNotNull(JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[2].senderTime"));
                assertNotNull(JsonPath.parse(inbox.get("pszymczyk")).read("$.messages[2].inboxTime"));
                assertEquals("We have a special discount for you!", JsonPath.parse(inbox.get("andrzej123")).read("$.messages[0].message"));
                assertNotNull(JsonPath.parse(inbox.get("andrzej123")).read("$.messages[0].senderTime"));
                assertNotNull(JsonPath.parse(inbox.get("andrzej123")).read("$.messages[0].inboxTime"));
            });
    }
}
