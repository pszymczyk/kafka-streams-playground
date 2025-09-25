package com.pszymczyk.app8;

import com.jayway.jsonpath.JsonPath;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.pszymczyk.app8.App8.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;


class App8IntegrationTest extends IntegrationTest {

    static KafkaStreams kafkaStreams;

    @BeforeAll
    static void setupSpec() {
        kafkaStreams = new StreamsRunner().run(bootstrapServers, "LoanApplicationProcess-app-v1", App8.buildKafkaStreamsTopology(), Map.of(), new NewTopic(SOURCE_TOPIC, 1, (short) 1), new NewTopic(SINK_TOPIC, 1, (short) 1));
    }

    @AfterAll
    static void cleanupSpec() {
        kafkaStreams.close();
    }

    @Test
    void Should_count_loans_and_limit_given_loan_amount() {
        kafkaConsumer.assign(List.of(new TopicPartition(SINK_TOPIC, 0), new TopicPartition(NONOVOLUNTARY_OPERATIONS_TOPIC, 0)));

        produceMessage(SOURCE_TOPIC, "09:01,Frank,Allys,apple,10");
        produceMessage(SOURCE_TOPIC, "10:01,John,Allys,apple,1");
        produceMessage(SOURCE_TOPIC, "09:01,Arnold,Allys,amazon,12");
        produceMessage(SOURCE_TOPIC, "09:01,John,Allys,apple,99");

        Map<String, List<String>> operations = new HashMap<>();
        Map<String, List<String>> nonvoluntaryOperations = new HashMap<>();

        await().atMost(DEFAULT_AWAIT_TIMEOUT, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
                    logger.info("Received {} events", consumerRecords.count());

                    consumerRecords.records(SINK_TOPIC).forEach(record -> {
                        logger.info("Received {}:{}", record.key(), new String(record.value()));
                        List<String> operationsForSingleClient = operations.getOrDefault(record.key(), new ArrayList<>());
                        operationsForSingleClient.add(new String(record.value()));
                        operations.put(record.key(), operationsForSingleClient);
                    });
                    assertTrue(countTotalNumberOfElements(operations) >= 8);

                    consumerRecords.records(NONOVOLUNTARY_OPERATIONS_TOPIC).forEach(record -> {
                        logger.info("Received {}:{}", record.key(), new String(record.value()));
                        List<String> nonVolunteerOperationsForSingleClient = nonvoluntaryOperations.getOrDefault(record.key(), new ArrayList<>());
                        nonVolunteerOperationsForSingleClient.add(new String(record.value()));
                        nonvoluntaryOperations.put(record.key(), nonVolunteerOperationsForSingleClient);
                    });
                    assertTrue(countTotalNumberOfElements(nonvoluntaryOperations) >= 2);
                });

        assertEquals(1, operations.get("Frank").size());
        assertEquals(4, operations.get("Allys").size());
        assertEquals(2, operations.get("John").size());
        assertEquals(1, operations.get("Arnold").size());

        var franksOperation = JsonPath.parse(operations.get("Frank").getFirst());
        assertEquals("business-transaction", franksOperation.read("$.type", String.class));
        assertEquals("Frank", franksOperation.read("$.user", String.class));
        assertEquals("apple", franksOperation.read("$.stock", String.class));
        assertEquals(10, franksOperation.read("$.number", Integer.class));
    }

    private int countTotalNumberOfElements(Map<String, List<String>> decisions) {
        return decisions.values().stream().reduce(0, (acc, list) -> list.size() + acc, Integer::sum);
    }
}
