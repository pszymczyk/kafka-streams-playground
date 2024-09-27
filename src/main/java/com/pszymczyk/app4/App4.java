package com.pszymczyk.app4;

import com.pszymczyk.app3.App3;
import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Scanner;

import static com.pszymczyk.common.Utils.createCompactedTopic;

class App4 {

    private static final Logger logger = LoggerFactory.getLogger(App4.class);

    public static void main(String[] args) throws InterruptedException {
        StreamsBuilder builder = App3.buildKafkaStreamsTopology();
        KafkaStreams kafkaStreams = new StreamsRunner().run(
            "localhost:9092",
            "app4",
            builder,
            Map.of(),
            new NewTopic(App3.APP_3_SOURCE, 1, (short) 1),
            createCompactedTopic(App3.APP_3_SINK));

        while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
            logger.info("KafkaStreams state is {}", kafkaStreams.state());
            logger.info("Waiting........");
            Thread.sleep(200);
        }

        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                logger.info("Enter key:");
                String line = scanner.nextLine();

                if (line.equals("wq")) {
                    break;
                }

                StateQueryRequest<Inbox> request = StateQueryRequest.inStore(App3.APP_3_STATE_STORE_NAME).withQuery(KeyQuery.withKey(line));
                StateQueryResult<Inbox> result = kafkaStreams.query(request);

                if (result.getPartitionResults()
                    .values()
                    .stream()
                    .anyMatch(r -> r.getResult() != null)) {
                    logger.info("Value {}.", result.getOnlyPartitionResult().getResult());
                } else {
                    logger.warn("Query into state store {} failed.", App3.APP_3_STATE_STORE_NAME);
                }
            }
        }
        kafkaStreams.close();
    }
}
