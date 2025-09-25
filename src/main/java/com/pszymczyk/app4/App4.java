package com.pszymczyk.app4;

import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Scanner;

import static com.pszymczyk.common.Utils.createCompactedTopic;

class App4 {

    private static final Logger logger = LoggerFactory.getLogger(App4.class);
    private static final String APPLICATION_ID = "app4";

    public static void main(String[] args) throws InterruptedException {
        StreamsBuilder builder = App4Stream.buildKafkaStreamsTopology();
        KafkaStreams kafkaStreams = new StreamsRunner().run(
            "localhost:9092",
            APPLICATION_ID,
            builder,
            Map.of(),
            new NewTopic(App4Stream.APP_4_SOURCE, 1, (short) 1),
            createCompactedTopic(APPLICATION_ID + "-" + App4Stream.APP_4_STATE_STORE_NAME + "-changelog")
        );

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

                //TODO

                Inbox inbox = null;

                if (inbox != null) {
                    logger.info("{}.", inbox);
                } else {
                    logger.warn("Query failed.");
                }
            }
        }
        kafkaStreams.close();
    }
}
