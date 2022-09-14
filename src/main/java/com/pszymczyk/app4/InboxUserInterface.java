package com.pszymczyk.app4;

import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Scanner;

import static com.pszymczyk.app3.InboxApp.*;
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

class InboxUserInterface {

    private static final Logger logger = LoggerFactory.getLogger(InboxUserInterface.class);

    public static void main(String[] args) throws InterruptedException {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        KafkaStreams kafkaStreams = new StreamsRunner().run(
                "localhost:9092",
                "messages-app-main",
                builder,
                Map.of(),
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(INBOX, 1, (short) 1));

        while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
            logger.info("KafkaStreams state is {}", kafkaStreams.state());
            logger.info("Waiting........");
            Thread.sleep(200);
        }

        Scanner scanner = new Scanner(System.in);
        while (true) {
            logger.info("Enter key:");
            String line = scanner.nextLine();

            if (line.equals("wq")) {
                break;
            }

            ReadOnlyKeyValueStore<String, Inbox> store = kafkaStreams.store(fromNameAndType(STATE_STORE_NAME, keyValueStore()));
            logger.info("Value {}", store.get(line));
        }

        kafkaStreams.close();
    }
}
