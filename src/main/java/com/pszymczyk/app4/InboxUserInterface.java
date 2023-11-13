package com.pszymczyk.app4;

import com.pszymczyk.app3.InboxApp;
import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.query.StateQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Scanner;

import static com.pszymczyk.app3.InboxApp.INBOX;
import static com.pszymczyk.app3.InboxApp.MESSAGES;

class InboxUserInterface {

    private static final Logger logger = LoggerFactory.getLogger(InboxUserInterface.class);

    public static void main(String[] args) throws InterruptedException {
        StreamsBuilder builder = InboxApp.buildKafkaStreamsTopology();
        KafkaStreams kafkaStreams = new StreamsRunner().run(
            "localhost:9092",
            "inbox-user-interface-app-main",
            builder,
            Map.of(),
            new NewTopic(MESSAGES, 1, (short) 1),
            new NewTopic(INBOX, 1, (short) 1));

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

                StateQueryResult<Inbox> result = null;
                logger.info("Value {}.", result.getOnlyPartitionResult().getResult());
            }
        }
        kafkaStreams.close();
    }
}
