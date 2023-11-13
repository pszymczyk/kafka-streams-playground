package com.pszymczyk.app3;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import java.util.Map;

import static com.pszymczyk.common.Utils.createCompactedTopic;

public class InboxApp {

    public static final String MESSAGES = "app3-messages";
    public static final String INBOX = "app3-inbox";
    public static final String STATE_STORE_NAME = "inbox-store";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();

        new StreamsRunner().run(
            "localhost:9092",
            "inbox-app-main",
            builder,
            Map.of(),
            new NewTopic(MESSAGES, 1, (short) 1),
            createCompactedTopic(INBOX));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        return builder;
    }
}
