package com.pszymczyk.app3;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.Map;

class InboxApp {

    static final String MESSAGES = "messages";
    static final String INBOX = "inbox";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "messages-app-main",
                builder,
                Map.of(),
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(INBOX, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        /*
         * Simple stream of messages
         * [
         *  key: "pszymczyk", value: "pszymczyk#Hi Paweł, how are you?"
         *  key: "andrzej123", value: "andrzej123#Hello, how are you?"
         *  key: "pszymczyk", value: "pszymczyk#Special discount for you!"
         *  ]
         */
        KStream<String, Message> stream = builder.stream(MESSAGES, Consumed.with(Serdes.String(), MessageSerde.newSerde()));

        /*
         * Group messages by user name
         * [
         *  key: "pszymczyk", value: "pszymczyk#Hi Paweł, how are you?"
         *  key: "pszymczyk", value: "pszymczyk#Hi Paweł, how are you?"
         * ],
         * [
         *  key: "andrzej123", value: "Hello, how are you?"
         * ]
         */
        KGroupedStream<String, Message> groupedStream = stream.groupBy((nullKey, value) -> value.user());

        /*
         * Aggregate groups to single inbox
         * [
         *  key: "pszymczyk",
         *  value: {
         *      "messages": [
         *          {"timestamp": 1659201474894, "message": "Hi Paweł, how are you?"},
         *          {"timestamp": 1659201474952, "message": "Hi! what is going on?"}
         *      ]
         * },
         * key: "andrzej123",
         * value: {
         *      "messages": [
         *          {"timestamp": 1659201474894, "message": "Hi Paweł, how are you?"},
         *          {"timestamp": 1659201474952, "message": "Hi! what is going on?"}
         *      ]
         * }
         */
        KTable<String, Inbox> aggregate = groupedStream
                .aggregate(() -> new Inbox(new ArrayList<>()),
                        (key, message, inbox1) -> inbox1.add(message),
                        Materialized.with(Serdes.String(), InboxSerde.newSerde()));
        aggregate
                .toStream()
                .to(INBOX);

        return builder;
    }
}
