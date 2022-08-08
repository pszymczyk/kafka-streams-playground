package com.pszymczyk.app1;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Map;

class MessagesCountApp {

    static final String MESSAGES = "app1-messages";
    static final String MESSAGES_COUNT = "app1-messages-count";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "messages-app-main",
                builder,
                Map.of(),
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(MESSAGES_COUNT, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        /*
         * Simple stream of messages
         * [
         *  key: null, value: "pszymczyk#andrzej123#Hi Paweł, how are you?"
         *  key: null, value: "andrzej123#pszymczyk#Hello, how are you?"
         *  key: null, value: "telemarketing#pszymczyk#Special discount for you!"
         *  ]
         */
        KStream<String, String> messages = builder.stream(MESSAGES);

        /*
         * Map and group messages by receiver name
         * [
         *  key: "pszymczyk", value: ""
         *  key: "pszymczyk", value: ""
         * ],
         * [
         *  key: "andrzej123", value: ""
         * ]
         */
        KGroupedStream<String, String> messagesGroupedByUser = messages
                .map((nullKey, message) -> new KeyValue<>(message.split("#")[1], ""))
                .groupByKey();

        /*
         * Count all messages in groups
         * [
         *  key: "pszymczyk", value: 2
         *  key: "andrzej123", value: 1
         * ]
         */
        KTable<String, Long> messagesCount = messagesGroupedByUser.count();
        /*
         * Convert Table -> Stream
         * [
         *  key: "pszymczyk", value: "2"
         *  key: "andrzej12"3, value: "1"
         * ]
         */
        messagesCount.toStream()
                .mapValues(Object::toString)
                .to(MESSAGES_COUNT);

        return builder;
    }
}
