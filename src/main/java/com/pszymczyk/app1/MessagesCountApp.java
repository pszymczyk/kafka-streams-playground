package com.pszymczyk.app1;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Map;

class MessagesCountApp {

    static final String MESSAGES = "messages";
    static final String MESSAGES_COUNT = "messages-count";

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
         *  key: null, value: pszymczyk#Hi Paweł, how are you?
         *  key: null, value: andrzej123#Hello, how are you?
         *  key: null, value: pszymczyk#Special discount for you!
         *  ]
         */
        KStream<String, String> messages = builder.stream(MESSAGES);

        /*
         * Group messages by user name
         * [
         *  key: pszymczyk, value: pszymczyk#Hi Paweł, how are you?
         *  key: null, value: pszymczyk#Special discount for you!
         * ],
         * [
         *  key: null, value: andrzej123#Hello, how are you?
         * ]
         */
        KGroupedStream<String, String> messagesGroupedByUser = messages.groupBy((nullKey, message) -> message.split("#")[0]);

        /*
         * Count all messages in groups
         * [
         *  key: pszymczyk, value: 2
         *  key: andrzej123, value: 1
         * ]
         */
        KTable<String, Long> messagesCount = messagesGroupedByUser.count();
        /*
         * Convert Table -> Stream
         * [
         *  key: pszymczyk, value: 2
         *  key: andrzej123, value: 1
         * ]
         */
        messagesCount.toStream()
                .mapValues(aLong -> "" + aLong) // we change count value from Long -> String
                .to(MESSAGES_COUNT);

        return builder;
    }
}
