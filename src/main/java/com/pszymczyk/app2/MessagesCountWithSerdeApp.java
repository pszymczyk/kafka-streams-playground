package com.pszymczyk.app2;

import com.pszymczyk.common.Message;
import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Map;
import java.util.Objects;

class MessagesCountWithSerdeApp {

    static final String MESSAGES = "app2-messages";
    static final String MESSAGES_COUNT = "app2-messages-count";

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
         *  key: null, value: "1232#pszymczyk#Hi Paweł, how are you?"
         *  key: null, value: "42324#andrzej123#Hello, how are you?"
         *  key: null, value: "31234#pszymczyk#Special discount for you!"
         *  ]
         */
        KStream<String, Message> messages = builder.stream(MESSAGES, Consumed.with(Serdes.String(), MessageSerde.newSerde()));

        KGroupedStream<String, String> messagesGroupedByUser = messages
                .map((nullKey, message) -> new KeyValue<>(message.receiver(), ""))
                .groupByKey();

        /*
         * Count all messages in groups
         * [
         *  key: "pszymczyk", value: 2
         *  key: "andrzej123", value: 1
         * ]
         */
        KTable<String, String> messagesCount = messagesGroupedByUser
                .count()
                .mapValues(v -> Objects.toString(v));
        /*
         * Convert Table -> Stream
         * [
         *  key: "pszymczyk", value: "2"
         *  key: "andrzej123", value: "1"
         * ]
         */
        messagesCount.toStream()
                .to(MESSAGES_COUNT);

        return builder;
    }
}
