package com.pszymczyk.app2;

import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Map;

import static com.pszymczyk.common.Utils.createCompactedTopic;

class MessagesCountWithSerdeApp {

    static final String MESSAGES = "app2-messages";
    static final String MESSAGES_COUNT = "app2-messages-count";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "messages-count-with-serde-app-main",
            builder,
            Map.of(),
            new NewTopic(MESSAGES, 1, (short) 1),
            createCompactedTopic(MESSAGES_COUNT));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(MESSAGES, Consumed.with(Serdes.Void(), MessageSerde.newSerde()))
            .map((nullKey, message) -> new KeyValue<>(message.receiver(), ""))
            .groupByKey()
            .count(Materialized.as("messages-count-store"))
            .toStream()
            .to(MESSAGES_COUNT);

        return builder;
    }
}
