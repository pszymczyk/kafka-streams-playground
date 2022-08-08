package com.pszymczyk.app3;

import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.ArrayList;
import java.util.Map;

class InboxApp {

    static final String MESSAGES = "app3-messages";
    static final String INBOX = "app3-inbox";

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

        builder.stream(MESSAGES, Consumed.with(Serdes.String(), MessageSerde.newSerde())).groupBy((nullKey, value) -> value.receiver())
                .aggregate(() -> new Inbox(new ArrayList<>()),
                        (key, message, inbox1) -> inbox1.add(message),
                        Materialized.with(Serdes.String(), JsonSerdes.forA(Inbox.class)))
                .toStream()
                .to(INBOX);

        return builder;
    }
}
