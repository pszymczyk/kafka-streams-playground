package com.pszymczyk.app6;

import com.pszymczyk.common.*;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

class ThreeDaysInboxApp {

    static final String MESSAGES = "messages";
    static final String THREE_DAYS_INBOX = "three-days-inbox";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "ThreeDaysInboxApp-main",
                builder,
                Map.of(),
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(THREE_DAYS_INBOX, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Message> messagesStream = builder.stream(MESSAGES, Consumed.with(Serdes.String(), MessageSerde.newSerde()));

        KTable<Windowed<String>, Inbox> articlesRankingTable = messagesStream
                .groupBy((nullKey, message) -> message.receiver())
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(3)).advanceBy(Duration.ofDays(1)))
                .aggregate(() -> new Inbox(new ArrayList<>()),
                        (key, message, inbox) -> inbox.add(message),
                        Materialized.with(Serdes.String(), JsonSerdes.forA(Inbox.class))
                );

        articlesRankingTable
                .toStream((key, value) -> String.format("%s-%s-%s", key.window().startTime(), key.window().endTime(), key.key()))
                .to(THREE_DAYS_INBOX);

        return builder;
    }
}