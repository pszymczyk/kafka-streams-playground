package com.pszymczyk.app6;

import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.Message;
import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import com.pszymczyk.common.Utils;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

class ThreeDaysInboxApp {

    static final String MESSAGES = "app6-messages";
    static final String THREE_DAYS_INBOX = "app6-three-days-inbox";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "three-days-inbox-app-main",
            builder,
            Map.of(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MessageTimeExtractor.class),
            new NewTopic(MESSAGES, 1, (short) 1),
            Utils.createCompactedTopic(THREE_DAYS_INBOX));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Void, Message> messagesStream = builder.stream(MESSAGES, Consumed.with(Serdes.Void(), MessageSerde.newSerde()));

        KTable<Windowed<String>, Inbox> threeDaysInbox = messagesStream
            .groupBy((nullKey, message) -> message.receiver())
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(3)).advanceBy(Duration.ofDays(1)))
            .aggregate(() -> new Inbox(new ArrayList<>()),
                (key, message, inbox) -> inbox.add(message),
                Materialized.with(Serdes.String(), JsonSerdes.newSerdes(Inbox.class))
            );

        threeDaysInbox
            .toStream((key, value) -> String.format("%s-%s-%s", key.window().startTime(), key.window().endTime(), key.key()))
            .to(THREE_DAYS_INBOX);

        return builder;
    }
}