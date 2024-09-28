package com.pszymczyk.app7;

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

class App7 {

    static final String APP_7_SOURCE = "app7-source";
    static final String APP_7_SINK = "app7-sink";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "app7",
            builder,
            Map.of(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MessageTimeExtractor.class),
            new NewTopic(APP_7_SOURCE, 1, (short) 1),
            Utils.createCompactedTopic(APP_7_SINK));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Void, Message> messagesStream = builder.stream(APP_7_SOURCE, Consumed.with(Serdes.Void(), MessageSerde.newSerde()));

        KTable<Windowed<String>, Inbox> threeDaysInbox = messagesStream
            .groupBy((nullKey, message) -> message.receiver())
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(3)).advanceBy(Duration.ofDays(1)))
            .aggregate(() -> new Inbox(new ArrayList<>()),
                (key, message, inbox) -> inbox.add(message),
                Materialized.with(Serdes.String(), JsonSerdes.newSerdes(Inbox.class))
            );

        threeDaysInbox
            .toStream((key, value) -> String.format("%s-%s-%s", key.window().startTime(), key.window().endTime(), key.key()))
            .to(APP_7_SINK);

        return builder;
    }
}