package com.pszymczyk.app6;

import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.StreamsRunner;
import com.pszymczyk.common.Utils;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;

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

        KTable<Windowed<String>, Inbox> threeDaysInbox = null;
        threeDaysInbox
                .toStream((key, value) -> String.format("%s-%s-%s", key.window().startTime(), key.window().endTime(), key.key()))
                .to(THREE_DAYS_INBOX);

        return builder;
    }
}