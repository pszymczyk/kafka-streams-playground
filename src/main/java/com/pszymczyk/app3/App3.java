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
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.Map;

import static com.pszymczyk.common.Utils.createCompactedTopic;

class App3 {

    public static final String APP_3_SOURCE = "app3-source";
    public static final String APP_3_SINK = "app3-sink";
    public static final String APP_3_STATE_STORE_NAME = "app3-store";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();

        new StreamsRunner().run(
            "localhost:9092",
            "app3",
            builder,
            Map.of(),
            new NewTopic(APP_3_SOURCE, 1, (short) 1),
            createCompactedTopic(APP_3_SINK));
    }

    public static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        //TODO

        return builder;
    }
}
