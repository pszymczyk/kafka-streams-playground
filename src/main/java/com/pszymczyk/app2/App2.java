package com.pszymczyk.app2;

import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Map;

import static com.pszymczyk.common.Utils.createCompactedTopic;

class App2 {

    static final String APP_2_SOURCE = "app2-source";
    static final String APP_2_SINK = "app2-sink";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "app2",
            builder,
            Map.of(),
            new NewTopic(APP_2_SOURCE, 1, (short) 1),
            createCompactedTopic(APP_2_SINK));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(APP_2_SOURCE, Consumed.with(Serdes.Void(), MessageSerde.newSerde()))
            .groupBy((k,v) -> v.receiver())
            .count()
            .toStream()
            .to(APP_2_SINK);

        return builder;
    }
}
