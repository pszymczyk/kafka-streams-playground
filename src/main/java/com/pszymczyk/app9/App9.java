package com.pszymczyk.app9;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

class App9 {

    static final String APP_9_SOURCE = "app9-source";
    static final String APP_9_STATE = "app9-state";
    static final String APP_9_SINK = "app9-sink";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "app9",
            builder,
            Map.of(),
            new NewTopic(APP_9_SOURCE, 1, (short) 1),
            new NewTopic(APP_9_SINK, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, UnsortedEvents>> transferProcessKeyValueStore = Stores
            .keyValueStoreBuilder(Stores.inMemoryKeyValueStore(APP_9_STATE), Serdes.String(), JsonSerdes.newSerdes(UnsortedEvents.class));
        builder.addStateStore(transferProcessKeyValueStore);

        builder.stream(APP_9_SOURCE, Consumed.with(Serdes.Void(), JsonSerdes.newSerdes(UnsortedEvent.class)))
            .selectKey((key, value) -> value.processId())
            .process(SortingProcess::new, APP_9_STATE)
            .to(APP_9_SINK, Produced.with(Serdes.String(), JsonSerdes.newSerdes(UnsortedEvent.class)));

        return builder;
    }
}