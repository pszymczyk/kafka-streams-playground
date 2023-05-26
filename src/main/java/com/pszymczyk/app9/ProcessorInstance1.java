package com.pszymczyk.app9;

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

class ProcessorInstance1 {

    static final String UNSORTED_EVENTS = "unsorted-events";
    static final String SORTED_EVENTS = "sorted-events";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "SortingEventsApp-app-main",
                builder,
                Map.of(),
                new NewTopic(UNSORTED_EVENTS, 1, (short) 1),
                new NewTopic(SORTED_EVENTS, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> transferProcessKeyValueStore = Stores
                .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("unsorted-events"), Serdes.String(), Serdes.String());
        builder.addStateStore(transferProcessKeyValueStore);

        builder.stream(UNSORTED_EVENTS, Consumed.with(Serdes.String(), Serdes.String()))
                .process(() -> new TestingKeyValueStorePartitions("1"), "unsorted-events")
                .to(SORTED_EVENTS, Produced.with(Serdes.String(), Serdes.String()));

        return builder;
    }
}