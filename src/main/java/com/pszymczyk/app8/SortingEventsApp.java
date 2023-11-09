package com.pszymczyk.app8;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;

class SortingEventsApp {

    static final String UNSORTED_EVENTS = "unsorted-events";
    static final String UNSORTED_EVENTS_STORE = "unsorted-events-store";
    static final String SORTED_EVENTS = "sorted-events";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "sorting-events-app-main",
                builder,
                Map.of(),
                new NewTopic(UNSORTED_EVENTS, 1, (short) 1),
                new NewTopic(SORTED_EVENTS, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, SomeUnsortedEvents>> transferProcessKeyValueStore = Stores
                .keyValueStoreBuilder(Stores.inMemoryKeyValueStore(UNSORTED_EVENTS_STORE), Serdes.String(), JsonSerdes.forA(SomeUnsortedEvents.class));
        builder.addStateStore(transferProcessKeyValueStore);

        builder.stream(UNSORTED_EVENTS, Consumed.with(Serdes.String(), JsonSerdes.forA(SomeUnsortedEvent.class)))
                .selectKey((key, value) -> value.getProcessId())
                .process(SortingProcess::new, UNSORTED_EVENTS_STORE)
                .to(SORTED_EVENTS, Produced.with(Serdes.String(), JsonSerdes.forA(SomeUnsortedEvent.class)));

        return builder;
    }
}