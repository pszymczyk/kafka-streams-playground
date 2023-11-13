package com.pszymczyk.app8;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
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

        return builder;
    }
}