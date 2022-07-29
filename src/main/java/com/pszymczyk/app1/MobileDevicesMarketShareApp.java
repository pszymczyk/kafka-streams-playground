package com.pszymczyk.app1;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Map;

class MobileDevicesMarketShareApp {

    static final String CLICKS_TOPIC = "clicks";
    static final String CLICKS_COUNT = "clicks-count";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "mobile-devices-market-share-app-main",
                builder,
                Map.of(),
                new NewTopic(CLICKS_TOPIC, 1, (short) 1),
                new NewTopic(CLICKS_COUNT, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        /*
         * Simple stream of events on clicks' topic
         * [
         *  key: null, value: 2134553#button123#firefox,
         *  key: null, value: 2134553#button123#firefox,
         *  key: null, value: 2134553#button123#edge,
         *  ]
         */
        KStream<String, String> clicks = builder.stream(CLICKS_TOPIC);

        /*
         * Group events by browser name
         * [
         *  key: firefox, value: 2134553#button123#firefox,
         *  key: firefox, value: 2134553#button123#firefox
         * ],
         * [
         *  key: edge, value: 2134553#button123#edge,
         * ]
         */
        KGroupedStream<String, String> clicksGroupedByBrowserName = clicks.groupBy((nullKey, click) -> click.split("#")[2]);

        /*
         * Count all events in groups
         * [
         *  key: firefox, value: 2
         *  key: edge, value: 1
         * ]
         */
        KTable<String, Long> clicksCount = clicksGroupedByBrowserName
                .count();
        /*
         * Convert Table -> Stream
         * [
         *  key: firefox, value: 2
         *  key: edge, value: 1
         * ]
         */
        clicksCount.toStream()
                .mapValues(aLong -> Long.toString(aLong)) // we change clicks count value type from Long -> String to read the data using console consumer with ease
                .to(CLICKS_COUNT);

        return builder;
    }
}
