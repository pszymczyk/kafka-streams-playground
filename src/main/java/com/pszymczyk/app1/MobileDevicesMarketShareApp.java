package com.pszymczyk.app1;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public class MobileDevicesMarketShareApp {

    public static final String CLICKS_TOPIC = "clicks";
    public static final String CLICKS_COUNT = "clicks-count";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "mobile-devices-market-share-main",
            builder,
            new NewTopic(CLICKS_TOPIC, 1, (short) 1),
            new NewTopic(CLICKS_COUNT, 1, (short) 1));
    }

    public static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> clicks = builder.stream(CLICKS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> clicksCount = clicks
            .mapValues(value -> value.split("#"))
            .filterNot((key, value) -> value.length < 3)
            .map((key, value) -> new KeyValue<>(value[2], "_"))
            .groupByKey()
            .count();

        clicksCount
            .mapValues(longValue -> Long.toString(longValue))
            .toStream().to(CLICKS_COUNT);

        return builder;
    }
}
