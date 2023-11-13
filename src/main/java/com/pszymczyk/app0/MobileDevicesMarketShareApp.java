package com.pszymczyk.app0;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

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

        builder.stream(CLICKS_TOPIC, Consumed.with(Serdes.Void(), Serdes.String()))
            .groupBy((nullKey, click) -> click.split("#")[2])
            .count()
            .toStream().to(CLICKS_COUNT);

        return builder;
    }
}

