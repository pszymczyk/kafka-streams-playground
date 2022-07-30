package com.pszymczyk.app1;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

class MobileDevicesMarketShareAppFileStorage {

    public static final String MOBILE_DEVICES_MARKET_SHARE_STORE = "mobile-devices-market-share-store";

    static final String CLICKS = "clicks";
    static final String CLICKS_COUNT = "clicks-count";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "abc",
                builder,
                Map.of(),
                new NewTopic(CLICKS, 1, (short) 1),
                new NewTopic(CLICKS_COUNT, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(MOBILE_DEVICES_MARKET_SHARE_STORE);
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, Long>as(storeSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long());

        KTable<String, Long> clicksCount = builder.<String, String>stream(CLICKS)
                .groupBy((key, value) -> value.split("#")[2])
                .count(materialized);

        clicksCount.toStream()
                .mapValues(aLong -> "" + aLong)
                .to(CLICKS_COUNT);

        return builder;
    }
}
