package com.pszymczyk.app1;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

class MobileDevicesMarketShareAppFileStorage {

    static final String CLICKS_TOPIC = "clicks";
    static final String CLICKS_COUNT = "clicks-count";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "abc",
                builder,
                Map.of(),
                new NewTopic(CLICKS_TOPIC, 1, (short) 1),
                new NewTopic(CLICKS_COUNT, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        /*
          key: null, value: 2134553#button123#firefox
          key: null, value: 2134553#button123#chrome
          key: null, value: 2134553#button9292#chrome
         */
        KStream<String, String> clicks = builder.stream(CLICKS_TOPIC);

        /*
            [key: firefox, value: 2134553#button123#firefox]
            [key: edge, value: 2134553#button123#chrome ,key: edge, value: 2134553#button9292#chrome]
         */
        KStream<String, String> filter = clicks.filter((k, v) -> v.split("#").length == 3);
        KGroupedStream<String, String> groupedByBrowserName = filter.groupBy((key, value) -> {
            final String s = value.split("#")[2];
            return s;
        });

        /*
            [key: firefox, value: 1,
             key: edge, value: 2]
         */

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore("queryable-store-name");
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, Long>as(storeSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long());

        KTable<String, Long> clicksCount = groupedByBrowserName.count(materialized);

        clicksCount.toStream()
                .mapValues(aLong -> "" + aLong)
                .to(CLICKS_COUNT);

        return builder;
    }
}
