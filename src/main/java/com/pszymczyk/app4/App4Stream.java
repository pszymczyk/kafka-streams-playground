package com.pszymczyk.app4;

import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.MessageSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;

class App4Stream {

    public static final String APP_4_SOURCE = "app4-source";
    public static final String APP_4_STATE_STORE_NAME = "app4-store";
    private static final String APP_4_GLOBAL_STATE_STORE_NAME = "app4-global-store";

    private static GlobalKTable<String, Inbox> globalKTable;

    public static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(APP_4_SOURCE, Consumed.with(Serdes.Void(), MessageSerde.newSerde()))
            .groupBy((nullKey, value) -> value.receiver())
            .aggregate(() -> new Inbox(new ArrayList<>()),
                (key, message, inbox1) -> inbox1.add(message),
                Materialized.<String, Inbox>as(Stores.inMemoryKeyValueStore(APP_4_STATE_STORE_NAME))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(JsonSerdes.newSerdes(Inbox.class)));

        globalKTable = builder.globalTable("app4-" + APP_4_STATE_STORE_NAME + "-changelog",
            Consumed.with(Serdes.String(), JsonSerdes.newSerdes(Inbox.class)),
            Materialized.as(APP_4_GLOBAL_STATE_STORE_NAME));

        return builder;
    }

    static String getGlobalStoreName() {
        return globalKTable.queryableStoreName();
    }
}
