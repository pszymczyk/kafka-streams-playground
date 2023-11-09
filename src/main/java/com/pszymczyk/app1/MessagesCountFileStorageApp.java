package com.pszymczyk.app1;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

class MessagesCountFileStorageApp {

    static final String MESSAGES = "app1-fs-messages";
    static final String MESSAGES_COUNT = "app1-fs-messages-count";
    static final String STATE_STORE_NAME = "app1-fs-messages-count-state-store";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "messages-app-main",
                builder,
                Map.of(),
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(MESSAGES_COUNT, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> messages = builder.stream(MESSAGES);

        /*
         * Map and group messages by receiver name
         * [
         *  key: "pszymczyk", value: ""
         *  key: "pszymczyk", value: ""
         * ],
         * [
         *  key: "andrzej123", value: ""
         * ]
         */
        KGroupedStream<String, String> messagesGroupedByUser = messages
                .map((nullKey, message) -> new KeyValue<>(message.split("#")[2], ""))
                .groupByKey();

        /*
         * Prepare file based Materialized
         */
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(STATE_STORE_NAME);
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, Long>as(storeSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long());

        /*
         * Count all messages in groups
         * [
         *  key: "pszymczyk", value: 2
         *  key: "andrzej123", value: 1
         * ]
         */
        KTable<String, Long> messagesCount = messagesGroupedByUser.count(materialized);
        /*
         * Convert Table -> Stream
         * [
         *  key: "pszymczyk", value: "2"
         *  key: "andrzej12"3, value: "1"
         * ]
         */
        messagesCount.toStream()
                .mapValues(Object::toString)
                .to(MESSAGES_COUNT);

        return builder;
    }
}
