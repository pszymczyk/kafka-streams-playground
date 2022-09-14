package com.pszymczyk.app3;

import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.Map;

public class InboxApp {

    public static final String MESSAGES = "app3-messages";
    public static final String INBOX = "app3-inbox";
    public static final String STATE_STORE_NAME = "app3-inbox-state-store";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "messages-app-main",
                builder,
                Map.of(),
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(INBOX, 1, (short) 1));
    }

    public static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        var materialized = Materialized.<String, Inbox>as(Stores.inMemoryKeyValueStore(STATE_STORE_NAME))
                .withKeySerde(Serdes.String())
                .withValueSerde(JsonSerdes.forA(Inbox.class));


        builder.stream(MESSAGES, Consumed.with(Serdes.String(), MessageSerde.newSerde())).groupBy((nullKey, value) -> value.receiver())
                .aggregate(() -> new Inbox(new ArrayList<>()),
                        (key, message, inbox1) -> inbox1.add(message),
                        materialized)
                .toStream()
                .to(INBOX);

        return builder;
    }
}
