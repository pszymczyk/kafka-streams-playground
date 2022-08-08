package com.pszymczyk.app8;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

class InboxProcessorApp {

    static final String INBOX_STATE_STORE = "inbox-state-store";

    static final String MESSAGES = "messages";
    static final String RECEIVED_MESSAGES = "received-messages";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "inbox-process-app-main",
            builder,
            Map.of(),
            new NewTopic(MESSAGES, 1, (short) 1),
            new NewTopic(RECEIVED_MESSAGES, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(INBOX_STATE_STORE), Serdes.String(), JsonSerdes.forA(Inbox.class)));

        builder.stream(MESSAGES, Consumed.with(Serdes.String(), MessageSerde.newSerde()))
            .transform(InboxProcess::new, INBOX_STATE_STORE)
            .to(RECEIVED_MESSAGES, Produced.with(Serdes.String(), ReceivedMessageSerde.newSerde()));

        return builder;
    }
}