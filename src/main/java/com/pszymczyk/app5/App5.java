package com.pszymczyk.app5;

import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import com.pszymczyk.common.User;
import com.pszymczyk.common.UserSerde;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

import static com.pszymczyk.common.Utils.createCompactedTopic;

class App5 {

    static final String APP_5_SOURCE = "app5-source";
    static final String APP_5_STATE = "app5-state";
    static final String APP_5_SINK = "app5-sink";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "app5",
            builder,
            Map.of(),
            new NewTopic(APP_5_SOURCE, 1, (short) 1),
            new NewTopic(APP_5_SINK, 1, (short) 1),
            createCompactedTopic(APP_5_STATE));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, User> itemDetailsTable = builder.globalTable(APP_5_STATE, Consumed.with(Serdes.String(), UserSerde.newSerde()),
            Materialized.as(Stores.inMemoryKeyValueStore("user-details-store")));

        KStream<String, String> userFriendlyMessagesStream = builder.stream(APP_5_SOURCE, Consumed.with(Serdes.String(), MessageSerde.newSerde()))
            .join(itemDetailsTable,
                (nullKey, value) -> value.receiver(),
                (message, user) -> message.value().replace("<user>", getPrettyUsername(user)));

        userFriendlyMessagesStream.to(APP_5_SINK);

        return builder;
    }

    private static String getPrettyUsername(User user) {
        return user.firstName() + " " + user.lastName();
    }
}