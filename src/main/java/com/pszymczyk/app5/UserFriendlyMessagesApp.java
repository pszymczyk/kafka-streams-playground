package com.pszymczyk.app5;

import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

class UserFriendlyMessagesApp {

    static final String MESSAGES = "app5-messages";
    static final String USERS = "app5-users";
    static final String USER_FRIENDLY_MESSAGES = "app5-user-friendly-messages";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "user-friendly-messages-main",
            builder,
            Map.of(),
            new NewTopic(MESSAGES, 1, (short) 1),
            new NewTopic(USER_FRIENDLY_MESSAGES, 1, (short) 1),
            new NewTopic(USERS, 1, (short) 1));
    }

    /**
     * GlobalKTable and no repartition topic
     */
    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, User> itemDetailsTable = builder.globalTable(USERS, Consumed.with(Serdes.String(), UserSerde.newSerde()),
            Materialized.as(Stores.inMemoryKeyValueStore("user-details-store")));

        KStream<String, String> userFriendlyMessagesStream = builder.stream(MESSAGES, Consumed.with(Serdes.String(), MessageSerde.newSerde()))
            .join(itemDetailsTable,
                (nullKey, value) -> value.receiver(),
                (message, user) -> message.value().replace("<user>", getPrettyUsername(user)));

        userFriendlyMessagesStream.to(USER_FRIENDLY_MESSAGES);

        return builder;
    }

    /**
     * KTable and repartition topic
     */
    static StreamsBuilder buildKafkaStreamsTopologyV2() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, User> itemDetailsTable = builder.table(USERS, Consumed.with(Serdes.String(), UserSerde.newSerde()),
            Materialized.as(Stores.inMemoryKeyValueStore("user-details-store")));

        KStream<String, String> userFriendlyMessagesStream = builder.stream(MESSAGES, Consumed.with(Serdes.String(), MessageSerde.newSerde()))
            .selectKey((k, v) -> v.receiver(), Named.as("select-receiver-key"))
            .join(itemDetailsTable,
                (message, user) -> message.value().replace("<user>", getPrettyUsername(user)));

        userFriendlyMessagesStream.to(USER_FRIENDLY_MESSAGES);

        return builder;
    }

    private static String getPrettyUsername(User user) {
        return user.firstName() + " " + user.lastName();
    }
}