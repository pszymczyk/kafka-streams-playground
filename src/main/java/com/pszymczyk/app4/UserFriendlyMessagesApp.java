package com.pszymczyk.app4;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Map;

class UserFriendlyMessagesApp {

    static final String MESSAGES = "messages";
    static final String USERS = "users";
    static final String USER_FRIENDLY_MESSAGES = "user-friendly-messages";

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

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> itemDetailsTable = builder.table(USERS);

        builder.<String,String>stream(MESSAGES)
                .join(itemDetailsTable, (message, user) -> message.replaceAll("<user>", user))
                .to(USER_FRIENDLY_MESSAGES);

        return builder;
    }
}