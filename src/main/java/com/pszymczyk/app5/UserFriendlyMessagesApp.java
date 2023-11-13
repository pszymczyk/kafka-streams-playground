package com.pszymczyk.app5;

import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Map;

import static com.pszymczyk.common.Utils.createCompactedTopic;

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
            createCompactedTopic(USERS));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        return builder;
    }

    private static String getPrettyUsername(User user) {
        return user.firstName() + " " + user.lastName();
    }
}