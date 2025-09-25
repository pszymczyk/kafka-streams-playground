package com.pszymczyk.app6;

import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.StreamsRunner;
import com.pszymczyk.common.User;
import com.pszymczyk.common.UserSerde;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

import static com.pszymczyk.common.Utils.createCompactedTopic;

class App6 {

    static final String APP_6_SOURCE = "app6-source";
    static final String APP_6_STATE = "app6-state";
    static final String APP_6_SINK = "app6-sink";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "app6",
            builder,
            Map.of(),
            new NewTopic(APP_6_SOURCE, 1, (short) 1),
            new NewTopic(APP_6_SINK, 1, (short) 1),
            createCompactedTopic(APP_6_STATE));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        //TODO

        return builder;
    }

    private static String getPrettyUsername(User user) {
        return user.firstName() + " " + user.lastName();
    }
}
