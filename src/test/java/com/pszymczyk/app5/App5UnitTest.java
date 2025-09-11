package com.pszymczyk.app5;

import com.pszymczyk.IntegrationSpec;
import com.pszymczyk.common.Message;
import com.pszymczyk.common.MessageSerde;
import com.pszymczyk.common.User;
import com.pszymczyk.common.UserSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class App5UnitTest extends IntegrationSpec {

    @Test
    void Should_enrich_messages_with_full_user_name() {
        var properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        StreamsBuilder streamsBuilder = App5.buildKafkaStreamsTopology();
        var topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), properties);
        var source = topologyTestDriver.createInputTopic(App5.APP_5_SOURCE,
            Serdes.String().serializer(),
            MessageSerde.newSerde().serializer());
        var state = topologyTestDriver.createInputTopic(App5.APP_5_STATE,
            Serdes.String().serializer(),
            UserSerde.newSerde().serializer());
        var sink = topologyTestDriver.createOutputTopic(App5.APP_5_SINK,
            Serdes.String().deserializer(),
            Serdes.String().deserializer());

        state.pipeInput("andrzej123", new User("Andrzej", "Golara"));
        state.pipeInput("pszymczyk", new User("Pawel", "Szymczyk"));
        source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "Hello <user>! how are you?"));
        source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "ping"));
        source.pipeInput(new Message(System.currentTimeMillis(), "ecom", "andrzej123", "Hi <user>, we have some special offer for you"));

        assertEquals(
            List.of(
                "Hello Pawel Szymczyk! how are you?",
                "ping",
                "Hi Andrzej Golara, we have some special offer for you"),
            sink.readValuesToList());

        topologyTestDriver.close();
    }
}
