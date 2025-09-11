package com.pszymczyk.app2;

import com.pszymczyk.common.Message;
import com.pszymczyk.common.MessageSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class App2UnitTest {

    @Test
    void Should_count_messages_sent_to_every_user() {
        var properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        StreamsBuilder streamsBuilder = App2.buildKafkaStreamsTopology();
        var topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), properties);
        var source = topologyTestDriver.createInputTopic(App2.APP_2_SOURCE,
            Serdes.String().serializer(),
            MessageSerde.newSerde().serializer());
        var sink = topologyTestDriver.createOutputTopic(App2.APP_2_SINK,
            Serdes.String().deserializer(),
            Serdes.Long().deserializer());

        source.pipeInput(new Message(1234L, "andrzej123", "pszymczyk", "Hello! how are you?"));
        source.pipeInput(new Message(1235L, "andrzej123", "pszymczyk", "Hello! how are you?"));
        source.pipeInput(new Message(1236L, "andrzej123", "pszymczyk", "Hello! how are you?"));
        source.pipeInput(new Message(1237L, "pszymczyk", "andrzej123", "Hello! how are you?"));

        Map<String, Long> messagesCount = sink.readKeyValuesToMap();

        assertEquals(3L, messagesCount.get("pszymczyk"));
        assertEquals(1L, messagesCount.get("andrzej123"));
    }
}
