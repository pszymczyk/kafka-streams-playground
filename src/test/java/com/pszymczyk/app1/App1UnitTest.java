package com.pszymczyk.app1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;


class App1UnitTest {

    @Test
    void Should_split_Hello_World_string() {
        var properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        var topologyTestDriver = new TopologyTestDriver(App1.getTopology(), properties);

        TestInputTopic<String, String> source = topologyTestDriver
            .createInputTopic(
                "app1-source",
                Serdes.String().serializer(),
                Serdes.String().serializer());
        TestOutputTopic<String, String> sink = topologyTestDriver
            .createOutputTopic(
                "app1-sink",
                Serdes.String().deserializer(),
                Serdes.String().deserializer());

        source.pipeInput("Hello World!");

        assertEquals(sink.readValuesToList(), List.of("Hello", "World!"));
    }
}
