package com.pszymczyk.app7;


import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.Message;
import com.pszymczyk.common.MessageSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class App7UnitTest {

    static long today = Instant.parse("2007-12-15T10:15:30.00Z").toEpochMilli();
    static long yesterday = Instant.parse("2007-12-14T10:15:30.00Z").toEpochMilli();
    static long dayBeforeYesterday = Instant.parse("2007-12-13T10:15:30.00Z").toEpochMilli();

    @Test
    void Should_aggregate_three_days_long_inbox() {
        var properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MessageTimeExtractor.class);
        StreamsBuilder streamsBuilder = App7.buildKafkaStreamsTopology();
        var topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), properties);
        var source = topologyTestDriver.createInputTopic(App7.APP_7_SOURCE,
                Serdes.String().serializer(),
                MessageSerde.newSerde().serializer());
        var sink = topologyTestDriver.createOutputTopic(App7.APP_7_SINK,
                Serdes.String().deserializer(),
                JsonSerdes.newSerdes(Inbox.class).deserializer());

        source.pipeInput(new Message(today, "andrzej123", "pszymczyk", "Hello! how are you?"));
        source.pipeInput(new Message(today, "andrzej123", "pszymczyk", "trololo"));
        source.pipeInput(new Message(yesterday, "andrzej123", "pszymczyk", "ping"));
        source.pipeInput(new Message(dayBeforeYesterday, "andrzej123", "pszymczyk", "pong"));

        Map<String, Inbox> inboxMap = sink.readKeyValuesToMap();

        assertEquals(4, inboxMap.get("2007-12-13T00:00:00Z-2007-12-16T00:00:00Z-pszymczyk").messages().size());
        assertEquals(3, inboxMap.get("2007-12-14T00:00:00Z-2007-12-17T00:00:00Z-pszymczyk").messages().size());
        assertEquals(2, inboxMap.get("2007-12-15T00:00:00Z-2007-12-18T00:00:00Z-pszymczyk").messages().size());
    }
}
