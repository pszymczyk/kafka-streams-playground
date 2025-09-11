package com.pszymczyk.app3;


import com.pszymczyk.IntegrationSpec;
import com.pszymczyk.common.Inbox;
import com.pszymczyk.common.InboxMessage;
import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.Message;
import com.pszymczyk.common.MessageSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class App3UnitTest extends IntegrationSpec {

    @Test
    void Should_aggregate_messages() {
        var properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        StreamsBuilder streamsBuilder = App3.buildKafkaStreamsTopology();
        var topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), properties);
        var source = topologyTestDriver.createInputTopic(App3.APP_3_SOURCE,
            Serdes.String().serializer(),
            MessageSerde.newSerde().serializer());
        var sink = topologyTestDriver.createOutputTopic(App3.APP_3_SINK,
            Serdes.String().deserializer(),
            JsonSerdes.newSerdes(Inbox.class).deserializer());

        source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "Hello! how are you?"));
        source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "trololo"));
        source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "ping"));
        source.pipeInput(new Message(System.currentTimeMillis(), "pszymczyk", "andrzej123", "Hello!"));

        Map<String, Inbox> inboxMap = sink.readKeyValuesToMap();

        assertEquals(3, inboxMap.get("pszymczyk").messages().size());
        assertEquals(1, inboxMap.get("andrzej123").messages().size());

        InboxMessage firstMessage = inboxMap.get("pszymczyk").messages().get(0);
        assertEquals("Hello! how are you?", firstMessage.message());
        assertEquals("andrzej123", firstMessage.sender());
        assertTrue(firstMessage.inboxTime() >= firstMessage.senderTime());

        InboxMessage secondMessage = inboxMap.get("pszymczyk").messages().get(1);
        assertEquals("trololo", secondMessage.message());
        assertEquals("andrzej123", secondMessage.sender());
        assertTrue(secondMessage.inboxTime() >= secondMessage.senderTime());

        InboxMessage thirdMessage = inboxMap.get("pszymczyk").messages().get(2);
        assertEquals("ping", thirdMessage.message());
        assertEquals("andrzej123", thirdMessage.sender());
        assertTrue(thirdMessage.inboxTime() >= thirdMessage.senderTime());

        InboxMessage firstAndrzejMessage = inboxMap.get("andrzej123").messages().get(0);
        assertEquals("Hello!", firstAndrzejMessage.message());
        assertEquals("pszymczyk", firstAndrzejMessage.sender());
        assertTrue(firstAndrzejMessage.inboxTime() >= firstAndrzejMessage.senderTime());

        topologyTestDriver.close();
    }
}
