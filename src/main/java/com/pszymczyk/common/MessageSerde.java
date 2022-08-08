package com.pszymczyk.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

public class MessageSerde {

    public static Serde<Message> newSerde() {
        return serdeFrom(new MessageSerializer(), new MessageDeserializer());
    }

    static class MessageSerializer implements Serializer<Message> {

        @Override
        public byte[] serialize(String topic, Message message) {
            //TODO
            return null;
        }
    }

    static class MessageDeserializer implements Deserializer<Message> {

        @Override
        public Message deserialize(String topic, byte[] data) {
            //TODO
            return null;
        }
    }
}
