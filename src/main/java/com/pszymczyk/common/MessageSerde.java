package com.pszymczyk.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

public class MessageSerde {

    public static Serde<Message> newSerde() {
        return serdeFrom(new MessageSerializer(), new MessageDeserializer());
    }

    static class MessageSerializer implements Serializer<Message> {

        @Override
        public byte[] serialize(String topic, Message message) {
            return toStringFormat(message).getBytes(StandardCharsets.UTF_8);
        }

        private String toStringFormat(Message message) {
            return String.format("%d#%s#%s#%s", message.timestamp(), message.sender(), message.receiver(), message.value());
        }
    }

    static class MessageDeserializer implements Deserializer<Message> {

        @Override
        public Message deserialize(String topic, byte[] data) {
            final String s = new String(data);
            final String[] split = s.split("#");
            return new Message(Long.parseLong(split[0]), split[1], split[2], split[3]);
        }
    }
}
