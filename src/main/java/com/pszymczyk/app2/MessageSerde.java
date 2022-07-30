package com.pszymczyk.app2;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

class MessageSerde {

    public static Serde<Message> newSerde() {
        return serdeFrom(new MessageSerializer(), new MessageDeserializer());
    }

    static class MessageSerializer implements Serializer<Message> {

        @Override
        public byte[] serialize(String topic, Message message) {
            return (message.user() + "#" + message.value()).getBytes(Charset.forName(StandardCharsets.UTF_8.name()));
        }
    }

    static class MessageDeserializer implements Deserializer<Message> {

        @Override
        public Message deserialize(String topic, byte[] data) {
            final String s = new String(data);
            final String[] split = s.split("#");
            return new Message(split[0], split[1]);
        }
    }
}
