package com.pszymczyk.app8;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

public class ReceivedMessageSerde {

    public static Serde<ReceivedMessage> newSerde() {
        return serdeFrom(new ReceivedMessageSerializer(), new ReceivedMessageDeserializer());
    }

    static class ReceivedMessageSerializer implements Serializer<ReceivedMessage> {

        @Override
        public byte[] serialize(String topic, ReceivedMessage receivedMessage) {
            return receivedMessage.messageId().getBytes(Charset.forName(StandardCharsets.UTF_8.name()));
        }
    }

    static class ReceivedMessageDeserializer implements Deserializer<ReceivedMessage> {

        @Override
        public ReceivedMessage deserialize(String topic, byte[] data) {
            return new ReceivedMessage(new String(data));
        }
    }
}
