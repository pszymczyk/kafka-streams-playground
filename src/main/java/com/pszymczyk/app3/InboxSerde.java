package com.pszymczyk.app3;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

class InboxSerde {

    public final static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .setVisibility(PropertyAccessor.CREATOR, ANY)
            .setVisibility(PropertyAccessor.FIELD, NONE)
            .setVisibility(PropertyAccessor.GETTER, ANY)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
            .configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, true)
            .configure(MapperFeature.AUTO_DETECT_FIELDS, true);

    public static Serde<Inbox> newSerde() {
        return serdeFrom(new InboxSerializer(), new InboxDeserializer());
    }

    static class InboxSerializer implements Serializer<Inbox> {

        @Override
        public byte[] serialize(String topic, Inbox inbox) {
            try {
                return objectMapper.writeValueAsBytes(inbox);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class InboxDeserializer implements Deserializer<Inbox> {

        @Override
        public Inbox deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, Inbox.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
