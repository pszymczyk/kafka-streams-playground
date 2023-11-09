package com.pszymczyk.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonDeserializer<T> implements Deserializer<T> {

    private static final Logger log = LoggerFactory.getLogger(JsonDeserializer.class);

    private final Class<T> aClass;
    private final ObjectMapper objectMapper;

    public JsonDeserializer(Class<T> aClass, ObjectMapper objectMapper) {
        this.aClass = aClass;
        this.objectMapper = objectMapper;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, aClass);
        } catch (IOException e) {
            log.error("Cannot deserialize Kafka record!, Topic: {}, data: {}.", topic, new String(data), e);
            throw new RuntimeException(e);
        }
    }
}
