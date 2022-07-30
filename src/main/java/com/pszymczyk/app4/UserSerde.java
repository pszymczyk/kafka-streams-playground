package com.pszymczyk.app4;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

class UserSerde {

    public static Serde<User> newSerde() {
        return serdeFrom(new UserSerializer(), new UserDeserializer());
    }

    static class UserSerializer implements Serializer<User> {

        @Override
        public byte[] serialize(String topic, User user) {
            return (user.firstName() + "#" + user.lastName()).getBytes(Charset.forName(StandardCharsets.UTF_8.name()));
        }
    }

    static class UserDeserializer implements Deserializer<User> {

        @Override
        public User deserialize(String topic, byte[] data) {
            final String s = new String(data);
            final String[] split = s.split("#");
            return new User(split[0], split[1]);
        }
    }
}
