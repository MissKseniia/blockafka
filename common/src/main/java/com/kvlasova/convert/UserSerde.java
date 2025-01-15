package com.kvlasova.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kvlasova.model.User;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class UserSerde implements Serde<User> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<User> serializer() {
        return (topic, user) -> {
            try {
                return user != null ? objectMapper.writeValueAsBytes(user) : null;
            } catch (Exception e) {
                throw new RuntimeException("Error serializing User", e);
            }
        };
    }

    @Override
    public Deserializer<User> deserializer() {
        return (topic, data) -> {
            try {
                return data != null ? objectMapper.readValue(data, User.class) : null;
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing User", e);
            }
        };
    }
}
