package com.kvlasova.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kvlasova.model.Message;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MessageSerde implements Serde<Message> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<Message> serializer() {
        return (topic, message) -> {
            try {
                return message != null ? objectMapper.writeValueAsBytes(message) : null;
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Message", e);
            }
        };
    }

    @Override
    public Deserializer<Message> deserializer() {
        return (topic, data) -> {
            try {
                return data != null ? objectMapper.readValue(data, Message.class) : null;
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing Message", e);
            }
        };
    }
}
