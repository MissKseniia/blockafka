package com.kvlasova.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kvlasova.model.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MessageSerializer implements Serializer<Message> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Message data) {
        try {
            if (data == null){
                return null;
            }
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Message to byte[]");
        }
    }

    @Override
    public void close() {
    }

}
