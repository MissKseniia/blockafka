package com.kvlasova.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kvlasova.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class MessageDeserializer implements Deserializer<Message> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                return null;
            }
            return objectMapper.readValue(data, Message.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Message");
        }
    }

    @Override
    public void close() {
    }
}
