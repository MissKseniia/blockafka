package com.kvlasova.adminuser.config;

import com.kvlasova.model.User;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class AdminUserConfig {
    private final KafkaProperties properties;

    @Bean
    public KafkaProducer<String, Boolean> adminUserProducer() {
        return new KafkaProducer<>(getProperties(BooleanSerializer.class));
    }

    @Bean
    public KafkaProducer<String, User> userInfoProducer() {
        return new KafkaProducer<>(getProperties(JsonSerializer.class));
    }

    private Map<String, Object> getProperties(Class<?> clazz) {
        var result = properties.buildProducerProperties();
        result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, clazz.getName());
        return result;
    }
}
