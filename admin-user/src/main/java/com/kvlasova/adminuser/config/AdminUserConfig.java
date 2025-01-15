package com.kvlasova.adminuser.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class AdminUserConfig {
    private final KafkaProperties properties;

    @Bean
    public KafkaProducer<String, Boolean> adminUserProducer() {
        return new KafkaProducer<>(properties.buildProducerProperties());
    }
}
