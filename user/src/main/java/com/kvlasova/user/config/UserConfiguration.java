package com.kvlasova.user.config;

import com.kvlasova.convert.MessageDeserializer;
import com.kvlasova.convert.MessageSerializer;
import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.model.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class UserConfiguration {
    private final KafkaProperties kafkaProperties;

    public UserConfiguration(@Autowired KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public KafkaConsumer<String, Message> kafkaConsumer() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put("spring.json.trusted.packages", "*");
        props.put("spring.kafka.properties.allow.deserialization", "com.kvlasova.model.Message");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props);
        // Подписка на топик
        consumer.subscribe(Collections.singleton(KafkaTopics.TOPIC_PROCESSED_MESSAGES.getTopicName()));
        return consumer;
    }

    @Bean
    public KafkaProducer<String, Message> kafkaMessageProducer() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

//    @Bean
//    public KafkaProducer<String, User> kafkaUserInfoProducer() {
//        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
//        props.compute(ProducerConfig.CLIENT_ID_CONFIG,
//                (k, v) -> v = "user-info-sender");
//        return new KafkaProducer<>(props);
//    }
}
