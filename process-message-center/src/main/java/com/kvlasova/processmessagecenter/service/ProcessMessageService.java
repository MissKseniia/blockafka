package com.kvlasova.processmessagecenter.service;

import com.kvlasova.enums.Users;
import com.kvlasova.model.Message;
import com.kvlasova.model.User;
import com.kvlasova.convert.MessageSerde;
import com.kvlasova.convert.UserSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.kvlasova.enums.KafkaTopics.TOPIC_CENZ_WORDS;
import static com.kvlasova.enums.KafkaTopics.TOPIC_NEW_MESSAGES;
import static com.kvlasova.enums.KafkaTopics.TOPIC_UNBLOCKED_MESSAGES;
import static com.kvlasova.enums.KafkaTopics.TOPIC_USER_INFO;

@Service
@Slf4j
public class ProcessMessageService {

    private final KafkaProperties kafkaProperties;
    private AtomicInteger messagesToProcess;

    public ProcessMessageService(@Autowired KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.messagesToProcess = new AtomicInteger(Users.getUsersLimit());
    }

    public boolean areAllMessagesProcessed() {
        return messagesToProcess.get() == 0;
    }

    protected StreamsConfig getStreamsConfig() {
        return new StreamsConfig(kafkaProperties.buildStreamsProperties(null));
    }

    public void decreaseMessagesToProcess() {
        messagesToProcess.getAndDecrement();
    }

}
