package com.kvlasova.processmessagecenter.service;

import com.kvlasova.enums.Users;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.nonNull;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessMessageService {

    private final KafkaProperties kafkaProperties;
    private AtomicInteger messagesToProcess = new AtomicInteger(Users.getUsersLimit());

    public boolean areAllMessagesProcessed() {
        return messagesToProcess.get() == 0;
    }

    protected StreamsConfig getStreamsConfig(String appId) {
        var configs = kafkaProperties.buildStreamsProperties(null);
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        return new StreamsConfig(configs);
    }

    public void decreaseMessagesToProcess(String key) {
        if (nonNull(key))
            messagesToProcess.getAndDecrement();
    }

}
