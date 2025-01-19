package com.kvlasova.processmessagecenter.service;

import com.kvlasova.convert.MessageSerde;
import com.kvlasova.convert.UserSerde;
import com.kvlasova.model.Message;
import com.kvlasova.model.User;
import com.kvlasova.enums.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.kvlasova.enums.KafkaTopics.TOPIC_NEW_MESSAGES;
import static com.kvlasova.enums.KafkaTopics.TOPIC_USER_INFO;

@Service
@Slf4j
public class BlockMessageService {

    private KafkaStreams kafkaStreams;
    private final ProcessMessageService processMessageService;

    public BlockMessageService(ProcessMessageService processMessageService) {
        this.processMessageService = processMessageService;
    }

    public void blockUsers() {
        kafkaStreams = getKafkaStreamsForBlockingUsers();
        kafkaStreams.start();
        log.info("kafkaStreams: started");
    }

    public void closeStreams() {
        kafkaStreams.close();
    }

    private KafkaStreams getKafkaStreamsForBlockingUsers() {
        // Создание топологии
        StreamsBuilder builder = new StreamsBuilder();
        // Создаём KStream из топика с входными данными
        var inputStream = builder.stream(TOPIC_NEW_MESSAGES.getTopicName(), Consumed.with(Serdes.String(), new MessageSerde()));
        //основная логика обработки
        processMessage(inputStream, new StreamsBuilder());
        var configs = processMessageService.getStreamsConfig();
        return new KafkaStreams(builder.build(), configs);
    }

    private void processMessage(KStream<String, Message> inputStream, StreamsBuilder builder) {
        KStream<String, Message> joinedStream = inputStream
                .peek((k,v)->log.info("Got Message: {}",v))
                .leftJoin(getKafkaTableFromUserInfo(builder), this::defineStatus);
        var blockedStream = joinedStream.filter((k, v) -> v.isBlocked());
        var unBlockedStream = joinedStream.filter((k, v) -> !v.isBlocked());

        blockedStream.peek((k, v) -> {
            if (k != null) {
                processMessageService.decreaseMessagesToProcess();
            }
        }).to(KafkaTopics.TOPIC_BLOCKED_MESSAGES.getTopicName());
        unBlockedStream.to(KafkaTopics.TOPIC_UNBLOCKED_MESSAGES.getTopicName());

    }

    //Список должен храниться на диске.
    private KTable<String, User> getKafkaTableFromUserInfo(StreamsBuilder builder) {
        var userSerdes = new UserSerde();
        return builder.stream(TOPIC_USER_INFO.getTopicName(), Consumed.with(Serdes.String(), userSerdes))
                .groupByKey()
                .reduce((userOld, userNew) -> new User(userOld.userName(), updateBlockedUsers(userOld.blockedUsers(), userNew.blockedUsers())))
                .toStream()
                .toTable(
                        Materialized.<String, User>as(Stores.persistentKeyValueStore("user-info-store"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerdes));
    }

    private List<String> updateBlockedUsers(List<String> usersOld, List<String> usersNew) {
        usersOld.addAll(usersNew);
        return usersOld.stream().distinct().collect(Collectors.toCollection(ArrayList::new));
    }

    private Message defineStatus(Message streamValue, User tableValue) {
        var blockedUsers = tableValue.blockedUsers();
        var toBlock = blockedUsers.stream()
                .anyMatch(streamValue.sender()::equalsIgnoreCase);
        return new Message(streamValue.content(), streamValue.receiver(), streamValue.sender(), toBlock);
    }
}
