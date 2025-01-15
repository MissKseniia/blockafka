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
    private MessageSerde messageSerde;
    private AtomicInteger messagesToProcess;

    public ProcessMessageService(@Autowired KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.messageSerde = new MessageSerde();
        this.messagesToProcess = new AtomicInteger(Users.getUsersLimit());
    }

    public boolean areAllMessagesProcessed() {
        return messagesToProcess.get() == 0;
    }

    protected KStream<String, Message> getKafkaStreamFromNewMessages(StreamsBuilder builder) {
        return builder.stream(TOPIC_NEW_MESSAGES.getTopicName(), Consumed.with(Serdes.String(), messageSerde));
    }

    //Список должен храниться на диске.
    protected KTable<String, User> getKafkaTableFromUserInfo(StreamsBuilder builder) {
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

    protected KStream<String, Boolean> getKafkaStreamFromCenzWords(StreamsBuilder builder) {
        return builder.stream(TOPIC_CENZ_WORDS.getTopicName(), Consumed.with(Serdes.String(), Serdes.Boolean()))
                .groupByKey()
                .reduce((toCenzOld, toCenzNew) -> toCenzNew)
                .toStream()
                .filter((word, toCenz) -> toCenz);
    }

    protected KStream<String, Message> getKafkaStreamFromUnBlockingMessages(StreamsBuilder builder) {
        return builder.stream(TOPIC_UNBLOCKED_MESSAGES.getTopicName(), Consumed.with(Serdes.String(), messageSerde));
    }

    protected StreamsConfig getStreamsConfig() {
        return new StreamsConfig(kafkaProperties.buildStreamsProperties(null));
    }

    private List<String> updateBlockedUsers(List<String> usersOld, List<String> usersNew) {
        usersOld.addAll(usersNew);
        return usersOld.stream().distinct().collect(Collectors.toCollection(ArrayList::new));
    }

    public void decreaseMessagesToProcess() {
        messagesToProcess.getAndDecrement();
    }

}
