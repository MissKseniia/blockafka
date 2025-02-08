package com.kvlasova.processmessagecenter.service;

import com.kvlasova.convert.MessageSerde;
import com.kvlasova.convert.UserSerde;
import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.model.Message;
import com.kvlasova.model.User;
import lombok.RequiredArgsConstructor;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.kvlasova.enums.KafkaTopics.TOPIC_NEW_MESSAGES;
import static com.kvlasova.enums.KafkaTopics.TOPIC_USER_INFO;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Service
@Slf4j
@RequiredArgsConstructor
public class BlockMessageService {

    private KafkaStreams kafkaStreams;
    private KTable<String, User> userTable;
    private final ProcessMessageService processMessageService;

    public void blockUsers() {
        kafkaStreams = getKafkaStreamsForBlockingUsers();
        kafkaStreams.start();
        log.info("KafkaStreamsForBlockingUsers: started");
    }

    public void closeStreams() {
        try {
            if (nonNull(kafkaStreams)) {
                kafkaStreams.close(Duration.ofSeconds(10)); // Указываем время ожидания для Graceful shutdown
                kafkaStreams.cleanUp(); // Очищает временные данные state store, если требуется
            }
        } catch (Exception e) {
            log.error("Ошибка при закрытии KafkaStreamsForBlockingUsers", e);
        }
    }

    private KafkaStreams getKafkaStreamsForBlockingUsers() {
        var builder = new StreamsBuilder();
        userTable = getKafkaTableFromUserInfo(builder);
        // Создаём KStream из топика с входными данными
        var messagesStream = builder.stream(TOPIC_NEW_MESSAGES.getTopicName(), Consumed.with(Serdes.String(),
                new MessageSerde()));
        //основная логика обработки
        processMessage(messagesStream);
        var configs = processMessageService.getStreamsConfig("block-message-streams");
        return new KafkaStreams(builder.build(), configs);
    }

    private void processMessage(KStream<String, Message> inputStream) {
        KStream<String, Message> joinedStream = inputStream
                .peek((k, v) -> log.info("Получено сообщение: {}", v))
                .leftJoin(userTable, this::defineStatus);
        var blockedStream = joinedStream.filter((k, v) -> v.isBlocked());
        var unBlockedStream = joinedStream.filter((k, v) -> !v.isBlocked());

        blockedStream.peek((k, v) -> {
            log.info("Сообщение заблокировано: {}", v);
            processMessageService.decreaseMessagesToProcess(k);})
        .to(KafkaTopics.TOPIC_BLOCKED_MESSAGES.getTopicName());
        unBlockedStream.to(KafkaTopics.TOPIC_UNBLOCKED_MESSAGES.getTopicName());

    }

    //Список должен храниться на диске.
    private KTable<String, User> getKafkaTableFromUserInfo(StreamsBuilder builder) {
        var userSerdes = new UserSerde();

        return builder.stream(TOPIC_USER_INFO.getTopicName(), Consumed.with(Serdes.String(), userSerdes))
                .groupByKey()
                .reduce((userOld, userNew) ->
                        isNull(userOld) ? userNew : new User(userOld.userName(),
                                updateBlockedUsers(userOld.blockedUsers(), userNew.blockedUsers())))
                .toStream()
                .peek((k, v) -> log.info("Обновлены данные пользователя: {} {}", k, v))
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
        var blockedUsers = isNull(tableValue) ? new ArrayList<String>() :
                Optional.ofNullable(tableValue.blockedUsers()).orElse(List.of());
        var toBlock = blockedUsers.stream()
                .anyMatch(streamValue.getSender()::equalsIgnoreCase);
        return new Message(streamValue.getContent(), streamValue.getReceiver(), streamValue.getSender(), toBlock);
    }
}
