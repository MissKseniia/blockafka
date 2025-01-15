package com.kvlasova.user.service;

import com.kvlasova.enums.Users;
import com.kvlasova.model.Message;
import com.kvlasova.model.User;
import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.utils.UsersUtils;
import com.kvlasova.utils.WordsUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserService {
    private final KafkaConsumer<String, Message> userReceiver;
    private final KafkaProducer<String, Message> userSender;
    private final KafkaProducer<String, User> userInfoSender;
    private ConcurrentMap<String, Integer> usersLimits = new ConcurrentHashMap<>();
    private final Random random = new Random();
    private AtomicInteger messagesToRead = new AtomicInteger(0);

    {
        Arrays.stream(Users.values()).forEach(user -> usersLimits.put(user.getName(), user.getLimit()));
        messagesToRead.set(usersLimits.values().stream().reduce(0, Integer::sum));
    }

    public void exchangeMessages() {
        //пока остались сообщения для отправки или получения
        while (usersLimits.values().stream().anyMatch(limit -> limit > 0) || messagesToRead.get() > 0) {

            sendMessage();

            ConsumerRecords<String, Message> records = null;

            try {
                records = userReceiver.poll(Duration.ofMillis(20));
            } catch (Exception e) {
                log.error("При получении сообщения произошла ошибка: {}", e.getMessage());
            }

            if (isNull(records) || records.isEmpty()) {
                continue;
            }

            readMessage(records);

        }

        //Закрываем ресурсы
        userSender.close();
        userReceiver.close();

    }

    //Заменить везде на метод по получению имен пользователей в ютилс
    public void fillUserInfo() {
        var users = Users.getUsersNames().stream()
                .map(userName -> new User(userName, UsersUtils.getRandomBlockedUsers(userName)))
                .toList();
        users.forEach(this::sendUserInfo);
        log.info("---Лимиты по сообщениям и список заблокированных лиц у пользователей:--- \n{}\n\n{}\n"
                , usersLimits, users);
    }

    private void sendUserInfo(User user) {
        var record = createUserInfoRecord(user);
        try {
            var feedback = userInfoSender.send(record);
            logSending(feedback,
                    record.value().userName(),
                    null,
                    record.value().toString());
        } catch (Exception e) {
            logError(record, e);
        }
    }

    private void sendMessage() {
        var users = usersLimits.keySet().toArray(String[]::new);
        var user = getUser(users);
        log.info("User - {}", user);

        if (nonNull(user)) {
            var userToSend = UsersUtils.getRandomUserToSend(user);
            var message = createMessageRecord(user, userToSend);
            log.info("Message - {}", message);
            try {
                var feedback = userSender.send(message);
                logSending(feedback,
                        message.value().sender(),
                        message.value().receiver(),
                        message.value().content());
            } catch (Exception e) {
                logError(message, e);
            }

        }
    }

    private void logSending(Future<RecordMetadata> feedback, String sender, String receiver, String content) throws ExecutionException, InterruptedException {
        if (Objects.nonNull(feedback) && feedback.get().hasOffset()) {
            log.debug("Время: {}\nПользователь {} отправил {} сообщение: \n{}\n",
                    Instant.now().toString(),
                    sender,
                    receiver,
                    content
            );
        }
    }

    private void logError(ProducerRecord<String, ?> record, Exception e) {
        log.error("При отправке сообщения {} произошла ошибка: {}", record, e.getMessage());
    }

    private void readMessage(ConsumerRecords<String, Message> records) {
        for (ConsumerRecord<String, Message> record : records) {
            log.debug("Время: {}\nПользователем {} от {} получено сообщение: \n{}\n",
                    Instant.now().toString(),
                    record.value().receiver(),
                    record.value().sender(),
                    record.value().content()
            );
            messagesToRead.getAndAdd(-records.count());
            if (analyzeMessageContent(record.value().content()) > 2L)
                updateBlockedUsers(record.value().sender(), record.value().receiver());

        }
    }

    private void updateBlockedUsers(String sender, String receiver) {
        var user = new User(receiver, List.of(sender));
        sendUserInfo(user);
        log.info("У пользователя {} был обновлен список заблокированных лиц - добавлен пользователь {}.",
                receiver, sender);
    }

    private long analyzeMessageContent(String content) {
        return Arrays.stream(content.split(" ")).filter(WordsUtils.CENZ_SYMBOLS::equals).count();
    }

    private int decreaseUserLimit(String user) {
        if (!usersLimits.containsKey(user) || usersLimits.get(user) == 0)
            return 0;
        return usersLimits.compute(user, (k, v) -> v - 1);
    }

    private String getUser(String[] users) {
        var array = Arrays.stream(users)
                    .filter(user -> decreaseUserLimit(user) != 0)
                    .toArray(String[]::new);
        return array.length > 0 ? array[random.nextInt(0, array.length)] : null;
    }

    private ProducerRecord<String, Message> createMessageRecord(String user, String userToSend) {
        var message = new Message(
                WordsUtils.getRandomMessage(),
                userToSend,
                user,
                false
        );
        return new ProducerRecord<>(KafkaTopics.TOPIC_NEW_MESSAGES.getTopicName(), userToSend, message);
    }

    private ProducerRecord<String, User> createUserInfoRecord(User user) {
        return new ProducerRecord<>(KafkaTopics.TOPIC_USER_INFO.getTopicName(), user.userName(), user);
    }

}
