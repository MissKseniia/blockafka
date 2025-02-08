package com.kvlasova.adminuser.service;

import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.enums.Users;
import com.kvlasova.model.User;
import com.kvlasova.utils.UsersUtils;
import com.kvlasova.utils.WordsUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

@Service
@Slf4j
@RequiredArgsConstructor
public class AdminUserService {
    private final KafkaProducer<String, Boolean> wordsProducer;
    private final KafkaProducer<String, User> userInfoProducer;

    public void updateCenzWords() {
        var cenzWords = WordsUtils.getRandomWords(null);
        var toLiftCenzWords = WordsUtils.getRandomWords(cenzWords);

        var records1 = createRecords(cenzWords, true);
        var records2 = createRecords(toLiftCenzWords, false);

        if (!records1.isEmpty() || !records2.isEmpty()) {
            log.info("Update cenz words: {} Update uncenz words{}\n",
                    records1.stream().map(ProducerRecord::key).toList(),
                    records2.stream().map(ProducerRecord::key).toList());
            records1.addAll(records2);

            records1.stream().forEachOrdered(record -> {
                try {
                    var result = wordsProducer.send(record);
                    if (nonNull(result) && nonNull(result.get().hasOffset())) {
                        log.info("Новое слово ({}-{}) было успешно отправлено в топик: {}",
                                record.key(), record.value(), result.get().topic());
                    }
                } catch (Exception e) {
                    log.error("При отправке сообщения {} для наложения и снятия цензуры произошла ошибка: {}",
                            record, e.getMessage());
                }
            });
        }

    }

    //Заменить везде на метод по получению имен пользователей в ютилс
    public void fillUserInfo() {
        var users = Users.getUsersNames().stream()
                .map(userName -> new User(userName, UsersUtils.getRandomBlockedUsers(userName)))
                .toList();
        users.forEach(this::sendUserInfo);
        log.info("\n---Лимиты по сообщениям и список заблокированных лиц у пользователей:--- \n{}}\n{}}\n",
                Arrays.stream(Users.values())
                        .map(user -> String.format("%s: лимит %d", user.getName(), user.getLimit())).toList(),
                users);
    }

    private void sendUserInfo(User user) {
        var record = createUserInfoRecord(user);
        try {
            var feedback = userInfoProducer.send(record);
            log.info("В топик {} отправлена информация по пользователю {} -- {}",
                    feedback.get().topic(),
                    record.value().userName(),
                    record.value().toString());
        } catch (Exception e) {
            log.error("Во время отправки в топик {} сообщения {} произошла ошибка {}",
                    KafkaTopics.TOPIC_USER_INFO.getTopicName(), record, e.getMessage());
        }
    }

    public void closeWordsProducer() {wordsProducer.close();}

    public void closeUserInfoProducer() {userInfoProducer.close();}

    private List<ProducerRecord<String, Boolean>> createRecords(List<String> cenzWords, boolean toCenz) {
        return cenzWords.stream()
                .map(w -> new ProducerRecord<>(KafkaTopics.TOPIC_CENZ_WORDS.getTopicName(), w, toCenz))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private ProducerRecord<String, User> createUserInfoRecord(User user) {
        return new ProducerRecord<>(KafkaTopics.TOPIC_USER_INFO.getTopicName(), user.userName(), user);
    }
}
