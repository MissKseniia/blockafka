package com.kvlasova.adminuser.service;

import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.utils.WordsUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

@Service
@Slf4j
@RequiredArgsConstructor
public class AdminUserService {
    private final KafkaProducer<String, Boolean> kafkaProducer;

    public void updateCenzWords() {
        var cenzWords = WordsUtils.getRandomWords(null);
        var toLiftCenzWords = WordsUtils.getRandomWords(cenzWords);

        var records1 = createRecords(cenzWords, true);
        var records2 = createRecords(toLiftCenzWords, false);
        if (!records1.isEmpty() || !records2.isEmpty()) {
            log.info("Update cenz words: {}\nUpdate uncenz words{}\n",
                    records1.stream().map(ProducerRecord::key).toList(),
                    records2.stream().map(ProducerRecord::key).toList());
            records1.addAll(records2);

            try {
                records1.stream().map(kafkaProducer::send)
                        .forEachOrdered(result -> {
                            try {
                                if (nonNull(result) && nonNull(result.get().hasOffset())) {
                                    log.info("Новые слова были успешно отправлены в топик: {}",
                                            records1);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            } catch (Exception e) {
                log.error("При отправке новых слов для наложения и снятия цензуры произошла ошибка: {}",
                        e.getMessage());
            }

        }

    }

    public void closeProducer() {
        kafkaProducer.close();
    }

    private List<ProducerRecord<String, Boolean>> createRecords(List<String> cenzWords, boolean toCenz) {
        return cenzWords.stream()
                .map(w -> new ProducerRecord<>(KafkaTopics.TOPIC_CENZ_WORDS.getTopicName(), w, toCenz))
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
