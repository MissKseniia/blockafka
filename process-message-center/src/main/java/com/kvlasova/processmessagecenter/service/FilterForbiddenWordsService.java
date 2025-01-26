package com.kvlasova.processmessagecenter.service;

import com.kvlasova.convert.MessageDeserializer;
import com.kvlasova.convert.MessageSerde;
import com.kvlasova.convert.MessageSerializer;
import com.kvlasova.model.Message;
import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.utils.WordsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.kvlasova.enums.KafkaTopics.TOPIC_CENZ_WORDS;
import static com.kvlasova.enums.KafkaTopics.TOPIC_UNBLOCKED_MESSAGES;

@Service
@Slf4j
public class FilterForbiddenWordsService {

    private KafkaStreams kafkaStreamsForCenz;
    private final ProcessMessageService processMessageService;

    public FilterForbiddenWordsService(ProcessMessageService processMessageService) {
        this.processMessageService = processMessageService;
    }

    public void cenzMessageContent() {
        kafkaStreamsForCenz = getKafkaStreamsForCenzWords();
        kafkaStreamsForCenz.start();
    }

    public void closeStreamsForCenz() {
        kafkaStreamsForCenz.close();
    }

    private KafkaStreams getKafkaStreamsForCenzWords() {
        // Создание топологии
        StreamsBuilder builder = new StreamsBuilder();
        // Создаём KStream из топика с входными данными
        var messagesStream = builder.stream(TOPIC_UNBLOCKED_MESSAGES.getTopicName(), Consumed.with(Serdes.String(), new MessageSerde()));
        var forbiddenWordsStream = getKafkaStreamFromCenzWords(builder);
        var wordsSet = new HashSet<String>();
        forbiddenWordsStream.foreach((k, v) -> wordsSet.add(k));
        //основная логика обработки
        messagesStream.mapValues(m -> processMessageContent(m, wordsSet))
                .peek((k, v) -> {
                    if (k != null) {
                        processMessageService.decreaseMessagesToProcess();
                    }
                })
                .to(KafkaTopics.TOPIC_PROCESSED_MESSAGES.getTopicName());

        var configs = processMessageService.getStreamsConfig("filter-words-streams");

        return new KafkaStreams(builder.build(), configs);
    }

    private Message processMessageContent(Message message, Set<String> forbiddenWords) {
        var messageContent = message.getContent().split(" ");
        log.info("Forbidden words: {}", forbiddenWords);
        var content = Arrays.stream(messageContent)
                .map(word -> forbiddenWords.stream().anyMatch(word::equalsIgnoreCase)
                                        ? WordsUtils.CENZ_SYMBOLS : word)
                .collect(Collectors.joining(" "));
        log.info("Transformed message: {}", content);

        return new Message(content, message.getReceiver(), message.getSender(), message.isBlocked());
    }

    protected KStream<String, Boolean> getKafkaStreamFromCenzWords(StreamsBuilder builder) {
        return builder.stream(TOPIC_CENZ_WORDS.getTopicName(), Consumed.with(Serdes.String(), Serdes.Boolean()))
                .peek((k,v) -> log.info("Cenz word Before Filter: {}", k))
                .groupByKey()
                .reduce((toCenzOld, toCenzNew) -> toCenzNew)
                .toStream()
                .filter((word, toCenz) -> toCenz)
                .peek((k,v) -> log.info("Cenz word After Filter: {}", k));
    }
}

//Надо проверить, попадают ли сообщения в топики, из которых потом происходит фильтрация контента.
//Логов по блокировке больше не было, узнать почему???
//Прописать путь записи в таблицу)