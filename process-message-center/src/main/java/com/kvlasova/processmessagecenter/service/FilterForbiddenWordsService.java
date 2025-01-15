package com.kvlasova.processmessagecenter.service;

import com.kvlasova.model.Message;
import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.utils.WordsUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class FilterForbiddenWordsService {

    private KafkaStreams kafkaStreams;
    private final ProcessMessageService processMessageService;

    public FilterForbiddenWordsService(ProcessMessageService processMessageService) {
        this.processMessageService = processMessageService;
    }

    public void cenzMessageContent() {
        kafkaStreams = getKafkaStreamsForCenzWords();
        kafkaStreams.start();
    }

    public void closeStreams() {
        kafkaStreams.close();
    }

    private KafkaStreams getKafkaStreamsForCenzWords() {
        // Создание топологии
        StreamsBuilder builder = new StreamsBuilder();
        // Создаём KStream из топика с входными данными
        var inputStream = processMessageService.getKafkaStreamFromUnBlockingMessages(builder);

        //основная логика обработки
        inputStream.mapValues(v -> processMessageContent(v, builder))
                .peek((k, v) -> {
                    if (k != null) {
                        processMessageService.decreaseMessagesToProcess();
                    }
                })
                .to(KafkaTopics.TOPIC_PROCESSED_MESSAGES.getTopicName());

        var configs = processMessageService.getStreamsConfig();
        configs.originals().put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-words-service");

        return new KafkaStreams(builder.build(), configs);
    }

    private Message processMessageContent(Message message, StreamsBuilder builder) {
        var messageContent = message.content().split(" ");
        var cenzWords = getCenzWords(builder);
        var content = Arrays.stream(messageContent)
                .map(word -> cenzWords.stream().anyMatch(word::equalsIgnoreCase)
                                        ? WordsUtils.CENZ_SYMBOLS : word)
                .collect(Collectors.joining(" "));

        return new Message(content, message.receiver(), message.sender(), message.isBlocked());
    }

    private Set<String> getCenzWords(StreamsBuilder builder) {
        var wordsToCenz = processMessageService.getKafkaStreamFromCenzWords(builder);
        var wordsSet = new HashSet<String>();
        wordsToCenz.foreach((k, v) -> wordsSet.add(k));
        return wordsSet;
    }
}
