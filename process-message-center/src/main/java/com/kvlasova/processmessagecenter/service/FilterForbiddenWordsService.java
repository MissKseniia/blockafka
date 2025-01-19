package com.kvlasova.processmessagecenter.service;

import com.kvlasova.convert.MessageSerde;
import com.kvlasova.model.Message;
import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.utils.WordsUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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
public class FilterForbiddenWordsService {

    private KafkaStreams kafkaStreamsForCenz;
    private final ProcessMessageService processMessageService;
    private Set<String> forbiddenWords;

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
        var inputStream = builder.stream(TOPIC_UNBLOCKED_MESSAGES.getTopicName(), Consumed.with(Serdes.String(), new MessageSerde()));

        //основная логика обработки
        inputStream.mapValues(this::processMessageContent)
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

    private Message processMessageContent(Message message) {
        var messageContent = message.content().split(" ");
        var stream = getStreamForUpdateWords();
        stream.start();
        var content = Arrays.stream(messageContent)
                .map(word -> forbiddenWords.stream().anyMatch(word::equalsIgnoreCase)
                                        ? WordsUtils.CENZ_SYMBOLS : word)
                .collect(Collectors.joining(" "));
        stream.close();

        return new Message(content, message.receiver(), message.sender(), message.isBlocked());
    }

    private KafkaStreams getStreamForUpdateWords() {
        StreamsBuilder builder = new StreamsBuilder();
        var wordsToCenz = getKafkaStreamFromCenzWords(builder);
        var wordsSet = new HashSet<String>();
        wordsToCenz.foreach((k, v) -> wordsSet.add(k));
        forbiddenWords = wordsSet;
        var configs = processMessageService.getStreamsConfig();
        configs.originals().put(StreamsConfig.APPLICATION_ID_CONFIG, "update-words-service");
        return new KafkaStreams(builder.build(), configs);
    }

    protected KStream<String, Boolean> getKafkaStreamFromCenzWords(StreamsBuilder builder) {
        return builder.stream(TOPIC_CENZ_WORDS.getTopicName(), Consumed.with(Serdes.String(), Serdes.Boolean()))
                .groupByKey()
                .reduce((toCenzOld, toCenzNew) -> toCenzNew)
                .toStream()
                .filter((word, toCenz) -> toCenz);
    }
}
