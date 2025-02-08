package com.kvlasova.processmessagecenter.service;

import com.kvlasova.convert.MessageSerde;
import com.kvlasova.enums.KafkaTopics;
import com.kvlasova.model.Message;
import com.kvlasova.utils.WordsUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.kvlasova.enums.KafkaTopics.TOPIC_CENZ_WORDS;
import static com.kvlasova.enums.KafkaTopics.TOPIC_UNBLOCKED_MESSAGES;
import static java.util.Objects.nonNull;

@Service
@Slf4j
@RequiredArgsConstructor
public class FilterForbiddenWordsService {

    private KafkaStreams kafkaStreamsForCenz;
    private final ProcessMessageService processMessageService;

    public void cenzMessageContent() {
        kafkaStreamsForCenz = getKafkaStreamsForCenzWords();
        kafkaStreamsForCenz.start();
        log.info("KafkaStreamsForCenzWords: started");
    }

    public void closeStreamsForCenz() {
        try {
            if (nonNull(kafkaStreamsForCenz)) {
                kafkaStreamsForCenz.close(Duration.ofSeconds(10));
                kafkaStreamsForCenz.cleanUp();
            }
        } catch (Exception e) {
            log.error("Ошибка при закрытии KafkaStreamsForCenzWords", e);
        }
    }

    private KafkaStreams getKafkaStreamsForCenzWords() {
        StreamsBuilder builder = new StreamsBuilder();
        var messagesStream = builder.stream(TOPIC_UNBLOCKED_MESSAGES.getTopicName(), Consumed.with(Serdes.String(),
                new MessageSerde()));
        var forbiddenWordsStream = getKafkaStreamFromCenzWords(builder);
        var wordsSet = new HashSet<String>();
        forbiddenWordsStream.foreach((k, v) -> wordsSet.add(k));

        messagesStream.mapValues(m -> processMessageContent(m, wordsSet))
                .peek((k, v) -> processMessageService.decreaseMessagesToProcess(k))
                .to(KafkaTopics.TOPIC_PROCESSED_MESSAGES.getTopicName());

        var configs = processMessageService.getStreamsConfig("filter-words-streams");

        return new KafkaStreams(builder.build(), configs);
    }

    private Message processMessageContent(Message message, Set<String> forbiddenWords) {
        var messageContent = message.getContent().split(" ");
        log.info("Запрещенные слова: {}", forbiddenWords);
        var content = Arrays.stream(messageContent)
                .map(word -> forbiddenWords.stream().anyMatch(word::equalsIgnoreCase)
                        ? WordsUtils.CENZ_SYMBOLS : word)
                .collect(Collectors.joining(" "));
        log.info("Сообщение после обработки: {}", content);

        return new Message(content, message.getReceiver(), message.getSender(), message.isBlocked());
    }

    protected KStream<String, Boolean> getKafkaStreamFromCenzWords(StreamsBuilder builder) {
        return builder.stream(TOPIC_CENZ_WORDS.getTopicName(), Consumed.with(Serdes.String(), Serdes.Boolean()))
                .groupByKey()
                .reduce((toCenzOld, toCenzNew) -> toCenzNew)
                .toStream()
                .filter((word, toCenz) -> toCenz);
    }
}