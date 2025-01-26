package com.kvlasova.utils;

import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

@UtilityClass
public class WordsUtils {
    private static final String content = "Apache Kafka — это распределённая платформа потоковой передачи данных, разработанная для обработки больших объёмов информации в реальном времени.Kafka используется для создания надежных и масштабируемых систем обмена сообщениями между различными приложениями. Архитектура Kafka основана на концепции публикации и подписки, где производители (producers) публикуют сообщения в тему (topic), а потребители (consumers) подписываются на эти темы.Одной из ключевых особенностей Kafka является возможность хранения сообщений в виде журнала, что позволяет восстанавливать прерванные сессии.Kafka поддерживает горизонтальное масштабирование, что делает его идеальным выбором для обработки потоковых данных в системах с высокой нагрузкой.В Kafka сообщения могут быть организованы в несколько партиций, что повышает производительность обработки данных.Для того чтобы добиться высокой доступности и отказоустойчивости, Kafka использует механизм репликации, где данные копируются на несколько брокеров.Kafka позволяет обрабатывать данные в режиме реального времени, что делает его популярным выбором для обработки событий в таких областях, как финансы и аналитика.Поддержка различных языков программирования, таких как Java, Python и Go, делает Kafka универсальным инструментом для разработчиков.Kafka широко используется в больших данных для интеграции данных из различных источников и их агрегации.";
    public static final String CENZ_SYMBOLS = "***";
    private static final String[] sentences = content.split("\\.");
    private static final String[] words = Arrays.stream(content.split("\\W+")).distinct().toArray(String[]::new);
    private static final Random random = new Random();

    public static List<String> getRandomWords(List<String> cenzuredWords) {
        var newWords = shuffleWords(cenzuredWords);
        return newWords.subList(0, random.nextInt(0, newWords.size()));
    }

    public static String getRandomMessage() {
        return sentences[random.nextInt(0, sentences.length)];
    }

    private static List<String> shuffleWords(List<String> cenzuredWords) {
        var list = Arrays.stream(WordsUtils.words)
                .filter(w -> checkWord(w, cenzuredWords))
                .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(list);
        return list;
    }

    private static boolean checkWord(String word, List<String> cenzuredWords) {
        return isNull(cenzuredWords) || cenzuredWords.isEmpty() || cenzuredWords.stream().noneMatch(word::equalsIgnoreCase);
    }
}
