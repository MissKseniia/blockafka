package com.kvlasova.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
public enum KafkaTopics {
    TOPIC_CENZ_WORDS("cenz_words"),
    TOPIC_NEW_MESSAGES("new_messages"),
    TOPIC_USER_INFO("user_info"),
    TOPIC_UNBLOCKED_MESSAGES("unblocked_messages"),
    TOPIC_BLOCKED_MESSAGES("blocked_messages"),
    TOPIC_PROCESSED_MESSAGES("processed_messages");

    private final String topicName;

    public static List<String> getTopicsNames() {
        return Arrays.stream(KafkaTopics.values()).map(KafkaTopics::getTopicName).collect(Collectors.toList());
    }
}
