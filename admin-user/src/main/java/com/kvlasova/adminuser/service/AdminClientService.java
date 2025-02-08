package com.kvlasova.adminuser.service;

import com.kvlasova.enums.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

@Service
@Slf4j
@RequiredArgsConstructor
public class AdminClientService {
    private final KafkaProperties kafkaProperties;

    public boolean createAllTopics() {
        var topics = KafkaTopics.getTopicsNames();
        var admin = getAdmin();
        AtomicBoolean topicsCreated = new AtomicBoolean(true);

        if (nonNull(admin)) {
            try (admin) {
                int partitions = 3;
                short replicationFactor = 2;
                var newTopics = topics.stream()
                        .map(topicName -> new NewTopic(topicName, partitions, replicationFactor))
                        .toList();

                var existingTopics = admin.listTopics().names().get();
                if (nonNull(existingTopics) && !existingTopics.isEmpty()) {
                    var customTopics = existingTopics.stream().filter(topics::contains).collect(Collectors.toSet());
                    admin.deleteTopics(customTopics).all().get();
                    log.info("Были удалены топики: {}", customTopics);
                }

                CreateTopicsResult result = admin.createTopics(newTopics);
                topics.forEach(topicName -> {
                    try {
                        result.values().get(topicName).get();
                        log.info("Был создан топик: {}", topicName);
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("При создании топика {} произошла ошибка: {}",
                                topicName,
                                e.getMessage());
                        if (topicsCreated.get())
                            topicsCreated.set(false);
                    }
                });
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return topicsCreated.get();

    }

    private Admin getAdmin() {
        try {
            var adminProps = kafkaProperties.buildAdminProperties(null);
            return Admin.create(adminProps);
        } catch (Exception e) {
            log.error("Ошибка при создании Admin: {}", e.getMessage());
            return null;
        }
    }
}
