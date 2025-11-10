package ru.lab.yana.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {
    
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topicName;

    public void sendMessage(String key, String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, key, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message: [{}] with key: [{}] to partition: [{}] offset: [{}]",
                        message, key, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send message: [{}] with key: [{}] due to: {}",
                        message, key, ex.getMessage());
            }
        });
    }
}
