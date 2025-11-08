package ru.lab.yana.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topicName;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String key, String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, key, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Sent message: [{}] with key: [{}] to partition: [{}] offset: [{}]",
                        message, key, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                logger.error("Unable to send message: [{}] with key: [{}] due to: {}",
                        message, key, ex.getMessage());
            }
        });
    }

    public void sendBatchMessages(String keyPrefix, String messagePrefix, int count) {
        logger.info("Starting to send batch of {} messages to topic: {}", count, topicName);

        int successCount = 0;
        int errorCount = 0;

        for (int i = 1; i <= count; i++) {
            String key = keyPrefix != null ? keyPrefix + "-" + i : String.valueOf(i);
            String message = messagePrefix + " #" + i;

            try {
                sendMessage(key, message);
                successCount++;

                // Небольшая задержка для наглядности
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.warn("Message sending interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error sending message {}/{}: {}", i, count, e.getMessage());
                errorCount++;
            }
        }

        logger.info("Completed sending batch: {} successful, {} failed, total: {}",
                successCount, errorCount, count);
    }
}
