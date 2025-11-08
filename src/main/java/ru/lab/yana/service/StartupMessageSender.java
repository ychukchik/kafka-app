package ru.lab.yana.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class StartupMessageSender implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(StartupMessageSender.class);

    private final KafkaProducerService kafkaProducerService;

    @Value("${app.kafka.batch.count:10}")
    private int batchCount;

    @Value("${app.kafka.batch.message-prefix:Default Message}")
    private String messagePrefix;

    @Value("${app.kafka.batch.key-prefix:batch}")
    private String keyPrefix;

    public StartupMessageSender(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Application started - sending batch messages to Kafka...");
        logger.info("Configuration: {} messages with prefix: '{}', key prefix: '{}'",
                batchCount, messagePrefix, keyPrefix);

        long startTime = System.currentTimeMillis();

        kafkaProducerService.sendBatchMessages(keyPrefix, messagePrefix, batchCount);

        long duration = System.currentTimeMillis() - startTime;
        logger.info("Batch message sending initiated in {} ms. Application is ready.", duration);
    }
}
