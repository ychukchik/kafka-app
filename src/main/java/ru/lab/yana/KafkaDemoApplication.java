package ru.lab.yana;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaDemoApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDemoApplication.class);

    public static void main(String[] args) {
        logger.info("Starting Kafka Stream Processing Application...");
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}