package ru.lab.yana.contorller;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.*;

@RestController
public class RandomMessageController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.name:binance-trades}")
    private String topicName;

    @GetMapping("/get-random-message")
    public String getRandomMessageSimple() {
        try {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "temp-random-consumer-" + System.currentTimeMillis());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(topicName));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

                if (records.isEmpty()) {
                    return "No messages found in topic: " + topicName;
                }

                List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
                records.forEach(recordList::add);

                Random random = new Random();
                ConsumerRecord<String, String> randomRecord = recordList.get(random.nextInt(recordList.size()));

                return String.format("Partition: %d, Offset: %d, Key: %s, Value: %s",
                        randomRecord.partition(), randomRecord.offset(), randomRecord.key(), randomRecord.value());
            }

        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }
}
