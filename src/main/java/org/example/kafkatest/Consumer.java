package org.example.kafkatest;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class Consumer {

    @KafkaListener(
            topics = "my-topic", groupId = "my-group-id",
            containerFactory = "myKafkaListenerContainerFactory",
            errorHandler = "batchKafkaErrorHandler"
    )
    public void consume(ConsumerRecords<String, String> records, org.apache.kafka.clients.consumer.Consumer<String, String> consumer) {
        log.info("records count: {}", records.count());

        Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

        records.forEach(record -> {
            String value = record.value();

//            if (value.equals("message-3")) throw new RuntimeException("error");

            log.info("offset: {}, value: {}", record.offset(), value);

            currentOffset.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1)
            );
            consumer.commitSync(currentOffset);
        });

    }
}
