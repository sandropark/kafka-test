package org.example.kafkatest;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Consumer {

    @KafkaListener(
            topics = "my-topic", groupId = "my-group-id",
            containerFactory = "myKafkaListenerContainerFactory",
            errorHandler = "batchKafkaErrorHandler"
    )
    public void consume(ConsumerRecords<String, String> records, Acknowledgment ack) {
        log.info("records count: {}", records.count());

        records.forEach(record -> {
            String value = record.value();

            if (value.equals("message-5")) throw new RuntimeException("error");

            log.info("offset: {}, value: {}", record.offset(), value);
//            ack.acknowledge();
        });

    }
}
