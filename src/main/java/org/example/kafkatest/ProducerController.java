package org.example.kafkatest;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RequestMapping("/producer")
@RestController
public class ProducerController {

    private final Producer producer;
    private int count = 0;

    @PostMapping
    public void produce() {
        for (int i = 0; i < 10; i++)
            producer.send("my-topic", "message-" + count++);
    }
}
