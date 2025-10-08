package com.action.kafka02springboot.controller;

import com.action.kafka02springboot.producer.KafkaProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaProducer kafkaProducer;

    public KafkaController(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestParam String message) {
        try {
            kafkaProducer.sendMessage(message);
            return ResponseEntity.ok("消息已发送: " + message);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("发送失败: " + e.getMessage());
        }
    }
}