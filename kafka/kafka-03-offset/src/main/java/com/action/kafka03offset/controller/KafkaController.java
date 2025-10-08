package com.action.kafka03offset.controller;

import com.action.kafka03offset.producer.KafkaProducer;
import com.action.kafka03offset.service.OffsetResetService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka演示控制器
 * 提供REST接口用于测试不同偏移量策略的消费行为
 * 所有接口都使用GET方法，简化测试
 */
@RestController
@RequestMapping("/api/kafka/demo")
@Slf4j
public class KafkaController {

    private final KafkaProducer kafkaProducer;
    private final OffsetResetService offsetResetService;

    public KafkaController(KafkaProducer kafkaProducer, OffsetResetService offsetResetService) {
        this.kafkaProducer = kafkaProducer;
        this.offsetResetService = offsetResetService;
    }

    /**
     * 发送单条消息接口
     * 用于测试不同消费者组的消费行为
     *
     * @param message 要发送的消息内容
     * @return 发送结果
     */
    @GetMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestParam String message) {
        kafkaProducer.sendDemoMessage(message);
        return ResponseEntity.ok("消息已发送: " + message);
    }

    /**
     * 批量发送消息接口
     * 用于生成测试数据，演示不同偏移量策略
     *
     * @param count 要发送的消息数量，默认10条
     * @return 发送结果
     */
    @GetMapping("/send-batch")
    public ResponseEntity<String> sendBatchMessages(@RequestParam(defaultValue = "10") int count) {
        kafkaProducer.sendBatchMessages(count);
        return ResponseEntity.ok("已发送 " + count + " 条测试消息");
    }

    /**
     * 重置消费者组偏移量接口
     * 通过Admin API重置指定消费者组的偏移量
     *
     * @param groupId 消费者组ID
     * @param topic   Topic名称
     * @param resetTo 重置目标位置（earliest/latest）
     * @return 重置结果
     */
    @GetMapping("/reset-offset")
    public ResponseEntity<String> resetOffset(@RequestParam String groupId,
                                              @RequestParam String topic,
                                              @RequestParam(defaultValue = "earliest") String resetTo) {
        try {
            offsetResetService.resetConsumerOffset(groupId, topic, resetTo);
            return ResponseEntity.ok("消费者组 " + groupId + " 偏移量已重置到 " + resetTo);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("重置失败: " + e.getMessage());
        }
    }

}


