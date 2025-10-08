package com.action.kafka04offset.controller;

import com.action.kafka04offset.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 偏移量测试控制器
 * 提供各种偏移量策略的测试接口
 * 
 * @author action
 * @since 2024
 */
@Slf4j
@RestController
@RequestMapping("/api/kafka/offset")
@RequiredArgsConstructor
public class KafkaOffsetController {

    private final KafkaProducerService kafkaProducerService;

    /**
     * 发送测试消息到 earliest 主题
     * 
     * @param count 消息数量，默认 5 条
     * @return 响应结果
     */
    @GetMapping("/earliest/send")
    public ResponseEntity<Map<String, Object>> sendEarliestMessages(
            @RequestParam(defaultValue = "5") int count) {
        
        log.info("收到请求：发送 {} 条消息到 earliest 主题", count);
        
        try {
            kafkaProducerService.sendEarliestTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功发送 " + count + " 条消息到 earliest 主题");
            response.put("topic", "kafka-04-offset-earliest");
            response.put("count", count);
            response.put("offsetStrategy", "earliest");
            response.put("description", "earliest 策略：自动将偏移量重置为最早的偏移量，会消费所有历史消息");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("发送 earliest 消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送测试消息到 latest 主题
     * 
     * @param count 消息数量，默认 5 条
     * @return 响应结果
     */
    @GetMapping("/latest/send")
    public ResponseEntity<Map<String, Object>> sendLatestMessages(
            @RequestParam(defaultValue = "5") int count) {
        
        log.info("收到请求：发送 {} 条消息到 latest 主题", count);
        
        try {
            kafkaProducerService.sendLatestTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功发送 " + count + " 条消息到 latest 主题");
            response.put("topic", "kafka-04-offset-latest");
            response.put("count", count);
            response.put("offsetStrategy", "latest");
            response.put("description", "latest 策略：自动将偏移量重置为最新偏移量，只消费新产生的消息");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("发送 latest 消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送测试消息到 none 主题
     * 
     * @param count 消息数量，默认 5 条
     * @return 响应结果
     */
    @GetMapping("/none/send")
    public ResponseEntity<Map<String, Object>> sendNoneMessages(
            @RequestParam(defaultValue = "5") int count) {
        
        log.info("收到请求：发送 {} 条消息到 none 主题", count);
        
        try {
            kafkaProducerService.sendNoneTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功发送 " + count + " 条消息到 none 主题");
            response.put("topic", "kafka-04-offset-none");
            response.put("count", count);
            response.put("offsetStrategy", "none");
            response.put("description", "none 策略：如果没有为消费者组找到以前的偏移量，则向消费者抛出异常");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("发送 none 消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送测试消息到通用测试主题
     * 
     * @param count 消息数量，默认 5 条
     * @return 响应结果
     */
    @GetMapping("/test/send")
    public ResponseEntity<Map<String, Object>> sendTestMessages(
            @RequestParam(defaultValue = "5") int count) {
        
        log.info("收到请求：发送 {} 条消息到测试主题", count);
        
        try {
            kafkaProducerService.sendTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功发送 " + count + " 条消息到测试主题");
            response.put("topic", "kafka-04-offset-test");
            response.put("count", count);
            response.put("description", "通用测试主题：所有消费者都会监听此主题，用于对比不同偏移量策略");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("发送测试消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送消息到所有主题
     * 
     * @param count 每个主题的消息数量，默认 3 条
     * @return 响应结果
     */
    @GetMapping("/batch/send")
    public ResponseEntity<Map<String, Object>> sendBatchMessages(
            @RequestParam(defaultValue = "3") int count) {
        
        log.info("收到请求：批量发送消息，每个主题 {} 条", count);
        
        try {
            // 发送到所有主题
            kafkaProducerService.sendEarliestTestMessages(count);
            kafkaProducerService.sendLatestTestMessages(count);
            kafkaProducerService.sendNoneTestMessages(count);
            kafkaProducerService.sendTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功批量发送消息到所有主题");
            response.put("totalCount", count * 4);
            response.put("topics", new String[]{
                "kafka-04-offset-earliest",
                "kafka-04-offset-latest", 
                "kafka-04-offset-none",
                "kafka-04-offset-test"
            });
            response.put("countPerTopic", count);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("批量发送消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "批量发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

}
