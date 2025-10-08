package com.action.kafka06producerpartitionstrategy.controller;

import com.action.kafka06producerpartitionstrategy.service.PartitionStrategyProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 分区策略控制器
 */
@Slf4j
@RestController
@RequestMapping("/api/partition-strategy")
public class PartitionStrategyController {

    @Autowired
    private PartitionStrategyProducerService partitionStrategyProducerService;

    /**
     * 发送默认分区策略消息
     */
    @PostMapping("/default")
    public Map<String, Object> sendDefaultPartitionMessage(
            @RequestParam String key,
            @RequestParam String message) {
        
        log.info("接收到默认分区策略消息发送请求: key={}, message={}", key, message);
        
        try {
            partitionStrategyProducerService.sendWithDefaultPartition(key, message);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "默认分区策略消息发送成功");
            response.put("key", key);
            response.put("content", message);
            
            return response;
        } catch (Exception e) {
            log.error("发送默认分区策略消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "消息发送失败: " + e.getMessage());
            
            return response;
        }
    }

    /**
     * 发送自定义分区策略消息
     */
    @PostMapping("/custom")
    public Map<String, Object> sendCustomPartitionMessage(
            @RequestParam String key,
            @RequestParam String message) {
        
        log.info("接收到自定义分区策略消息发送请求: key={}, message={}", key, message);
        
        try {
            partitionStrategyProducerService.sendWithCustomPartition(key, message);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "自定义分区策略消息发送成功");
            response.put("key", key);
            response.put("content", message);
            
            return response;
        } catch (Exception e) {
            log.error("发送自定义分区策略消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "消息发送失败: " + e.getMessage());
            
            return response;
        }
    }

    /**
     * 发送基于 Key 分区策略消息
     */
    @PostMapping("/key-based")
    public Map<String, Object> sendKeyBasedPartitionMessage(
            @RequestParam String key,
            @RequestParam String message) {
        
        log.info("接收到基于Key分区策略消息发送请求: key={}, message={}", key, message);
        
        try {
            partitionStrategyProducerService.sendWithKeyBasedPartition(key, message);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "基于Key分区策略消息发送成功");
            response.put("key", key);
            response.put("content", message);
            
            return response;
        } catch (Exception e) {
            log.error("发送基于Key分区策略消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "消息发送失败: " + e.getMessage());
            
            return response;
        }
    }

    /**
     * 发送轮询分区策略消息
     */
    @PostMapping("/round-robin")
    public Map<String, Object> sendRoundRobinPartitionMessage(
            @RequestParam(required = false) String key,
            @RequestParam String message) {
        
        log.info("接收到轮询分区策略消息发送请求: key={}, message={}", key, message);
        
        try {
            partitionStrategyProducerService.sendWithRoundRobinPartition(key, message);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "轮询分区策略消息发送成功");
            response.put("key", key);
            response.put("content", message);
            
            return response;
        } catch (Exception e) {
            log.error("发送轮询分区策略消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "消息发送失败: " + e.getMessage());
            
            return response;
        }
    }

    /**
     * 批量发送消息演示
     */
    @PostMapping("/batch-demo")
    public Map<String, Object> sendBatchMessages() {
        log.info("接收到批量发送消息演示请求");
        
        try {
            partitionStrategyProducerService.sendBatchMessages();
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "批量发送消息演示完成，请查看日志了解分区分配情况");
            
            return response;
        } catch (Exception e) {
            log.error("批量发送消息演示失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "批量发送消息演示失败: " + e.getMessage());
            
            return response;
        }
    }

    /**
     * 获取分区策略说明
     */
    @GetMapping("/info")
    public Map<String, Object> getPartitionStrategyInfo() {
        Map<String, Object> info = new HashMap<>();
        
        info.put("default", "默认分区策略 - 使用 Kafka 默认的轮询分区策略");
        info.put("custom", "自定义分区策略 - 根据 key 前缀进行分区：ORDER->0, USER->1, SYSTEM->2, 其他->hash");
        info.put("key-based", "基于Key分区策略 - 根据 key 的 hash 值进行分区");
        info.put("round-robin", "轮询分区策略 - 按照轮询方式分配消息到不同分区");
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "分区策略说明");
        response.put("strategies", info);
        
        return response;
    }
}
