package com.action.kafka12consumerinterceptor.controller;

import com.action.kafka12consumerinterceptor.service.ProducerDemoService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 消费者拦截器演示控制器
 * 
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试消息发送
 * 2) 演示消费者拦截器在消息消费过程中的作用
 * 3) 支持指定分区发送和自动分区发送
 * 4) 提供 RESTful API 接口
 * 
 * 接口说明：
 * - GET /api/send/partition: 发送消息到指定分区
 * - GET /api/send/auto: 发送消息到所有分区（自动分区）
 * - GET /api/send/important: 发送重要消息（会被拦截器保留）
 * - GET /api/send/normal: 发送普通消息（会被拦截器过滤）
 * - GET /api/send/batch/partition: 批量发送消息到指定分区
 * - GET /api/send/batch/auto: 批量发送消息到所有分区（自动分区）
 * 
 * 使用示例：
 * curl "http://localhost:8080/api/send/partition?partition=0&key=test-key&value=test-message"
 * curl "http://localhost:8080/api/send/auto?key=auto-key&value=auto-message"
 * curl "http://localhost:8080/api/send/important?key=important-key&message=urgent task"
 * curl "http://localhost:8080/api/send/normal?key=normal-key&message=regular task"
 */
@RestController
@RequestMapping("/api/send")
public class ConsumerDemoController {

    /**
     * 生产者演示服务
     * 负责实际的消息发送逻辑
     */
    private final ProducerDemoService producerService;

    /**
     * 构造函数注入服务依赖
     * 
     * @param producerService 生产者演示服务
     */
    public ConsumerDemoController(ProducerDemoService producerService) {
        this.producerService = producerService;
    }

    /**
     * 发送消息到指定分区接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/partition
     * 
     * 参数说明：
     * - partition: 目标分区号（必填，0-2）
     * - key: 消息键（可选）
     * - value: 消息值（必填）
     * 
     * 响应说明：
     * - 成功：返回成功信息和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param partition 目标分区号
     * @param key 消息键（可选）
     * @param value 消息值
     * @return HTTP 响应实体
     */
    @GetMapping("/partition")
    public ResponseEntity<Map<String, Object>> sendMessageToPartition(
            @RequestParam("partition") int partition,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("value") String value) {
        try {
            // 验证分区号
            if (partition < 0 || partition > 2) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "分区号必须在 0-2 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层发送消息到指定分区
            if (key != null && !key.isEmpty()) {
                producerService.sendMessageToPartition(partition, key, value);
            } else {
                producerService.sendMessageToPartition(partition, value);
            }
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "消息发送到指定分区成功");
            response.put("data", Map.of("partition", partition, "key", key, "value", value));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "消息发送到指定分区失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送消息到所有分区接口（自动分区）
     * 
     * 请求方式：GET
     * 请求路径：/api/send/auto
     * 
     * 参数说明：
     * - key: 消息键（可选）
     * - value: 消息值（必填）
     * 
     * 响应说明：
     * - 成功：返回成功信息和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param key 消息键（可选）
     * @param value 消息值
     * @return HTTP 响应实体
     */
    @GetMapping("/auto")
    public ResponseEntity<Map<String, Object>> sendMessageAuto(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("value") String value) {
        try {
            // 调用服务层发送消息（自动分区）
            if (key != null && !key.isEmpty()) {
                producerService.sendMessage(key, value);
            } else {
                producerService.sendMessage(value);
            }
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "消息发送成功（自动分区）");
            response.put("data", Map.of("key", key, "value", value));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送重要消息接口（用于测试拦截器过滤功能）
     * 
     * 请求方式：GET
     * 请求路径：/api/send/important
     * 
     * 参数说明：
     * - key: 消息键（可选）
     * - message: 消息内容（必填）
     * 
     * 响应说明：
     * - 成功：返回成功信息和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * 注意：重要消息包含 "important" 关键词，会被拦截器保留
     * 
     * @param key 消息键（可选）
     * @param message 消息内容
     * @return HTTP 响应实体
     */
    @GetMapping("/important")
    public ResponseEntity<Map<String, Object>> sendImportantMessage(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("message") String message) {
        try {
            // 调用服务层发送重要消息
            producerService.sendImportantMessage(key, message);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "重要消息发送成功（会被拦截器保留）");
            response.put("data", Map.of("key", key, "message", message));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "重要消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送普通消息接口（用于测试拦截器过滤功能）
     * 
     * 请求方式：GET
     * 请求路径：/api/send/normal
     * 
     * 参数说明：
     * - key: 消息键（可选）
     * - message: 消息内容（必填）
     * 
     * 响应说明：
     * - 成功：返回成功信息和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * 注意：普通消息不包含 "important" 关键词，会被拦截器过滤掉
     * 
     * @param key 消息键（可选）
     * @param message 消息内容
     * @return HTTP 响应实体
     */
    @GetMapping("/normal")
    public ResponseEntity<Map<String, Object>> sendNormalMessage(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("message") String message) {
        try {
            // 调用服务层发送普通消息
            producerService.sendNormalMessage(key, message);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "普通消息发送成功（会被拦截器过滤）");
            response.put("data", Map.of("key", key, "message", message));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "普通消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送消息到指定分区接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch/partition
     * 
     * 参数说明：
     * - partition: 目标分区号（必填，0-2）
     * - count: 要发送的消息数量（可选，默认 5）
     * 
     * 响应说明：
     * - 成功：返回成功信息和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param partition 目标分区号
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch/partition")
    public ResponseEntity<Map<String, Object>> sendBatchMessagesToPartition(
            @RequestParam("partition") int partition,
            @RequestParam(value = "count", defaultValue = "5") int count) {
        try {
            // 验证分区号
            if (partition < 0 || partition > 2) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "分区号必须在 0-2 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > 50) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-50 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层批量发送消息到指定分区
            producerService.sendBatchMessagesToPartition(partition, count);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "批量消息发送到指定分区成功");
            response.put("data", Map.of("partition", partition, "count", count));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "批量消息发送到指定分区失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送消息到所有分区接口（自动分区）
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch/auto
     * 
     * 参数说明：
     * - count: 要发送的消息数量（可选，默认 10）
     * 
     * 响应说明：
     * - 成功：返回成功信息和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch/auto")
    public ResponseEntity<Map<String, Object>> sendBatchMessagesAuto(
            @RequestParam(value = "count", defaultValue = "10") int count) {
        try {
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > 100) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-100 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层批量发送消息（自动分区）
            producerService.sendBatchMessages(count);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "批量消息发送成功（自动分区）");
            response.put("data", Map.of("count", count));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "批量消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
