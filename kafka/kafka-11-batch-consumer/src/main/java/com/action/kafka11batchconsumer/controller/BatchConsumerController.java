package com.action.kafka11batchconsumer.controller;

import com.action.kafka11batchconsumer.service.BatchProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 批量消息消费演示控制器
 * <p>
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试批量消息发送
 * 2) 演示如何向不同分区发送批量消息
 * 3) 支持指定分区发送和自动分区发送
 * 4) 提供 RESTful API 接口
 * <p>
 * 接口说明：
 * - GET /api/send/single: 发送单条消息
 * - GET /api/send/batch: 批量发送消息
 * - GET /api/send/batch/partition: 批量发送消息到指定分区
 * - GET /api/send/batch/user-behavior: 发送用户行为数据（模拟真实业务场景）
 * <p>
 * 使用示例：
 * curl "http://localhost:9101/api/send/single?key=test-key&value=test-message"
 * curl "http://localhost:9101/api/send/batch?count=10"
 * curl "http://localhost:9101/api/send/batch/partition?partition=0&count=5"
 * curl "http://localhost:9101/api/send/batch/user-behavior?partition=1&count=8"
 */
@RestController
@RequestMapping("/api/send")
public class BatchConsumerController {

    /**
     * 批量消息最大发送数量限制
     * 用于防止过载和资源消耗过大
     */
    private static final int MAX_BATCH_MESSAGE_COUNT = 1000;

    /**
     * 批量消息发送演示生产者服务
     * 负责实际的消息发送逻辑
     */
    private final BatchProducerService producerService;

    /**
     * 构造函数注入服务依赖
     *
     * @param producerService 批量消息发送演示生产者服务
     */
    public BatchConsumerController(BatchProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * 发送单条消息接口
     * <p>
     * 请求方式：GET
     * 请求路径：/api/send/single
     * <p>
     * 参数说明：
     * - key: 消息键（可选）
     * - value: 消息值（必填）
     * <p>
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     *
     * @param key   消息键（可选）
     * @param value 消息值
     * @return HTTP 响应实体
     */
    @GetMapping("/single")
    public ResponseEntity<Map<String, Object>> sendSingleMessage(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("value") String value) {
        try {
            // 调用服务层发送单条消息
            if (key != null && !key.isEmpty()) {
                producerService.sendMessage(key, value);
            } else {
                producerService.sendMessage(value);
            }

            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "单条消息发送成功");
            response.put("data", Map.of("key", key, "value", value));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "单条消息发送失败: " + e.getMessage());

            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送消息接口（自动分区）
     * <p>
     * 请求方式：GET
     * 请求路径：/api/send/batch
     * <p>
     * 参数说明：
     * - count: 要发送的消息数量（可选，默认 10）
     * <p>
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     *
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatchMessages(
            @RequestParam(value = "count", defaultValue = "10") int count) {
        try {
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > MAX_BATCH_MESSAGE_COUNT) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-" + MAX_BATCH_MESSAGE_COUNT + " 之间");

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

    /**
     * 批量发送消息到指定分区接口
     * <p>
     * 请求方式：GET
     * 请求路径：/api/send/batch/partition
     * <p>
     * 参数说明：
     * - partition: 目标分区号（必填，0-2）
     * - count: 要发送的消息数量（可选，默认 5）
     * <p>
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     *
     * @param partition 目标分区号
     * @param count     要发送的消息数量
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
            if (count <= 0 || count > MAX_BATCH_MESSAGE_COUNT) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-" + MAX_BATCH_MESSAGE_COUNT + " 之间");

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
     * 发送用户行为数据接口（模拟真实业务场景）
     * <p>
     * 请求方式：GET
     * 请求路径：/api/send/batch/user-behavior
     * <p>
     * 参数说明：
     * - partition: 目标分区号（必填，0-2）
     * - count: 要发送的消息数量（可选，默认 8）
     * <p>
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     *
     * @param partition 目标分区号
     * @param count     要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch/user-behavior")
    public ResponseEntity<Map<String, Object>> sendUserBehaviorMessages(
            @RequestParam("partition") int partition,
            @RequestParam(value = "count", defaultValue = "8") int count) {
        try {
            // 验证分区号
            if (partition < 0 || partition > 2) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "分区号必须在 0-2 之间");

                return ResponseEntity.badRequest().body(response);
            }

            // 限制批量发送数量，避免过载
            if (count <= 0 || count > MAX_BATCH_MESSAGE_COUNT) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-" + MAX_BATCH_MESSAGE_COUNT + " 之间");

                return ResponseEntity.badRequest().body(response);
            }

            // 调用服务层发送用户行为数据
            producerService.sendUserBehaviorMessages(partition, count);

            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "用户行为数据发送成功");
            response.put("data", Map.of("partition", partition, "count", count));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "用户行为数据发送失败: " + e.getMessage());

            return ResponseEntity.internalServerError().body(response);
        }
    }
}
