package com.action.kafka13consumersendto.controller;

import com.action.kafka13consumersendto.service.MessageForwardProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 消息转发演示控制器
 * 
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试消息转发
 * 2) 演示多种消息转发方式
 * 3) 支持通过 URL 参数指定消息内容
 * 4) 提供 RESTful API 接口
 * 
 * 接口说明：
 * - GET /api/send/basic: 发送基础转发测试消息
 * - GET /api/send/enhance: 发送增强转发测试消息
 * - GET /api/send/conditional: 发送条件转发测试消息
 * - GET /api/send/batch: 批量发送转发测试消息
 * - GET /api/send/various: 发送各种类型的转发测试消息
 * - GET /api/send/partition: 发送指定分区的转发测试消息
 * 
 * 使用示例：
 * curl "http://localhost:9103/api/send/basic?message=hello world"
 * curl "http://localhost:9103/api/send/enhance?message=enhance test"
 * curl "http://localhost:9103/api/send/conditional?message=conditional forward test"
 * curl "http://localhost:9103/api/send/batch?count=5"
 * curl "http://localhost:9103/api/send/various"
 * curl "http://localhost:9103/api/send/partition?partition=0&message=partition test"
 */
@RestController
@RequestMapping("/api/send")
public class MessageForwardController {

    /**
     * 消息转发演示生产者服务
     * 负责实际的消息发送逻辑
     */
    private final MessageForwardProducerService producerService;

    /**
     * 构造函数注入服务依赖
     * 
     * @param producerService 消息转发演示生产者服务
     */
    public MessageForwardController(MessageForwardProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * 发送基础转发测试消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/basic
     * 
     * 参数说明：
     * - message: 要发送的消息内容（必填）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param message 要发送的消息内容
     * @return HTTP 响应实体
     */
    @GetMapping("/basic")
    public ResponseEntity<Map<String, Object>> sendBasicForwardMessage(@RequestParam("message") String message) {
        try {
            // 调用服务层发送基础转发测试消息
            producerService.sendBasicForwardMessage(message);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "基础转发测试消息发送成功");
            response.put("data", message);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "基础转发测试消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送增强转发测试消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/enhance
     * 
     * 参数说明：
     * - message: 要发送的消息内容（必填）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param message 要发送的消息内容
     * @return HTTP 响应实体
     */
    @GetMapping("/enhance")
    public ResponseEntity<Map<String, Object>> sendEnhanceForwardMessage(@RequestParam("message") String message) {
        try {
            // 调用服务层发送增强转发测试消息
            producerService.sendEnhanceForwardMessage(message);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "增强转发测试消息发送成功");
            response.put("data", message);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "增强转发测试消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送条件转发测试消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/conditional
     * 
     * 参数说明：
     * - message: 要发送的消息内容（必填）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param message 要发送的消息内容
     * @return HTTP 响应实体
     */
    @GetMapping("/conditional")
    public ResponseEntity<Map<String, Object>> sendConditionalForwardMessage(@RequestParam("message") String message) {
        try {
            // 调用服务层发送条件转发测试消息
            producerService.sendConditionalForwardMessage(message);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "条件转发测试消息发送成功");
            response.put("data", message);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "条件转发测试消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送转发测试消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch
     * 
     * 参数说明：
     * - count: 要发送的消息数量（可选，默认 5）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatchForwardMessages(@RequestParam(value = "count", defaultValue = "5") int count) {
        try {
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > 50) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-50 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层批量发送转发测试消息
            producerService.sendBatchForwardMessages(count);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "批量转发测试消息发送成功");
            response.put("data", Map.of("count", count));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "批量转发测试消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送各种类型的转发测试消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/various
     * 
     * 功能说明：
     * - 发送多种类型的消息到源主题
     * - 演示不同类型的消息转发
     * - 包含基础转发、增强转发、条件转发等
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @return HTTP 响应实体
     */
    @GetMapping("/various")
    public ResponseEntity<Map<String, Object>> sendVariousForwardMessages() {
        try {
            // 调用服务层发送各种类型的转发测试消息
            producerService.sendVariousForwardMessages();
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "各种类型转发测试消息发送成功");
            response.put("data", "包含基础转发、增强转发、条件转发等消息");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "各种类型转发测试消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送指定分区的转发测试消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/partition
     * 
     * 参数说明：
     * - partition: 目标分区号（必填，0-2）
     * - message: 要发送的消息内容（必填）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param partition 目标分区号
     * @param message 要发送的消息内容
     * @return HTTP 响应实体
     */
    @GetMapping("/partition")
    public ResponseEntity<Map<String, Object>> sendForwardMessageToPartition(
            @RequestParam("partition") int partition,
            @RequestParam("message") String message) {
        try {
            // 验证分区号
            if (partition < 0 || partition > 2) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "分区号必须在 0-2 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层发送指定分区的转发测试消息
            producerService.sendForwardMessageToPartition(partition, message);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "指定分区转发测试消息发送成功");
            response.put("data", Map.of("partition", partition, "message", message));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "指定分区转发测试消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
