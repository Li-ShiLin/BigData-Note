package com.action.kafka09objectmessage.controller;

import com.action.kafka09objectmessage.model.User;
import com.action.kafka09objectmessage.service.ObjectMessageProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 对象消息演示控制器
 * 
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试对象消息发送
 * 2) 演示多种对象消息发送方式
 * 3) 支持通过 URL 参数指定消息内容
 * 4) 提供 RESTful API 接口
 * 
 * 接口说明：
 * - GET /api/send/string: 发送字符串消息
 * - GET /api/send/user: 发送用户对象消息
 * - GET /api/send/user-with-key: 发送带键的用户对象消息
 * - GET /api/send/batch: 批量发送用户对象消息
 * - POST /api/send/custom-user: 发送自定义用户对象消息
 * 
 * 使用示例：
 * curl "http://localhost:8080/api/send/string?message=hello world"
 * curl "http://localhost:8080/api/send/user"
 * curl "http://localhost:8080/api/send/user-with-key?key=user-001"
 * curl "http://localhost:8080/api/send/batch?count=5"
 * curl -X POST "http://localhost:8080/api/send/custom-user" -H "Content-Type: application/json" -d '{"id":123,"phone":"13800138000"}'
 */
@RestController
@RequestMapping("/api/send")
public class ObjectMessageController {

    /**
     * 对象消息生产者服务
     * 负责实际的消息发送逻辑
     */
    private final ObjectMessageProducerService producerService;

    /**
     * 构造函数注入服务依赖
     * 
     * @param producerService 对象消息生产者服务
     */
    public ObjectMessageController(ObjectMessageProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * 发送字符串消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/string
     * 
     * 参数说明：
     * - message: 要发送的字符串消息（必填）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param message 要发送的字符串消息
     * @return HTTP 响应实体
     */
    @GetMapping("/string")
    public ResponseEntity<Map<String, Object>> sendStringMessage(@RequestParam("message") String message) {
        try {
            // 调用服务层发送字符串消息
            producerService.sendStringMessage(message);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "字符串消息发送成功");
            response.put("data", message);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "字符串消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送用户对象消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/user
     * 
     * 功能说明：
     * - 创建一个默认的用户对象
     * - 将对象序列化为 JSON 字符串
     * - 发送到 Kafka 主题
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @return HTTP 响应实体
     */
    @GetMapping("/user")
    public ResponseEntity<Map<String, Object>> sendUserMessage() {
        try {
            // 创建默认用户对象
            User user = User.builder()
                    .id(1001)
                    .phone("13709090909")
                    .birthDay(new Date())
                    .build();
            
            // 调用服务层发送用户对象消息
            producerService.sendUserMessage(user);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "用户对象消息发送成功");
            response.put("data", user);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "用户对象消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送带键的用户对象消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/user-with-key
     * 
     * 参数说明：
     * - key: 消息键（可选），用于分区路由
     * 
     * 功能说明：
     * - 创建一个默认的用户对象
     * - 使用指定的键发送消息
     * - 键用于分区路由和消息去重
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param key 消息键（可选）
     * @return HTTP 响应实体
     */
    @GetMapping("/user-with-key")
    public ResponseEntity<Map<String, Object>> sendUserMessageWithKey(@RequestParam(value = "key", required = false) String key) {
        try {
            // 创建默认用户对象
            User user = User.builder()
                    .id(1002)
                    .phone("13709090908")
                    .birthDay(new Date())
                    .build();
            
            // 如果没有提供键，使用默认值
            if (key == null || key.isEmpty()) {
                key = "default-user-key";
            }
            
            // 调用服务层发送带键的用户对象消息
            producerService.sendUserMessageWithKey(key, user);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "带键的用户对象消息发送成功");
            response.put("data", Map.of("key", key, "user", user));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "带键的用户对象消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送用户对象消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch
     * 
     * 参数说明：
     * - count: 要发送的消息数量（可选，默认 5）
     * 
     * 功能说明：
     * - 创建指定数量的用户对象
     * - 为每个对象生成唯一的键
     * - 批量发送到 Kafka 主题
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatchUserMessages(@RequestParam(value = "count", defaultValue = "5") int count) {
        try {
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > 100) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-100 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层批量发送用户对象消息
            producerService.sendBatchUserMessages(count);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "批量用户对象消息发送成功");
            response.put("data", Map.of("count", count));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "批量用户对象消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送自定义用户对象消息接口
     * 
     * 请求方式：POST
     * 请求路径：/api/send/custom-user
     * 
     * 请求体说明：
     * - id: 用户ID（必填）
     * - phone: 手机号码（必填）
     * - key: 消息键（可选）
     * 
     * 功能说明：
     * - 根据请求参数创建用户对象
     * - 将对象序列化为 JSON 字符串
     * - 发送到 Kafka 主题
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param request 包含用户信息的请求体
     * @return HTTP 响应实体
     */
    @PostMapping("/custom-user")
    public ResponseEntity<Map<String, Object>> sendCustomUserMessage(@RequestBody Map<String, Object> request) {
        try {
            // 验证必需参数
            if (!request.containsKey("id") || !request.containsKey("phone")) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "缺少必需参数: id 和 phone");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 提取参数
            int id = Integer.parseInt(request.get("id").toString());
            String phone = request.get("phone").toString();
            String key = request.containsKey("key") ? request.get("key").toString() : null;
            
            // 调用服务层发送自定义用户对象消息
            producerService.sendCustomUserMessage(id, phone, key);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "自定义用户对象消息发送成功");
            response.put("data", request);
            
            return ResponseEntity.ok(response);
        } catch (NumberFormatException e) {
            // 返回参数格式错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "参数格式错误: id 必须是数字");
            
            return ResponseEntity.badRequest().body(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "自定义用户对象消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
