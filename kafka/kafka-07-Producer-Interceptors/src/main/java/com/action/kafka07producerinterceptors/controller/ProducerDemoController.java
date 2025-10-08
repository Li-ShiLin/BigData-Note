package com.action.kafka07producerinterceptors.controller;

import com.action.kafka07producerinterceptors.service.ProducerDemoService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 生产者演示控制器
 * 
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试消息发送
 * 2) 演示拦截器在消息发送过程中的作用
 * 3) 支持通过 URL 参数指定消息键和值
 * 
 * 接口说明：
 * - GET /demo/send?key=xxx&value=yyy: 发送消息到 Kafka
 * - key 参数可选，value 参数必填
 * - 返回简单的成功响应
 * 
 * 使用示例：
 * curl "http://localhost:8080/demo/send?key=k1&value=hello world"
 * curl "http://localhost:8080/demo/send?value=test message"
 */
@RestController
public class ProducerDemoController {

    /**
     * 生产者演示服务
     * 负责实际的消息发送逻辑
     */
    private final ProducerDemoService producerDemoService;

    /**
     * 构造函数注入服务依赖
     * 
     * @param producerDemoService 生产者演示服务
     */
    public ProducerDemoController(ProducerDemoService producerDemoService) {
        this.producerDemoService = producerDemoService;
    }

    /**
     * 发送消息接口
     * 
     * 请求方式：GET
     * 请求路径：/demo/send
     * 
     * 参数说明：
     * - key: 消息键（可选），用于分区路由和消息去重
     * - value: 消息值（必填），实际的消息内容
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * 拦截器处理流程：
     * 1) ModifyRecordInterceptor: 为消息添加前缀 "[modified-by-interceptor] " 和审计 Header
     * 2) MetricsProducerInterceptor: 统计发送成功/失败次数
     * 
     * @param key 消息键（可选）
     * @param value 消息值（必填）
     * @return HTTP 响应实体
     */
    @GetMapping("/demo/send")
    public ResponseEntity<String> send(@RequestParam(value = "key", required = false) String key,
                                       @RequestParam("value") String value) {
        // 调用服务层发送消息
        // 注意：这里使用异步发送，不等待发送结果
        // 实际生产环境中可能需要处理发送异常
        producerDemoService.sendMessage(key, value);
        
        // 返回成功响应
        // 注意：这里只表示消息已提交发送，不代表发送成功
        // 真正的发送结果会在拦截器的 onAcknowledgement() 中处理
        return ResponseEntity.ok("send ok");
    }
}


