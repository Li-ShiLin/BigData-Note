package com.action.kafka14consumerpartitionassignor.controller;

import com.action.kafka14consumerpartitionassignor.service.AssignorProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * 分区策略演示 - 测试控制器
 * 为每个分区分配策略提供独立的测试接口
 */
@RestController
@RequestMapping("/api/assignor")
public class AssignorController {

    private final AssignorProducerService producerService;

    public AssignorController(AssignorProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * RangeAssignor策略测试接口
     */
    @GetMapping("/range/batch")
    public ResponseEntity<Map<String, Object>> sendRangeAssignorBatch(
            @RequestParam(value = "count", defaultValue = "12") int count) {
        producerService.sendRangeAssignorBatch(count);
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "success");
        resp.put("message", "RangeAssignor策略批量消息发送成功");
        resp.put("data", Map.of("strategy", "RangeAssignor", "count", count));
        return ResponseEntity.ok(resp);
    }

    /**
     * RoundRobinAssignor策略测试接口
     */
    @GetMapping("/roundrobin/batch")
    public ResponseEntity<Map<String, Object>> sendRoundRobinAssignorBatch(
            @RequestParam(value = "count", defaultValue = "12") int count) {
        producerService.sendRoundRobinAssignorBatch(count);
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "success");
        resp.put("message", "RoundRobinAssignor策略批量消息发送成功");
        resp.put("data", Map.of("strategy", "RoundRobinAssignor", "count", count));
        return ResponseEntity.ok(resp);
    }

    /**
     * StickyAssignor策略测试接口
     */
    @GetMapping("/sticky/batch")
    public ResponseEntity<Map<String, Object>> sendStickyAssignorBatch(
            @RequestParam(value = "count", defaultValue = "12") int count) {
        producerService.sendStickyAssignorBatch(count);
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "success");
        resp.put("message", "StickyAssignor策略批量消息发送成功");
        resp.put("data", Map.of("strategy", "StickyAssignor", "count", count));
        return ResponseEntity.ok(resp);
    }

    /**
     * CooperativeStickyAssignor策略测试接口
     */
    @GetMapping("/coop-sticky/batch")
    public ResponseEntity<Map<String, Object>> sendCoopStickyAssignorBatch(
            @RequestParam(value = "count", defaultValue = "12") int count) {
        producerService.sendCoopStickyAssignorBatch(count);
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "success");
        resp.put("message", "CooperativeStickyAssignor策略批量消息发送成功");
        resp.put("data", Map.of("strategy", "CooperativeStickyAssignor", "count", count));
        return ResponseEntity.ok(resp);
    }
}


