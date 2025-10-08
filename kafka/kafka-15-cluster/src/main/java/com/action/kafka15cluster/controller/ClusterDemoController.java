package com.action.kafka15cluster.controller;

import com.action.kafka15cluster.producer.ClusterEventProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/cluster")
public class ClusterDemoController {

    @Resource
    private ClusterEventProducer producer;

    /**
     * 触发批量发送，用于观察集群分区与副本效果
     */
    @GetMapping("/sendBatch")
    public Map<String, Object> sendBatch(@RequestParam(defaultValue = "10") int count) {
        producer.sendBatchUsers(count);
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "success");
        resp.put("message", "批量消息已发送");
        resp.put("count", count);
        return resp;
    }
}


