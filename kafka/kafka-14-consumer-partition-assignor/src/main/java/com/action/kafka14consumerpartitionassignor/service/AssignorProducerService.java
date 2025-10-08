package com.action.kafka14consumerpartitionassignor.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 分区策略演示 - 生产者服务
 * 为每个分区分配策略提供独立的消息发送方法
 */
@Service
public class AssignorProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public AssignorProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 为RangeAssignor策略发送批量消息
     */
    public CompletableFuture<Void> sendRangeAssignorBatch(int count) {
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        for (int i = 0; i < count; i++) {
            String key = "range-key-" + i;
            String value = "range-msg-" + i;
            futures[i] = kafkaTemplate.send("range-assignor-topic", key, value).thenApply(md -> null);
        }
        return CompletableFuture.allOf(futures);
    }

    /**
     * 为RoundRobinAssignor策略发送批量消息
     */
    public CompletableFuture<Void> sendRoundRobinAssignorBatch(int count) {
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        for (int i = 0; i < count; i++) {
            String key = "rr-key-" + i;
            String value = "rr-msg-" + i;
            futures[i] = kafkaTemplate.send("roundrobin-assignor-topic", key, value).thenApply(md -> null);
        }
        return CompletableFuture.allOf(futures);
    }

    /**
     * 为StickyAssignor策略发送批量消息
     */
    public CompletableFuture<Void> sendStickyAssignorBatch(int count) {
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        for (int i = 0; i < count; i++) {
            String key = "sticky-key-" + i;
            String value = "sticky-msg-" + i;
            futures[i] = kafkaTemplate.send("sticky-assignor-topic", key, value).thenApply(md -> null);
        }
        return CompletableFuture.allOf(futures);
    }

    /**
     * 为CooperativeStickyAssignor策略发送批量消息
     */
    public CompletableFuture<Void> sendCoopStickyAssignorBatch(int count) {
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        for (int i = 0; i < count; i++) {
            String key = "coop-key-" + i;
            String value = "coop-msg-" + i;
            futures[i] = kafkaTemplate.send("coop-sticky-assignor-topic", key, value).thenApply(md -> null);
        }
        return CompletableFuture.allOf(futures);
    }
}


