package com.action.kafka03offset.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 偏移量重置服务
 * 通过Kafka Admin API重置消费者组的偏移量
 * 支持重置到earliest或latest位置
 */
@Service
@Slf4j
public class OffsetResetService {

    private final KafkaAdmin kafkaAdmin;

    public OffsetResetService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * 重置消费者组偏移量
     * 使用Admin API将指定消费者组的偏移量重置到指定位置
     * 
     * @param groupId 消费者组ID
     * @param topic Topic名称
     * @param resetTo 重置目标位置（earliest/latest）
     */
    public void resetConsumerOffset(String groupId, String topic, String resetTo) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {

            // 获取Topic的所有分区
            Set<TopicPartition> partitions = getTopicPartitions(adminClient, topic);
            if (partitions.isEmpty()) {
                throw new RuntimeException("Topic " + topic + " 不存在或没有分区");
            }

            // 准备新的偏移量映射
            Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();

            // 根据重置策略获取目标偏移量
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets;
            if ("earliest".equalsIgnoreCase(resetTo)) {
                // 重置到最早位置
                Map<TopicPartition, OffsetSpec> spec = partitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));
                offsets = adminClient.listOffsets(spec).all().get(10, TimeUnit.SECONDS);
            } else if ("latest".equalsIgnoreCase(resetTo)) {
                // 重置到最新位置
                Map<TopicPartition, OffsetSpec> spec = partitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
                offsets = adminClient.listOffsets(spec).all().get(10, TimeUnit.SECONDS);
            } else {
                throw new IllegalArgumentException("不支持的resetTo参数: " + resetTo);
            }

            // 构建新的偏移量映射
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e : offsets.entrySet()) {
                newOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()));
            }

            // 执行偏移量重置
            adminClient.alterConsumerGroupOffsets(groupId, newOffsets).all().get(10, TimeUnit.SECONDS);
            log.info("✅ 消费者组 {} 的偏移量已重置到 {}", groupId, resetTo);
        } catch (Exception e) {
            log.error("❌ 重置消费者组偏移量失败: {}", e.getMessage());
            throw new RuntimeException("重置失败", e);
        }
    }

    /**
     * 获取Topic的所有分区信息
     * 
     * @param adminClient Admin客户端
     * @param topic Topic名称
     * @return 分区集合
     * @throws Exception 异常
     */
    private Set<TopicPartition> getTopicPartitions(AdminClient adminClient, String topic) throws Exception {
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singleton(topic));
        TopicDescription topicDescription = topicsResult.topicNameValues().get(topic).get();
        return topicDescription.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toSet());
    }
}


