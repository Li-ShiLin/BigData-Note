package com.action.kafka06producerpartitionstrategy.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 基于 Key 的分区策略
 * 根据 key 的 hash 值进行分区
 */
@Component
public class KeyBasedPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        if (key == null) {
            // 如果没有 key，随机选择一个分区
            return Math.abs(Utils.murmur2(valueBytes)) % numPartitions;
        }
        
        // 使用 key 的 hash 值进行分区
        return Math.abs(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {
        // 清理资源
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // 配置分区器
    }
}
