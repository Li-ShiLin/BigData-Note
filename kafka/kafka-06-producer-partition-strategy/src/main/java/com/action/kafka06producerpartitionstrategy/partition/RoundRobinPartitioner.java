package com.action.kafka06producerpartitionstrategy.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询分区策略
 * 按照轮询方式分配消息到不同分区
 */
@Component
public class RoundRobinPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        if (key != null) {
            // 如果有 key，使用 key 的 hash 值
            return Math.abs(Utils.murmur2(keyBytes)) % numPartitions;
        } else {
            // 如果没有 key，使用轮询策略
            return Math.abs(counter.getAndIncrement()) % numPartitions;
        }
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
