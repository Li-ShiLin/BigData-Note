package com.action.kafka06producerpartitionstrategy.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 自定义分区策略
 * 根据消息类型进行分区
 */
@Component
public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        if (key == null) {
            // 如果没有 key，使用轮询策略
            return Math.abs(Utils.murmur2(valueBytes)) % numPartitions;
        }
        
        String keyStr = key.toString();
        
        // 根据 key 的不同前缀进行分区
        if (keyStr.startsWith("ORDER")) {
            // 订单相关消息分配到分区 0
            return 0;
        } else if (keyStr.startsWith("USER")) {
            // 用户相关消息分配到分区 1
            return 1;
        } else if (keyStr.startsWith("SYSTEM")) {
            // 系统相关消息分配到分区 2
            return 2;
        } else {
            // 其他消息使用 hash 分区
            return Math.abs(Utils.murmur2(keyBytes)) % numPartitions;
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
