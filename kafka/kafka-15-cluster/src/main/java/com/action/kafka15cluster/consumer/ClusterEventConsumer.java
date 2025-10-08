package com.action.kafka15cluster.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class ClusterEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ClusterEventConsumer.class);

    @Value("${kafka.topic.cluster:clusterTopic}")
    private String clusterTopic;

    @Value("${spring.kafka.consumer.group-id:clusterGroup}")
    private String consumerGroupId;

    public ClusterEventConsumer() {
        log.info("ClusterEventConsumer 初始化完成");
    }

    /**
     * 集群消费演示：并发 3，手动确认
     */
    @KafkaListener(topics = "${kafka.topic.cluster:clusterTopic}", groupId = "${spring.kafka.consumer.group-id:clusterGroup}", concurrency = "3")
    public void onEvent(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("收到消息，开始处理: topic={}, partition={}, offset={}", 
                record.topic(), record.partition(), record.offset());
        try {
            long tid = Thread.currentThread().getId();
            log.info("{} -> 消费成功: topic={}, partition={}, offset={}, key={}, value={}",
                    tid, record.topic(), record.partition(), record.offset(), record.key(), record.value());
            // 手动确认（若开启 auto-commit=true，本行等价演示；生产建议按需配置）
            ack.acknowledge();
        } catch (Exception e) {
            log.error("消费异常: {}", e.getMessage(), e);
        }
    }
}


