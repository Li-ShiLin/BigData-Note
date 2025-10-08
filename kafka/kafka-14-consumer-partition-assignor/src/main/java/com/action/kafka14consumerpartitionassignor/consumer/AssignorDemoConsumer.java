package com.action.kafka14consumerpartitionassignor.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * 分区分配策略演示消费者
 *
 * 说明：
 * - 同一个应用中，使用不同的消费者组和不同的分区分配策略进行对比
 * - 每个监听器并发=3，可在日志中观察分区如何在同组内的并发容器之间分配
 */
@Component
public class AssignorDemoConsumer {

    private static final Logger log = LoggerFactory.getLogger(AssignorDemoConsumer.class);

    // RangeAssignor 演示 - 3个并发消费者
    @KafkaListener(topics = "range-assignor-topic",
            groupId = "${kafka.group.range}",
            containerFactory = "rangeKafkaListenerContainerFactory",
            concurrency = "3")
    public void onRange(
            ConsumerRecord<String, String> record,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
            @Header(value = KafkaHeaders.OFFSET) String offset,
            Acknowledgment ack) {
        long threadId = Thread.currentThread().getId();
        String threadName = Thread.currentThread().getName();
        log.info("[RangeAssignor] Thread[{}] Consumer[{}] topic={}, partition={}, offset={}, key={}, value={}",
                threadId, threadName, topic, partition, offset, record.key(), record.value());
        ack.acknowledge();
    }

    // RoundRobinAssignor 演示 - 3个并发消费者
    @KafkaListener(topics = "roundrobin-assignor-topic",
            groupId = "${kafka.group.rr}",
            containerFactory = "rrKafkaListenerContainerFactory",
            concurrency = "3")
    public void onRoundRobin(
            ConsumerRecord<String, String> record,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
            @Header(value = KafkaHeaders.OFFSET) String offset,
            Acknowledgment ack) {
        long threadId = Thread.currentThread().getId();
        String threadName = Thread.currentThread().getName();
        log.info("[RoundRobinAssignor] Thread[{}] Consumer[{}] topic={}, partition={}, offset={}, key={}, value={}",
                threadId, threadName, topic, partition, offset, record.key(), record.value());
        ack.acknowledge();
    }

    // StickyAssignor 演示 - 3个并发消费者
    @KafkaListener(topics = "sticky-assignor-topic",
            groupId = "${kafka.group.sticky}",
            containerFactory = "stickyKafkaListenerContainerFactory",
            concurrency = "3")
    public void onSticky(
            ConsumerRecord<String, String> record,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
            @Header(value = KafkaHeaders.OFFSET) String offset,
            Acknowledgment ack) {
        long threadId = Thread.currentThread().getId();
        String threadName = Thread.currentThread().getName();
        log.info("[StickyAssignor] Thread[{}] Consumer[{}] topic={}, partition={}, offset={}, key={}, value={}",
                threadId, threadName, topic, partition, offset, record.key(), record.value());
        ack.acknowledge();
    }

    // CooperativeStickyAssignor 演示 - 3个并发消费者
    @KafkaListener(topics = "coop-sticky-assignor-topic",
            groupId = "${kafka.group.coop}",
            containerFactory = "coopStickyKafkaListenerContainerFactory",
            concurrency = "3")
    public void onCoopSticky(
            ConsumerRecord<String, String> record,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
            @Header(value = KafkaHeaders.OFFSET) String offset,
            Acknowledgment ack) {
        long threadId = Thread.currentThread().getId();
        String threadName = Thread.currentThread().getName();
        log.info("[CooperativeStickyAssignor] Thread[{}] Consumer[{}] topic={}, partition={}, offset={}, key={}, value={}",
                threadId, threadName, topic, partition, offset, record.key(), record.value());
        ack.acknowledge();
    }
}


