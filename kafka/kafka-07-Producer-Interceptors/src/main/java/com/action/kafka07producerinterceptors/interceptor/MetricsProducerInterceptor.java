package com.action.kafka07producerinterceptors.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 指标统计拦截器 - 收集生产者发送成功与失败的统计信息
 * 
 * 功能说明：
 * 1) 全局统计：记录总的发送成功和失败次数
 * 2) 分主题统计：按 topic 分别统计成功发送次数
 * 3) 监控告警：在发送失败时记录警告日志
 * 4) 生命周期管理：在拦截器关闭时输出统计报告
 * 
 * 实现细节：
 * - 使用 AtomicLong 确保多线程环境下的计数准确性
 * - 使用 ConcurrentHashMap 存储分主题统计，支持并发访问
 * - 在 onAcknowledgement() 中根据异常情况更新计数器
 * - 在 close() 方法中输出最终统计结果
 * 
 * 关键参数说明：
 * - AtomicLong: 线程安全的原子长整型，用于计数
 * - ConcurrentHashMap: 线程安全的哈希表，存储分主题统计
 * - RecordMetadata: 发送成功时包含 topic、partition、offset 等信息
 */
public class MetricsProducerInterceptor implements ProducerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(MetricsProducerInterceptor.class);

    // 全局成功发送计数器（线程安全）
    private final AtomicLong successCount = new AtomicLong(0);
    
    // 全局失败发送计数器（线程安全）
    private final AtomicLong failureCount = new AtomicLong(0);
    
    // 分主题成功发送计数器（线程安全）
    // Key: topic名称, Value: 该topic的成功发送次数
    private final Map<String, AtomicLong> topicSuccess = new ConcurrentHashMap<>();

    /**
     * 消息发送前的拦截处理
     * 
     * 执行时机：消息被发送到序列化器之前
     * 作用：本拦截器专注于统计，不修改消息内容
     * 
     * @param record 消息记录
     * @return 原消息记录（不做修改）
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 本拦截器不修改消息内容，直接返回原记录
        // 可以在这里添加发送前的统计逻辑，如记录发送尝试次数等
        return record;
    }

    /**
     * 消息发送确认回调 - 核心统计逻辑
     * 
     * 执行时机：消息发送成功或失败后
     * 作用：根据发送结果更新各种计数器
     * 
     * @param metadata 发送成功时的元数据信息（包含 topic、partition、offset 等）
     * @param exception 发送失败时的异常信息（成功时为 null）
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null && metadata != null) {
            // 发送成功：更新成功计数器
            successCount.incrementAndGet();
            
            // 更新对应主题的成功计数
            // computeIfAbsent: 如果key不存在则创建新的AtomicLong(0)，然后递增
            topicSuccess.computeIfAbsent(metadata.topic(), k -> new AtomicLong(0)).incrementAndGet();
            
            log.debug("Message sent successfully: topic={}, partition={}, offset={}", 
                     metadata.topic(), metadata.partition(), metadata.offset());
        } else {
            // 发送失败：更新失败计数器
            failureCount.incrementAndGet();
            
            if (exception != null) {
                // 记录失败原因，便于问题排查
                log.warn("Message send failed: topic={}, error={}", 
                        metadata != null ? metadata.topic() : "unknown", 
                        exception.getMessage());
            }
        }
    }

    /**
     * 拦截器关闭时的统计报告输出
     * 
     * 执行时机：Producer 关闭时
     * 作用：输出最终的统计结果，用于监控和运维分析
     */
    @Override
    public void close() {
        // 输出全局统计信息
        long totalSuccess = successCount.get();
        long totalFailure = failureCount.get();
        long totalMessages = totalSuccess + totalFailure;
        
        log.info("=== Producer Metrics Report ===");
        log.info("Total messages: {}, Success: {}, Failure: {}", totalMessages, totalSuccess, totalFailure);
        
        if (totalMessages > 0) {
            double successRate = (double) totalSuccess / totalMessages * 100;
            log.info("Success rate: {:.2f}%", successRate);
        }
        
        // 输出分主题统计信息
        if (!topicSuccess.isEmpty()) {
            log.info("=== Per-Topic Success Count ===");
            topicSuccess.forEach((topic, count) -> 
                log.info("Topic: {}, Success count: {}", topic, count.get())
            );
        }
        
        log.info("=== End of Metrics Report ===");
    }

    /**
     * 拦截器配置初始化
     * 
     * 执行时机：拦截器创建时
     * 作用：读取配置参数、初始化统计数据结构等
     * 
     * @param configs 生产者配置参数
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 本拦截器使用默认配置，无需特殊初始化
        // 可以在这里读取自定义配置，如统计间隔、输出格式等
        log.info("MetricsProducerInterceptor configured with {} parameters", configs.size());
    }
}


