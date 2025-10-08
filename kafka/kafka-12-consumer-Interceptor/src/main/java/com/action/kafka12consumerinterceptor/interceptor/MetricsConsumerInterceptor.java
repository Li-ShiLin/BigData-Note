package com.action.kafka12consumerinterceptor.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 指标统计拦截器 - 收集消费者消费成功与失败的统计信息
 * <p>
 * 功能说明：
 * 1) 全局统计：记录总的消费成功和失败次数
 * 2) 分主题统计：按 topic 分别统计成功消费次数
 * 3) 分分区统计：按 topic-partition 分别统计消费次数
 * 4) 监控告警：在消费失败时记录警告日志
 * 5) 生命周期管理：在拦截器关闭时输出统计报告
 * <p>
 * 实现细节：
 * - 使用 AtomicLong 确保多线程环境下的计数准确性
 * - 使用 ConcurrentHashMap 存储分主题和分分区统计，支持并发访问
 * - 在 onConsume() 中统计消费的消息数量
 * - 在 onCommit() 中统计提交的偏移量信息
 * - 在 close() 方法中输出最终统计结果
 * <p>
 * 关键参数说明：
 * - AtomicLong: 线程安全的原子长整型，用于计数
 * - ConcurrentHashMap: 线程安全的哈希表，存储分主题和分分区统计
 * - ConsumerRecords: 消费的消息记录集合
 * - OffsetAndMetadata: 偏移量和元数据信息
 */
public class MetricsConsumerInterceptor implements ConsumerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(MetricsConsumerInterceptor.class);

    // 全局成功消费计数器（线程安全）
    private final AtomicLong successCount = new AtomicLong(0);

    // 全局失败消费计数器（线程安全）
    private final AtomicLong failureCount = new AtomicLong(0);

    // 分主题成功消费计数器（线程安全）
    // Key: topic名称, Value: 该topic的成功消费次数
    private final Map<String, AtomicLong> topicSuccess = new ConcurrentHashMap<>();

    // 分分区成功消费计数器（线程安全）
    // Key: topic-partition, Value: 该分区的成功消费次数
    private final Map<String, AtomicLong> partitionSuccess = new ConcurrentHashMap<>();

    // 偏移量提交统计（线程安全）
    private final AtomicLong commitCount = new AtomicLong(0);

    /**
     * 消息消费前的拦截处理
     * <p>
     * 执行时机：消息被反序列化之后，传递给消费者之前
     * 作用：本拦截器专注于统计，不修改消息内容
     *
     * @param records 消息记录集合
     * @return 原消息记录集合（不做修改）
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        try {
            if (records.isEmpty()) {
                return records;
            }

            // 统计消费的消息数量
            long messageCount = records.count();
            successCount.addAndGet(messageCount);

            // 按主题统计
            records.partitions().forEach(partition -> {
                String topic = partition.topic();
                int partitionNum = partition.partition();
                String partitionKey = topic + "-" + partitionNum;

                long partitionMessageCount = records.records(partition).size();

                // 更新主题统计
                topicSuccess.computeIfAbsent(topic, k -> new AtomicLong(0))
                        .addAndGet(partitionMessageCount);

                // 更新分区统计
                partitionSuccess.computeIfAbsent(partitionKey, k -> new AtomicLong(0))
                        .addAndGet(partitionMessageCount);

                log.info("[MetricsConsumerInterceptor] 消息消费统计: topic={}, partition={}, count={}",
                        topic, partitionNum, partitionMessageCount);
            });

            return records;
        } catch (Exception ex) {
            // 消费失败：更新失败计数器
            failureCount.incrementAndGet();
            log.error("[MetricsConsumerInterceptor] 消息消费失败: {} records", records.count(), ex);
            throw ex;
        }
    }

    /**
     * 偏移量提交前的回调处理 - 核心统计逻辑
     * <p>
     * 执行时机：消费者提交偏移量之前
     * 作用：统计偏移量提交信息，用于监控消费进度
     *
     * @param offsets 要提交的偏移量映射
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            // 统计提交的偏移量数量
            commitCount.addAndGet(offsets.size());

            // 记录提交详情
            offsets.forEach((partition, offsetMetadata) -> {
                log.info("[MetricsConsumerInterceptor] Offset提交: topic={}, partition={}, offset={}",
                        partition.topic(), partition.partition(), offsetMetadata.offset());
            });

        } catch (Exception ex) {
            log.error("[MetricsConsumerInterceptor] Offset提交失败: {} partitions", offsets.size(), ex);
        }
    }

    /**
     * 拦截器关闭时的统计报告输出
     * <p>
     * 执行时机：Consumer 关闭时
     * 作用：输出最终的统计结果，用于监控和运维分析
     */
    @Override
    public void close() {
        // 输出全局统计信息
        long totalSuccess = successCount.get();
        long totalFailure = failureCount.get();
        long totalMessages = totalSuccess + totalFailure;
        long totalCommits = commitCount.get();

        log.info("[MetricsConsumerInterceptor] === 消费者指标统计报告 ===");
        log.info("[MetricsConsumerInterceptor] 总消息消费数: {}, 成功: {}, 失败: {}", totalMessages, totalSuccess, totalFailure);
        log.info("[MetricsConsumerInterceptor] 总Offset提交数: {}", totalCommits);

        if (totalMessages > 0) {
            double successRate = (double) totalSuccess / totalMessages * 100;
            log.info("[MetricsConsumerInterceptor] 成功率: {:.2f}%", successRate);
        }

        // 输出分主题统计信息
        if (!topicSuccess.isEmpty()) {
            log.info("[MetricsConsumerInterceptor] === 按Topic消费统计 ===");
            topicSuccess.forEach((topic, count) ->
                    log.info("[MetricsConsumerInterceptor] Topic: {}, 消费数量: {}", topic, count.get())
            );
        }

        // 输出分分区统计信息
        if (!partitionSuccess.isEmpty()) {
            log.info("[MetricsConsumerInterceptor] === 按Partition消费统计 ===");
            partitionSuccess.forEach((partition, count) ->
                    log.info("[MetricsConsumerInterceptor] Partition: {}, 消费数量: {}", partition, count.get())
            );
        }

        log.info("[MetricsConsumerInterceptor] === 消费者指标统计报告结束 ===");
    }

    /**
     * 拦截器配置初始化
     * <p>
     * 执行时机：拦截器创建时
     * 作用：读取配置参数、初始化统计数据结构等
     *
     * @param configs 消费者配置参数
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 本拦截器使用默认配置，无需特殊初始化
        // 可以在这里读取自定义配置，如统计间隔、输出格式等
        log.info("[MetricsConsumerInterceptor] 拦截器配置完成，参数数量: {}", configs.size());
    }
}
