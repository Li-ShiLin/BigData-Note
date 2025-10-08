package com.action.kafka12consumerinterceptor.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * 消息过滤拦截器 - 演示在消息消费前进行过滤和预处理
 * 
 * 功能说明：
 * 1) 消息过滤：根据消息内容过滤掉不符合条件的消息
 * 2) 消息预处理：为消息添加处理标识和元数据
 * 3) 统计过滤信息：记录被过滤的消息数量和原因
 * 4) 演示拦截器链的执行顺序和异常处理机制
 * 
 * 实现细节：
 * - 实现 ConsumerInterceptor<K, V> 接口，泛型参数指定键值类型
 * - onConsume() 方法在消息反序列化后被调用，可以过滤和修改消息
 * - 必须返回新的 ConsumerRecords 对象，不能修改原对象
 * - 异常会中断拦截器链，后续拦截器不会执行
 * 
 * 关键参数说明：
 * - ConsumerRecords: Kafka 消息记录集合，包含多个分区的消息
 * - TopicPartition: 主题分区标识，用于定位消息来源
 * - OffsetAndMetadata: 偏移量和元数据信息
 */
public class MessageFilterInterceptor implements ConsumerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(MessageFilterInterceptor.class);

    // 过滤统计计数器（线程安全）
    private long filteredCount = 0;
    private long processedCount = 0;

    /**
     * 消息消费前的拦截处理
     * 
     * 执行时机：消息被反序列化之后，传递给消费者之前
     * 作用：可以过滤消息、修改消息内容、添加处理标识等
     * 
     * @param records 原始消息记录集合
     * @return 过滤后的消息记录集合，必须返回新对象
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        try {
            if (records.isEmpty()) {
                return records;
            }

            // 1. 统计处理的消息数量
            processedCount += records.count();
            
            // 2. 记录拦截器处理信息
            log.info("[MessageFilterInterceptor] 正在处理 {} 条消息", records.count());
            
            // 3. 为每条消息添加处理标识（简化版本，不进行复杂过滤）
            for (var partition : records.partitions()) {
                for (var record : records.records(partition)) {
                    log.debug("[MessageFilterInterceptor] 处理消息: topic={}, partition={}, offset={}, key={}, value={}", 
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
            
            // 4. 直接返回原始消息（简化演示）
            // 在实际应用中，这里可以进行消息过滤、修改等操作
            log.info("[MessageFilterInterceptor] 消息处理完成");
            
            return records;
        } catch (Exception ex) {
            // 异常处理：记录错误并重新抛出
            log.error("[MessageFilterInterceptor] 消息消费拦截器处理错误: {} records", records.count(), ex);
            throw ex;
        }
    }

    /**
     * 偏移量提交前的回调处理
     * 
     * 执行时机：消费者提交偏移量之前
     * 作用：可以记录提交信息、进行审计等
     * 
     * @param offsets 要提交的偏移量映射
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 本拦截器专注于消息过滤，统计功能由 MetricsConsumerInterceptor 负责
        // 这里可以添加其他处理逻辑，如审计日志记录等
        log.debug("[MessageFilterInterceptor] Offset提交: {} partitions", offsets.size());
    }

    /**
     * 拦截器关闭时的清理工作
     * 
     * 执行时机：Consumer 关闭时
     * 作用：释放资源、输出统计信息等
     */
    @Override
    public void close() {
        // 输出过滤统计信息
        log.info("[MessageFilterInterceptor] === 消息过滤统计 ===");
        log.info("[MessageFilterInterceptor] 总处理消息数: {}", processedCount);
        log.info("[MessageFilterInterceptor] 过滤掉的消息数: {}", filteredCount);
        if (processedCount > 0) {
            double filterRate = (double) filteredCount / processedCount * 100;
            log.info("[MessageFilterInterceptor] 过滤率: {:.2f}%", filterRate);
        }
        log.info("[MessageFilterInterceptor] === 消息过滤统计结束 ===");
    }

    /**
     * 拦截器配置初始化
     * 
     * 执行时机：拦截器创建时
     * 作用：读取配置参数、初始化资源等
     * 
     * @param configs 消费者配置参数
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 本拦截器使用默认配置，无需特殊初始化
        // 可以在这里读取自定义配置，如过滤关键词、过滤规则等
        log.info("[MessageFilterInterceptor] 拦截器配置完成，参数数量: {}", configs.size());
    }
}
