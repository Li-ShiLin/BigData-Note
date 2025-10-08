package com.action.kafka07producerinterceptors.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 消息修改拦截器 - 演示在消息发送前进行定制化处理
 * 
 * 功能说明：
 * 1) 修改消息内容：为所有字符串消息添加统一前缀标识
 * 2) 添加审计 Header：为消息添加审计标识，便于后续追踪和审计
 * 3) 演示拦截器链的执行顺序和异常处理机制
 * 
 * 实现细节：
 * - 实现 ProducerInterceptor<K, V> 接口，泛型参数指定键值类型
 * - onSend() 方法在消息序列化前被调用，可以修改消息内容
 * - 必须返回新的 ProducerRecord 对象，不能修改原对象
 * - 异常会中断拦截器链，后续拦截器不会执行
 * 
 * 关键参数说明：
 * - ProducerRecord: Kafka 消息记录，包含 topic、partition、key、value、headers 等
 * - RecordHeader: 消息头，用于存储元数据信息
 * - StandardCharsets.UTF_8: 确保字符编码一致性
 */
public class ModifyRecordInterceptor implements ProducerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(ModifyRecordInterceptor.class);

    /**
     * 消息发送前的拦截处理
     * 
     * 执行时机：消息被发送到序列化器之前
     * 作用：可以修改消息内容、添加/修改 headers、进行数据转换等
     * 
     * @param record 原始消息记录
     * @return 修改后的消息记录，必须返回新对象
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        try {
            // 1. 获取原始消息值并进行修改
            String newValue = record.value();
            if (newValue != null) {
                // 为消息添加统一前缀，便于识别经过拦截器处理的消息
                newValue = "[modified-by-interceptor] " + newValue;
            }

            // 2. 创建新的 ProducerRecord 对象
            // 注意：必须创建新对象，不能直接修改原 record
            ProducerRecord<String, String> newRecord = new ProducerRecord<>(
                    record.topic(),        // 主题名称
                    record.partition(),    // 分区号（null 表示由分区器决定）
                    record.timestamp(),    // 时间戳（null 表示使用当前时间）
                    record.key(),          // 消息键
                    newValue,              // 修改后的消息值
                    record.headers()       // 原始 headers（会被复制）
            );

            // 3. 添加自定义审计 Header
            // 用于标识消息已被拦截器处理，便于后续审计和追踪
            newRecord.headers().add(new RecordHeader("x-audit", "intercepted".getBytes(StandardCharsets.UTF_8)));
            
            log.debug("Message modified by interceptor: topic={}, key={}, originalValue={}, newValue={}", 
                     record.topic(), record.key(), record.value(), newValue);
            
            return newRecord;
        } catch (Exception ex) {
            // 4. 异常处理：记录错误并重新抛出
            // 拦截器异常会中断整个发送流程，后续拦截器不会执行
            log.error("ModifyRecordInterceptor onSend error for topic={}, key={}", 
                     record.topic(), record.key(), ex);
            throw ex;
        }
    }

    /**
     * 消息发送确认回调
     * 
     * 执行时机：消息发送成功或失败后
     * 作用：可以用于统计、监控、日志记录等
     * 
     * @param metadata 发送成功时的元数据信息（包含 topic、partition、offset 等）
     * @param exception 发送失败时的异常信息（成功时为 null）
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 本拦截器专注于消息修改，统计功能由 MetricsProducerInterceptor 负责
        // 这里可以添加其他处理逻辑，如审计日志记录等
        if (exception != null) {
            log.warn("Message send failed in ModifyRecordInterceptor: {}", exception.getMessage());
        }
    }

    /**
     * 拦截器关闭时的清理工作
     * 
     * 执行时机：Producer 关闭时
     * 作用：释放资源、输出统计信息等
     */
    @Override
    public void close() {
        // 本拦截器无需特殊清理工作
        log.info("ModifyRecordInterceptor closed");
    }

    /**
     * 拦截器配置初始化
     * 
     * 执行时机：拦截器创建时
     * 作用：读取配置参数、初始化资源等
     * 
     * @param configs 生产者配置参数
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 本拦截器无需特殊配置
        log.info("ModifyRecordInterceptor configured with {} parameters", configs.size());
    }
}


