package com.action.kafka05kafkatemplatesend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 系统事件消息对象
 * 用于演示系统事件消息发送
 *
 * @author action
 * @since 2024
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SystemEventMessage {
    
    /**
     * 事件ID
     */
    private String eventId;
    
    /**
     * 事件类型
     */
    private String eventType;
    
    /**
     * 事件级别
     */
    private String level;
    
    /**
     * 事件描述
     */
    private String description;
    
    /**
     * 事件时间
     */
    private LocalDateTime eventTime;
    
    /**
     * 服务名称
     */
    private String serviceName;
    
    /**
     * 主机IP
     */
    private String hostIp;
    
    /**
     * 用户ID
     */
    private Long userId;
    
    /**
     * 扩展属性
     */
    private Map<String, Object> attributes;
    
    /**
     * 状态
     */
    private String status;
}