package com.action.kafka05kafkatemplatesend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 用户消息对象
 * 用于演示简单对象消息发送
 *
 * @author action
 * @since 2024
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserMessage {
    
    /**
     * 用户ID
     */
    private Long userId;
    
    /**
     * 用户名
     */
    private String username;
    
    /**
     * 消息内容
     */
    private String content;
    
    /**
     * 消息类型
     */
    private String messageType;
    
    /**
     * 创建时间
     */
    private LocalDateTime createTime;
    
    /**
     * 状态
     */
    private String status;
    
    // 移除扩展信息字段
}