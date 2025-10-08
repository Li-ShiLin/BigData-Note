package com.action.kafka06producerpartitionstrategy.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 消息模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {
    
    /**
     * 消息ID
     */
    private String messageId;
    
    /**
     * 消息内容
     */
    private String content;
    
    /**
     * 消息类型
     */
    private String messageType;
    
    /**
     * 发送时间
     */
    private LocalDateTime sendTime;
    
    /**
     * 分区键
     */
    private String partitionKey;
    
    /**
     * 用户ID
     */
    private String userId;
    
    /**
     * 优先级
     */
    private Integer priority;
    
    public Message(String messageId, String content, String messageType) {
        this.messageId = messageId;
        this.content = content;
        this.messageType = messageType;
        this.sendTime = LocalDateTime.now();
    }
    
    public Message(String messageId, String content, String messageType, String partitionKey) {
        this.messageId = messageId;
        this.content = content;
        this.messageType = messageType;
        this.partitionKey = partitionKey;
        this.sendTime = LocalDateTime.now();
    }
}
