package com.action.kafka11batchconsumer.model;

/**
 * 用户行为数据模型
 * 
 * 功能说明：
 * 1) 定义用户行为数据的结构
 * 2) 用于模拟真实业务场景
 * 3) 支持JSON序列化和反序列化
 * 
 * 字段说明：
 * - userId: 用户ID
 * - action: 用户行为（login/view/purchase）
 * - timestamp: 行为发生时间戳
 * - partition: 目标分区号
 */
public class UserBehavior {
    
    /**
     * 用户ID
     */
    private String userId;
    
    /**
     * 用户行为
     * 可能的值：login（登录）、view（浏览）、purchase（购买）
     */
    private String action;
    
    /**
     * 行为发生时间戳
     */
    private Long timestamp;
    
    /**
     * 目标分区号
     */
    private Integer partition;
    
    /**
     * 默认构造函数
     */
    public UserBehavior() {
    }
    
    /**
     * 全参构造函数
     * 
     * @param userId 用户ID
     * @param action 用户行为
     * @param timestamp 时间戳
     * @param partition 分区号
     */
    public UserBehavior(String userId, String action, Long timestamp, Integer partition) {
        this.userId = userId;
        this.action = action;
        this.timestamp = timestamp;
        this.partition = partition;
    }
    
    // Getter 和 Setter 方法
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getAction() {
        return action;
    }
    
    public void setAction(String action) {
        this.action = action;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Integer getPartition() {
        return partition;
    }
    
    public void setPartition(Integer partition) {
        this.partition = partition;
    }
    
    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", action='" + action + '\'' +
                ", timestamp=" + timestamp +
                ", partition=" + partition +
                '}';
    }
}
