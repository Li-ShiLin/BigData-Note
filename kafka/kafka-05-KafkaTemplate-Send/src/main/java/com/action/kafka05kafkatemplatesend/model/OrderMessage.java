package com.action.kafka05kafkatemplatesend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 订单消息对象
 * 用于演示复杂对象消息发送
 *
 * @author action
 * @since 2024
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessage {
    
    /**
     * 订单ID
     */
    private String orderId;
    
    /**
     * 用户ID
     */
    private Long userId;
    
    /**
     * 订单状态
     */
    private String status;
    
    /**
     * 订单总金额
     */
    private BigDecimal totalAmount;
    
    /**
     * 订单商品项列表
     */
    private List<OrderItem> items;
    
    /**
     * 创建时间
     */
    private LocalDateTime createTime;
    
    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
    
    /**
     * 备注
     */
    private String remark;
    
    /**
     * 订单商品项
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItem {
        /**
         * 商品ID
         */
        private String productId;
        
        /**
         * 商品名称
         */
        private String productName;
        
        /**
         * 单价
         */
        private BigDecimal price;
        
        /**
         * 数量
         */
        private Integer quantity;
        
        /**
         * 小计
         */
        private BigDecimal subtotal;
    }
}