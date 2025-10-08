package com.action.kafka09objectmessage.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * 用户实体类
 * 
 * 功能说明：
 * 1) 用于演示 Kafka 对象消息的序列化和反序列化
 * 2) 包含基本的用户信息字段
 * 3) 实现 Serializable 接口，支持序列化
 * 
 * 实现细节：
 * - 使用 Lombok 注解简化代码编写
 * - @Builder: 提供建造者模式，便于对象构建
 * - @AllArgsConstructor: 生成全参构造函数
 * - @NoArgsConstructor: 生成无参构造函数
 * - @Data: 生成 getter、setter、toString、equals、hashCode 方法
 * - 实现 Serializable 接口，确保对象可以被序列化
 * 
 * 字段说明：
 * - id: 用户ID，唯一标识
 * - phone: 手机号码，联系方式
 * - birthDay: 生日，日期类型
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class User implements Serializable {

    /**
     * 用户ID
     * 唯一标识用户
     */
    private int id;

    /**
     * 手机号码
     * 用户联系方式
     */
    private String phone;

    /**
     * 生日
     * 用户出生日期
     */
    private Date birthDay;
}
