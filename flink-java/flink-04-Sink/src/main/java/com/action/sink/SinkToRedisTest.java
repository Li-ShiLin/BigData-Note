package com.action.sink;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 输出到 Redis 示例
 * 使用 RedisSink 将数据写入 Redis
 * <p>
 * 特点说明：
 * - 功能：将数据流写入 Redis 数据库
 * - 连接器：使用 Apache Bahir 提供的 Flink-Redis 连接器
 * - 数据结构：支持多种 Redis 数据结构（string、hash、list、set 等）
 * - 使用场景：实时缓存、数据存储、会话管理、计数器等
 */
public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建一个到 redis 连接的配置
        // 注意：如果 Redis 配置了密码，需要使用 setPassword() 方法设置密码
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("server01")
                .setPort(6379)
                .setPassword("Redis@2024!Secure#Pass")
                .build();

        env.addSource(new ClickSource())
                .addSink(new RedisSink<Event>(conf, new MyRedisMapper()));

        env.execute();
    }

    /**
     * Redis 映射类
     * 实现 RedisMapper 接口，定义如何将数据转换成可以写入 Redis 的类型
     */
    public static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public String getKeyFromData(Event e) {
            return e.user;
        }

        @Override
        public String getValueFromData(Event e) {
            return e.url;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }
    }
}

