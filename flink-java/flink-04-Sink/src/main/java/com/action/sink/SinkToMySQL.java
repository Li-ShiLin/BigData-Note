package com.action.sink;

import com.action.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 输出到 MySQL（JDBC）示例
 * 使用 JdbcSink 将数据写入 MySQL 数据库
 * <p>
 * 特点说明：
 * - 功能：将数据流写入 MySQL 关系型数据库
 * - 连接器：Flink 官方提供的 JDBC 连接器
 * - 批处理：支持批量写入，提高写入效率
 * - 使用场景：结果数据存储、数据归档、报表数据、业务数据存储等
 * <p>
 * 启动 MySQL，创建数据库和表:
 * CREATE DATABASE IF NOT EXISTS userbehavior;
 * USE userbehavior;
 * <p>
 * CREATE TABLE clicks (
 * `user` VARCHAR(20) NOT NULL,
 * `url` VARCHAR(100) NOT NULL
 * );
 * <p>
 * select * from clicks;
 */
public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        stream.addSink(
                JdbcSink.sink(
                        "INSERT INTO clicks (user, url) VALUES (?, ?)",
                        (statement, r) -> {
                            statement.setString(1, r.user);
                            statement.setString(2, r.url);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/userbehavior?useSSL=false")
                                // 对于MySQL 5.x，用"com.mysql.jdbc.Driver"
                                // 对于MySQL 8.0+，用"com.mysql.cj.jdbc.Driver"
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                )
        );

        env.execute();
    }
}

