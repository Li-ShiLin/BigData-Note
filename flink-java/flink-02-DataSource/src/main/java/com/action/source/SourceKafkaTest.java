package com.action.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从 Kafka 读取数据源示例
 * 使用 FlinkKafkaConsumer 从 Kafka 主题读取数据
 * 
 * 前置条件：
 * 1. 在 server01 上使用 KRaft 模式启动 Kafka 集群
 * 2. 创建主题：./kafka-topics.sh --create --topic clicks --bootstrap-server server01:9092 --partitions 1 --replication-factor 1
 * 3. 向主题发送数据：./kafka-console-producer.sh --topic clicks --bootstrap-server server01:9092
 * 
 * 特点说明：
 * - 适用场景：从 Kafka 主题读取实时数据流
 * - 数据特点：无界数据流
 * - 配置要点：
 *   - bootstrap.servers：Kafka 服务器地址
 *   - group.id：消费者组 ID
 *   - key.deserializer / value.deserializer：反序列化器
 *   - auto.offset.reset：偏移量重置策略（latest/earliest）
 * 
 * 运行方式：
 * 
 * 完整测试流程：
 * 
 * 步骤1：在 server01 上启动 Kafka（KRaft 模式）
 * # 在 server01 上执行
 * cd /opt/module/kafka_2.13-3.7.0/bin
 * # 生成 UUID 并格式化（首次启动需要）
 * ./kafka-storage.sh random-uuid
 * ./kafka-storage.sh format -t <生成的UUID> -c ../config/kraft/server.properties
 * # 启动 Kafka
 * ./kafka-server-start.sh -daemon ../config/kraft/server.properties
 * 
 * 步骤2：创建主题
 * cd /opt/module/kafka_2.13-3.7.0/bin
 * ./kafka-topics.sh --create \
 *     --topic clicks \
 *     --bootstrap-server server01:9092 \
 *     --partitions 1 \
 *     --replication-factor 1
 * 
 * 步骤3：启动 Flink 程序（在本地或客户端）
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceKafkaTest"
 * 
 * 步骤4：向主题发送数据（在 server01 上）
 * cd /opt/module/kafka_2.13-3.7.0/bin
 * ./kafka-console-producer.sh \
 *     --topic clicks \
 *     --bootstrap-server server01:9092
 * # 输入消息，每行一条
 * # message1
 * # message2
 * # message3
 * 
 * 步骤5：观察 Flink 程序输出
 * Flink 程序会实时打印从 Kafka 接收到的消息：
 * Kafka> message1
 * Kafka> message2
 * Kafka> message3
 */
public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 配置 Kafka 连接属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "server01:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 创建 Kafka 消费者
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(
                "clicks",                    // 主题名称
                new SimpleStringSchema(),    // 反序列化器
                properties                   // Kafka 配置
        ));

        stream.print("Kafka");

        env.execute();
    }
}

/**
 * 注意事项：
 * 
 * 1. Kafka 配置问题
 * 常见配置项：
 * - bootstrap.servers：Kafka 服务器地址
 * - group.id：消费者组 ID
 * - key.deserializer / value.deserializer：反序列化器
 * - auto.offset.reset：偏移量重置策略（latest/earliest）
 * 
 * 注意事项：
 * - 确保 Kafka 集群已启动
 * - 确保主题已创建
 * - 确保网络连接正常
 * 
 * 2. 无界流处理
 * 特点：
 * - 无界流程序不会自动结束
 * - 需要手动停止（IDE 停止按钮或 Ctrl+C）
 * 
 * 停止方式：
 * - 在 IDE 中点击停止按钮
 * - 在终端按 Ctrl+C
 * 
 * 常见问题：
 * 
 * Q2: Kafka 连接失败
 * 原因：Kafka 未启动、配置错误或网络不通
 * 解决方案：
 * # 检查 Kafka 是否启动
 * ps -ef | grep kafka
 * # 检查主题是否存在
 * cd /opt/module/kafka_2.13-3.7.0/bin
 * ./kafka-topics.sh --list --bootstrap-server server01:9092
 * # 测试网络连接
 * telnet server01 9092
 * # 或
 * telnet 192.168.56.11 9092
 * # 检查配置文件中的 advertised.listeners 是否正确
 * cat /opt/module/kafka_2.13-3.7.0/config/kraft/server.properties | grep advertised.listeners
 * # 确保 advertised.listeners 配置为可访问的地址
 * # 如果使用主机名，确保客户端能解析该主机名
 * # 如果使用 IP，确保 IP 地址正确
 */

