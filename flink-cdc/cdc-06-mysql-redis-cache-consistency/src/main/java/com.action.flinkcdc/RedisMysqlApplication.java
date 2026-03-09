package com.action.flinkcdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@Slf4j
public class RedisMysqlApplication {

    @Bean
    public CommandLineRunner init() {
        return args -> {
            log.info("开始初始化 Flink CDC 作业...");
            // 1. 构建 MySQL CDC Source（显式指定启动模式为 initial，确保先快照再消费 binlog）
            MySqlSource<String> source = MySqlSource.<String>builder()
                    .hostname("192.168.56.12")
                    .port(3306)
                    .databaseList("sakila")
                    .tableList("sakila.actor")
                    .username("root")
                    .password("MySql@1111")
                    .serverTimeZone("Asia/Shanghai")  // 设置正确时区
                    .includeSchemaChanges(true)  // 排除 schema 变更事件
                    .startupOptions(StartupOptions.initial())  // 先全量后增量
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .build();

            // 2. 配置 Flink 环境（Checkpoint 持久化 + Web UI 端口）
            Configuration flinkConfig = new Configuration();
            flinkConfig.setInteger(RestOptions.PORT, 8081);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

            env.enableCheckpointing(5000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.getCheckpointConfig().setCheckpointTimeout(10000);
            env.setParallelism(1);

            // 3. 构建数据流并添加 Sink
            DataStreamSource<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL-CDC-Source");
            dataStream.addSink(new CacheSink()).name("Redis-Cache-Sink");

            log.info("Flink CDC 作业初始化完成，开始执行...");
            env.execute("MySQL-To-Redis-Cache-Consistency");
            log.info("Flink CDC 作业已停止");
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(RedisMysqlApplication.class, args);
    }

}
