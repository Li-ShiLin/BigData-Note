package com.action.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * 自定义 Sink 输出示例（HBase）
 * 使用 RichSinkFunction 自定义实现 HBase Sink
 * <p>
 * 特点说明：
 * - 功能：自定义实现将数据写入 HBase 的 Sink
 * - 实现方式：继承 RichSinkFunction，实现生命周期方法
 * - 生命周期：使用 open() 初始化连接，close() 关闭连接
 * - 使用场景：Flink 没有提供连接器的外部系统、自定义存储逻辑等
 */
public class SinkCustomtoHBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("hello", "world", "flink", "hbase")
                .addSink(
                        new RichSinkFunction<String>() {
                            public org.apache.hadoop.conf.Configuration configuration; // 管理Hbase的配置信息,这里因为Configuration的重名问题，将类以完整路径导入
                            public Connection connection; // 管理Hbase连接

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                configuration = HBaseConfiguration.create();
                                configuration.set("hbase.zookeeper.quorum", "server01:2181");
                                connection = ConnectionFactory.createConnection(configuration);
                            }

                            @Override
                            public void invoke(String value, Context context) throws Exception {
                                Table table = connection.getTable(TableName.valueOf("test")); // 表名为test
                                Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8)); // 指定rowkey

                                put.addColumn("info".getBytes(StandardCharsets.UTF_8) // 指定列名
                                        , value.getBytes(StandardCharsets.UTF_8) // 写入的数据
                                        , "1".getBytes(StandardCharsets.UTF_8)); // 写入的数据
                                table.put(put); // 执行put操作
                                table.close(); // 将表关闭
                            }

                            @Override
                            public void close() throws Exception {
                                super.close();
                                connection.close(); // 关闭连接
                            }
                        }
                );

        env.execute();
    }
}

