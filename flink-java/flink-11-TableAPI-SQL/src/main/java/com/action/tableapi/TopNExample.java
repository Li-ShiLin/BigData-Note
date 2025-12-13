package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Top N 示例
 * 演示如何使用 OVER 窗口和 ROW_NUMBER() 函数实现 Top N 查询
 *
 * 功能说明：
 * 1. 从数据流中读取用户点击事件
 * 2. 使用 OVER 窗口和 ROW_NUMBER() 函数为每行数据计算行号
 * 3. 筛选出行号小于等于 N 的数据，得到 Top N 结果
 * 4. Top N 是更新查询，结果表会有更新操作
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表并注册为虚拟表 ====================
        tableEnv.createTemporaryView("EventTable", eventStream);

        // ==================== 4. 执行 Top N 查询 ====================
        // 统计每个用户的访问事件中，按照字符长度排序的前两个 url
        // 使用嵌套查询：
        // 1. 内层查询：使用 ROW_NUMBER() OVER 窗口为每行数据计算行号
        //    - PARTITION BY `user`：按用户分组（user 是保留关键字，需要使用反引号转义）
        //    - ORDER BY CHAR_LENGTH(url) desc：按 url 字符长度降序排列
        // 2. 外层查询：筛选 row_num <= 2 的数据
        // 注意：timestamp 和 user 都是 Flink SQL 的保留关键字，需要使用反引号转义
        Table result = tableEnv.sqlQuery(
                "SELECT `user`, url, `timestamp`, row_num " +
                        "FROM (" +
                        "SELECT *, " +
                        "ROW_NUMBER() OVER (" +
                        "PARTITION BY `user` " +
                        "ORDER BY CHAR_LENGTH(url) desc" +
                        ") AS row_num " +
                        "FROM EventTable " +
                        ") " +
                        "WHERE row_num <= 2"
        );

        // ==================== 5. 将表转换成更新日志流并打印输出 ====================
        // Top N 是更新查询，必须使用 toChangelogStream() 转换
        tableEnv.toChangelogStream(result).print("Top N 结果");

        // ==================== 6. 执行程序 ====================
        env.execute();
    }
}

