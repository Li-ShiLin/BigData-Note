package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 表和流转换示例
 * 演示如何将表转换成数据流，包括仅追加流和更新日志流
 *
 * 功能说明：
 * 1. 演示追加查询（Append Query）：使用 toDataStream() 转换
 * 2. 演示更新查询（Update Query）：使用 toChangelogStream() 转换
 * 3. 展示两种查询的区别和适用场景
 */
public class TableToStreamExample {
    public static void main(String[] args) throws Exception {
        // ==================== 1. 获取流环境 ====================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 2. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 3. 获取表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 4. 将数据流转换成表并注册为虚拟表 ====================
        // 创建虚拟视图，方便在 SQL 中引用
        tableEnv.createTemporaryView("EventTable", eventStream);

        // ==================== 5. 追加查询示例：查询 Alice 的访问 url 列表 ====================
        // 这是一个简单的条件查询，结果表中只有插入操作，没有更新操作
        // 因此可以使用 toDataStream() 直接转换
        Table aliceVisitTable = tableEnv.sqlQuery(
                "SELECT url, user FROM EventTable WHERE user = 'Alice'"
        );

        // ==================== 6. 更新查询示例：统计每个用户的点击次数 ====================
        // 这是一个分组聚合查询，结果表中会有更新操作
        // 因此必须使用 toChangelogStream() 转换，记录更新日志
        Table urlCountTable = tableEnv.sqlQuery(
                "SELECT user, COUNT(url) as cnt FROM EventTable GROUP BY user"
        );

        // ==================== 7. 将表转换成数据流，在控制台打印输出 ====================
        // 追加查询结果：使用 toDataStream()，输出只有 INSERT 操作
        tableEnv.toDataStream(aliceVisitTable).print("alice visit");

        // 更新查询结果：使用 toChangelogStream()，输出包含 INSERT、UPDATE_BEFORE、UPDATE_AFTER 操作
        tableEnv.toChangelogStream(urlCountTable).print("count");

        // ==================== 8. 执行程序 ====================
        env.execute();
    }
}

