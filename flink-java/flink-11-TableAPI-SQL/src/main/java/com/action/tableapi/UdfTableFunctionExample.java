package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 自定义表函数（UDF）示例
 * 演示如何实现和使用自定义表函数
 *
 * 功能说明：
 * 1. 自定义一个表函数 SplitFunction，用于将字符串拆分成多行
 * 2. 在表环境中注册自定义函数
 * 3. 在 SQL 中使用 LATERAL TABLE 语法调用表函数
 * 4. 表函数是"一对多"的转换，输入一个值，输出多行数据
 */
public class UdfTableFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // ==================== 3. 将数据流转换成表并注册为虚拟表 ====================
        tableEnv.createTemporaryView("EventTable", eventStream);

        // ==================== 4. 注册自定义表函数 ====================
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        // ==================== 5. 在 SQL 中调用表函数 ====================
        // 使用 LATERAL TABLE 语法生成侧向表，然后与原始表进行联结
        // 方式一：交叉联结（CROSS JOIN）
        Table result1 = tableEnv.sqlQuery(
                "SELECT user, url, word, length " +
                        "FROM EventTable, LATERAL TABLE(SplitFunction(url)) AS T(word, length)"
        );

        // 方式二：左联结（LEFT JOIN）
        Table result2 = tableEnv.sqlQuery(
                "SELECT user, url, word, length " +
                        "FROM EventTable " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(url)) AS T(word, length) ON TRUE"
        );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(result1).print("交叉联结结果");
        tableEnv.toDataStream(result2).print("左联结结果");

        // ==================== 7. 执行程序 ====================
        env.execute();
    }

    /**
     * 自定义表函数：分隔字符串
     * 继承 TableFunction<T> 抽象类，T 是返回数据的类型
     * 实现 eval() 求值方法，通过 collect() 方法发送输出数据
     * 输出类型为 Row，包含两个字段：word（字符串）和 length（长度）
     */
    public static class SplitFunction extends TableFunction<Tuple2<String, Integer>> {
        // eval() 方法没有返回类型，通过 collect() 方法发送数据
        public void eval(String str) {
            // 按照 "?" 分隔字符串
            String[] fields = str.split("\\?");
            for (String field : fields) {
                // 使用 collect() 方法发送一行数据
                collect(Tuple2.of(field, field.length()));
            }
        }
    }
}

