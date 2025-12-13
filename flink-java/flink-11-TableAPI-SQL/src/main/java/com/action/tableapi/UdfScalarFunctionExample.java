package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 自定义标量函数（UDF）示例
 * 演示如何实现和使用自定义标量函数
 *
 * 功能说明：
 * 1. 自定义一个标量函数 HashFunction，用于计算对象的哈希值
 * 2. 在表环境中注册自定义函数
 * 3. 在 SQL 中调用自定义函数
 * 4. 标量函数是"一对一"的转换，输入一个值，输出一个值
 */
public class UdfScalarFunctionExample {
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

        // ==================== 4. 注册自定义标量函数 ====================
        // createTemporarySystemFunction() 方法创建临时系统函数，函数名是全局的
        tableEnv.createTemporarySystemFunction("HashFunction", HashFunction.class);

        // ==================== 5. 在 SQL 中调用自定义函数 ====================
        // 调用方式与内置系统函数完全一样
        // 注意：user 是 Flink SQL 的保留关键字，需要使用反引号转义
        Table result = tableEnv.sqlQuery(
                "SELECT `user`, HashFunction(`user`) as hashValue FROM EventTable"
        );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }

    /**
     * 自定义标量函数：计算哈希值
     * 继承 ScalarFunction 抽象类，实现 eval() 求值方法
     * eval() 方法必须是 public，名字必须是 eval，可以重载多次
     */
    public static class HashFunction extends ScalarFunction {
        // 接受字符串类型输入，返回 INT 型输出
        // 注意：如果使用 Object 类型，需要使用 @DataTypeHint 注解指定类型
        // 这里直接使用 String 类型，因为 SQL 中调用的是 HashFunction(user)，user 是字符串类型
        public int eval(String s) {
            return s != null ? s.hashCode() : 0;
        }
    }
}

