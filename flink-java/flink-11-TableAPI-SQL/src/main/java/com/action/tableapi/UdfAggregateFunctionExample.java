package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 自定义聚合函数（UDF）示例
 * 演示如何实现和使用自定义聚合函数
 *
 * 功能说明：
 * 1. 自定义一个聚合函数 WeightedAvg，用于计算加权平均值
 * 2. 在表环境中注册自定义函数
 * 3. 在 SQL 中调用自定义函数进行分组聚合
 * 4. 聚合函数是"多对一"的转换，多行数据聚合成一个值
 */
public class UdfAggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 2. 读取数据源 ====================
        // 模拟学生分数表，包含：学生姓名、分数、权重
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
        // 这里为了演示，我们将 timestamp 字段作为分数，url 字段作为权重（简化处理）
        tableEnv.createTemporaryView("ScoreTable", eventStream,
                $("user").as("student"),
                $("timestamp").as("score"),
                $("url").as("weight"));

        // ==================== 4. 注册自定义聚合函数 ====================
        tableEnv.createTemporarySystemFunction("WeightedAvg", WeightedAvg.class);

        // ==================== 5. 在 SQL 中调用自定义聚合函数 ====================
        // 计算每个学生的加权平均分
        // 注意：这里为了演示，权重统一设为 1
        Table result = tableEnv.sqlQuery(
                "SELECT student, WeightedAvg(score, 1) as weighted_avg " +
                        "FROM ScoreTable " +
                        "GROUP BY student"
        );

        // ==================== 6. 将结果表转换成更新日志流并打印输出 ====================
        // 分组聚合是更新查询，必须使用 toChangelogStream() 转换
        tableEnv.toChangelogStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }

    /**
     * 累加器类型定义
     * 用于存储聚合的中间结果：加权总和和数据个数
     */
    public static class WeightedAvgAccumulator {
        public long sum = 0;    // 加权和
        public int count = 0;   // 数据个数
    }

    /**
     * 自定义聚合函数：计算加权平均值
     * 继承 AggregateFunction<T, ACC>，T 是输出结果类型，ACC 是累加器类型
     * 必须实现的方法：
     * - createAccumulator()：创建累加器
     * - accumulate()：累加计算方法，每来一行数据都会调用
     * - getValue()：得到最终返回结果
     */
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> {
        /**
         * 创建累加器
         * 返回类型为累加器类型 ACC
         */
        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        /**
         * 得到最终返回结果
         * 输入参数是累加器，输出类型为 T（Long）
         */
        @Override
        public Long getValue(WeightedAvgAccumulator acc) {
            if (acc.count == 0) {
                return null;    // 防止除数为 0
            } else {
                return acc.sum / acc.count;    // 计算平均值并返回
            }
        }

        /**
         * 累加计算方法，每来一行数据都会调用
         * 第一个参数是累加器，后面的参数是函数调用时传入的参数
         * 方法名必须为 accumulate，必须是 public，且无法直接 override
         */
        public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
            acc.sum += iValue * iWeight;    // 更新加权和
            acc.count += iWeight;           // 更新数据个数
        }
    }
}

