package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 自定义表聚合函数（UDF）示例
 * 演示如何实现和使用自定义表聚合函数
 *
 * 功能说明：
 * 1. 自定义一个表聚合函数 Top2，用于查询一组数中最大的两个
 * 2. 在表环境中注册自定义函数
 * 3. 在 Table API 中使用 flatAggregate() 方法调用表聚合函数
 * 4. 表聚合函数是"多对多"的转换，多行数据聚合成多行数据
 */
public class UdfTableAggregateFunctionExample {
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
        // 为了演示，我们将 timestamp 字段作为数值
        tableEnv.createTemporaryView("MyTable", eventStream,
                $("user").as("myField"),
                $("timestamp").as("value"));

        // ==================== 4. 注册表聚合函数 ====================
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        // ==================== 5. 在 Table API 中调用表聚合函数 ====================
        // 使用 flatAggregate() 方法调用表聚合函数
        // 对 MyTable 中数据按 myField 字段进行分组聚合，统计 value 值最大的两个
        // 并将聚合结果的两个字段重命名为 value 和 rank
        Table result = tableEnv.from("MyTable")
                .groupBy($("myField"))
                .flatAggregate(call("Top2", $("value")).as("value", "rank"))
                .select($("myField"), $("value"), $("rank"));

        // ==================== 6. 将结果表转换成更新日志流并打印输出 ====================
        // 表聚合函数是更新查询，必须使用 toChangelogStream() 转换
        tableEnv.toChangelogStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }

    /**
     * 聚合累加器的类型定义
     * 包含最大的第一和第二两个数据
     */
    public static class Top2Accumulator {
        public Long first;
        public Long second;
    }

    /**
     * 自定义表聚合函数：查询一组数中最大的两个
     * 继承 TableAggregateFunction<T, ACC>，T 是输出结果类型，ACC 是累加器类型
     * 必须实现的方法：
     * - createAccumulator()：创建累加器
     * - accumulate()：累加计算方法，每来一行数据都会调用
     * - emitValue()：输出最终计算结果，通过 Collector 发送多行数据
     */
    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {
        /**
         * 创建累加器
         */
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Long.MIN_VALUE;    // 为方便比较，初始值给最小值
            acc.second = Long.MIN_VALUE;
            return acc;
        }

        /**
         * 累加计算方法，每来一个数据调用一次，判断是否更新累加器
         * 方法名必须为 accumulate，必须是 public
         */
        public void accumulate(Top2Accumulator acc, Long value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        /**
         * 输出最终计算结果
         * 输出 (数值，排名) 的二元组，输出两行数据
         * 方法名必须为 emitValue，必须是 public
         * 通过 Collector 的 collect() 方法发送多行数据
         */
        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Long, Integer>> out) {
            if (acc.first != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }
}

