package com.action.transformation.reduce;


import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 转换算子示例
 * 使用 reduce() 方法对数据流进行归约操作
 * <p>
 * 特点说明：
 * - 功能：对数据流进行归约操作，将两个元素合并为一个元素
 * - 输入输出：两个输入元素合并为一个输出元素
 * - 实现方式：实现 ReduceFunction<T> 接口，定义两个元素的合并逻辑
 * - 使用场景：累加统计、最大值/最小值计算、自定义聚合等
 * - 注意事项：需要先使用 keyBy() 进行分组
 * <p>
 * 预期输出：
 * (用户, PV统计值)
 * ...
 * (最大PV用户, 最大PV值)
 */
public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这里的使用了之前自定义数据源小节中的ClickSource()
        env.addSource(new ClickSource())
                // 将Event数据类型转换成元组类型
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event e) throws Exception {
                        return Tuple2.of(e.user, 1L);
                    }
                })
                .keyBy(r -> r.f0) // 使用用户名来进行分流
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 每到一条数据，用户pv的统计值加1
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .keyBy(r -> true) // 为每一条数据分配同一个key，将聚合结果发送到一条流中去
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 将累加器更新为当前最大的pv统计值，然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                })
                .print();

        env.execute();

    }
}

