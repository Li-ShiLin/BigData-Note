package com.action.transformation;



import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * POJO 聚合算子示例
 * 演示对 POJO 类型数据进行聚合操作
 * 
 * 特点说明：
 * - 功能：对 POJO 类型数据进行聚合操作
 * - 聚合方法：
 *   - max()：最大值（只更新指定字段，其他字段保持第一个元素的值）
 *   - min()：最小值（只更新指定字段，其他字段保持第一个元素的值）
 *   - maxBy()：最大值（返回完整记录）
 *   - minBy()：最小值（返回完整记录）
 * - 字段指定：通过字段名称指定聚合字段
 * - 使用场景：对象字段聚合、分组统计等
 * 
 * 预期输出：
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:04.0}
 * Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 */
public class TransPojoAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./cart", 3000L),
                new Event("Mary", "./fav", 4000L)
        );

        stream.keyBy(e -> e.user)
//                .maxBy("timestamp")
                .max("timestamp")    // 指定字段名称
                .print();

        env.execute();
    }
}

