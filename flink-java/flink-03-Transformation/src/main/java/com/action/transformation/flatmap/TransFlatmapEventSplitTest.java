package com.action.transformation.flatmap;


import com.action.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 事件拆分示例
 * 使用 flatMap() 方法将一个事件对象拆分成多个字段值
 * <p>
 * 特点说明：
 * - 功能：将 Event 对象拆分成多个字段值（user、url、timestamp）
 * - 输入输出：一个 Event 对象对应三个字段值字符串
 * - 实现方式：通过 Collector 收集每个字段的值
 * - 使用场景：数据扁平化、字段提取、数据转换等
 * <p>
 * 预期输出：
 * user:Mary
 * url:./home
 * timestamp:1000
 * user:Bob
 * url:./cart
 * timestamp:2000
 */
public class TransFlatmapEventSplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 将每个事件拆分成多个字段
        stream.flatMap(new EventSplitter()).print();

        env.execute();
    }

    public static class EventSplitter implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            // 将事件的每个字段都输出为一条记录
            out.collect("user:" + event.user);
            out.collect("url:" + event.url);
            out.collect("timestamp:" + event.timestamp);
        }
    }
}

