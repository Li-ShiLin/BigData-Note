package com.action.transformation.flatmap;


import com.action.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 条件过滤和转换示例
 * 使用 flatMap() 方法根据条件输出不同数量的元素
 * <p>
 * 特点说明：
 * - 功能：根据事件的条件，输出不同数量的元素（可能为0个、1个或多个）
 * - 输入输出：根据条件决定输出元素的数量
 * - 实现方式：使用条件判断，决定是否输出以及输出多少个元素
 * - 使用场景：条件过滤、数据转换、规则匹配等
 * <p>
 * 预期输出：
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 */
public class TransFlatmapConditionalTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./fav", 3000L)
        );

        // 根据条件输出不同数量的元素
        stream.flatMap(new ConditionalFlatMap()).print();

        env.execute();
    }

    public static class ConditionalFlatMap implements FlatMapFunction<Event, Event> {
        @Override
        public void flatMap(Event event, Collector<Event> out) throws Exception {
            // 如果用户是 Mary，输出2次
            if ("Mary".equals(event.user)) {
                out.collect(event);
                out.collect(event);
            }
            // 如果用户是 Bob，输出3次
            else if ("Bob".equals(event.user)) {
                out.collect(event);
                out.collect(event);
                out.collect(event);
            }
            // 其他用户不输出（过滤掉）
            // 注意：这里不输出任何元素，相当于过滤
        }
    }
}

