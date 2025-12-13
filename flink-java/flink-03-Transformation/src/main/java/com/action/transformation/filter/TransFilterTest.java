package com.action.transformation.filter;


import com.action.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Filter 转换算子示例
 * 使用 filter() 方法对数据流中的元素进行过滤
 * <p>
 * 特点说明：
 * - 功能：对数据流中的元素进行过滤，只保留满足条件的元素
 * - 输入输出：一个输入元素可能对应零个或一个输出元素
 * - 实现方式：实现 FilterFunction<T> 接口，返回 true 表示保留，false 表示过滤
 * - 使用场景：数据清洗、条件筛选、异常数据过滤等
 * <p>
 * 预期输出：
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 */
public class TransFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 传入匿名类实现FilterFunction
        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event e) throws Exception {
                return e.user.equals("Mary");
            }
        });

        // 传入FilterFunction实现类
        stream.filter(new UserFilter()).print();

        env.execute();
    }

    public static class UserFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event e) throws Exception {
            return e.user.equals("Mary");
        }
    }
}

