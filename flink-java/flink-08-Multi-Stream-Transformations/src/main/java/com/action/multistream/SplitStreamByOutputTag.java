package com.action.multistream;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流示例：使用侧输出流（side output）实现分流
 * 
 * 这是推荐的分流方式，使用 ProcessFunction 的侧输出流功能
 * 可以灵活地输出不同类型的数据到不同的流中
 */
public class SplitStreamByOutputTag {
    // 定义输出标签，侧输出流的数据类型为三元组 (user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> MaryTag =
            new OutputTag<Tuple3<String, String, Long>>("Mary-pv") {};
    private static OutputTag<Tuple3<String, String, Long>> BobTag =
            new OutputTag<Tuple3<String, String, Long>>("Bob-pv") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        // 使用 ProcessFunction 处理数据，根据用户类型输出到不同的侧输出流
        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Mary")) {
                    // 将 Mary 的数据输出到侧输出流，转换为三元组
                    ctx.output(MaryTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Bob")) {
                    // 将 Bob 的数据输出到侧输出流，转换为三元组
                    ctx.output(BobTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else {
                    // 其他用户的数据输出到主流，保持 Event 类型
                    out.collect(value);
                }
            }
        });

        // 获取侧输出流并打印
        processedStream.getSideOutput(MaryTag).print("Mary pv");
        processedStream.getSideOutput(BobTag).print("Bob pv");
        // 打印主流数据
        processedStream.print("else");

        env.execute();
    }
}

