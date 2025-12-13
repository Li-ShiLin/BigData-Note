package com.action.chapter08;

import com.action.chapter05.ClickSource;
import com.action.chapter05.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 侧输出流拆分：通过 OutputTag 将不同用户的事件分流，同时保留主流。
 * 相比多个 filter，更适合需要共享状态并统一在一个算子里判断的场景。
 */
public class SplitStreamByOutputTag {
    // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-pv"){};
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-pv"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource());

        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Mary")){
                    // Mary 的事件走 MaryTag 侧输出
                    ctx.output(MaryTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Bob")){
                    // Bob 的事件走 BobTag 侧输出
                    ctx.output(BobTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else {
                    // 其他用户继续沿主流流转
                    out.collect(value);
                }
            }
        });

        processedStream.getSideOutput(MaryTag).print("Mary pv");
        processedStream.getSideOutput(BobTag).print("Bob pv");
        processedStream.print("else");

        env.execute();
    }
}

