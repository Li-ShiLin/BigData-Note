package com.action.window;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 处理迟到数据示例
 * 演示如何使用水位线延迟、窗口允许延迟和侧输出流来处理迟到数据
 * 在终端执行以下命令启动 Socket 服务器：
 * 运行说明
 * （1）启动 Socket 服务器*
 * 在终端执行以下命令启动 Socket 服务器：
 * nc -lk 7777
 * （2）运行代码
 * 直接运行 `ProcessLateDataExample` 类的 `main` 方法。
 * （3）输入测试数据
 * 在 Socket 服务器终端依次输入以下数据（注意：数据格式为 `user url timestamp`，用空格分隔）：
 * ```
 * Alice ./home 1000
 * Alice ./home 2000
 * Alice ./home 10000
 * Alice ./home 9000
 * Alice ./cart 12000
 * Alice ./prod?id=100 15000
 * Alice ./home 9000
 * Alice ./home 8000
 * Alice ./prod?id=200 70000
 * Alice ./home 8000
 * Alice ./prod?id=300 72000
 * Alice ./home 8000
 * ```
 */
public class ProcessLateDataExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取socket 文本流
        SingleOutputStreamOperator<Event> stream =
                env.socketTextStream("server01", 7777)
                        .map(new MapFunction<String, Event>() {
                            @Override
                            public Event map(String value) throws Exception {
                                String[] fields = value.split(" ");
                                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                            }
                        })
                        // 方式一：设置 watermark 延迟时间 ，2秒钟
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));

        // 定义侧输出流标签
        OutputTag<Event> outputTag = new OutputTag<Event>("late") {
        };

        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 方式二：允许窗口处理迟到数据 ，设置1分钟的等待时间
                .allowedLateness(Time.minutes(1))
                // 方式三： 将最后的迟到数据输出到 侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        result.print("result");
        result.getSideOutput(outputTag).print("late");

        // 为方便观察， 可以将原始数据也输出
        stream.print("input");

        env.execute();
    }

    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }
}

