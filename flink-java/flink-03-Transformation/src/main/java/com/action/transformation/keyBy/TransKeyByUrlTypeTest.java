package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 按 URL 类型分组示例
 * 使用 keyBy 按 URL 类型（如 home、cart、prod）进行分组统计
 * <p>
 * 特点说明：
 * - 功能：按 URL 类型进行分组，统计每种类型的访问次数
 * - 实现方式：提取 URL 类型作为 key，然后进行分组统计
 * - 使用场景：页面访问分析、URL 类型统计、业务指标分析等
 * <p>
 * 预期输出：
 * urlType> (home,3)
 * urlType> (cart,2)
 * urlType> (prod,2)
 */
public class TransKeyByUrlTypeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L),
                new Event("Alice", "./cart", 5000L)
        );

        // 按 URL 类型分组统计
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        // 提取 URL 类型（如 ./home -> home, ./cart -> cart, ./prod?id=1 -> prod）
                        String urlType = extractUrlType(value.url);
                        return Tuple2.of(urlType, 1L);
                    }

                    /**
                     * 提取 URL 类型
                     */
                    private String extractUrlType(String url) {
                        if (url.contains("./home")) {
                            return "home";
                        } else if (url.contains("./cart")) {
                            return "cart";
                        } else if (url.contains("./prod")) {
                            return "prod";
                        } else {
                            return "other";
                        }
                    }
                })
                .keyBy(r -> r.f0)  // 按 URL 类型分组
                .sum(1)             // 统计每种类型的访问次数
                .print("urlType");

        env.execute();
    }
}

