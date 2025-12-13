package com.action.transformation.flatmap;


import com.action.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap URL 解析示例
 * 使用 flatMap() 方法解析 URL 参数并输出多个键值对
 * <p>
 * 特点说明：
 * - 功能：解析 URL 中的查询参数，将每个参数作为独立的键值对输出
 * - 输入输出：一个包含查询参数的 URL 对应多个键值对
 * - 实现方式：解析 URL 参数，分割成键值对，收集到输出流
 * - 使用场景：URL 解析、参数提取、数据清洗等
 * <p>
 * 预期输出：
 * param:id=1
 * param:category=electronics
 * param:price=100
 * param:name=laptop
 * param:brand=dell
 */
public class TransFlatmapUrlParseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./prod?id=1&category=electronics&price=100", 1000L),
                new Event("Bob", "./prod?name=laptop&brand=dell", 2000L)
        );

        // 解析 URL 中的查询参数
        stream.flatMap(new UrlParameterParser()).print();

        env.execute();
    }

    public static class UrlParameterParser implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            String url = event.url;
            // 检查 URL 是否包含查询参数
            if (url.contains("?")) {
                // 提取查询参数字符串（? 后面的部分）
                String queryString = url.substring(url.indexOf("?") + 1);
                // 按 & 分割参数
                String[] params = queryString.split("&");
                // 遍历每个参数，收集到输出流
                for (String param : params) {
                    if (!param.isEmpty()) {
                        out.collect("param:" + param);
                    }
                }
            }
        }
    }
}

