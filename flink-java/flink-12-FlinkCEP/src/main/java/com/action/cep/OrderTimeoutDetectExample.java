package com.action.cep;

import com.action.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 订单超时检测示例
 * 演示如何使用 Flink CEP 检测订单创建后15分钟内未支付的情况
 *
 * 功能说明：
 * 1. 定义 Pattern：订单创建事件后，在15分钟内应该有支付事件
 * 2. 使用 within() 方法设置时间限制
 * 3. 使用 TimedOutPartialMatchHandler 处理超时的部分匹配
 * 4. 通过侧输出流输出超时订单信息
 */
public class OrderTimeoutDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 获取订单事件流，并提取时间戳、生成水位线 ====================
        KeyedStream<OrderEvent, String> stream = env
                .fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderEvent>() {
                                            @Override
                                            public long extractTimestamp(OrderEvent event, long l) {
                                                return event.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(order -> order.orderId);    // 按照订单ID分组

        // ==================== 2. 定义 Pattern ====================
        // 首先是下单事件，之后是支付事件；中间可以修改订单，使用宽松近邻
        // within() 限制在15分钟之内完成支付
        Pattern<OrderEvent, ?> pattern = Pattern
                .<OrderEvent>begin("create")    // 首先是下单事件
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")    // 之后是支付事件；中间可以修改订单，宽松近邻
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));    // 限制在15分钟之内

        // ==================== 3. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream ====================
        PatternStream<OrderEvent> patternStream = CEP.pattern(stream, pattern);

        // ==================== 4. 定义一个侧输出流标签，用于标识超时侧输出流 ====================
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };

        // ==================== 5. 处理匹配的复杂事件 ====================
        // 使用 PatternProcessFunction 并实现 TimedOutPartialMatchHandler 接口
        // 这样可以同时处理正常匹配和超时部分匹配的情况
        SingleOutputStreamOperator<String> payedOrderStream = patternStream
                .process(new OrderPayPatternProcessFunction());

        // ==================== 6. 将正常匹配和超时部分匹配的处理结果流打印输出 ====================
        payedOrderStream.print("payed");
        payedOrderStream.getSideOutput(timeoutTag).print("timeout");

        env.execute();
    }

    /**
     * 实现自定义的 PatternProcessFunction，需实现 TimedOutPartialMatchHandler 接口
     * 用于处理正常匹配和超时部分匹配的情况
     */
    public static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String>
            implements TimedOutPartialMatchHandler<OrderEvent> {

        // ==================== 处理正常匹配事件 ====================
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            // 提取支付事件
            OrderEvent payEvent = match.get("pay").get(0);
            out.collect("订单 " + payEvent.orderId + " 已支付！");
        }

        // ==================== 处理超时未支付事件 ====================
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            // 提取创建事件
            OrderEvent createEvent = match.get("create").get(0);
            // 通过侧输出流输出超时信息
            ctx.output(new OutputTag<String>("timeout") {
            }, "订单 " + createEvent.orderId + " 超时未支付！用户为：" + createEvent.userId);
        }
    }
}

