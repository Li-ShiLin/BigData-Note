package com.action.cep;

import com.action.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 登录失败检测示例（改进版）
 * 演示如何使用循环模式和 PatternProcessFunction 检测连续三次登录失败事件
 *
 * 功能说明：
 * 1. 使用 times(3).consecutive() 定义循环模式，检测连续三个登录失败事件
 * 2. 使用 PatternProcessFunction 处理匹配的复杂事件
 * 3. 这种方式比基础版更加简洁，易于扩展到更多次数的检测
 */
public class LoginFailDetectProExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 获取登录事件流，并提取时间戳、生成水位线 ====================
        KeyedStream<LoginEvent, String> stream = env
                .fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<LoginEvent>() {
                                            @Override
                                            public long extractTimestamp(LoginEvent loginEvent, long l) {
                                                return loginEvent.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(r -> r.userId); // 按照用户ID进行分组

        // ==================== 2. 定义 Pattern，使用循环模式检测连续三个登录失败事件 ====================
        // times(3) 表示匹配3次，consecutive() 表示严格连续（中间不能有其他事件）
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("fail")    // 第一个登录失败事件
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .times(3).consecutive();    // 指定是严格紧邻的三次登录失败

        // ==================== 3. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream ====================
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        // ==================== 4. 使用 PatternProcessFunction 处理匹配的复杂事件 ====================
        // PatternProcessFunction 是官方推荐的处理方式，功能更加强大和灵活
        SingleOutputStreamOperator<String> warningStream = patternStream
                .process(new PatternProcessFunction<LoginEvent, String>() {
                    @Override
                    public void processMatch(Map<String, List<LoginEvent>> match, Context ctx, Collector<String> out) throws Exception {
                        // 提取三次登录失败事件
                        // 由于使用了循环模式，所有匹配的事件都在同一个 List 中
                        LoginEvent firstFailEvent = match.get("fail").get(0);
                        LoginEvent secondFailEvent = match.get("fail").get(1);
                        LoginEvent thirdFailEvent = match.get("fail").get(2);

                        out.collect(firstFailEvent.userId + " 连续三次登录失败！登录时间：" +
                                firstFailEvent.timestamp + ", " +
                                secondFailEvent.timestamp + ", " +
                                thirdFailEvent.timestamp);
                    }
                });

        // ==================== 5. 打印输出 ====================
        warningStream.print("warning");

        env.execute();
    }
}

