package com.action.cep;

import com.action.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 登录失败检测示例（基础版）
 * 演示如何使用 Flink CEP 检测连续三次登录失败事件
 *
 * 功能说明：
 * 1. 定义 Pattern，检测连续三个登录失败事件
 * 2. 将 Pattern 应用到流上，检测匹配的复杂事件
 * 3. 提取匹配的复杂事件，包装成报警信息输出
 */
public class LoginFailDetectExample {
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

        // ==================== 2. 定义 Pattern，连续的三个登录失败事件 ====================
        // 使用 begin() 定义初始模式，next() 表示严格近邻关系（中间不能有其他事件）
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")    // 以第一个登录失败事件开始
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("second")    // 接着是第二个登录失败事件（严格近邻）
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("third")     // 接着是第三个登录失败事件（严格近邻）
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                });

        // ==================== 3. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream ====================
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        // ==================== 4. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出 ====================
        patternStream
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        // 从 Map 中提取匹配的事件，key 是模式名称，value 是匹配的事件列表
                        LoginEvent first = map.get("first").get(0);
                        LoginEvent second = map.get("second").get(0);
                        LoginEvent third = map.get("third").get(0);
                        return first.userId + " 连续三次登录失败！登录时间：" + first.timestamp
                                + ", " + second.timestamp + ", " + third.timestamp;
                    }
                })
                .print("warning");

        env.execute();
    }
}

