package com.action.cep;

import com.action.LoginEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * NFA 状态机示例
 * 演示如何使用状态机（NFA）的方式实现连续三次登录失败检测
 *
 * 功能说明：
 * 1. 使用状态机的方式实现复杂事件检测，不依赖 CEP 库
 * 2. 通过状态转移来跟踪匹配进度
 * 3. 当检测到匹配的复杂事件时，输出报警信息
 *
 * 注意：这是一个不使用 CEP 库的替代实现方式，展示了 CEP 底层的工作原理
 */
public class NFAExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 获取登录事件流，这里与时间无关，就不生成水位线了 ====================
        KeyedStream<LoginEvent, String> stream = env.fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
                )
                .keyBy(r -> r.userId); // 按照用户ID分组

        // ==================== 2. 将数据依次输入状态机进行处理 ====================
        DataStream<String> alertStream = stream
                .flatMap(new StateMachineMapper());

        alertStream.print("warning");

        env.execute();
    }

    /**
     * 状态机映射器
     * 使用状态机的方式检测连续三次登录失败
     */
    @SuppressWarnings("serial")
    public static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {

        // ==================== 声明当前用户对应的状态 ====================
        private ValueState<State> currentState;

        @Override
        public void open(Configuration conf) {
            // ==================== 获取状态对象 ====================
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent event, Collector<String> out) throws Exception {
            // ==================== 获取状态，如果状态为空，置为初始状态 ====================
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            // ==================== 基于当前状态，输入当前事件时跳转到下一状态 ====================
            State nextState = state.transition(event.eventType);

            if (nextState == State.Matched) {
                // ==================== 如果检测到匹配的复杂事件，输出报警信息 ====================
                out.collect(event.userId + " 连续三次登录失败");
                // 需要跳转回S2状态，这里直接不更新状态就可以了
            } else if (nextState == State.Terminal) {
                // ==================== 如果到了终止状态，就重置状态，准备重新开始 ====================
                currentState.update(State.Initial);
            } else {
                // ==================== 如果还没结束，更新状态（状态跳转），继续读取事件 ====================
                currentState.update(nextState);
            }
        }
    }

    /**
     * 状态机实现
     * 定义状态和状态转移规则
     */
    public enum State {

        Terminal,    // 匹配失败，当前匹配终止

        Matched,    // 匹配成功

        // S2状态：已经匹配了两次登录失败，再匹配一次失败就成功，匹配成功就终止
        S2(new Transition("fail", Matched), new Transition("success", Terminal)),

        // S1状态：已经匹配了一次登录失败，再匹配一次失败进入S2，匹配成功就终止
        S1(new Transition("fail", S2), new Transition("success", Terminal)),

        // 初始状态：匹配失败进入S1，匹配成功就终止
        Initial(new Transition("fail", S1), new Transition("success", Terminal));

        private final Transition[] transitions;    // 状态转移规则

        // ==================== 状态的构造方法，可以传入一组状态转移规则来定义状态 ====================
        State(Transition... transitions) {
            this.transitions = transitions;
        }

        // ==================== 状态的转移方法，根据当前输入事件类型，从定义好的转移规则中找到下一个状态 ====================
        public State transition(String eventType) {
            for (Transition t : transitions) {
                if (t.getEventType().equals(eventType)) {
                    return t.getTargetState();
                }
            }

            // ==================== 如果没有找到转移规则，说明已经结束，回到初始状态 ====================
            return Initial;
        }
    }

    /**
     * 定义状态转移类，包括两个属性：当前事件类型和目标状态
     */
    public static class Transition implements Serializable {
        private static final long serialVersionUID = 1L;

        // ==================== 触发状态转移的当前事件类型 ====================
        private final String eventType;

        // ==================== 转移的目标状态 ====================
        private final State targetState;

        public Transition(String eventType, State targetState) {
            this.eventType = checkNotNull(eventType);
            this.targetState = checkNotNull(targetState);
        }

        public String getEventType() {
            return eventType;
        }

        public State getTargetState() {
            return targetState;
        }
    }
}

