package com.action.watermark;

import com.action.ClickSourceWithWatermark;
import com.action.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 在自定义数据源中发送水位线示例
 * 
 * 注意：
 * 1. 在自定义数据源中发送了水位线以后，就不能再在程序中使用 
 *    assignTimestampsAndWatermarks 方法来生成水位线了。二者只能取其一。
 * 2. 在自定义水位线中生成水位线相比 assignTimestampsAndWatermarks 方法更加灵活，
 *    可以任意的产生周期性的、非周期性的水位线，以及水位线的大小也完全由我们自定义。
 *    所以非常适合用来编写 Flink 的测试程序，测试 Flink 的各种各样的特性。
 */
public class EmitWatermarkInSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSourceWithWatermark()).print();

        env.execute();
    }
}

