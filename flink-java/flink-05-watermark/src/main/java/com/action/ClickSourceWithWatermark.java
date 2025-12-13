package com.action;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义数据源：在数据源中发送水位线
 * 用于演示在 SourceFunction 中直接生成和发送水位线
 * 
 * 注意：在自定义数据源中发送了水位线以后，就不能再在程序中使用 
 * assignTimestampsAndWatermarks 方法来生成水位线了。二者只能取其一。
 */
public class ClickSourceWithWatermark implements SourceFunction<Event> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] userArr = {"Mary", "Bob", "Alice"};
        String[] urlArr = {"./home", "./cart", "./prod?id=1"};

        while (running) {
            long currTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间戳
            String username = userArr[random.nextInt(userArr.length)];
            String url = urlArr[random.nextInt(urlArr.length)];
            Event event = new Event(username, url, currTs);
            
            // 使用 collectWithTimestamp 方法将数据发送出去，并指明数据中的时间戳的字段
            sourceContext.collectWithTimestamp(event, event.timestamp);
            
            // 发送水位线
            sourceContext.emitWatermark(new Watermark(event.timestamp - 1L));
            
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

