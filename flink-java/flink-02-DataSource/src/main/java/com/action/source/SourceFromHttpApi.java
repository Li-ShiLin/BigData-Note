package com.action.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * 从 HTTP API 读取数据源示例
 * 使用自定义 SourceFunction 从 HTTP REST API 读取数据
 * 
 * 特点说明：
 * - 适用场景：从 REST API 获取实时数据流
 * - 数据特点：可以是无界流（轮询）或有界流（单次请求）
 * - 实现方式：实现 SourceFunction<String> 接口，使用 HttpURLConnection 发送 HTTP 请求
 * - 使用场景：实时数据监控、API 数据采集、第三方数据集成等
 * 
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromHttpApi"
 * 
 * 预期输出：
 * http> {"id":1,"name":"Product 1","price":100.0}
 * http> {"id":2,"name":"Product 2","price":200.0}
 * ...
 */
public class SourceFromHttpApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 HTTP API 读取数据
        DataStreamSource<String> stream = env.addSource(new HttpApiSource(
                "https://jsonplaceholder.typicode.com/posts",  // API 地址
                5000  // 轮询间隔（毫秒），设置为 0 表示只请求一次
        ));

        stream.print("http");

        env.execute();
    }

    /**
     * HTTP API 数据源实现
     */
    public static class HttpApiSource implements SourceFunction<String> {
        private String apiUrl;
        private long interval;
        private boolean running = true;

        public HttpApiSource(String apiUrl, long interval) {
            this.apiUrl = apiUrl;
            this.interval = interval;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                try {
                    // 创建 HTTP 连接
                    URL url = new URL(apiUrl);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");
                    connection.setConnectTimeout(5000);
                    connection.setReadTimeout(5000);

                    // 读取响应
                    int responseCode = connection.getResponseCode();
                    if (responseCode == HttpURLConnection.HTTP_OK) {
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(connection.getInputStream())
                        );
                        String line;
                        while ((line = reader.readLine()) != null) {
                            // 发送数据到 Flink 流
                            ctx.collect(line);
                        }
                        reader.close();
                    } else {
                        System.err.println("HTTP 请求失败，响应码: " + responseCode);
                    }

                    connection.disconnect();

                    // 如果间隔为 0，只请求一次后退出
                    if (interval == 0) {
                        break;
                    }

                    // 等待指定时间后再次请求
                    Thread.sleep(interval);
                } catch (Exception e) {
                    System.err.println("HTTP 请求异常: " + e.getMessage());
                    // 发生异常时等待一段时间后重试
                    Thread.sleep(interval);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

/**
 * 注意事项：
 * 
 * 1. HTTP 请求配置
 * - 设置连接超时和读取超时，避免长时间阻塞
 * - 根据 API 要求设置请求头（如 Authorization、Content-Type 等）
 * - 处理 HTTP 错误响应码
 * 
 * 2. 轮询策略
 * - 单次请求：设置 interval = 0，请求一次后退出
 * - 轮询请求：设置 interval > 0，每隔指定时间请求一次
 * - 注意控制请求频率，避免对 API 服务器造成压力
 * 
 * 3. 异常处理
 * - 网络异常：捕获并重试
 * - HTTP 错误：记录错误码并处理
 * - 数据解析错误：根据实际情况处理
 * 
 * 4. 认证方式
 * - API Key：在请求头中添加 "X-API-Key: your-api-key"
 * - Bearer Token：在请求头中添加 "Authorization: Bearer your-token"
 * - Basic Auth：使用 Authenticator 或手动设置请求头
 * 
 * 示例：添加认证
 * connection.setRequestProperty("Authorization", "Bearer your-token");
 * connection.setRequestProperty("X-API-Key", "your-api-key");
 * 
 * 常见问题：
 * 
 * Q1: HTTP 连接超时
 * 原因：网络延迟或服务器响应慢
 * 解决方案：
 * - 增加连接超时时间：connection.setConnectTimeout(10000);
 * - 增加读取超时时间：connection.setReadTimeout(10000);
 * - 检查网络连接和服务器状态
 * 
 * Q2: API 限流
 * 原因：请求频率过高
 * 解决方案：
 * - 增加轮询间隔：设置更大的 interval 值
 * - 实现请求限流机制
 * - 使用指数退避策略重试
 */

