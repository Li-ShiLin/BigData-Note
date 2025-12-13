package com.action.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从 Socket 读取数据源示例
 * 使用 socketTextStream() 方法从网络 Socket 读取文本流
 * 
 * 特点说明：
 * - 适用场景：从网络 Socket 接收实时数据流
 * - 数据特点：无界数据流（持续接收数据）
 * - 使用场景：实时数据测试、流处理演示
 * - 注意事项：需要外部程序持续发送数据
 * 
 * 测试步骤：
 * 1. 启动 Socket 服务器（在 Linux 服务器上）
 *    # 安装 nc 工具（如果没有）
 *    sudo yum -y install nc
 *    # 或
 *    sudo apt-get install netcat-openbsd
 *    # 启动 nc 监听 7777 端口
 *    nc -lk 7777
 * 
 * 2. 运行 Flink 程序
 *    cd flink-02-DataSource
 *    mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromSocket"
 * 
 * 3. 在 nc 终端中输入数据
 *    hello
 *    world
 *    flink
 * 
 * 4. 观察输出
 *    socket> hello
 *    socket> world
 *    socket> flink
 * 
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromSocket"
 */
public class SourceFromSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 Socket 文本流读取
        // 参数：host（主机地址），port（端口号）
        // 这会创建一个无界流，持续接收数据
        DataStreamSource<String> stream = env.socketTextStream("server01", 7777);

        stream.print("socket");

        env.execute();
    }
}

/**
 * 注意事项：
 * 
 * 1. Socket 连接问题
 * 连接失败：
 * // 错误示例：无法连接到主机
 * env.socketTextStream("localhost", 7777);
 * 
 * 解决方案：
 * - 确保目标主机可访问
 * - 确保端口已开放
 * - 使用 nc -lk 7777 监听端口
 * - 检查防火墙设置
 * 
 * 测试连接：
 * # 测试连接
 * telnet server01 7777
 * # 或
 * nc -zv server01 7777
 * 
 * 2. 无界流处理
 * 特点：
 * - 无界流程序不会自动结束
 * - 需要手动停止（IDE 停止按钮或 Ctrl+C）
 * - Socket 断开连接后，Flink 会重连或抛出异常
 * 
 * 停止方式：
 * - 在 IDE 中点击停止按钮
 * - 在终端按 Ctrl+C
 * 
 * 常见问题：
 * 
 * Q1: Socket 连接失败
 * 原因：网络或权限问题
 * 解决方案：
 * # 检查端口是否被占用
 * netstat -tuln | grep 7777
 * # 测试连接
 * telnet server01 7777
 * # 使用 netcat 测试
 * nc -zv server01 7777
 */

