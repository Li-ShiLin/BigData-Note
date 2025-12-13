package com.action.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件读取数据源示例
 * 使用 readTextFile() 方法从本地文件系统读取数据
 * 
 * 特点说明：
 * - 适用场景：读取本地文件系统中的文本文件
 * - 数据特点：有界数据流（文件读取完毕后自动结束）
 * - 路径说明：文件路径相对于项目根目录
 * - 支持格式：文本文件（按行读取）
 * 
 * 运行方式：
 * # 方法1: 使用 Maven 运行
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromFile"
 * 
 * # 方法2: 在 IDE 中直接运行
 * # 右键 SourceFromFile.java -> Run 'SourceFromFile.main()'
 * 
 * 预期输出：
 * file> hello world
 * file> hello flink
 * file> hello java
 */
public class SourceFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        // 注意：文件路径相对于项目根目录
        DataStreamSource<String> stream = env.readTextFile("flink-02-DataSource/input/words.txt");

        stream.print("file");

        env.execute();
    }
}

/**
 * 常见问题：
 * 
 * Q3: 文件路径错误
 * 原因：文件路径不正确
 * 解决方案：
 * - 使用相对路径：flink-02-DataSource/input/words.txt（相对于项目根目录）
 * - 使用绝对路径：/home/user/project/flink-02-DataSource/input/words.txt
 * - 检查文件是否存在
 * 
 * 注意事项：
 * - 从 IDE 运行：工作目录通常是项目根目录
 * - 打包运行：需要指定正确的文件路径
 * - 集群运行：文件需要在集群所有节点可访问
 */
