package com.action.source;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从 CSV 文件读取并解析数据源示例
 * 使用 readTextFile() 方法读取 CSV 文件，然后使用 map() 转换算子解析数据
 * 
 * 特点说明：
 * - 适用场景：读取 CSV 格式的数据文件并解析为结构化数据
 * - 数据特点：有界数据流（文件读取完毕后自动结束）
 * - 实现方式：使用 readTextFile() 读取文件，使用 map() 解析 CSV 行
 * - 使用场景：数据导入、批量数据处理、数据迁移等
 * 
 * CSV 文件格式（clicks.csv）：
 * Mary, ./home, 1000
 * Alice, ./cart, 2000
 * Bob, ./prod?id=100, 3000
 * 
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromCsvFile"
 * 
 * 预期输出：
 * csv> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * csv> Event{user='Alice', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * csv> Event{user='Bob', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 */
public class SourceFromCsvFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 CSV 文件读取数据
        DataStreamSource<String> textStream = env.readTextFile("flink-02-DataSource/input/clicks.csv");

        // 解析 CSV 行并转换为 Event 对象
        textStream.filter(line -> {
                    // 过滤空行
                    return line != null && !line.trim().isEmpty();
                })
                .map(line -> {
                    // 按逗号分割 CSV 行
                    String[] fields = line.split(",\\s*");  // 使用正则表达式处理空格
                    
                    // 验证字段数量
                    if (fields.length < 3) {
                        System.err.println("CSV 行格式错误，字段数量不足: " + line);
                        return null;
                    }
                    
                    try {
                        // 解析字段
                        String user = fields[0].trim();
                        String url = fields[1].trim();
                        long timestamp = Long.parseLong(fields[2].trim());
                        
                        // 创建 Event 对象
                        return new Event(user, url, timestamp);
                    } catch (Exception e) {
                        System.err.println("CSV 行解析失败: " + line + ", 错误: " + e.getMessage());
                        return null;
                    }
                })
                .filter(event -> event != null)  // 过滤掉解析失败的行
                .print("csv");

        env.execute();
    }
}

/**
 * 注意事项：
 * 
 * 1. CSV 解析方式
 * - 简单分割：使用 split(",") 按逗号分割
 * - 处理空格：使用 split(",\\s*") 正则表达式
 * - 处理引号：使用专门的 CSV 解析库（如 OpenCSV、Apache Commons CSV）
 * 
 * 2. 使用 CSV 解析库（推荐）
 * 对于复杂的 CSV 文件（包含引号、换行等），建议使用专门的解析库：
 * 
 * 添加依赖（pom.xml）：
 * <dependency>
 *     <groupId>com.opencsv</groupId>
 *     <artifactId>opencsv</artifactId>
 *     <version>5.7.1</version>
 * </dependency>
 * 
 * 使用示例：
 * import com.opencsv.CSVReader;
 * import java.io.StringReader;
 * 
 * textStream.map(line -> {
 *     CSVReader reader = new CSVReader(new StringReader(line));
 *     String[] fields = reader.readNext();
 *     reader.close();
 *     
 *     String user = fields[0];
 *     String url = fields[1];
 *     long timestamp = Long.parseLong(fields[2]);
 *     
 *     return new Event(user, url, timestamp);
 * })
 * 
 * 3. 错误处理
 * - 空行处理：跳过空行或空字段
 * - 格式错误：捕获异常并记录日志
 * - 字段缺失：使用默认值或跳过该行
 * 
 * 4. 性能优化
 * - 使用并行度提高读取速度
 * - 对于大文件，考虑使用 Flink 的文件系统连接器
 * - 使用批处理模式处理历史数据
 * 
 * 5. 文件路径
 * - 相对路径：相对于项目根目录
 * - 绝对路径：完整的文件系统路径
 * - HDFS 路径：hdfs://namenode:port/path/to/file.csv
 * 
 * 常见问题：
 * 
 * Q1: CSV 解析错误
 * 原因：CSV 格式不规范（包含引号、换行、特殊字符等）
 * 解决方案：
 * - 使用专门的 CSV 解析库
 * - 预处理 CSV 文件，规范化格式
 * - 添加异常处理，跳过格式错误的行
 * 
 * Q2: 字段解析错误
 * 原因：字段类型不匹配或字段缺失
 * 解决方案：
 * - 添加字段验证和类型转换
 * - 使用 try-catch 捕获解析异常
 * - 记录错误日志，便于排查问题
 * 
 * Q3: 文件读取性能问题
 * 原因：文件过大或读取方式不当
 * 解决方案：
 * - 增加并行度：env.setParallelism(4)
 * - 使用 Flink 的文件系统连接器
 * - 考虑将大文件分割成多个小文件
 */

