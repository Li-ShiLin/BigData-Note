package com.action.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 从 JDBC 数据库读取数据源示例
 * 使用自定义 SourceFunction 从关系型数据库读取数据
 * <p>
 * 特点说明：
 * - 适用场景：从 MySQL、PostgreSQL、Oracle 等关系型数据库读取数据
 * - 数据特点：可以是无界流（轮询）或有界流（单次查询）
 * - 实现方式：实现 SourceFunction<String> 接口，使用 JDBC 连接数据库
 * - 使用场景：数据库数据同步、实时数据监控、数据迁移等
 * <p>
 * 前置条件：
 * 1. 安装并启动数据库（MySQL、PostgreSQL 等）
 * 2. 创建数据库和测试表
 * 3. 插入测试数据
 * 4. 确保 pom.xml 中已添加数据库驱动依赖（MySQL 驱动已添加）
 * <p>
 * MySQL 数据库建表建库完整步骤：
 * <p>
 * 步骤1：登录 MySQL
 * <pre>{@code
 * mysql -u root -p
 * }</pre>
 * <p>
 * 完整 SQL 脚本
 * <pre>{@code
 * -- 创建数据库
 * CREATE DATABASE IF NOT EXISTS testdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
 *
 * -- 使用数据库（重要：必须执行）
 * USE testdb;
 *
 * -- 创建表
 * CREATE TABLE IF NOT EXISTS events (
 * id INT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
 * user_name VARCHAR(50) NOT NULL COMMENT '用户名',
 * url VARCHAR(200) NOT NULL COMMENT 'URL地址',
 * timestamp BIGINT NOT NULL COMMENT '时间戳',
 * INDEX idx_user_name (user_name),
 * INDEX idx_timestamp (timestamp)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='事件表';
 *
 * -- 插入测试数据
 * INSERT INTO events (user_name, url, timestamp) VALUES
 * ('Mary', './home', 1000),
 * ('Bob', './cart', 2000),
 * ('Alice', './prod?id=1', 3000),
 * ('Mary', './home', 4000),
 * ('Bob', './prod?id=2', 5000);
 *
 * -- 验证数据
 * SELECT * FROM events;
 * }</pre>
 * <p>
 * 运行方式：
 * <pre>{@code
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromJdbc"
 * }</pre>
 * <p>
 * 预期输出：
 * <pre>{@code
 * jdbc> Mary,./home,1000
 * jdbc> Bob,./cart,2000
 * jdbc> Alice,./prod?id=1,3000
 * }</pre>
 */
public class SourceFromJdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 JDBC 数据库读取数据
        DataStreamSource<String> stream = env.addSource(new JdbcSource(
                "jdbc:mysql://localhost:3306/testdb",  // 数据库连接 URL
                "root",                                 // 用户名
                "password",                             // 密码
                "SELECT user_name, url, timestamp FROM events",  // SQL 查询语句
                0  // 轮询间隔（毫秒），设置为 0 表示只查询一次
        ));

        stream.print("jdbc");

        env.execute();
    }

    /**
     * JDBC 数据源实现
     */
    public static class JdbcSource implements SourceFunction<String> {
        private String jdbcUrl;
        private String username;
        private String password;
        private String sql;
        private long interval;
        private boolean running = true;

        public JdbcSource(String jdbcUrl, String username, String password, String sql, long interval) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.sql = sql;
            this.interval = interval;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                // 加载数据库驱动（MySQL）
                Class.forName("com.mysql.cj.jdbc.Driver");

                // 建立数据库连接
                connection = DriverManager.getConnection(jdbcUrl, username, password);

                while (running) {
                    // 执行 SQL 查询
                    statement = connection.prepareStatement(sql);
                    resultSet = statement.executeQuery();

                    // 读取查询结果
                    while (resultSet.next()) {
                        // 构建输出字符串（根据实际表结构调整）
                        String user = resultSet.getString("user_name");
                        String url = resultSet.getString("url");
                        long timestamp = resultSet.getLong("timestamp");

                        String output = user + "," + url + "," + timestamp;
                        ctx.collect(output);
                    }

                    // 关闭结果集和语句
                    if (resultSet != null) {
                        resultSet.close();
                        resultSet = null;
                    }
                    if (statement != null) {
                        statement.close();
                        statement = null;
                    }

                    // 如果间隔为 0，只查询一次后退出
                    if (interval == 0) {
                        break;
                    }

                    // 等待指定时间后再次查询
                    Thread.sleep(interval);
                }
            } catch (Exception e) {
                System.err.println("JDBC 查询异常: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // 关闭资源
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
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
 * <p>
 * 1. 数据库驱动依赖
 * 本项目已在 pom.xml 中添加了 MySQL 驱动依赖，如果使用其他数据库，需要添加对应的驱动依赖：
 * <p>
 * MySQL（已添加）:
 * <pre>{@code
 * <dependency>
 * <groupId>mysql</groupId>
 * <artifactId>mysql-connector-java</artifactId>
 * <version>8.0.33</version>
 * </dependency>
 * }</pre>
 * <p>
 * 注意：添加依赖后需要重新编译项目：
 * <pre>{@code
 * mvn clean compile
 * }</pre>
 * <p>
 * PostgreSQL:
 * <pre>{@code
 * <dependency>
 * <groupId>org.postgresql</groupId>
 * <artifactId>postgresql</artifactId>
 * <version>42.6.0</version>
 * </dependency>
 * }</pre>
 * <p>
 * 2. 数据库连接配置
 * - MySQL: jdbc:mysql://host:port/database
 * - PostgreSQL: jdbc:postgresql://host:port/database
 * - Oracle: jdbc:oracle:thin:@host:port:database
 * <p>
 * 3. 连接池使用
 * 生产环境建议使用连接池（如 HikariCP、Druid）：
 * - 提高连接复用率
 * - 减少连接创建开销
 * - 更好的连接管理
 * <p>
 * 4. SQL 查询优化
 * - 使用索引字段进行查询
 * - 限制查询结果数量（LIMIT）
 * - 使用增量查询（WHERE timestamp > ?）
 * <p>
 * 5. 增量数据读取
 * 对于增量数据读取，可以使用时间戳或自增 ID：
 *
 * <pre>{@code
 * // 记录上次读取的最大 ID
 * long lastId = 0;
 *
 * // 增量查询
 * String sql = "SELECT * FROM events WHERE id > ? ORDER BY id LIMIT 1000";
 * statement.setLong(1, lastId);
 *
 * // 更新 lastId
 * while (resultSet.next()) {
 * long currentId = resultSet.getLong("id");
 * if (currentId > lastId) {
 * lastId = currentId;
 * }
 * // 处理数据...
 * }
 * }</pre>
 * <p>
 * 常见问题：
 * <p>
 * Q1: 数据库连接失败
 * 原因：数据库未启动、连接信息错误或网络不通
 * 解决方案：
 * - 检查数据库是否启动：mysql -u root -p
 * - 验证连接信息：jdbc:mysql://localhost:3306/testdb
 * - 检查网络连接：telnet localhost 3306
 * - 检查防火墙设置
 * <p>
 * Q2: 驱动类未找到（ClassNotFoundException）
 * 原因：缺少数据库驱动依赖或未重新编译项目
 * 解决方案：
 * - 检查 pom.xml 中是否已添加对应的数据库驱动依赖（MySQL 驱动已添加）
 * - 确保驱动版本与数据库版本兼容
 * - 重新编译项目：mvn clean compile
 * - 如果使用 IDE，刷新 Maven 项目并重新构建
 * - 检查 classpath 中是否包含驱动 jar 包
 */

