基于 **Flink CDC** 的 MySQL 与 Redis 缓存一致性示例：监听 MySQL `sakila.actor` 表变更，**全量快照与 INSERT/UPDATE** 时写入或刷新 Redis 缓存（`actor:{id}`），**DELETE** 时删除对应缓存，以保持缓存与库表一致。

---

## 1.前置环境总览

| 组件        | 用途                                            | 本示例默认地址               |
|-----------|-----------------------------------------------|-----------------------|
| **MySQL** | 业务库，开启 binlog 供 Flink CDC 消费                  | `192.168.56.12:3306`  |
| **Redis** | 缓存，CDC 触发缓存失效                                 | `192.168.56.12:6379`  |
| **本应用**   | Spring Boot + 内嵌 Flink，CDC Source → CacheSink | 本地/任意机器，Web UI `8081` |

代码中 MySQL 连接：`RedisMysqlApplication.java`（hostname/port/username/password）；Redis 连接：`CacheSink.java`（`redis://192.168.56.12:6379`）。若你的环境 IP 不同，请按下方「修改连接信息」调整。

---

## 2.MySQL 环境搭建

### 2.1 安装 MySQL

本仓库已有详细文档：[mysql-5.7.38.md](../mysql-5.7.38.md)。按该文档完成：

- 安装 MySQL 5.7（或 8.x，需兼容 Flink CDC）
- 初始化、修改 root 密码、配置为系统服务并启动
- 如需远程访问：授权 root@'%' 并开放 3306 端口

设置时区：

```bash
mysql -uroot -p

show variables like '%time_zone%';
set persist time_zone='+8:00';
set time_zone='+8:00';
show variables like '%time_zone%';
exit
```

### 2.2 开启 Binlog（Flink CDC 必须）

Flink CDC 通过 MySQL binlog 捕获变更，**必须开启 binlog 且为 ROW 格式**。

**编辑 MySQL 配置文件**（如 `/etc/my.cnf`）：

```bash
sudo vi /etc/my.cnf
```

在 `[mysqld]` 下增加或修改为：

```ini
[mysqld]
# 已有配置保留，例如：basedir、datadir、port、server-id 等

# ========== 以下为 Flink CDC 所需 ==========
# 开启 binlog
log-bin=mysql-bin
# binlog 格式必须为 ROW
binlog_format=ROW
# 可选：binlog 过期天数（MySQL 5.7）；MySQL 8.x 请用 binlog_expire_logs_seconds=604800
expire_logs_days=7
# 数据同步工具核心参数，记录行的完整变更（含旧值+所有新值）
binlog_row_image=FULL


# 注意：有 binlog-do-db 时，是“白名单模式”：
# 只写这些库的 binlog，其余库完全不写。
binlog-do-db=sakila
```

当前`/etc/my.cnf`的完整配置：

```bash
[vagrant@server02 ~]$ cat /etc/my.cnf
[mysqld]
# 安装目录（和你的路径一致）
basedir=/opt/module/mysql-5.7.38
# 数据存储目录（自动创建，无需提前建）
datadir=/opt/module/mysql-5.7.38/data
# 端口（默认3306）
port=3306
# socket文件路径
socket=/tmp/mysql.sock
# 运行用户（你创建的mysql-5.7.38）
user=mysql-5.7.38
# 默认字符集
character-set-server=utf8
# 跳过DNS解析（提升连接速度）
skip-name-resolve
# 服务端ID（单机部署设为1）
server-id=1


# 启动binlog，文件名前缀（默认存储在datadir目录）
log-bin=mysql-bin
# binlog类型，Maxwell强制要求row行级日志
binlog_format=row
# 【必备】数据同步工具核心参数，记录行的完整变更（含旧值+所有新值）
binlog_row_image=FULL
# 指定需要生成binlog的数据库（test和test_route均生效）
binlog-do-db=test
binlog-do-db=test_route
binlog-do-db=sakila


default-time-zone = '+8:00'



[mysql]
# 客户端默认字符集
default-character-set=utf8
socket=/tmp/mysql.sock

[mysqld_safe]
# 日志文件路径（自动生成）
log-error=/opt/module/mysql-5.7.38/data/mysqld.log
# 进程ID文件路径
pid-file=/opt/module/mysql-5.7.38/data/mysqld.pid
```

保存后**重启 MySQL**：

```bash
sudo service mysqld restart
# 或
sudo systemctl restart mysqld
```

**验证 binlog**（登录 MySQL 后执行）：

```sql
SHOW VARIABLES LIKE 'log_bin';        -- 应为 ON
SHOW VARIABLES LIKE 'binlog_format';  -- 应为 ROW
SHOW BINARY LOGS;                     -- 能看到 binlog 文件列表
```

### 2.3 创建 CDC 用户并授权（可选，与 root 二选一）

若使用 root，需确保 root 具有复制与库表权限。若单独建用户，建议：

```sql
-- 创建 CDC 用户（与代码中 username/password 一致时可复用 root）
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY 'root';

-- 授权：全局复制、sakila 库所有权限（Flink CDC 需要读表结构 + binlog）
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON sakila.* TO 'root'@'%';
FLUSH PRIVILEGES;
```

本示例代码使用 `username="root"`，`password` 在 `RedisMysqlApplication.java` 中配置（当前为 `MySql@1111`），若你使用其他用户或密码需在该文件中修改。

### 2.4 导入 sakila 示例库

本模块已包含 `sakila-all.sql`（与 MySQL 官方示例库一致，含 `actor` 表）。

```bash
# 下载SQL脚本
wget http://manongbiji.oss-cn-beijing.aliyuncs.com/ittailkshow/it300/download/sakila-all.sql
```

**方式一：在服务器上已有该文件时**

```bash
mysql -uroot -p < /path/to/sakila-all.sql
```

**方式二：从本机上传后导入**

```bash
# 在 Windows 本机（PowerShell）上传
scp -i "你的私钥路径" "D:\github\BigData-Note\flink-cdc\cdc-06-mysql-redis-cache-consistency\sakila-all.sql" vagrant@192.168.56.11:/home/vagrant/

# SSH 到 MySQL 所在机器后导入
mysql -uroot -p < /home/vagrant/sakila-all.sql
```

**验证**：

```sql
USE sakila;
SHOW TABLES;
SELECT COUNT(*) FROM actor;
```

## 3.Redis 环境搭建

Redis 安装详细文档：[redis-6.2.6.md](../redis-6.2.6.md)

## 4.代码实现与验证

### 4.1 项目结构

```
cdc-06-mysql-redis-cache-consistency/
├── pom.xml
├── README.md
├── sakila-all.sql
└── src/main/java/com/action/flinkcdc/
    ├── RedisMysqlApplication.java   # 入口：Spring Boot + Flink CDC 编排
    ├── CacheSink.java               # Sink：根据 CDC 事件写/删 Redis 缓存
    ├── Actor.java                  # 实体：sakila.actor 表对应，Gson 反序列化
    └── JsonUtils.java              # 工具：JSON 解析与 OGNL 路径取值
```

- **RedisMysqlApplication**：Spring Boot 启动后通过 `CommandLineRunner` 构建 Flink 流（MySqlSource → CacheSink），并开启 Web UI（8081）。
- **CacheSink**：继承 `RichSinkFunction<String>`，消费 Debezium JSON；`op=r`/`op=c`/`op=u` 时从 `after` 解析整行并 **set** Redis `actor:{id}`，`op=d` 时从 `before` 解析并 **delete** 对应 key。
- **Actor**：与 Debezium 的 `after`/`before` 节点字段对应，使用 `@SerializedName` 映射 snake_case（如 `actor_id` → `actorId`）。
- **JsonUtils**：Gson 将 JSON 转为 Map，OGNL 按路径（如 `"op"`、`"after"`、`"before"`）取值，`transferToEntity` 转成目标类型供 CacheSink 使用。

---

### 4.2 代码实现说明

#### 4.2.1 整体数据流

```
MySQL (sakila.actor, binlog)
    → MySqlSource (Debezium JSON)
    → DataStream<String>
    → CacheSink (解析 op，r/c/u 写 Redis、d 删 Redis)
    → Redis
```

- **CDC 语义**：默认 `StartupOptions.initial()`（Flink CDC MySQL 2.x 未显式设置时为 initial），先全量快照再跟 binlog。全量快照为 `op=r`，增量为 `op=c`/`op=u`/`op=d`。
- **缓存策略**：**r/c/u** 从 `after` 解析整行为 `Actor`，写入 Redis `actor:{id}`（set）；**d** 从 `before` 解析 `Actor`，删除 Redis `actor:{id}`（delete），从而保持缓存与库表一致。

#### 4.2.2 pom.xml

模块继承父工程 `flink-cdc`，主要依赖与用途如下（与仓库一致）：

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.action.flinkcdc</groupId>
        <artifactId>flink-cdc</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>cdc-06-mysql-redis-cache-consistency</artifactId>
    <packaging>jar</packaging>

    <name>cdc-06-mysql-redis-cache-consistency</name>
    <url>http://maven.apache.org</url>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <spring-boot.version>2.7.18</spring-boot.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.redisson</groupId>
            <artifactId>redisson</artifactId>
            <version>3.13.6</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
            <version>${spring-boot.version}</version>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>1.18.30</version>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <version>${spring-boot.version}</version>
            <scope>test</scope>
        </dependency>

        <!--Flink connector连接器基础包-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-base</artifactId>
            <version>1.14.0</version>
        </dependency>
        <!--Flink CDC MySQL源-->
        <dependency>
            <groupId>com.ververica</groupId>
            <artifactId>flink-sql-connector-mysql-cdc</artifactId>
            <version>2.3.0</version>
        </dependency>
        <!--Flink的DataStream数据流API-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_2.12</artifactId>
            <version>1.14.0</version>
        </dependency>
        <!--Flink的Java 客户端-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_2.12</artifactId>
            <version>1.14.0</version>
        </dependency>
        <!--开启WebUI支持,端口8081,默认没有开启-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime-web_2.12</artifactId>
            <version>1.14.0</version>
        </dependency>
        <!--Flink的Table API&SQL程序可以连接到其他外部系统，用于读写批处理表和流式表。-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime_2.12</artifactId>
            <version>1.14.0</version>
        </dependency>
        <dependency>
            <groupId>ognl</groupId>
            <artifactId>ognl</artifactId>
            <version>3.1.1</version>
        </dependency>
        <dependency>
            <groupId>com.google.code.gson</groupId>
            <artifactId>gson</artifactId>
            <version>2.9.0</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <source>${maven.compiler.source}</source>
                    <target>${maven.compiler.target}</target>
                    <encoding>${project.build.sourceEncoding}</encoding>
                    <annotationProcessorPaths>
                        <path>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                            <version>1.18.30</version>
                        </path>
                    </annotationProcessorPaths>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- **Redisson**：写/删 Redis，单机地址在 `CacheSink` 中写死 `redis://192.168.56.12:6379`。
- **Flink 1.14 + Flink CDC MySQL 2.3**：Source 输出 Debezium JSON 字符串。
- **OGNL + Gson**：JsonUtils 中按路径解析 JSON 并反序列化为实体。

#### 4.2.3 sakila-all.sql

本模块自带 `sakila-all.sql`，与 MySQL 官方示例库一致，包含 `sakila` 库及 `actor` 表。**表结构**（与 CDC 消费字段对应）如下：

```sql
CREATE DATABASE sakila;
USE sakila;

SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

DROP SCHEMA IF EXISTS sakila;
CREATE SCHEMA sakila;
USE sakila;

--
-- Table structure for table `actor`
--

CREATE TABLE actor (
  actor_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  first_name VARCHAR(45) NOT NULL,
  last_name VARCHAR(45) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY  (actor_id),
  KEY idx_actor_last_name (last_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

脚本内随后为 `SET AUTOCOMMIT=0;` 及约 200 条 `INSERT INTO actor VALUES (...)`，对应 Debezium 的 `after` 节点字段：`actor_id`、`first_name`、`last_name`、`last_update`。导入后可通过 `SELECT COUNT(*) FROM actor;` 校验。

#### 4.2.4 RedisMysqlApplication.java

入口：Spring Boot 启动后通过 `CommandLineRunner` 创建 MySqlSource、StreamExecutionEnvironment（含 Web UI 8081、checkpoint 5s），将 Source 与 CacheSink 串联并 `env.execute()`。MySQL 连接参数（hostname/port/username/password）在此处配置，若环境不同请修改。

```java
package com.action.flinkcdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@Slf4j
public class RedisMysqlApplication {

    @Bean
    public CommandLineRunner init() {
        return args -> {
            log.info("开始初始化 Flink CDC 作业...");
            // 1. 构建 MySQL CDC Source（显式指定启动模式为 initial，确保先快照再消费 binlog）
            MySqlSource<String> source = MySqlSource.<String>builder()
                    .hostname("192.168.56.12")
                    .port(3306)
                    .databaseList("sakila")
                    .tableList("sakila.actor")
                    .username("root")
                    .password("MySql@1111")
                    .serverTimeZone("Asia/Shanghai")  // 设置正确时区
                    .includeSchemaChanges(true)  // 排除 schema 变更事件
                    .startupOptions(StartupOptions.initial())  // 先全量后增量
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .build();

            // 2. 配置 Flink 环境（Checkpoint 持久化 + Web UI 端口）
            Configuration flinkConfig = new Configuration();
            flinkConfig.setInteger(RestOptions.PORT, 8081);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

            env.enableCheckpointing(5000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.getCheckpointConfig().setCheckpointTimeout(10000);
            env.setParallelism(1);

            // 3. 构建数据流并添加 Sink
            DataStreamSource<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL-CDC-Source");
            dataStream.addSink(new CacheSink()).name("Redis-Cache-Sink");

            log.info("Flink CDC 作业初始化完成，开始执行...");
            env.execute("MySQL-To-Redis-Cache-Consistency");
            log.info("Flink CDC 作业已停止");
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(RedisMysqlApplication.class, args);
    }

}
```

#### 4.2.5 CacheSink.java

Sink 逻辑：每条 Debezium JSON 先打 `log.info(json)`，再用 `JsonUtils.transferToEntity(json, PATH_OP, String.class)` 得到 `op`；若 `op` 为 null 则直接 return。根据 `op`：**r/c/u** 用 `PATH_AFTER` 解析为 `Actor` 并 `setCache`（写 Redis）；**d** 用 `PATH_BEFORE` 解析为 `Actor` 并 `deleteCache`。Redisson 在 `open()` 中创建、`close()` 中 shutdown。

| 要点 | 说明 |
|------|------|
| 常量 | `PATH_OP="op"`、`PATH_AFTER="after"`、`PATH_BEFORE="before"`；`OP_READ/CREATE/UPDATE/DELETE` 对应 r/c/u/d。 |
| 解析 | `op` 用 `transferToEntity(json, PATH_OP, String.class)`；r/c/u 用 `PATH_AFTER` 解析整行为 `Actor`，d 用 `PATH_BEFORE`。 |
| Redis | 单机地址 `REDIS_ADDRESS = "redis://192.168.56.12:6379"`；set 时 `bucket.set(JsonUtils.toJson(actor))`，delete 时 `bucket.delete()`。 |
| 判空 | `actor == null` 或 `actor.getActorId() == null` 时不写不删；判空使用 `Objects.isNull` / `Objects.nonNull`。 |
| 日志 | `log.info("cache set: op={}, actorId={}", op, actor.getActorId())`；`log.info("cache delete: op={}, actorId={}, deleted={}", ...)`。 |

```java
package com.action.flinkcdc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.Objects;

@Slf4j
public class CacheSink extends RichSinkFunction<String> {

    private static final String REDIS_ADDRESS = "redis://192.168.56.12:6379";

    /**
     * Debezium 操作类型：r=快照读, c=插入, u=更新, d=删除
     */
    private static final String OP_READ = "r";
    private static final String OP_CREATE = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";

    /**
     * Debezium 中 op 节点路径，用于解析操作类型
     */
    private static final String PATH_OP = "op";
    /**
     * Debezium 中 after 节点路径，用于解析整行实体（set 缓存）
     */
    private static final String PATH_AFTER = "after";
    /**
     * Debezium 中 before 节点路径，用于解析整行实体（delete 缓存）
     */
    private static final String PATH_BEFORE = "before";

    private RedissonClient redissonClient;

    public CacheSink() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Config config = new Config();
            config.setCodec(StringCodec.INSTANCE);
            config.useSingleServer().setAddress(REDIS_ADDRESS).setConnectionPoolSize(10).setConnectionMinimumIdleSize(2);
            redissonClient = Redisson.create(config);
            log.info("Redis 客户端初始化成功，地址：{}", REDIS_ADDRESS);
        } catch (Exception e) {
            log.error("Redis 客户端初始化失败", e);
            throw new RuntimeException("Redis 连接失败", e);
        }
    }

    @Override
    public void invoke(String json, Context context) throws Exception {
        super.invoke(json, context);
        log.info("接收到 CDC 事件：{}", json);

        try {
            String op = JsonUtils.transferToEntity(json, PATH_OP, String.class);
            if (Objects.isNull(op)) {
                log.info("CDC 事件解析 op 为空，JSON：{}", json);
                return;
            }
            log.info("解析到操作类型：op={}", op);

            Actor actor;
            switch (op) {
                case OP_READ:
                    actor = JsonUtils.transferToEntity(json, PATH_AFTER, Actor.class);
                    setCache(actor, OP_READ);
                    break;
                case OP_CREATE:
                    actor = JsonUtils.transferToEntity(json, PATH_AFTER, Actor.class);
                    setCache(actor, OP_CREATE);
                    break;
                case OP_UPDATE:
                    actor = JsonUtils.transferToEntity(json, PATH_AFTER, Actor.class);
                    setCache(actor, OP_UPDATE);
                    break;
                case OP_DELETE:
                    actor = JsonUtils.transferToEntity(json, PATH_BEFORE, Actor.class);
                    deleteCache(actor, OP_DELETE);
                    break;
                default:
                    log.error("忽略未知操作类型：op={}, JSON={}", op, json);
            }
        } catch (Exception e) {
            log.error("处理 CDC 事件失败，JSON：{}", json, e);
        }
    }

    private void setCache(Actor actor, String op) {
        if (Objects.isNull(actor) || Objects.isNull(actor.getActorId())) {
            return;
        }
        try {
            String key = "actor:" + actor.getActorId();
            RBucket<String> bucket = redissonClient.getBucket(key);
            bucket.set(JsonUtils.toJson(actor));
            log.info("缓存写入成功：op={}, key={}, actor={}", op, key, actor);
        } catch (Exception e) {
            log.error("缓存写入失败：op={}, actorId={}", op, actor.getActorId(), e);
        }
    }

    private boolean deleteCache(Actor actor, String op) {
        if (Objects.isNull(actor) || Objects.isNull(actor.getActorId())) {
            return false;
        }
        try {
            String key = "actor:" + actor.getActorId();
            RBucket<String> bucket = redissonClient.getBucket(key);
            boolean deleted = bucket.delete();
            log.info("缓存删除结果：op={}, key={}, deleted={}", op, key, deleted);
            return deleted;
        } catch (Exception e) {
            log.error("缓存删除失败：op={}, actorId={}", op, actor.getActorId(), e);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (Objects.nonNull(redissonClient)) {
            try {
                redissonClient.shutdown();
                log.info("Redis 客户端已关闭");
            } catch (Exception e) {
                log.error("Redis 客户端关闭失败", e);
            }
        }
    }
}
```

#### 4.2.6 Actor.java

与 `sakila.actor` 表及 Debezium `after`/`before` 节点字段对应；Debezium 输出为 snake_case，使用 Gson `@SerializedName` 映射到 Java 驼峰字段，供 `JsonUtils.transferToEntity` 反序列化及 `JsonUtils.toJson` 写 Redis 使用。

```java
package com.action.flinkcdc;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 对应 sakila.actor 表，用于 CDC 后写入 Redis 的实体。
 * Debezium 输出为 snake_case，需用 @SerializedName 与 Gson 映射。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Actor {

    @SerializedName("actor_id")
    private Integer actorId;

    @SerializedName("first_name")
    private String firstName;

    @SerializedName("last_name")
    private String lastName;

    @SerializedName("last_update")
    private String lastUpdate;
}
```

#### 4.2.7 JsonUtils.java

将 JSON 转为 Map，再按 OGNL 路径取子节点并反序列化为目标类型；判空使用 `Objects.isNull`/`Objects.nonNull`。

| 方法 | 说明 |
|------|------|
| `transferToMap(String json)` | 使用 Gson 将 JSON 转为 `Map<String, Object>`，支持嵌套对象与数组。 |
| `transferToEntity(String json, String path, Class<T> clazz)` | 先 `transferToMap`，再用 OGNL 取 `path`（如 `"op"`、`"after"`、`"before"`）；若节点非 null 则 `GSON.toJson(node)` 后 `GSON.fromJson(..., clazz)` 返回实体；异常或 node 为 null 时返回 null。 |
| `toJson(Object src)` | `Objects.nonNull(src) ? GSON.toJson(src) : null`，供 CacheSink 写 Redis 使用。 |

```java
package com.action.flinkcdc;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import ognl.Ognl;
import ognl.OgnlContext;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class JsonUtils {

    private static final Gson GSON = new Gson();

    /**
     * 将指定JSON转为Map对象，Key固定为String，对应JSONkey
     * Value分情况：
     * 1. Value是字符串，自动转为字符串,例如:{"a","b"}
     * 2. Value是其他JSON对象，自动转为Map，例如：{"a":{"b":"2"}}}
     * 3. Value是数组，自动转为List<Map>，例如：{"a":[{"b":"2"},"c":"3"]}
     *
     * @param json 输入的JSON对象
     * @return 动态的Map集合
     */
    public static Map<String, Object> transferToMap(String json) {
        try {
            return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {
            }.getType());
        } catch (Exception e) {
            log.error("JSON 转 Map 失败，json={}", json, e);
            return null;
        }
    }

    /**
     * 按OGNL路径解析JSON并转为指定类型
     *
     * @param json  原始的JSON数据
     * @param path  OGNL规则表达式
     * @param clazz Value对应的目标类
     * @return clazz对应数据
     */
    public static <T> T transferToEntity(String json, String path, Class<T> clazz) {
        try {
            Map<String, Object> root = transferToMap(json);
            OgnlContext ctx = new OgnlContext();
            ctx.setRoot(root);
            Object node = Ognl.getValue(path, ctx, ctx.getRoot());
            if (Objects.isNull(node)) {
                return null;
            }
            return GSON.fromJson(GSON.toJson(node), clazz);
        } catch (Exception e) {
            log.error("解析 JSON 失败：path={}, json={}", path, json, e);
            return null;
        }
    }

    /**
     * 将任意对象序列化为 JSON 字符串
     */
    public static String toJson(Object src) {
        return Objects.nonNull(src) ? GSON.toJson(src) : null;
    }
}
```

---

### 4.3 编译与运行

**编译**（在仓库根目录或本模块目录）：

```bash
cd D:\github\BigData-Note\flink-cdc
mvn clean package -pl cdc-06-mysql-redis-cache-consistency -am -DskipTests
```

或在 IDE 中打开 `flink-cdc` 父工程，对 `cdc-06-mysql-redis-cache-consistency` 执行 Maven 打包。

**运行**：

- **IDE**：运行 `com.action.flinkcdc.RedisMysqlApplication` 的 `main`。
- **命令行**：

```bash
cd cdc-06-mysql-redis-cache-consistency
java -jar target/cdc-06-mysql-redis-cache-consistency-1.0-SNAPSHOT.jar
```

启动后：Flink CDC 连接 `sakila.actor`（先全量快照再 binlog），CDC 事件经 CacheSink：r/c/u 写入或刷新 Redis `actor:{id}`，d 删除对应 key；Flink Web UI：`http://localhost:8081`。

---

### 4.4 验证流程（详细步骤）

#### 步骤 1：准备 MySQL 与 Redis

- MySQL：已开启 binlog（ROW）、已导入 `sakila`（含 `actor` 表），CDC 用户可读 `sakila` 与 binlog。
- Redis：已启动，地址与 `CacheSink` 中一致（默认 `192.168.56.12:6379`）。

#### 步骤 2：在 Redis 中预置缓存（可选）

启动应用后，全量快照（op=r）会自动将 actor 数据写入 Redis。若在启动前手动 SET `actor:1`、`actor:2`，可对比启动后是否被快照覆盖；或先启动再在 MySQL 中 UPDATE/DELETE，观察日志与 key 变化。

#### 步骤 3：启动应用

运行 `RedisMysqlApplication`（IDE 或 `java -jar`）。控制台应出现 Flink 启动与 CDC 相关日志；全量快照阶段会输出大量 `op=r` 的 JSON（可忽略或通过日志级别控制）。

#### 步骤 4：MySQL 侧触发 UPDATE

在 MySQL 中执行（示例：修改 `actor_id=1` 的 last_name）：

```sql
USE sakila;
UPDATE actor SET last_name = 'GUINESS-UPDATED' WHERE actor_id = 1;
```

**预期**：

- 应用控制台：出现一条 Debezium JSON（`op=u`，`before`/`after` 含 `actor_id=1`），以及 CacheSink 日志 `cache set: op=u, actorId=1`。
- Redis：`GET actor:1` 应为更新后的 JSON（含 `last_name: GUINESS-UPDATED`），即缓存被刷新。

```bash
127.0.0.1:6379> GET actor:1
"{\"actor_id\":1,\"first_name\":\"PENELOPE\",\"last_name\":\"GUINESS-UPDATED\",\"last_update\":\"2026-03-09T17:27:02Z\"}"
```

#### 步骤 5：MySQL 侧触发 DELETE

```sql
DELETE FROM actor WHERE actor_id = 2;
```

**预期**：

- 控制台：一条 `op=d` 的 JSON，以及日志 `cache delete: op=d, actorId=2, deleted=true`。
- Redis：`GET actor:2` 应为 `(nil)` 或 key 不存在。

#### 步骤 6：Redis 侧复核

```bash
redis-cli -h 192.168.56.12 -p 6379
GET actor:1
GET actor:2
KEYS actor:*
```

经步骤 4、5 后，`actor:1` 存在且为更新后的内容，`actor:2` 已被删除；其余 `actor:*` 为全量快照或后续 c/u 写入。

#### 步骤 7：验证 INSERT 写缓存

在 MySQL 中插入一条新演员（例如 `actor_id=201`）：

```sql
USE sakila;
INSERT INTO actor(actor_id, first_name, last_name, last_update)
VALUES (201, 'CDC', 'INSERT-DEMO', NOW());
```

**预期**：

- 应用控制台：出现一条 Debezium JSON（`op=c`，`after.actor_id=201`），以及 CacheSink 日志 `cache set: op=c, actorId=201`。
- Redis：在 Redis 客户端中执行 `GET actor:201`，应返回包含 `first_name: CDC`、`last_name: INSERT-DEMO` 的 JSON，新 key `actor:201` 即为本次插入写入的缓存。

```bash
127.0.0.1:6379> GET actor:201
"{\"actor_id\":201,\"first_name\":\"CDC\",\"last_name\":\"INSERT-DEMO\",\"last_update\":\"2026-03-09T16:32:50Z\"}"
127.0.0.1:6379>
```

---

**验证小结**：

| 操作 | MySQL | 应用日志（示例） | Redis |
|------|--------|------------------|--------|
| 全量快照 / 启动 | — | `cache set: op=r, actorId=1` 等 | 全表写入 `actor:{id}` |
| UPDATE actor SET ... WHERE actor_id=1 | 执行成功 | `cache set: op=u, actorId=1` | `actor:1` 更新为新值 |
| DELETE FROM actor WHERE actor_id=2 | 执行成功 | `cache delete: op=d, actorId=2, deleted=true` | `actor:2` 被删除 |
| INSERT 新行 | 执行成功 | `cache set: op=c, actorId=201` | 新增 `actor:201` |

按上述步骤完成即可确认：本模块的代码实现与“MySQL 变更 → Flink CDC → Redis 缓存写/删”的验证流程一致。

---

## 5.依赖与配置说明

- **pom.xml**：Flink 1.14、Flink CDC MySQL 2.3、Redisson、Spring Boot 2.7、Lombok、Gson、OGNL 等；见模块 `pom.xml`。
- **application.yml**：仅配置了 Flink / CDC 相关日志级别；MySQL/Redis 连接均在代码中写死，可按需改为配置文件读取。

按上述步骤完成 **MySQL（含 binlog + sakila）** 与 **Redis** 搭建后，即可运行 `RedisMysqlApplication` 并验证 MySQL 与 Redis
的缓存一致性流程。

