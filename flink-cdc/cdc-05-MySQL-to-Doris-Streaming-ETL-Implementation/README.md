<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MySQL 到 Doris 的 Streaming ETL 实现（Flink CDC 3.0）](#mysql-%E5%88%B0-doris-%E7%9A%84-streaming-etl-%E5%AE%9E%E7%8E%B0flink-cdc-30)
    - [1.环境与前置条件](#1%E7%8E%AF%E5%A2%83%E4%B8%8E%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6)
    - [2.核心概念：同步变更与路由变更](#2%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5%E5%90%8C%E6%AD%A5%E5%8F%98%E6%9B%B4%E4%B8%8E%E8%B7%AF%E7%94%B1%E5%8F%98%E6%9B%B4)
        - [2.1 同步变更（Sync Change）](#21-%E5%90%8C%E6%AD%A5%E5%8F%98%E6%9B%B4sync-change)
        - [2.2 路由变更（Route Change）](#22-%E8%B7%AF%E7%94%B1%E5%8F%98%E6%9B%B4route-change)
        - [2.3 对比小结](#23-%E5%AF%B9%E6%AF%94%E5%B0%8F%E7%BB%93)
    - [3.安装 Flink CDC 3.0](#3%E5%AE%89%E8%A3%85-flink-cdc-30)
        - [3.1 上传安装包到 Linux](#31-%E4%B8%8A%E4%BC%A0%E5%AE%89%E8%A3%85%E5%8C%85%E5%88%B0-linux)
        - [3.2 在 Linux 上解压到指定目录](#32-%E5%9C%A8-linux-%E4%B8%8A%E8%A7%A3%E5%8E%8B%E5%88%B0%E6%8C%87%E5%AE%9A%E7%9B%AE%E5%BD%95)
        - [3.3 确认 Flink CDC 目录结构](#33-%E7%A1%AE%E8%AE%A4-flink-cdc-%E7%9B%AE%E5%BD%95%E7%BB%93%E6%9E%84)
        - [3.4 放入 MySQL / Doris Pipeline 连接器](#34-%E6%94%BE%E5%85%A5-mysql--doris-pipeline-%E8%BF%9E%E6%8E%A5%E5%99%A8)
    - [4.MySQL 源端准备](#4mysql-%E6%BA%90%E7%AB%AF%E5%87%86%E5%A4%87)
        - [4.1 MySQL 安装](#41-mysql-%E5%AE%89%E8%A3%85)
        - [4.2 开启 binlog](#42-%E5%BC%80%E5%90%AF-binlog)
        - [4.3 开启MySQL访问权限](#43-%E5%BC%80%E5%90%AFmysql%E8%AE%BF%E9%97%AE%E6%9D%83%E9%99%90)
        - [4.4 修改MySQL时区](#44-%E4%BF%AE%E6%94%B9mysql%E6%97%B6%E5%8C%BA)
        - [4.5 创建供同步变更使用的库表](#45-%E5%88%9B%E5%BB%BA%E4%BE%9B%E5%90%8C%E6%AD%A5%E5%8F%98%E6%9B%B4%E4%BD%BF%E7%94%A8%E7%9A%84%E5%BA%93%E8%A1%A8)
        - [4.6 创建供路由变更使用的库表](#46-%E5%88%9B%E5%BB%BA%E4%BE%9B%E8%B7%AF%E7%94%B1%E5%8F%98%E6%9B%B4%E4%BD%BF%E7%94%A8%E7%9A%84%E5%BA%93%E8%A1%A8)
    - [5.安装 Doris（当前未安装时执行）](#5%E5%AE%89%E8%A3%85-doris%E5%BD%93%E5%89%8D%E6%9C%AA%E5%AE%89%E8%A3%85%E6%97%B6%E6%89%A7%E8%A1%8C)
        - [5.1 安装 Doris](#51-%E5%AE%89%E8%A3%85-doris)
        - [5.2 Doris 中创建目标库（同步与路由会用）](#52-doris-%E4%B8%AD%E5%88%9B%E5%BB%BA%E7%9B%AE%E6%A0%87%E5%BA%93%E5%90%8C%E6%AD%A5%E4%B8%8E%E8%B7%AF%E7%94%B1%E4%BC%9A%E7%94%A8)
    - [6.Flink 集群与 Checkpoint](#6flink-%E9%9B%86%E7%BE%A4%E4%B8%8E-checkpoint)
        - [6.1 下载 Flink 1.18.0 安装包](#61-%E4%B8%8B%E8%BD%BD-flink-1180-%E5%AE%89%E8%A3%85%E5%8C%85)
        - [6.2 调整 Flink 端口与网络绑定配置](#62-%E8%B0%83%E6%95%B4-flink-%E7%AB%AF%E5%8F%A3%E4%B8%8E%E7%BD%91%E7%BB%9C%E7%BB%91%E5%AE%9A%E9%85%8D%E7%BD%AE)
        - [6.3 开启 Checkpoint](#63-%E5%BC%80%E5%90%AF-checkpoint)
        - [6.4 启动flink集群](#64-%E5%90%AF%E5%8A%A8flink%E9%9B%86%E7%BE%A4)
    - [7.同步变更：MySQL → Doris（整库/多表）](#7%E5%90%8C%E6%AD%A5%E5%8F%98%E6%9B%B4mysql-%E2%86%92-doris%E6%95%B4%E5%BA%93%E5%A4%9A%E8%A1%A8)
        - [7.1 创建 job 目录与配置文件](#71-%E5%88%9B%E5%BB%BA-job-%E7%9B%AE%E5%BD%95%E4%B8%8E%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
        - [7.2 写入同步变更配置](#72-%E5%86%99%E5%85%A5%E5%90%8C%E6%AD%A5%E5%8F%98%E6%9B%B4%E9%85%8D%E7%BD%AE)
        - [7.3 启动同步任务](#73-%E5%90%AF%E5%8A%A8%E5%90%8C%E6%AD%A5%E4%BB%BB%E5%8A%A1)
        - [7.4 验证](#74-%E9%AA%8C%E8%AF%81)
    - [8.路由变更：MySQL 多表到 Doris 指定表](#8%E8%B7%AF%E7%94%B1%E5%8F%98%E6%9B%B4mysql-%E5%A4%9A%E8%A1%A8%E5%88%B0-doris-%E6%8C%87%E5%AE%9A%E8%A1%A8)
        - [8.1 编写路由配置文件](#81-%E7%BC%96%E5%86%99%E8%B7%AF%E7%94%B1%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
        - [8.2 启动路由同步任务](#82-%E5%90%AF%E5%8A%A8%E8%B7%AF%E7%94%B1%E5%90%8C%E6%AD%A5%E4%BB%BB%E5%8A%A1)
        - [8.3 验证](#83-%E9%AA%8C%E8%AF%81)
    - [9.常见问题](#9%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)
        - [9.1 Doris BE 启动报
          `file descriptors limit is too small` 的处理](#91-doris-be-%E5%90%AF%E5%8A%A8%E6%8A%A5-file-descriptors-limit-is-too-small-%E7%9A%84%E5%A4%84%E7%90%86)
        - [9.2 Flink RPC 超时（akka.ask.timeout）处理](#92-flink-rpc-%E8%B6%85%E6%97%B6akkaasktimeout%E5%A4%84%E7%90%86)
    - [10.步骤总结](#10%E6%AD%A5%E9%AA%A4%E6%80%BB%E7%BB%93)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MySQL 到 Doris 的 Streaming ETL 实现（Flink CDC 3.0）

本文档给出基于 **Flink CDC 3.0** 的「MySQL → Doris」流式 ETL 的完整实现步骤，从环境准备到同步变更、路由变更，尽量做到可按步执行。
**若当前 Linux 服务器尚未安装 Doris，将按下面步骤一并完成安装与配置。**

---

## 1.环境与前置条件

| 组件    | 说明                                      |
|-------|-----------------------------------------|
| 操作系统  | Linux（CentOS 7+ / Ubuntu 16.04+），x86_64 |
| JDK   | 1.8（Flink、Doris、Flink CDC 均依赖）          |
| MySQL | 已安装并开启 binlog（作为源端）                     |
| Flink | 1.18.x 集群（用于提交 Flink CDC 任务）            |
| Doris | 1.2.x（本文以 1.2.4 为例，未安装则按第4节安装）          |

**节点约定（可随实际修改）：**

- **server01**：Flink JobManager、Doris、Flink CDC 安装与任务提交
- **server02**：MySQL（源端）

---

## 2.核心概念：同步变更与路由变更

### 2.1 同步变更（Sync Change）

**含义**：按「源表与目标表一一对应、同名」的方式，将 MySQL 的变更实时同步到 Doris。

- **表映射规则**：MySQL 的 `库名.表名` 自动对应 Doris 的 `库名.表名`，不做改名、不改库
- **典型场景**：整库或指定库下多表同步，希望源端与目标端表结构、命名保持一致
- **示例**：配置 `tables: test\.\*` 时，MySQL `test.t1`、`test.t2` 分别同步到 Doris 的 `test.t1`、`test.t2`

```
MySQL test.t1  →  Doris test.t1
MySQL test.t2  →  Doris test.t2
```

### 2.2 路由变更（Route Change）

**含义**：通过显式的 `route` 配置，自定义「源表 → 目标表」的映射关系，支持改库名、表名，以及多表写入同一目标表。

- **表映射规则**：在配置中用 `source-table` / `sink-table` 逐条指定映射，源与目标可不同名
- **典型场景**：需要改库/表名、多表合并到一张 Doris 表、一表拆分到多表等
- **示例**：`test_route.t1` → `doris_test_route.doris_t1`，`test_route.t2` → `doris_test_route.doris_t2`，`test_route.t3` →
  `doris_test_route.doris_t3`（改库名/表名的一一映射）

```
MySQL test_route.t1  →  Doris doris_test_route.doris_t1
MySQL test_route.t2  →  Doris doris_test_route.doris_t2
MySQL test_route.t3  →  Doris doris_test_route.doris_t3
```

### 2.3 对比小结

| 对比项       | 同步变更                  | 路由变更                      |
|-----------|-----------------------|---------------------------|
| 映射方式      | 按源库表名自动映射到 Doris 同名库表 | 通过 `route` 显式配置每条「源表→目标表」 |
| 是否可改库名/表名 | 否，源名=目标名              | 可以                        |
| 多表写入同一目标表 | 不支持                   | 可以                        |
| 配置复杂度     | 只配 source/sink，较简单    | 需配置 route 列表              |

---

## 3.安装 Flink CDC 3.0

### 3.1 上传安装包到 Linux

在 **Windows** 上使用 `scp`（请按实际路径修改密钥和项目路径）：

```bash
scp -i "E:\vm\vagrant\server01\.vagrant\machines\default\virtualbox\private_key" "D:\github\flink-cdc\cdc-05-MySQL-to-Doris-Streaming-ETL-Implementation\flink-cdc-3.0.0-bin.tar.gz" vagrant@192.168.56.11:/home/vagrant/
```

### 3.2 在 Linux 上解压到指定目录

在 **server01** 上执行：

```bash
[vagrant@server01 ~]$ pwd
/home/vagrant
[vagrant@server01 ~]$ ls
flink-cdc-3.0.0-bin.tar.gz

# 若 /opt/module 不存在，先创建：sudo mkdir -p /opt/module && sudo chown -R vagrant:vagrant /opt/module
[vagrant@server01 ~]$ tar -zxvf flink-cdc-3.0.0-bin.tar.gz -C /opt/module/
flink-cdc-3.0.0/lib/flink-cdc-dist-3.0.0.jar
flink-cdc-3.0.0/bin/
flink-cdc-3.0.0/bin/flink-cdc.sh
flink-cdc-3.0.0/conf/
flink-cdc-3.0.0/conf/log4j-cli.properties
flink-cdc-3.0.0/conf/flink-cdc.yaml
flink-cdc-3.0.0/log/

[vagrant@server01 ~]$ cd /opt/module/
[vagrant@server01 module]$ ls
flink-cdc-3.0.0
```

### 3.3 确认 Flink CDC 目录结构

```bash
[vagrant@server01 flink-cdc-3.0.0]$ ls -la
bin/    conf/   lib/    log/
```

后续将把 **MySQL 与 Doris 的 Pipeline 连接器 jar** 放入 `lib/` 目录。

### 3.4 放入 MySQL / Doris Pipeline 连接器

将 Flink CDC 3.0 的 Pipeline 连接器 jar 放到 Flink CDC 的 `lib` 目录，任务提交时会由 Flink CDC 带给 Flink 集群。

1. **在 Windows 上上传两个 connector jar 到 Linux（server01）**

假设本工程根目录在 `D:\github\flink-cdc`，私钥与前文一致，执行：

```bash
# 上传 MySQL Connector
scp -i "E:\vm\vagrant\server01\.vagrant\machines\default\virtualbox\private_key"  "D:\github\flink-cdc\cdc-05-MySQL-to-Doris-Streaming-ETL-Implementation\flink-cdc-pipeline-connector-mysql-3.0.0.jar"   vagrant@192.168.56.11:/home/vagrant/

# 上传 Doris Connector
scp -i "E:\vm\vagrant\server01\.vagrant\machines\default\virtualbox\private_key"  "D:\github\flink-cdc\cdc-05-MySQL-to-Doris-Streaming-ETL-Implementation\flink-cdc-pipeline-connector-doris-3.0.0.jar"   vagrant@192.168.56.11:/home/vagrant/
```

2. **在 Linux（server01）上将 jar 移动到 Flink CDC 的 `lib` 目录**

```bash
[vagrant@server01 ~]$ ls flink-cdc-pipeline-connector-*.jar
flink-cdc-pipeline-connector-doris-3.0.0.jar
flink-cdc-pipeline-connector-mysql-3.0.0.jar

# 将两个 connector jar 移动到 Flink CDC 安装目录的 lib 下
[vagrant@server01 ~]$ mv flink-cdc-pipeline-connector-mysql-3.0.0.jar /opt/module/flink-cdc-3.0.0/lib/
[vagrant@server01 ~]$ mv flink-cdc-pipeline-connector-doris-3.0.0.jar /opt/module/flink-cdc-3.0.0/lib/

# 确认 lib 目录中已有这三个 jar
[vagrant@server01 ~]$ cd /opt/module/flink-cdc-3.0.0
[vagrant@server01 flink-cdc-3.0.0]$ ls lib/
flink-cdc-dist-3.0.0.jar
flink-cdc-pipeline-connector-doris-3.0.0.jar
flink-cdc-pipeline-connector-mysql-3.0.0.jar
```

## 4.MySQL 源端准备

### 4.1 MySQL 安装

完整安装步骤详见 [mysql-5.7.38.md](../mysql-5.7.38.md)。安装完成后，确认 MySQL 已开启 binlog（`my.cnf` 或 `my.ini` 中）：

```ini
[mysqld]
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
```

修改后重启 MySQL。

### 4.2 开启 binlog

开启MySQL Binlog并重启MySQL

```bash
[vagrant@server01 ~]$ sudo vim /etc/my.cnf
```

在配置文件中添加如下配置信息，开启 `test` 以及 `test_route` 数据库的Binlog：

```ini
[mysqld]
# 数据库id，单实例1即可，主从集群需全局唯一
server-id = 1
# 启动binlog，文件名前缀（默认存储在datadir目录）
log-bin=mysql-bin
# binlog类型，Maxwell强制要求row行级日志
binlog_format=row
# 【必备】数据同步工具核心参数，记录行的完整变更（含旧值+所有新值）
binlog_row_image=FULL
# 【正确配置】指定需要生成binlog的数据库（test和test_route均生效）
binlog-do-db=test
binlog-do-db=test_route
```

重启MySQL服务，让配置生效

```sql
# 通用重启命令，适配大多数Linux发行版
sudo systemctl restart mysqld
# 验证服务是否启动成功（显示active(running)即正常）
sudo systemctl status mysqld
```

登录MySQL，验证配置是否生效

```sql
# 登录MySQL客户端
mysql -uroot -p


-- 1. 检查binlog是否开启（log_bin为ON即正常）
show variables like '%log_bin%';
-- 2. 检查binlog格式和行镜像（必须为ROW和FULL）
show variables like 'binlog_format';
show variables like 'binlog_row_image';
-- 3. 检查指定的同步库（显示test和test_route即生效）
show variables like 'binlog_do_db';
-- 若用了replicate-wild-do-table，执行以下命令检查
show variables like 'replicate_wild_do_table';
```

### 4.3 开启MySQL访问权限

> 说明：当前MySQL安装在server02上，doris安装在server01（192.168.56.11）

```bash
[vagrant@server02 ~]$ mysql -uroot -p
-- 允许 root 从 server01（192.168.56.11） 这个主机访问 server02 上的MySQL

GRANT ALL PRIVILEGES ON *.* TO 'root'@'192.168.56.11' IDENTIFIED BY '000000' WITH GRANT OPTION;

FLUSH PRIVILEGES;
```

### 4.4 修改MySQL时区

步骤 1：先确认 MySQL 真实时区（关键）

```bash
# 登录 MySQL
mysql -uroot -p000000

# 查看 MySQL 全局时区和会话时区
SELECT @@global.time_zone, @@session.time_zone;

# 查看 MySQL 时间与 UTC 时间的偏移（核心）
SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP);


# 如果输出 00:00:00 → MySQL 时区是 UTC；
# 如果输出 08:00:00 → MySQL 时区是东八区（Asia/Shanghai）。
```

步骤 2：统一时区为 Asia/Shanghai（需修改 MySQL 时区，生产推荐）

```bash
# 1. 登录 server02 的 MySQL，临时修改时区（立即生效，重启失效）
mysql -uroot -p000000
SET GLOBAL time_zone = '+8:00';
SET time_zone = '+8:00';
FLUSH PRIVILEGES;
exit;

# 2. 永久修改 MySQL 时区（修改配置文件）
vim /etc/my.cnf  # 或 /etc/mysql/my.cnf
# 在 [mysqld] 段添加
default-time-zone = '+8:00'

# 3. 重启 MySQL
systemctl restart mysqld
# 或者
sudo service mysqld restart


# 4. 修改 Flink CDC 配置（用标准格式 Asia/Shanghai）
vim job/mysql-to-doris.yaml
# 将 server-time-zone 改为：
server-time-zone: Asia/Shanghai
```

### 4.5 创建供同步变更使用的库表

在 MySQL 中执行（示例库 `test`，可按需建多张表）：

```sql
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE IF NOT EXISTS t1 (
  id INT PRIMARY KEY,
  name VARCHAR(64)
);
INSERT INTO t1 VALUES (1, 'a'), (2, 'b');
INSERT INTO t1 VALUES (3, 'c'), (4, 'd');


 select * from t1;
```

### 4.6 创建供路由变更使用的库表

```sql
CREATE DATABASE IF NOT EXISTS test_route;
USE test_route;
CREATE TABLE t1 (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255));
CREATE TABLE t2 (id VARCHAR(255) PRIMARY KEY, name VARCHAR(255));
CREATE TABLE t3 (id VARCHAR(255) PRIMARY KEY, sex VARCHAR(255));
```

## 5.安装 Doris（当前未安装时执行）

### 5.1 安装 Doris

完整安装步骤详见 [Doris-1.2.4-Deploy.md](../Doris-1.2.4-Deploy.md)。

### 5.2 Doris 中创建目标库（同步与路由会用）

```bash
mysql -h server01 -P 9030 -uroot -p000000
```

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE DATABASE IF NOT EXISTS doris_test_route;
```

## 6.Flink 集群与 Checkpoint

Flink CDC 3.0 提交的任务会跑在 Flink 集群上，需先有 Flink 1.18 集群并开启 Checkpoint。

### 6.1 下载 Flink 1.18.0 安装包

例如安装在 `/opt/module/flink-1.18.0`，且 `bin/start-cluster.sh` 可用。

```bash
# 步骤 1：解压并安装 Flink 1.18.0
# wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
[vagrant@server01 ~]$ wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz

# 解压（路径按实际修改）
tar -zxvf /home/vagrant/flink-1.18.0-bin-scala_2.12.tgz -C /opt/module/

# 假设解压目录为 /opt/module/flink-1.18.0
ls /opt/module
# 应该能看到 flink-1.18.0


# 步骤 2：把 FLINK_HOME 指向 1.18.0
# 编辑 /etc/profile（或你之前设置 Flink 的地方），确保是这样的：
# vim /etc/profile
export FLINK_HOME=/opt/module/flink-1.18.0
export PATH=$FLINK_HOME/bin:$PATH

# 保存后：
source /etc/profile
# 确认现在的 flink 命令来自 1.18.0
which flink
flink --version
```

### 6.2 调整 Flink 端口与网络绑定配置

```bash
# 步骤1：修改 flink-conf.yaml
cd $FLINK_HOME

# 1）备份原配置
cp conf/flink-conf.yaml conf/flink-conf.yaml.bak

# 2）删除可能已有的 rest 配置（可多次执行，安全）
sed -i '/^rest.address:/d'      conf/flink-conf.yaml
sed -i '/^rest.bind-address:/d' conf/flink-conf.yaml
sed -i '/^rest.port:/d'         conf/flink-conf.yaml

# 3）在文件末尾追加新的配置
cat << 'EOF' >> conf/flink-conf.yaml
rest.address: 0.0.0.0
rest.bind-address: 0.0.0.0
rest.port: 8081
EOF


# 4）查看数据
grep -n "^rest\." conf/flink-conf.yaml
```

### 6.3 开启 Checkpoint

编辑 **JobManager 所在节点** 的 `conf/flink-conf.yaml`：

```bash
cd $FLINK_HOME
[vagrant@server01 flink-1.18.0]$ vim conf/flink-conf.yaml
```

增加或修改：

```yaml
execution.checkpointing.interval: 5000
```

保存后，若为多节点集群，将 `flink-conf.yaml` 分发到其他节点，再启动集群

### 6.4 启动flink集群

```bash
# 在 server01 节点服务器上执行 start-cluster.sh 启动 Flink 集群
[vagrant@server01 flink-1.18.0]$ cd $FLINK_HOME
[vagrant@server01 flink-1.18.0]$ pwd
/opt/module/flink-1.18.0
[vagrant@server01 flink-1.18.0]$ ls
bin  conf  examples  lib  LICENSE  licenses  log  NOTICE  opt  plugins  README.txt
# 启动集群
# bin/start-cluster.sh
[vagrant@server01 flink-1.18.0]$ bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host server01.
Starting taskexecutor daemon on host server02.
Starting taskexecutor daemon on host server03.

# 查看启动状态
# bin/flink list

# 验证集群状态
# 在浏览器中访问（从宿主机）
http://192.168.56.11:8081


# 常用管理命令
# 正常停止整个集群
bin/stop-cluster.sh

# 重启集群
bin/stop-cluster.sh
bin/start-cluster.sh

# 查看集群信息
bin/flink --help
```

通过 Web UI（如 `http://server01:8081`）确认集群正常。

## 7.同步变更：MySQL → Doris（整库/多表）

实现「MySQL 指定库下所有表」同步到 Doris 同名库表。

### 7.1 创建 job 目录与配置文件

```bash
cd /opt/module/flink-cdc-3.0.0
[vagrant@server01 flink-cdc-3.0.0]$ mkdir -p job
[vagrant@server01 flink-cdc-3.0.0]$ cd job
[vagrant@server01 job]$ vim mysql-to-doris.yaml
```

### 7.2 写入同步变更配置

`mysql-to-doris.yaml` 内容如下（按实际修改主机名、端口、账号密码、时区）：

```yaml
source:
  type: mysql
  hostname: server02
  port: 3306
  username: root
  password: "000000"
  tables: test.\.*
  server-id: 5400-5404
  server-time-zone: UTC+8
sink:
  type: doris
  fenodes: server01:8030
  username: root
  password: "000000"
  table.create.properties.light_schema_change: true
  table.create.properties.replication_num: 1
pipeline:
  name: Sync MySQL Database to Doris
  parallelism: 1
```

### 7.3 启动同步任务

在 **Flink CDC 安装目录** 下执行：

```bash
cd /opt/module/flink-cdc-3.0.0

# 确认目录结构
ls
# 看到 bin/ conf/ lib/ job/ 等

bin/flink-cdc.sh job/mysql-to-doris.yaml
```

若 Flink 未在默认地址，可通过 `conf/flink-cdc.yaml` 或环境变量指定 JobManager 地址。提交成功后，在 Flink Web UI 中能看到名为
“Sync MySQL Database to Doris” 的作业。

### 7.4 验证

1. **在 Doris 中查看同步结果**

```bash
# 通过 Doris 的 MySQL 协议端口 9030 连接（server01 上执行）
mysql -h server01 -P 9030 -uroot -p000000
```

连接成功后，在 Doris 中执行：

```sql
-- 进入 test 库
USE test;

-- 查看表列表（确认和 MySQL 中一致）
SHOW TABLES;

-- 查看具体表数据（以 t1 为例）
SELECT * FROM t1;
```

2. **在 MySQL 中对源表做增删改/加列，再回 Doris 验证**

```bash
# 连接 MySQL（server02 上执行，或从 server01 远程连接）
mysql -h server02 -P 3306 -uroot -p000000
```

在 MySQL 中执行一些操作（以 `test.t1` 为例）：

```sql
USE test;

-- 新增数据
INSERT INTO t1 (id, name) VALUES (3, 'c'), (4, 'd');

-- 修改数据
UPDATE t1 SET name = 'a-updated' WHERE id = 1;

-- 删除数据
DELETE FROM t1 WHERE id = 2;

-- 新增列（演示 schema 变更，注意生产环境需谨慎）
ALTER TABLE t1 ADD COLUMN age INT DEFAULT 18;

-- 再插入一条包含新列的数据
INSERT INTO t1 (id, name, age) VALUES (5, 'e', 25);
```

随后再次回到 Doris（`mysql -h server01 -P 9030 -uroot -p000000`），执行：

```sql
USE test;
SELECT * FROM t1;
```

确认：

- 新增 / 修改 / 删除的数据已实时反映到 Doris。
- 若开启了 `light_schema_change`，新增列 `age` 也已在 Doris 中生效。

---

## 8.路由变更：MySQL 多表到 Doris 指定表

实现「源表与目标表非一一对应」的映射（多张 MySQL 表可写入同一张 Doris 表等）。

### 8.1 编写路由配置文件

```bash
cd /opt/module/flink-cdc-3.0.0
[vagrant@server01 flink-cdc-3.0.0]$ mkdir -p job
[vagrant@server01 flink-cdc-3.0.0]$ cd job
[vagrant@server01 job]$ vim mysql-to-doris-route.yaml
```

内容示例：

```yaml
source:
  type: mysql
  hostname: server02
  port: 3306
  username: root
  password: "000000"
  tables: test_route.\.*
  server-id: 5400-5404
  server-time-zone: UTC+8
  scan.startup.mode: initial  # initial=全量+增量，latest-offset=仅增量
sink:
  type: doris
  fenodes: server01:8030
  benodes: server01:8040   # 可选，BE HTTP 地址，单机即本机；不填则通过 FE 转发
  username: root
  password: "000000"
  table.create.properties.light_schema_change: true
  table.create.properties.replication_num: 1
  sink.max-retries: 10
  sink.batch-size: 20
  sink.batch-interval: 1000
route:
  - source-table: test_route.t1
    sink-table: doris_test_route.doris_t1
  - source-table: test_route.t2
    sink-table: doris_test_route.doris_t2
  - source-table: test_route.t3
    sink-table: doris_test_route.doris_t3
pipeline:
  name: Sync MySQL Database to Doris
  parallelism: 1
```

### 8.2 启动路由同步任务

```bash
cd /opt/module/flink-cdc-3.0.0
[vagrant@server01 flink-cdc-3.0.0]$ bin/flink-cdc.sh job/mysql-to-doris-route.yaml
```

### 8.3 验证

1. **在 Doris 中确认目标表已创建**

```bash
# 通过 Doris 的 MySQL 协议端口 9030 连接（server01 上执行）
mysql -h server01 -P 9030 -uroot -p000000
```

连接成功后，在 Doris 中执行：

```sql
-- 进入路由目标库
USE doris_test_route;

-- 查看表列表（应包含 doris_t1、doris_t2、doris_t3）
SHOW TABLES;

-- 查看各目标表结构（t1→doris_t1，t2→doris_t2，t3→doris_t3）
DESC doris_t1;
DESC doris_t2;
DESC doris_t3;
```

2. **在 MySQL 中对源表做增删改，再回 Doris 验证路由**

```bash
# 连接 MySQL（server02 上执行，或从 server01 远程连接）
mysql -h server02 -P 3306 -uroot -p000000
```

在 MySQL 中执行（按路由：t1 → doris_t1，t2 → doris_t2，t3 → doris_t3）：

```sql
USE test_route;

-- 可选：确认表结构
SHOW TABLES;
DESC t1;
DESC t2;
DESC t3;

-- 向 t1 插入数据
INSERT INTO t1 (id, name) VALUES ('1', 'a'), ('2', 'b');

-- 向 t2 插入数据（t2 写入 doris_t2；t2 结构与 t1 同为 id、name）
INSERT INTO t2 (id, name) VALUES ('100', 'v10'), ('200', 'v20');

-- 向 t3 插入数据（t3 写入 doris_t3；t3 结构为 id、sex）
INSERT INTO t3 (id, sex) VALUES ('1', 'M'), ('2', 'F');

-- 更新 t1 中数据
UPDATE t1 SET name = 'a-updated' WHERE id = '1';

-- 删除 t2 中一条数据
DELETE FROM t2 WHERE id = '100';
```

3. **在 Doris 中验证路由结果**

```bash
# 连接 Doris
mysql -h server01 -P 9030 -uroot -p000000
```

```sql
USE doris_test_route;

-- 验证 doris_t1：应包含来自 t1 的数据
SELECT * FROM doris_t1;

-- 验证 doris_t3：应仅包含来自 t3 的数据
SELECT * FROM doris_t3;
```

确认：

- `doris_t1` 中能看到 `test_route.t1` 和 `test_route.t2` 的合并数据，且增删改已实时同步；
- `doris_t3` 中能看到 `test_route.t3` 的数据，且与 MySQL 一致。

---

## 9.常见问题

| 现象                                                                                             | 建议                                                                                                                                                                      |
|------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Flink CDC 提交失败：连不上 Flink                                                                       | 检查 `conf/flink-cdc.yaml` 或环境变量中的 JobManager 地址与端口（如 8081），并确认 Flink 集群已启动。                                                                                              |
| 连接 Doris 报错                                                                                    | 确认 `fenodes` 的 IP/主机名和端口（如 7030/9020）与 FE `fe.conf` 中配置一致；防火墙、安全组放行相应端口。                                                                                                |
| Doris BE 未 Alive，`ErrMsg` 为 `Connection refused`，BE 日志中有 `file descriptors limit is too small` | 说明 BE 进程因系统文件描述符上限过小启动失败。请参考下方「Doris BE 启动报 file descriptors limit is too small」小节，先提高系统 `ulimit -n` 与 `vm.max_map_count`，再重新启动 BE，并用 `SHOW BACKENDS;` 确认 `Alive=true`。 |
| MySQL 无 binlog                                                                                 | 确认 `log_bin`、`binlog_format=ROW`、`server-id` 已配置并重启 MySQL。                                                                                                              |
| 同步延迟或卡住                                                                                        | 看 Flink 作业背压与 Checkpoint 是否成功；检查 Doris 导入队列与 BE 磁盘。                                                                                                                     |

---

### 9.1 Doris BE 启动报 `file descriptors limit is too small` 的处理

当执行 `./start_be.sh --daemon` 后，BE 进程很快退出，`SHOW BACKENDS;` 中 `Alive=false`，`ErrMsg` 为
`java.net.ConnectException: Connection refused`，且 `be.INFO` 日志中包含：

```text
check fd number failed, error: [INTERNAL_ERROR]file descriptors limit is too small
fail to open StorageEngine, res=[INTERNAL_ERROR]file descriptors limit is too small
```

说明当前系统的文件描述符上限过小，需要按如下步骤处理（在 server01 上执行）：

```bash
# 步骤 1：临时提升当前会话的文件描述符限制（立即生效）
# 切换到root用户（必须root才能修改系统限制）
[vagrant@server01 bin]$ sudo -i

# 提升系统级文件描述符限制
[root@server01 ~]# ulimit -n 65535

# 验证临时修改是否生效
[root@server01 ~]# ulimit -n
# 输出：65535 即成功

# 退出root用户
[root@server01 ~]# exit

# 步骤 2：永久提升文件描述符限制（重启服务器后仍有效）（必须 root）
# 1）提高手动会话的 fd 上限（立即生效）
ulimit -n 65535

# 2）写入 /etc/security/limits.conf，重启后也生效
# 1. 编辑limits.conf文件（系统级限制）
# 切换到root用户
[vagrant@server01 ~]$ sudo -i
[root@server01 bin]$ sudo vim /etc/security/limits.conf

# 在文件末尾添加以下内容（按i进入编辑模式）：
* soft nofile 65535
* hard nofile 65535
* soft nproc 65535
* hard nproc 65535
vagrant soft nofile 65535
vagrant hard nofile 65535
vagrant soft nproc 65535
vagrant hard nproc 65535

# 保存退出：Esc → :wq → 回车

# 2. 编辑limits.d配置（CentOS 7+需额外配置）
[root@server01 bin]$ sudo vim /etc/security/limits.d/20-nproc.conf

# 修改内容为：
* soft nproc 65535
root soft nproc unlimited

# 保存退出


# 步骤 3：让配置立即生效（无需重启服务器）
# 重新登录会话（使limits配置生效）
[root@server01 bin]$ exit
# 重新通过SSH连接server01（Windows PowerShell中重新执行ssh命令）
ssh -i "E:\vm\vagrant\server01\.vagrant\machines\default\virtualbox\private_key" vagrant@192.168.56.11

# 验证永久配置是否生效
[vagrant@server01 ~]$ ulimit -n
# 输出：65535 即成功


# 步骤 4：重新启动 Doris BE
# 进入BE的bin目录
[vagrant@server01 ~]$ cd /opt/module/doris/be/bin/

# 启动BE
[vagrant@server01 bin]$ ./start_be.sh --daemon

# 检查BE进程（此时应能看到进程）
[vagrant@server01 bin]$ ps -ef | grep palo_be | grep -v grep
# 正常输出示例：
vagrant   6000     1 10 17:00 ?        00:00:08 /opt/module/doris/be/lib/palo_be

# 验证BE状态（通过FE）
[vagrant@server01 bin]$ mysql -uroot -P9030 -h127.0.0.1
mysql> SHOW BACKENDS;
# 目标输出：
# Alive = true
# BePort = 8040（非-1）
# ErrMsg = 空
```

启动完成后，通过 FE 再次检查：

```bash
mysql -h 127.0.0.1 -P9030 -uroot -p000000
```

```sql
SHOW BACKENDS;
```

确认：

- backend 记录中的 `Alive` 为 `true`；
- `ErrMsg` 不再是 `Connection refused`。

只有在 FE + BE 都正常、backend 处于 Alive 状态时，Flink CDC 写入 Doris 的数据（如 `test.t1`）才会真正落盘并能在 Doris 中查询到。

### 9.2 Flink RPC 超时（akka.ask.timeout）处理

当 Flink 提交作业或运行中出现 `AskTimeoutException`、`RPC timeout` 等超时类错误时，可适当增大 RPC 与心跳超时。

步骤 1：修改 Flink 配置解决 RPC 超时

在 **Flink 集群的 JobManager 节点** 编辑 `conf/flink-conf.yaml`，增加 / 修改以下配置：

```bash
cd $FLINK_HOME

# 可选：先备份
cp conf/flink-conf.yaml conf/flink-conf.yaml.bak_rpc_timeout

# 删除可能已存在的同名配置（可重复执行，安全）
sed -i '/^akka\.ask\.timeout:/d' conf/flink-conf.yaml
sed -i '/^taskmanager\.rpc\.timeout:/d' conf/flink-conf.yaml
sed -i '/^heartbeat\.timeout:/d' conf/flink-conf.yaml

# 追加新配置到文件末尾
cat << 'EOF' >> conf/flink-conf.yaml

# 增加 RPC 超时时间（从默认 10s 改为 30s）
akka.ask.timeout: 30s
# 增加 TaskManager 与 JobManager 通信超时
taskmanager.rpc.timeout: 30s
# 增加心跳超时（可选，防止节点失联）
heartbeat.timeout: 60000
EOF

# 校验：确认配置已写入
grep -nE '^(akka\.ask\.timeout|taskmanager\.rpc\.timeout|heartbeat\.timeout):' conf/flink-conf.yaml
```

步骤 2：重启 Flink 集群使配置生效

```bash
cd $FLINK_HOME
bin/stop-cluster.sh
bin/start-cluster.sh
```

## 10.步骤总结

1. **安装 Flink CDC 3.0**：解压到 `/opt/module/flink-cdc-3.0.0`，并放入 MySQL/Doris Pipeline 连接器 jar。
2. **安装 Doris**（未安装时）：下载 FE/BE → 配置并启动 FE、BE → 将 BE 加入 FE → 创建 `test`、`doris_test_route` 库。
3. **Flink 集群**：安装 Flink 1.18，在 `flink-conf.yaml` 中设置 `execution.checkpointing.interval: 5000` 并启动集群。
4. **MySQL**：开启 binlog，建好 `test`、`test_route` 及表。
5. **同步变更**：在 `job/mysql-to-doris.yaml` 中配置 source/sink/pipeline，执行
   `bin/flink-cdc.sh job/mysql-to-doris.yaml`。
6. **路由变更**：在 `job/mysql-to-doris-route.yaml` 中配置 route，执行 `bin/flink-cdc.sh job/mysql-to-doris-route.yaml`。

