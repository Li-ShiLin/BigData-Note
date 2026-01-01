<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [1. 概述](#1-%E6%A6%82%E8%BF%B0)
  - [1.1 什么是DataX](#11-%E4%BB%80%E4%B9%88%E6%98%AFdatax)
  - [1.2 DataX的设计](#12-datax%E7%9A%84%E8%AE%BE%E8%AE%A1)
  - [1.3 支持的数据源](#13-%E6%94%AF%E6%8C%81%E7%9A%84%E6%95%B0%E6%8D%AE%E6%BA%90)
  - [1.4 框架设计](#14-%E6%A1%86%E6%9E%B6%E8%AE%BE%E8%AE%A1)
  - [1.5 运行原理](#15-%E8%BF%90%E8%A1%8C%E5%8E%9F%E7%90%86)
  - [1.6 DataX与Sqoop的对比](#16-datax%E4%B8%8Esqoop%E7%9A%84%E5%AF%B9%E6%AF%94)
- [2. 快速入门](#2-%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8)
  - [2.1 官方地址](#21-%E5%AE%98%E6%96%B9%E5%9C%B0%E5%9D%80)
  - [2.2 前置要求](#22-%E5%89%8D%E7%BD%AE%E8%A6%81%E6%B1%82)
  - [2.3 安装](#23-%E5%AE%89%E8%A3%85)
- [3. 使用案例](#3-%E4%BD%BF%E7%94%A8%E6%A1%88%E4%BE%8B)
  - [3.1 从stream流读取数据并打印到控制台](#31-%E4%BB%8Estream%E6%B5%81%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E5%B9%B6%E6%89%93%E5%8D%B0%E5%88%B0%E6%8E%A7%E5%88%B6%E5%8F%B0)
  - [3.2 读取MySQL中的数据存放到HDFS](#32-%E8%AF%BB%E5%8F%96mysql%E4%B8%AD%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AD%98%E6%94%BE%E5%88%B0hdfs)
  - [3.3 读取HDFS数据写入MySQL](#33-%E8%AF%BB%E5%8F%96hdfs%E6%95%B0%E6%8D%AE%E5%86%99%E5%85%A5mysql)
- [4. Oracle数据库](#4-oracle%E6%95%B0%E6%8D%AE%E5%BA%93)
  - [4.1 Oracle数据库简介](#41-oracle%E6%95%B0%E6%8D%AE%E5%BA%93%E7%AE%80%E4%BB%8B)
  - [4.2 安装前的准备](#42-%E5%AE%89%E8%A3%85%E5%89%8D%E7%9A%84%E5%87%86%E5%A4%87)
  - [4.3 安装Oracle数据库](#43-%E5%AE%89%E8%A3%85oracle%E6%95%B0%E6%8D%AE%E5%BA%93)
  - [4.4 设置Oracle监听](#44-%E8%AE%BE%E7%BD%AEoracle%E7%9B%91%E5%90%AC)
  - [4.5 创建数据库](#45-%E5%88%9B%E5%BB%BA%E6%95%B0%E6%8D%AE%E5%BA%93)
  - [4.6 简单使用](#46-%E7%AE%80%E5%8D%95%E4%BD%BF%E7%94%A8)
    - [4.6.1 开启、关闭监听服务](#461-%E5%BC%80%E5%90%AF%E5%85%B3%E9%97%AD%E7%9B%91%E5%90%AC%E6%9C%8D%E5%8A%A1)
    - [4.6.2 进入命令行](#462-%E8%BF%9B%E5%85%A5%E5%91%BD%E4%BB%A4%E8%A1%8C)
  - [4.7 Oracle与MySQL的SQL区别](#47-oracle%E4%B8%8Emysql%E7%9A%84sql%E5%8C%BA%E5%88%AB)
  - [4.8 DataX案例](#48-datax%E6%A1%88%E4%BE%8B)
    - [4.8.1 从Oracle中读取数据存到MySQL](#481-%E4%BB%8Eoracle%E4%B8%AD%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E5%AD%98%E5%88%B0mysql)
    - [4.8.2 读取Oracle的数据存入HDFS中](#482-%E8%AF%BB%E5%8F%96oracle%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AD%98%E5%85%A5hdfs%E4%B8%AD)
- [5. MongoDB](#5-mongodb)
  - [5.1 什么是MongoDB](#51-%E4%BB%80%E4%B9%88%E6%98%AFmongodb)
  - [5.2 MongoDB优缺点](#52-mongodb%E4%BC%98%E7%BC%BA%E7%82%B9)
  - [5.3 基础概念解析](#53-%E5%9F%BA%E7%A1%80%E6%A6%82%E5%BF%B5%E8%A7%A3%E6%9E%90)
  - [5.4 安装](#54-%E5%AE%89%E8%A3%85)
    - [5.4.1 下载地址](#541-%E4%B8%8B%E8%BD%BD%E5%9C%B0%E5%9D%80)
    - [5.4.2 安装](#542-%E5%AE%89%E8%A3%85)
  - [5.5 基础概念详解](#55-%E5%9F%BA%E7%A1%80%E6%A6%82%E5%BF%B5%E8%AF%A6%E8%A7%A3)
    - [5.5.1 数据库](#551-%E6%95%B0%E6%8D%AE%E5%BA%93)
    - [5.5.2 集合](#552-%E9%9B%86%E5%90%88)
    - [5.5.3 文档(Document)](#553-%E6%96%87%E6%A1%A3document)
  - [5.6 DataX导入导出案例](#56-datax%E5%AF%BC%E5%85%A5%E5%AF%BC%E5%87%BA%E6%A1%88%E4%BE%8B)
    - [5.6.1 读取MongoDB的数据导入到HDFS](#561-%E8%AF%BB%E5%8F%96mongodb%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AF%BC%E5%85%A5%E5%88%B0hdfs)
    - [5.6.2 读取MongoDB的数据导入MySQL](#562-%E8%AF%BB%E5%8F%96mongodb%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AF%BC%E5%85%A5mysql)
- [6. SQLServer](#6-sqlserver)
  - [6.1 什么是SQLServer](#61-%E4%BB%80%E4%B9%88%E6%98%AFsqlserver)
  - [6.2 安装](#62-%E5%AE%89%E8%A3%85)
    - [6.2.1 安装要求](#621-%E5%AE%89%E8%A3%85%E8%A6%81%E6%B1%82)
    - [6.2.2 安装步骤](#622-%E5%AE%89%E8%A3%85%E6%AD%A5%E9%AA%A4)
    - [6.2.3 安装配置](#623-%E5%AE%89%E8%A3%85%E9%85%8D%E7%BD%AE)
    - [6.2.4 安装命令行工具](#624-%E5%AE%89%E8%A3%85%E5%91%BD%E4%BB%A4%E8%A1%8C%E5%B7%A5%E5%85%B7)
  - [6.3 简单使用](#63-%E7%AE%80%E5%8D%95%E4%BD%BF%E7%94%A8)
    - [6.3.1 启停命令](#631-%E5%90%AF%E5%81%9C%E5%91%BD%E4%BB%A4)
    - [6.3.2 创建数据库](#632-%E5%88%9B%E5%BB%BA%E6%95%B0%E6%8D%AE%E5%BA%93)
  - [6.4 DataX导入导出案例](#64-datax%E5%AF%BC%E5%85%A5%E5%AF%BC%E5%87%BA%E6%A1%88%E4%BE%8B)
    - [6.4.1 读取SQLServer的数据导入到HDFS](#641-%E8%AF%BB%E5%8F%96sqlserver%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AF%BC%E5%85%A5%E5%88%B0hdfs)
    - [6.4.2 读取SQLServer的数据导入MySQL](#642-%E8%AF%BB%E5%8F%96sqlserver%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AF%BC%E5%85%A5mysql)
- [7. DB2](#7-db2)
  - [7.1 什么是DB2](#71-%E4%BB%80%E4%B9%88%E6%98%AFdb2)
  - [7.2 DB2数据库对象关系](#72-db2%E6%95%B0%E6%8D%AE%E5%BA%93%E5%AF%B9%E8%B1%A1%E5%85%B3%E7%B3%BB)
  - [7.3 安装前的准备](#73-%E5%AE%89%E8%A3%85%E5%89%8D%E7%9A%84%E5%87%86%E5%A4%87)
  - [7.4 安装](#74-%E5%AE%89%E8%A3%85)
  - [7.5 DataX导入导出案例](#75-datax%E5%AF%BC%E5%85%A5%E5%AF%BC%E5%87%BA%E6%A1%88%E4%BE%8B)
    - [7.5.1 注册DB2驱动](#751-%E6%B3%A8%E5%86%8Cdb2%E9%A9%B1%E5%8A%A8)
    - [7.5.2 读取DB2的数据导入到HDFS](#752-%E8%AF%BB%E5%8F%96db2%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AF%BC%E5%85%A5%E5%88%B0hdfs)
    - [7.5.3 读取DB2的数据导入MySQL](#753-%E8%AF%BB%E5%8F%96db2%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AF%BC%E5%85%A5mysql)
- [8. 执行流程源码分析](#8-%E6%89%A7%E8%A1%8C%E6%B5%81%E7%A8%8B%E6%BA%90%E7%A0%81%E5%88%86%E6%9E%90)
  - [8.1 总体流程](#81-%E6%80%BB%E4%BD%93%E6%B5%81%E7%A8%8B)
  - [8.2 程序入口](#82-%E7%A8%8B%E5%BA%8F%E5%85%A5%E5%8F%A3)
  - [8.3 Task 切分逻辑](#83-task-%E5%88%87%E5%88%86%E9%80%BB%E8%BE%91)
    - [8.3.1 并发数的确定](#831-%E5%B9%B6%E5%8F%91%E6%95%B0%E7%9A%84%E7%A1%AE%E5%AE%9A)
  - [8.4 调度](#84-%E8%B0%83%E5%BA%A6)
    - [8.4.1 确定组数和分组](#841-%E7%A1%AE%E5%AE%9A%E7%BB%84%E6%95%B0%E5%92%8C%E5%88%86%E7%BB%84)
    - [8.4.2 调度实现](#842-%E8%B0%83%E5%BA%A6%E5%AE%9E%E7%8E%B0)
  - [8.5 数据传输](#85-%E6%95%B0%E6%8D%AE%E4%BC%A0%E8%BE%93)
    - [8.5.1 限速的实现](#851-%E9%99%90%E9%80%9F%E7%9A%84%E5%AE%9E%E7%8E%B0)
- [9. DataX 使用优化](#9-datax-%E4%BD%BF%E7%94%A8%E4%BC%98%E5%8C%96)
  - [9.1 关键参数](#91-%E5%85%B3%E9%94%AE%E5%8F%82%E6%95%B0)
  - [9.2 优化1：提升每个channel 的速度](#92-%E4%BC%98%E5%8C%961%E6%8F%90%E5%8D%87%E6%AF%8F%E4%B8%AAchannel-%E7%9A%84%E9%80%9F%E5%BA%A6)
  - [9.3 优化2：提升DataX Job 内Channel 并发数](#93-%E4%BC%98%E5%8C%962%E6%8F%90%E5%8D%87datax-job-%E5%86%85channel-%E5%B9%B6%E5%8F%91%E6%95%B0)
    - [9.3.1 配置全局Byte 限速以及单Channel Byte 限速](#931-%E9%85%8D%E7%BD%AE%E5%85%A8%E5%B1%80byte-%E9%99%90%E9%80%9F%E4%BB%A5%E5%8F%8A%E5%8D%95channel-byte-%E9%99%90%E9%80%9F)
    - [9.3.2 配置全局Record 限速以及单Channel Record 限速](#932-%E9%85%8D%E7%BD%AE%E5%85%A8%E5%B1%80record-%E9%99%90%E9%80%9F%E4%BB%A5%E5%8F%8A%E5%8D%95channel-record-%E9%99%90%E9%80%9F)
    - [9.3.3 直接配置Channel 个数](#933-%E7%9B%B4%E6%8E%A5%E9%85%8D%E7%BD%AEchannel-%E4%B8%AA%E6%95%B0)
  - [9.4 优化3：提高JVM 堆内存](#94-%E4%BC%98%E5%8C%963%E6%8F%90%E9%AB%98jvm-%E5%A0%86%E5%86%85%E5%AD%98)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## 1. 概述

### 1.1 什么是DataX

DataX 是阿里巴巴开源的一个异构数据源离线同步工具，致力于实现包括关系型数据库(MySQL、Oracle 等)、HDFS、Hive、ODPS、HBase、FTP等各种异构数据源之间稳定高效的数据同步功能。

![image-20251214004741113](pics/image-20251214004741113.png)

### 1.2 DataX的设计

为了解决异构数据源同步问题，DataX将复杂的网状的同步链路变成了星型数据链路，DataX作为中间传输载体负责连接各种数据源。当需要接入一个新的数据源的时候，只需要将此数据源对接到DataX，便能跟已有的数据源做到无缝数据同步。

![image-20251214004733405](pics/image-20251214004733405.png)

### 1.3 支持的数据源

DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据计算系统都已经接入。DataXFrameWork提供了简单的接口与插件交互，提供简单的插件接入机制，只需要任意加上一种插件，就能无缝对接其他数据源。DataX将数据源读取和写入抽象成为Reader/Writer插件，经过几年积累，DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据存储系统都已经接入。DataX目前支持的数据源如下:

| 类型          | 数据源                 | Reader(读) | Writer(写) |                                                                                                                      文档                                                                                                                       |
|-------------|---------------------|-----------|-----------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| RDBMS关系型数据库 | MySQL               | √         | √         |                                       [读](https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md)、[写](https://github.com/alibaba/DataX/blob/master/mysqlwriter/doc/mysqlwriter.md)                                       |
|             | Oracle              | √         | √         |                                     [读](https://github.com/alibaba/DataX/blob/master/oraclereader/doc/oraclereader.md)、[写](https://github.com/alibaba/DataX/blob/master/oraclewriter/doc/oraclewriter.md)                                     |
|             | OceanBase           | √         | √         | [读](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/use-datax-to-full-migration-data-to-oceanbase)、[写](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/use-datax-to-full-migration-data-to-oceanbase) |
|             | SQLServer           | √         | √         |                               [读](https://github.com/alibaba/DataX/blob/master/sqlserverreader/doc/sqlserverreader.md)、[写](https://github.com/alibaba/DataX/blob/master/sqlserverwriter/doc/sqlserverwriter.md)                               |
|             | PostgreSQL          | √         | √         |                             [读](https://github.com/alibaba/DataX/blob/master/postgresqlreader/doc/postgresqlreader.md)、[写](https://github.com/alibaba/DataX/blob/master/postgresqlwriter/doc/postgresqlwriter.md)                             |
|             | DRDS                | √         | √         |                                         [读](https://github.com/alibaba/DataX/blob/master/drdsreader/doc/drdsreader.md)、[写](https://github.com/alibaba/DataX/blob/master/drdswriter/doc/drdswriter.md)                                         |
|             | 达梦                  | √         | √         |                                                                      [读](https://github.com/alibaba/DataX/blob/master)、[写](https://github.com/alibaba/DataX/blob/master)                                                                      |
|             | 通用RDBMS(支持所有关系型数据库) | √         | √         |                                                                      [读](https://github.com/alibaba/DataX/blob/master)、[写](https://github.com/alibaba/DataX/blob/master)                                                                      |
| 阿里云数仓数据存储   | ODPS                | √         | √         |                                        [读](https://github.com/alibaba/DataX/blob/master/odpsreader/doc/odpsreader.md)、[写](https://github.com/alibaba/DataX/blob/master/odpsswriter/doc/odpswriter.md)                                         |
|             | ADS                 |           | √         |                                                                                 [写](https://github.com/alibaba/DataX/blob/master/adswriter/doc/adswriter.md)                                                                                  |
|             | OSS                 | √         | √         |                                           [读](https://github.com/alibaba/DataX/blob/master/ossreader/doc/ossreader.md)、[写](https://github.com/alibaba/DataX/blob/master/osswriter/doc/osswriter.md)                                           |
|             | OCS                 | √         | √         |                                           [读](https://github.com/alibaba/DataX/blob/master/ocsreader/doc/ocsreader.md)、[写](https://github.com/alibaba/DataX/blob/master/ocswriter/doc/ocswriter.md)                                           |
| NoSQL数据存储   | OTS                 | √         | √         |                                           [读](https://github.com/alibaba/DataX/blob/master/otsreader/doc/otsreader.md)、[写](https://github.com/alibaba/DataX/blob/master/otswriter/doc/otswriter.md)                                           |
|             | HBase0.94           | √         | √         |                               [读](https://github.com/alibaba/DataX/blob/master/hbase094xreader/doc/hbase094xreader.md)、[写](https://github.com/alibaba/DataX/blob/master/hbase094xwriter/doc/hbase094xwriter.md)                               |
|             | HBase1.x            | √         | √         |                                 [读](https://github.com/alibaba/DataX/blob/master/hbase11xreader/doc/hbase11xreader.md)、[写](https://github.com/alibaba/DataX/blob/master/hbase11xwriter/doc/hbase11xwriter.md)                                 |
|             | HBase2.x            | √         | √         |                           [读](https://github.com/alibaba/DataX/blob/master/hbase20xsqlreader/doc/hbase20xsqlreader.md)、[写](https://github.com/alibaba/DataX/blob/master/hbase20xsqlwriter/doc/hbase20xsqlwriter.md)                           |
|             | MongoDB             | √         | √         |                                       [读](https://github.com/alibaba/DataX/blob/master/mongoreader/doc/mongoreader.md)、[写](https://github.com/alibaba/DataX/blob/master/mongowriter/doc/mongowriter.md)                                       |
|             | Hive                | √         | √         |                                         [读](https://github.com/alibaba/DataX/blob/master/hdfsreader/doc/hdfsreader.md)、[写](https://github.com/alibaba/DataX/blob/master/hdfswriter/doc/hdfswriter.md)                                         |
| 无结构化数据存储    | TxtFile             | √         | √         |                                   [读](https://github.com/alibaba/DataX/blob/master/txtfilereader/doc/txtfilereader.md)、[写](https://github.com/alibaba/DataX/blob/master/txtfilewriter/doc/txtfilewriter.md)                                   |
|             | FTP                 | √         | √         |                                           [读](https://github.com/alibaba/DataX/blob/master/ftpreader/doc/ftpreader.md)、[写](https://github.com/alibaba/DataX/blob/master/ftpwriter/doc/ftpwriter.md)                                           |
|             | HDFS                | √         | √         |                                         [读](https://github.com/alibaba/DataX/blob/master/hdfsreader/doc/hdfsreader.md)、[写](https://github.com/alibaba/DataX/blob/master/hdfswriter/doc/hdfswriter.md)                                         |
|             | Elasticsearch       |           | √         |                                                                       [写](https://github.com/alibaba/DataX/blob/master/elasticsearchwriter/doc/elasticsearchwriter.md)                                                                        |
|             | Clickhouse          |           | √         |                                                                                                                       写                                                                                                                       |

### 1.4 框架设计

![image-20251214004909993](pics/image-20251214004909993.png)

DataX框架包含以下核心组件：

- **Reader：** 数据采集模块，负责采集数据源的数据，将数据发送给Framework。
- **Writer：** 数据写入模块，负责不断向Framework取数据，并将数据写入到目的端。
- **Framework：** 用于连接reader和writer，作为两者的数据传输通道，并处理缓冲、流控、并发、数据转换等核心技术问题。

### 1.5 运行原理

DataX的运行架构包含以下核心概念：

- **Job：** 单个作业的管理节点，负责数据清理、子任务划分、TaskGroup监控管理。
- **Task：** 由Job切分而来，是DataX作业的最小单元，每个Task负责一部分数据的同步工作。
- **Schedule：** 将Task组成TaskGroup，单个TaskGroup的并发数量为5。
- **TaskGroup：** 负责启动Task。

![image-20251214005044667](pics/image-20251214005044667.png)

**举例说明：**

用户提交了一个DataX 作业，并且配置了20 个并发，目的是将一个100 张分表的mysql 数据同步到odps 里面。 DataX 的调度决策思路是：

1. DataXJob 根据分库分表切分成了100 个Task。
2. 根据20 个并发，DataX 计算共需要分配4 个TaskGroup。
3. 4 个TaskGroup 平分切分好的100 个Task，每一个TaskGroup 负责以5 个并发共计运行25 个Task。

### 1.6 DataX与Sqoop的对比

| 功能       | DataX          | Sqoop          |
|----------|----------------|----------------|
| 运行模式     | 单进程多线程         | MR             |
| MySQL 读写 | 单机压力大；读写粒度容易控制 | MR 模式重，写出错处理麻烦 |
| Hive 读写  | 单机压力大          | 很好             |
| 文件格式     | orc 支持         | orc 不支持，可添加    |
| 分布式      | 不支持，可以通过调度系统规避 | 支持             |
| 流控       | 有流控功能          | 需要定制           |
| 统计信息     | 已有一些统计，上报需定制   | 没有，分布式的数据收集不方便 |
| 数据校验     | 在core 部分有校验功能  | 没有，分布式的数据收集不方便 |
| 监控       | 需要定制           | 需要定制           |
| 社区       | 开源不久，社区不活跃     | 一直活跃，核心部分变动很少  |

---

## 2. 快速入门

### 2.1 官方地址

- **下载地址：**
    - http://datax-opensource.oss-cn-hangzhou.aliyuncs.com/datax.tar.gz
    - https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202308/datax.tar.gz

- **源码地址：** https://github.com/alibaba/DataX

### 2.2 前置要求

- Linux
- JDK(1.8 以上，推荐1.8)
- Python(推荐Python2.6.X)

### 2.3 安装

1.将下载好的datax.tar.gz 上传到server01

```bash
# win11系统上传Linux系统
scp -i "D:\application\VM-Vagrant\server01\.vagrant\machines\default\virtualbox\private_key" "E:\dataX-sgg\DataX\software\datax.tar.gz" vagrant@192.168.56.11:/home/vagrant/
```

2.解压datax.tar.gz 到/opt/module

```bash
[vagrant@server01 ~]$ tar -zxvf datax.tar.gz -C /opt/module/
```

3.查看目录结构

```bash
[vagrant@server01 datax]$ pwd
/opt/module/datax
[vagrant@server01 datax]$ sudo yum install tree -y
[vagrant@server01 datax]$ tree -L 3
.
├── bin
│   ├── datax.py
│   ├── dxprof.py
│   └── perftrace.py
├── conf
│   ├── core.json
│   └── logback.xml
├── job
│   └── job.json
├── lib
│   ├── commons-beanutils-1.9.2.jar
│   ├── commons-cli-1.2.jar
│   ├── commons-codec-1.9.jar
│   ├── commons-collections-3.2.1.jar
│   ├── commons-configuration-1.10.jar
│   ├── commons-io-2.4.jar
│   ├── commons-lang-2.6.jar
│   ├── commons-lang3-3.3.2.jar
│   ├── commons-logging-1.1.1.jar
│   ├── commons-math3-3.1.1.jar
│   ├── datax-common-0.0.1-SNAPSHOT.jar
│   ├── datax-core-0.0.1-SNAPSHOT.jar
│   ├── datax-transformer-0.0.1-SNAPSHOT.jar
│   ├── fastjson-1.1.46.sec01.jar
│   ├── fluent-hc-4.4.jar
│   ├── groovy-all-2.1.9.jar
│   ├── hamcrest-core-1.3.jar
│   ├── httpclient-4.4.jar
│   ├── httpcore-4.4.jar
│   ├── janino-2.5.16.jar
│   ├── logback-classic-1.0.13.jar
│   ├── logback-core-1.0.13.jar
│   └── slf4j-api-1.7.10.jar
├── plugin
│   ├── reader
│   │   ├── cassandrareader
│   │   ├── drdsreader
│   │   ├── ftpreader
│   │   ├── hbase094xreader
│   │   ├── hbase11xreader
│   │   ├── hdfsreader
│   │   ├── mongodbreader
│   │   ├── mysqlreader
│   │   ├── odpsreader
│   │   ├── oraclereader
│   │   ├── ossreader
│   │   ├── otsreader
│   │   ├── otsstreamreader
│   │   ├── postgresqlreader
│   │   ├── rdbmsreader
│   │   ├── sqlserverreader
│   │   ├── streamreader
│   │   └── txtfilereader
│   └── writer
│       ├── adswriter
│       ├── cassandrawriter
│       ├── drdswriter
│       ├── ftpwriter
│       ├── hbase094xwriter
│       ├── hbase11xsqlwriter
│       ├── hbase11xwriter
│       ├── hdfswriter
│       ├── mongodbwriter
│       ├── mysqlwriter
│       ├── ocswriter
│       ├── odpswriter
│       ├── oraclewriter
│       ├── osswriter
│       ├── otswriter
│       ├── postgresqlwriter
│       ├── rdbmswriter
│       ├── sqlserverwriter
│       ├── streamwriter
│       └── txtfilewriter
├── script
│   └── Readme.md
└── tmp
    └── readme.txt
```

4.查看配置模板

```bash
[vagrant@server01 datax]$ cat job/job.json
{
    "job": {
        "setting": {
            "speed": {
                "byte":10485760
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column" : [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19890604,
                                "type": "long"
                            },
                            {
                                "value": "1989-06-04 00:00:00",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 100000
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": false,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
}
```

5.运行自检脚本

```bash
[vagrant@server01 datax]$ pwd
/opt/module/datax
[vagrant@server01 datax]$ python bin/datax.py ./job/job.json
```

![image-20251220171005936](pics/image-20251220171005936.png)

---

## 3. 使用案例

### 3.1 从stream流读取数据并打印到控制台

**查看配置模板**

```bash
# 命令
# python datax.py -r streamreader -w streamwriter
[vagrant@server01 bin]$ python datax.py -r streamreader -w streamwriter
```

示例：

```bash
[vagrant@server01 datax]$ bin/datax.py -r streamreader -w streamwriter
DataX (DATAX-OPENSOURCE-3.0), From Alibaba !
Copyright (C) 2010-2017, Alibaba Group. All Rights Reserved.


Please refer to the streamreader document:
     https://github.com/alibaba/DataX/blob/master/streamreader/doc/streamreader.md

Please refer to the streamwriter document:
     https://github.com/alibaba/DataX/blob/master/streamwriter/doc/streamwriter.md

Please save the following configuration as a json file and  use
     python {DATAX_HOME}/bin/datax.py {JSON_FILE_NAME}.json
to run the job.

{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column": [],
                        "sliceRecordCount": ""
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "encoding": "",
                        "print": true
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": ""
            }
        }
    }
}
```

配置模板：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [],
            "sliceRecordCount": ""
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "",
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": ""
      }
    }
  }
}
```

**根据模板编写配置文件**

```bash
[vagrant@server01 job]$ sudo vim /opt/module/datax/job/stream2stream.json
```

填写以下内容：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "sliceRecordCount": 10,
            "column": [
              {
                "type": "long",
                "value": "10"
              },
              {
                "type": "string",
                "value": "hello，DataX"
              }
            ]
          }
        },
        "writer": {
          "name": "streamwriter",
          "parameter": {
            "encoding": "UTF-8",
            "print": true
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 1
      }
    }
  }
}
```

**运行**

```bash
[vagrant@server01 job]$ /opt/module/datax/bin/datax.py /opt/module/datax/job/stream2stream.json
```

![image-20251221193652618](pics/image-20251221193652618.png)

---

### 3.2 读取MySQL中的数据存放到HDFS

**查看官方模板**

```bash
[vagrant@server01 ~]$ python /opt/module/datax/bin/datax.py -r mysqlreader -w hdfswriter
```

配置模板：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "column": [],
            "connection": [
              {
                "jdbcUrl": [],
                "table": []
              }
            ],
            "password": "",
            "username": "",
            "where": ""
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "column": [],
            "compress": "",
            "defaultFS": "",
            "fieldDelimiter": "",
            "fileName": "",
            "fileType": "",
            "path": "",
            "writeMode": ""
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": ""
      }
    }
  }
}
```

**mysqlreader参数解析**

mysqlreader参数解析：

![image-20251221195828943](pics/image-20251221195828943.png)

```json
{
  "reader": {
    "name": "mysqlreader",
    "parameter": {
      "column": [],
      "connection": [
        {
          "jdbcUrl": [],
          "table": [],
          "querySql": []
        }
      ],
      "password": "",
      "username": "",
      "where": "",
      "splitPk": ""
    }
  }
}
```

**注意：** 【】中的参数为可选参数

- **name：** reader名
- **column：** 需要同步的列名集合，使用JSON数组描述自带信息，*代表所有列
- **jdbcUrl：** 对数据库的JDBC连接信息，使用JSON数组描述，支持多个连接地址
- **table：** 需要同步的表，支持多个
- **querySql：** 自定义SQL，配置它后，mysqlreader直接忽略table、column、where
- **password：** 数据库用户名对应的密码
- **username：** 数据库用户名
- **where：** 筛选条件
- **splitPK：** 数据分片字段，一般是主键，仅支持整型

**hdfswriter参数解析**

hdfswriter参数解析：

![image-20251221200250375](pics/image-20251221200250375.png)

```json
{
  "writer": {
    "name": "hdfswriter",
    "parameter": {
      "column": [],
      "compress": "",
      "defaultFS": "",
      "fieldDelimiter": "",
      "fileName": "",
      "fileType": "",
      "path": "",
      "writeMode": ""
    }
  }
}
```

- **name：** writer名
- **column：** 写入数据的字段，其中name指定字段名，type指定类型
- **compress：** hdfs文件压缩类型，默认不填写意味着没有压缩
- **defaultFS：** hdfs文件系统namenode节点地址，格式：hdfs://ip:端口
- **fieldDelimiter：** 字段分隔符
- **fileName：** 写入文件名
- **fileType：** 文件的类型，目前只支持用户配置为"text"或"orc"
- **path：** 存储到Hadoop hdfs文件系统的路径信息
- **writeMode：** hdfswriter写入前数据清理处理模式：
    - **append：** 写入前不做任何处理，DataX hdfswriter直接使用filename写入，并保证文件名不冲突
    - **nonConflict：** 如果目录下有fileName前缀的文件，直接报错

**准备数据**

1.创建student

```sql
mysql
> create
database datax;

mysql
> use datax;

mysql
>
create table student
(
    id   int,
    name varchar(20)
);
```

2.插入数据

```sql
mysql
> insert into student values(1001,'zhangsan'),(1002,'lisi'),(1003,'wangwu');
mysql
>
select *
from student;
+------+----------+
| id   | name     |
+------+----------+
| 1001 | zhangsan |
| 1002 | lisi     |
| 1003 | wangwu   |
+------+----------+
```

**编写配置文件**

```bash
[vagrant@server01 datax]$ sudo vim /opt/module/datax/job/mysql2hdfs.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "column": [
              "id",
              "name"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:mysql://server01:3306/datax?allowPublicKeyRetrieval=true"
                ],
                "table": [
                  "student"
                ]
              }
            ],
            "username": "remote_user",
            "password": "RemotePassword123!"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "defaultFS": "hdfs://server01:9000",
            "fieldDelimiter": "\t",
            "fileName": "student.txt",
            "fileType": "text",
            "path": "/",
            "writeMode": "append"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

**执行任务**

1.启动hdfs，启动yarn

```bash
# 启动hdfs，启动yarn
# start-dfs.sh && start-yarn.sh
start-dfs.sh && start-yarn.sh

# 验证HDFS进程（server01执行jps）
jps
# 预期进程：NameNode、SecondaryNameNode、Jps

# 检查HDFS状态
hdfs dfsadmin -report
# 检查Web UI（如果已启用）
# http://server01:50070

# 验证从节点进程（server02、server03执行jps）
ssh server02 jps  # 预期进程：DataNode、Jps
ssh server03 jps  # 预期进程：DataNode、Jps
# 或者
ssh server02 "source /etc/profile && jps"
ssh server03 "source /etc/profile && jps"

# HDFS WebUI：访问 http://192.168.56.11:50070，查看 DataNodes（应显示 2 个从节点）
# YARN WebUI：访问 http://192.168.56.11:8088，查看 Nodes（应显示 2 个 NodeManager）
# HDFS WebUI 地址
# Hadoop 3.x（主流版本）：http://<server01的IP或主机名>:9870
# 示例：http://192.168.1.100:9870 或 http://server01:9870
# Hadoop 2.x：http://<server01的IP或主机名>:50070
# YARN WebUI 地址（新旧版本端口一致）：http://<server01的IP或主机名>:8088示例：http://192.168.1.100:8088 或 http://server01:8088
```

2.查看HDFS的NameNode地址

```bash
# 查看HDFS的NameNode地址(配置到上面的DataX JSON配置文件中)
# sudo cat /opt/module/hadoop-2.7.2/etc/hadoop/core-site.xml
# NameNode地址:    hdfs://server01:9000
[vagrant@server01 ~]$ sudo cat /opt/module/hadoop-2.7.2/etc/hadoop/core-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->




<configuration>
    <!-- 指定HDFS的NameNode地址（主节点server01） -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://server01:9000</value>
    </property>
    <!-- 指定Hadoop临时目录（需手动创建） -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/module/hadoop-2.7.2/data/tmp</value>
    </property>
    <!-- 解决Hadoop 2.x与JDK 8的兼容性问题 -->
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>vagrant</value>
    </property>
</configuration>
```

3.下载兼容 MySQL 8.4.2 的 JDBC 驱动

```bash
[vagrant@server01 libs]$ mysql --version
mysql  Ver 8.4.2 for Linux on x86_64 (MySQL Community Server - GPL)

# 进入DataX的MySQLReader驱动目录
cd /opt/module/datax/plugin/reader/mysqlreader/libs/

# 下载最新的mysql-connector-j-8.0.33.jar（使用阿里云镜像）
wget https://maven.aliyun.com/repository/public/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

# 验证下载结果
ls -l | grep mysql-connector-j-8.0.33.jar
```

4.执行任务

```bash
[vagrant@server01 datax]$ /opt/module/datax/bin/datax.py /opt/module/datax/job/mysql2hdfs.json
```

执行结果示例：

```
2019-05-17 16:02:16.581 [job-0] INFO  JobContainer -
任务启动时刻                    : 2019-05-17 16:02:04
任务结束时刻                    : 2019-05-17 16:02:16
任务总计耗时                    :                 12s
任务平均流量                    :                3B/s
记录写入速度                    :              0rec/s
读出记录总数                    :                   3
读写失败总数                    :                   0
```

**查看hdfs**

访问hdfs文件：`http://192.168.56.11:50070/explorer.html#/`

![image-20251226001629483](pics/image-20251226001629483.png)

**注意：** HdfsWriter 实际执行时会在该文件名后添加随机的后缀作为每个线程写入实际文件名。

**关于HA的支持**

hdfs高可用配置：

```json
{
  "hadoopConfig": {
    "dfs.nameservices": "ns",
    "dfs.ha.namenodes.ns": "nn1,nn2",
    "dfs.namenode.rpc-address.ns.nn1": "主机名:端口",
    "dfs.namenode.rpc-address.ns.nn2": "主机名:端口",
    "dfs.client.failover.proxy.provider.ns": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
  }
}
```

完整配置示例：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "column": [
              "id",
              "name"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:mysql://server01:3306/datax?allowPublicKeyRetrieval=true"
                ],
                "table": [
                  "student"
                ]
              }
            ],
            "username": "remote_user",
            "password": "RemotePassword123!"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "defaultFS": "hdfs://server01:9000",
            "fieldDelimiter": "\t",
            "fileName": "student.txt",
            "fileType": "text",
            "path": "/",
            "writeMode": "append",
            "hadoopConfig": {
              "dfs.nameservices": "ns",
              "dfs.ha.namenodes.ns": "nn1,nn2",
              "dfs.namenode.rpc-address.ns.nn1": "主机名:端口",
              "dfs.namenode.rpc-address.ns.nn2": "主机名:端口",
              "dfs.client.failover.proxy.provider.ns": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            }
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

---

### 3.3 读取HDFS数据写入MySQL

**将上个案例上传的文件改名**

```bash
# 查看HDFS根目录下的文件（包含带后缀的student文件）
# hadoop fs -ls /
[vagrant@server01 ~]$ hadoop fs -ls /
Found 4 items
drwxr-xr-x   - vagrant supergroup          0 2025-11-18 17:09 /hbase
-rw-r--r--   3 vagrant supergroup         36 2025-12-25 16:13 /student.txt__7d7ae795_6133_4ae4_b10a_2cc161c04c70
drwxr-xr-x   - vagrant supergroup          0 2025-11-08 16:50 /tmp
drwx------   - vagrant supergroup          0 2025-11-08 16:50 /user

# 重命名
# hadoop fs -mv /student.txt__7d7ae795_6133_4ae4_b10a_2cc161c04c70 /student.txt
[vagrant@server01 ~]$ hadoop fs -mv /student.txt__7d7ae795_6133_4ae4_b10a_2cc161c04c70 /student.txt
```

**查看官方模板**

```bash
[vagrant@server01 datax]$ python bin/datax.py -r hdfsreader -w mysqlwriter
```

配置模板：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "hdfsreader",
          "parameter": {
            "column": [],
            "defaultFS": "",
            "encoding": "UTF-8",
            "fieldDelimiter": ",",
            "fileType": "orc",
            "path": ""
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "column": [],
            "connection": [
              {
                "jdbcUrl": "",
                "table": []
              }
            ],
            "password": "",
            "preSql": [],
            "session": [],
            "username": "",
            "writeMode": ""
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": ""
      }
    }
  }
}
```

**创建配置文件**

```bash
[vagrant@server01 datax]$ sudo vim /opt/module/datax/job/hdfs2mysql.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "hdfsreader",
          "parameter": {
            "column": [
              "*"
            ],
            "defaultFS": "hdfs://server01:9000",
            "encoding": "UTF-8",
            "fieldDelimiter": "\t",
            "fileType": "text",
            "path": "/student.txt"
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "column": [
              "id",
              "name"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://server01:3306/datax?allowPublicKeyRetrieval=true",
                "table": [
                  "student2"
                ]
              }
            ],
            "username": "remote_user",
            "password": "RemotePassword123!",
            "writeMode": "insert"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

**在MySQL的datax数据库中创建student2**

```sql
use
datax;

CREATE TABLE `student2`
(
    `id`   int                                    DEFAULT NULL,
    `name` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
```

**下载兼容 MySQL 8.4.2 的新驱动**

```bash
# 进入DataX的MySQLWriter驱动目录
cd /opt/module/datax/plugin/writer/mysqlwriter/libs/

# 下载mysql-connector-j-8.0.33.jar（阿里云镜像，国内稳定）
wget https://maven.aliyun.com/repository/public/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

# 若下载失败，使用华为云镜像备选
wget https://mirrors.huaweicloud.com/repository/maven/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
```

**执行任务**

```bash
[vagrant@server01 datax]$ bin/datax.py job/hdfs2mysql.json
```

执行结果示例：

```
2019-05-17 16:21:53.616 [job-0] INFO  JobContainer -
任务启动时刻                    : 2019-05-17 16:21:41
任务结束时刻                    : 2019-05-17 16:21:53
任务总计耗时                    :                 11s
任务平均流量                    :                3B/s
记录写入速度                    :              0rec/s
读出记录总数                    :                   3
读写失败总数                    :                   0
```

**查看student2表**

```sql
mysql
>
select *
from student2;
+------+----------+
| id   | name     |
+------+----------+
| 1001 | zhangsan |
| 1002 | lisi     |
| 1003 | wangwu   |
+------+----------+
3 rows in set (0.00 sec)
```

---

## 4. Oracle数据库

以下操作使用root账号。

### 4.1 Oracle数据库简介

Oracle Database，又名Oracle RDBMS，或简称Oracle。是甲骨文公司的一款关系数据库管理系统。它是在数据库领域一直处于领先地位的产品。可以说Oracle
数据库系统是目前世界上流行的关系数据库管理系统，系统可移植性好、使用方便、功能强，适用于各类大、中、小、微机环境。它是一种高效率、可靠性好的、适应高吞吐量的数据库解决方案。

### 4.2 安装前的准备

**安装依赖**

```bash
yum install -y bc binutils compat-libcap1 compat-libstdc++33 elfutils-libelf elfutils-libelf-devel fontconfig-devel glibc glibc-devel ksh libaio libaio-devel libX11 libXau libXi libXtst libXrender libXrender-devel libgcc libstdc++ libstdc++-devel libxcb make smartmontools sysstat kmod* gcc-c++ compat-libstdc++-33
```

**配置用户组**

Oracle 安装文件不允许通过root 用户启动，需要为oracle 配置一个专门的用户。

1. 创建sql 用户组

```bash
[root@server01 software]# groupadd sql
```

2. 创建oracle 用户并放入sql 组中

```bash
[root@server01 software]# useradd oracle -g sql
```

3. 修改oracle 用户登录密码，输入密码后即可使用oracle 用户登录系统

```bash
[root@server01 software]# passwd oracle
```

**上传安装包并解压**

**注意：** 19c 需要把软件包直接解压到ORACLE_HOME 的目录下

```bash
[root@server01 software]# mkdir -p /home/oracle/app/oracle/product/19.3.0/dbhome_1
[root@server01 software]# unzip LINUX.X64_193000_db_home.zip -d /home/oracle/app/oracle/product/19.3.0/dbhome_1
```

修改所属用户和组：

```bash
[root@server01 dbhome_1]# chown -R oracle:sql /home/oracle/app/
```

**修改配置文件sysctl.conf**

```bash
[root@server01 module]# vim /etc/sysctl.conf
```

删除里面的内容，添加如下内容：

```
net.ipv4.ip_local_port_range = 9000 65500
fs.file-max = 6815744
kernel.shmall = 10523004
kernel.shmmax = 6465333657
kernel.shmmni = 4096
kernel.sem = 250 32000 100 128
net.core.rmem_default=262144
net.core.wmem_default=262144
net.core.rmem_max=4194304
net.core.wmem_max=1048576
fs.aio-max-nr = 1048576
```

**参数解析：**

- **net.ipv4.ip_local_port_range：** 可使用的IPv4 端口范围
- **fs.file-max：** 该参数表示文件句柄的最大数量。文件句柄设置表示在linux 系统中可以打开的文件数量
- **kernel.shmall：** 该参数表示系统一次可以使用的共享内存总量（以页为单位）
- **kernel.shmmax：** 该参数定义了共享内存段的最大尺寸（以字节为单位）
- **kernel.shmmni：** 这个内核参数用于设置系统范围内共享内存段的最大数量
- **kernel.sem：** 该参数表示设置的信号量
- **net.core.rmem_default：** 默认的TCP 数据接收窗口大小（字节）
- **net.core.wmem_default：** 默认的TCP 数据发送窗口大小（字节）
- **net.core.rmem_max：** 最大的TCP 数据接收窗口（字节）
- **net.core.wmem_max：** 最大的TCP 数据发送窗口（字节）
- **fs.aio-max-nr：** 同时可以拥有的的异步IO 请求数目

**修改配置文件limits.conf**

```bash
[root@server01 module]# vim /etc/security/limits.conf
```

在文件末尾添加：

```
oracle soft nproc 2047
oracle hard nproc 16384
oracle soft nofile 1024
oracle hard nofile 65536
```

重启机器生效。

### 4.3 安装Oracle数据库

**设置环境变量**

```bash
[oracle@server01 dbhome_1]# vim /home/oracle/.bash_profile
```

添加：

```bash
#ORACLE_HOME
export ORACLE_BASE=/home/oracle/app/oracle
export ORACLE_HOME=/home/oracle/app/oracle/product/19.3.0/dbhome_1
export PATH=$PATH:$ORACLE_HOME/bin
export ORACLE_SID=orcl
export NLS_LANG=AMERICAN_AMERICA.ZHS16GBK
```

使环境变量生效：

```bash
[oracle@server01 ~]$ source /home/oracle/.bash_profile
```

**进入虚拟机图像化页面操作**

```bash
[oracle@server01 ~]# cd /opt/module/oracle
[oracle@server01 database]# ./runInstaller
```

**安装数据库**

1.选择仅安装数据库软件

![image-20251214005554714](pics/image-20251214005554714.png)

2.选择单实例数据库安装

![image-20260101184729428](pics/image-20260101184729428.png)

3.选择企业版，默认

![image-20260101184746037](pics/image-20260101184746037.png)

4.设置安装位置

![image-20260101184802204](pics/image-20260101184802204.png)

5.操作系统组设置

![image-20260101184816227](pics/image-20260101184816227.png)

6.配置root 脚本自动执行

![image-20260101184829894](pics/image-20260101184829894.png)

7.条件检查通过后，选择开始安装

![image-20260101184844778](pics/image-20260101184844778.png)

8.运行root 脚本

![image-20260101184857224](pics/image-20260101184857224.png)

9.安装完成

![image-20260101184903076](pics/image-20260101184903076.png)

### 4.4 设置Oracle监听

**命令行输入以下命令**

```bash
[oracle@hadoop2 ~]$ netca
```

![image-20260101184921871](pics/image-20260101184921871.png)

**选择添加**

![image-20260101184942205](pics/image-20260101184942205.png)

**设置监听名，默认即可**

![image-20260101184950356](pics/image-20260101184950356.png)

**选择协议，默认即可**

![image-20260101185007133](pics/image-20260101185007133.png)

**设置端口号，默认即可**

![image-20260101185023104](pics/image-20260101185023104.png)

**配置更多监听，默认**

![image-20260101185032715](pics/image-20260101185032715.png)

**完成**

![image-20260101185213938](pics/image-20260101185213938.png)

### 4.5 创建数据库

**进入创建页面**

```bash
[oracle@hadoop2 ~]$ dbca
```

**选择创建数据库**

![image-20260101185235030](pics/image-20260101185235030.png)

**选择高级配置**

![image-20260101185329103](pics/image-20260101185329103.png)

**选择数据仓库**

![image-20260101185343684](pics/image-20260101185343684.png)

**将图中所示对勾去掉**

![image-20260101185359755](pics/image-20260101185359755.png)

**存储选项**

![image-20260101185416210](pics/image-20260101185416210.png)

**快速恢复选项**

![image-20260101185432824](pics/image-20260101185432824.png)

**选择监听程序**

![image-20260101185446062](pics/image-20260101185446062.png)

**如图设置**

![image-20260101185503956](pics/image-20260101185503956.png)

**使用自动内存管理**

![image-20260101185514406](pics/image-20260101185514406.png)

**管理选项，默认**

![image-20260101185525018](pics/image-20260101185525018.png)

**设置统一密码**

![image-20260101185540790](pics/image-20260101185540790.png)

**创建选项，选择创建数据库**

![image-20260101185552351](pics/image-20260101185552351.png)

**概要，点击完成**

![image-20260101185609809](pics/image-20260101185609809.png)

**等待安装**

![image-20260101185629480](pics/image-20260101185629480.png)

![image-20260101185640873](pics/image-20260101185640873.png)

### 4.6 简单使用

#### 4.6.1 开启、关闭监听服务

开启服务：

```bash
[oracle@server01 ~]$ lsnrctl start
```

关闭服务：

```bash
[oracle@server01 ~]$ lsnrctl stop
```

#### 4.6.2 进入命令行

```bash
[oracle@server01 ~]$ sqlplus
```

输出示例：

```
[oracle@server01 ~]$ sqlplus
SQL*Plus: Release 19.0.0.0.0 - Production on Fri Sep 3 01:44:30 2021
Version 19.3.0.0.0


Copyright (c) 1982, 2019, Oracle.	All rights reserved. Enter user-name: system
Enter password: （这里输入之前配置的统一密码）

Connected to:
Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production Version 19.3.0.0.0

SQL>
```

**创建用户并授权**

```sql
SQL> create
user vagrant identified by 000000;
User created.
SQL> grant create
session,
create table, create view, create sequence,unlimited tablespace to vagrant;
Grant
succeeded
.
```

**进入vagrant账号，创建表**

```sql
SQL>
create TABLE student
(
    id   INTEGER,
    name VARCHAR2(20)
);
SQL> insert into student values (1,'zhangsan');
SQL>
select *
from student;
```

输出示例：

```
ID  NAME
---------- ----------------------------------------
1  zhangsan
```

**注意：** 安装完成后重启机器可能出现ORACLE not available 错误，解决方法如下：

```bash
[oracle@server01 ~]$ sqlplus / as sysdba
SQL> startup
SQL> conn vagrant
Enter password:
```

### 4.7 Oracle与MySQL的SQL区别

| 类型                       | Oracle            | MySQL        |
|--------------------------|-------------------|--------------|
| 整型                       | number(N)/integer | int/integer  |
| 浮点型                      | float             | float/double |
| 字符串类型                    | varchar2(N)       | varchar(N)   |
| NULL                     | ''                | null 和''不一样  |
| 分页                       | rownum            | limit        |
| ""                       | 限制很多，一般不让用        | 与单引号一样       |
| 价格                       | 闭源，收费             | 开源，免费        |
| 主键自动增长                   | ×                 | √            |
| if not exists            | ×                 | √            |
| auto_increment           | ×                 | √            |
| create database          | ×                 | √            |
| select * from table as t | ×                 | √            |

### 4.8 DataX案例

#### 4.8.1 从Oracle中读取数据存到MySQL

1. MySQL 中创建表

```bash
[oracle@server01 ~]$ mysql -uroot -p000000
```

```sql
mysql
> create
database oracle;
mysql
> use oracle;
mysql
>
create table student
(
    id   int,
    name varchar(20)
);
```

2. 编写datax 配置文件

```bash
[oracle@server01 ~]$ vim /opt/module/datax/job/oracle2mysql.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "oraclereader",
          "parameter": {
            "column": [
              "*"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:oracle:thin:@server01:1521:orcl"
                ],
                "table": [
                  "student"
                ]
              }
            ],
            "password": "000000",
            "username": "vagrant"
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "column": [
              "*"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://server01:3306/oracle",
                "table": [
                  "student"
                ]
              }
            ],
            "password": "000000",
            "username": "root",
            "writeMode": "insert"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

3. 执行命令

```bash
[oracle@server01 ~]$ /opt/module/datax/bin/datax.py /opt/module/datax/job/oracle2mysql.json
```

查看结果：

```sql
mysql
>
select *
from student;
+------+----------+
| id   | name     |
+------+----------+
|    1 | zhangsan |
+------+----------+
```

#### 4.8.2 读取Oracle的数据存入HDFS中

1. 编写配置文件

```bash
[oracle@server01 datax]$ vim job/oracle2hdfs.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "oraclereader",
          "parameter": {
            "column": [
              "*"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:oracle:thin:@server01:1521:orcl"
                ],
                "table": [
                  "student"
                ]
              }
            ],
            "password": "000000",
            "username": "vagrant"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "defaultFS": "hdfs://server01:9000",
            "fieldDelimiter": "\t",
            "fileName": "oracle.txt",
            "fileType": "text",
            "path": "/",
            "writeMode": "append"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

2. 执行

```bash
[oracle@server01 datax]$ bin/datax.py job/oracle2hdfs.json
```

3. 查看HDFS 结果

---

## 5. MongoDB

### 5.1 什么是MongoDB

MongoDB 是由C++语言编写的，是一个基于分布式文件存储的开源数据库系统。MongoDB 旨在为WEB
应用提供可扩展的高性能数据存储解决方案。MongoDB将数据存储为一个文档，数据结构由键值(key=>value)对组成。MongoDB 文档类似于
JSON 对象。字段值可以包含其他文档，数组及文档数组。

![image-20251228212115080](pics/image-20251228212115080.png)

### 5.2 MongoDB优缺点

**优点：**

1. MongoDB 是一个面向文档存储的数据库，操作起来比较简单和容易
2. 内置GridFS，支持大容量的存储
3. 可以在MongoDB记录中设置任何属性的索引
4. MongoDB支持各种编程语言:RUBY，PYTHON，JAVA，C++，PHP，C#等多种语言
5. 安装简单
6. 复制（复制集）和支持自动故障恢复
7. MapReduce 支持复杂聚合

**缺点：**

1. 不支持事务
2. 占用空间过大
3. 不能进行表关联
4. 复杂聚合操作通过MapReduce创建，速度慢
5. MongoDB 在你删除记录后不会在文件系统回收空间。除非你删掉数据库

### 5.3 基础概念解析

| SQL 术语/概念   | MongoDB 术语/概念 | 解释/说明                     |
|-------------|---------------|---------------------------|
| database    | database      | 数据库                       |
| table       | collection    | 数据库表/集合                   |
| row         | document      | 数据记录行/文档                  |
| column      | field         | 数据字段/域                    |
| index       | index         | 索引                        |
| table joins | 不支持           | 表连接,MongoDB 不支持           |
| primary key | primary key   | 主键,MongoDB 自动将_id 字段设置为主键 |

通过下图实例，我们也可以更直观的了解 Mongo 中的一些概念：

![image-20251228212610777](pics/image-20251228212610777.png)

### 5.4 安装

#### 5.4.1 下载地址

https://www.mongodb.com/download-center#community

#### 5.4.2 安装

1. 上传压缩包到虚拟机中,解压

```bash
[vagrant@server01 software]$ tar -zxvf mongodb-linux-x86_64-rhel70-5.0.2.tgz -C /opt/module/
```

2. 重命名

```bash
[vagrant@server01 module]$ mv mongodb-linux-x86_64-rhel70-5.0.2/ mongodb
```

3. 创建数据库目录

MongoDB 的数据存储在data 目录的db 目录下，但是这个目录在安装过程不会自动创建，所以需要手动创建data 目录，并在data 目录中创建db
目录。

```bash
[vagrant@server01 module]$ sudo mkdir -p /data/db
[vagrant@server01 mongodb]$ sudo chmod 777 -R /data/db/
```

4. 启动MongoDB 服务

```bash
[vagrant@server01 mongodb]$ bin/mongod
```

5. 进入shell 页面

```bash
[vagrant@server01 mongodb]$ bin/mongo
```

### 5.5 基础概念详解

#### 5.5.1 数据库

一个mongodb 中可以建立多个数据库。MongoDB 的默认数据库为"db"，该数据库存储在data 目录中。MongoDB
的单个实例可以容纳多个独立的数据库，每一个都有自己的集合和权限，不同的数据库也放置在不同的文件中。

1.显示所有数据库

```javascript
>
db
test
> show
dbs
admin
0.000
GB
config
0.000
GB
local
0.000
GB
```

- **admin：** 从权限的角度来看，这是"root"数据库。要是将一个用户添加到这个数据库，这个用户自动继承所有数据库的权限。一些特定的服务器端命令也只能从这个数据库运行，比如列出所有的数据库或者关闭服务器。
- **local：** 这个数据永远不会被复制，可以用来存储限于本地单台服务器的任意集合
- **config：** 当Mongo 用于分片设置时，config 数据库在内部使用，用于保存分片的相关信息

2.显示当前使用的数据库

```javascript
>
db
test
```

3.切换数据库

```javascript
>
use
local
switched
to
db
local
> db
local
>
```

#### 5.5.2 集合

集合就是 MongoDB 文档组，类似于MySQL 中的table。集合存在于数据库中，集合没有固定的结构，这意味着你在对集合可以插入不同格式和类型的数据，但通常情况下我们插入集合的数据都会有一定的关联性。

MongoDB 中使用 createCollection() 方法来创建集合。

**语法格式：**

```javascript
db.createCollection(name, options)
```

**参数说明：**

- **name：** 要创建的集合名称
- **options：** 可选参数, 指定有关内存大小及索引的选项，有以下参数：

| 字段          | 类型 | 描述                                                                                |
|-------------|----|-----------------------------------------------------------------------------------|
| capped      | 布尔 | （可选）如果为 true，则创建固定集合。固定集合是指有着固定大小的集合，当达到最大值时，它会自动覆盖最早的文档。当该值为 true 时，必须指定 size 参数 |
| autoIndexId | 布尔 | （可选）如为 true，自动在 _id 字段创建索引。默认为 false                                              |
| size        | 数值 | （可选）为固定集合指定一个最大值（以字节计）。如果 capped 为 true，也需要指定该字段                                  |
| max         | 数值 | （可选）指定固定集合中包含文档的最大数量                                                              |

**案例1：** 在test 库中创建一个vagrant 的集合

```javascript
>
use
test
switched
to
db
test
> db.createCollection("collect-test")
{
    "ok"
:
    1
}
>
show
tables
collect - test

> db.createCollection("product")
{
    "ok"
:
    1
}
```

插入数据：

```javascript
>
db.product.insert({"id": 1, "url": "www.baidu.com"})
WriteResult({"nInserted": 1})

// 方括号包裹集合名（字符串格式），解决连字符解析问题
// db["collect-test"].insert({"name": "zhangsan", "url": "www.baidu.com"})

> db["collect-test"].insert({"name": "zhangsan", "url": "www.baidu.com"})
WriteResult({"nInserted": 1})
```

查看数据：

```javascript
>
db.product.find()
{
    "_id"
:
    ObjectId("695139aad1f78889b449a1f2"), "id"
:
    1, "url"
:
    "www.baidu.com"
}
```

**说明：**

ObjectId 类似唯一主键，可以很快的去生成和排序，包含 12 bytes，由24 个16 进制数字组成的字符串（每个字节可以存储两个16
进制数字）,含义是：

- 前 4 个字节表示创建 unix 时间戳
- 接下来的 3 个字节是机器标识码
- 紧接的两个字节由进程 id 组成 PID
- 最后三个字节是随机数

**案例2：** 创建一个固定集合mycol

```javascript
>
db.createCollection("mycol", {capped: true, autoIndexId: true, size: 6142800, max: 1000})
> show
tables;
mycol
```

**案例3：** 自动创建集合

在 MongoDB 中，你不需要创建集合。当你插入一些文档时，MongoDB 会自动创建集合。

```javascript
>
db.mycol2.insert({"name": "vagrant"})
WriteResult({"nInserted": 1})
> show
collections
vagrant
mycol
mycol2
```

**案例4：** 删除集合

```javascript
>
db.mycol2.drop()
True
> show
tables;
mycol
```

#### 5.5.3 文档(Document)

文档是一组键值(key-value)对组成。MongoDB 的文档不需要设置相同的字段，并且相同的字段不需要相同的数据类型，这与关系型数据库有很大的区别，也是
MongoDB 非常突出的特点。

一个简单的例子：

```json
{
  "name": "vagrant"
}
```

**注意：**

1. 文档中的键/值对是有序的
2. MongoDB 区分类型和大小写
3. MongoDB 的文档不能有重复的键
4. 文档的键是字符串。除了少数例外情况，键可以使用任意UTF-8 字符

### 5.6 DataX导入导出案例

#### 5.6.1 读取MongoDB的数据导入到HDFS

1. 编写配置文件

```bash
[vagrant@server01 datax]$ vim job/mongdb2hdfs.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mongodbreader",
          "parameter": {
            "address": [
              "127.0.0.1:27017"
            ],
            "collectionName": "vagrant",
            "column": [
              {
                "name": "name",
                "type": "string"
              },
              {
                "name": "url",
                "type": "string"
              }
            ],
            "dbName": "test"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "column": [
              {
                "name": "name",
                "type": "string"
              },
              {
                "name": "url",
                "type": "string"
              }
            ],
            "defaultFS": "hdfs://server01:9000",
            "fieldDelimiter": "\t",
            "fileName": "mongo.txt",
            "fileType": "text",
            "path": "/",
            "writeMode": "append"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

2. mongodbreader 参数解析

- **address：** MongoDB 的数据地址信息，因为MonogDB 可能是个集群，则ip 端口信息需要以Json 数组的形式给出。【必填】
- **userName：** MongoDB 的用户名。【选填】
- **userPassword：** MongoDB 的密码。【选填】
- **collectionName：** MonogoDB 的集合名。【必填】
- **column：** MongoDB 的文档列名。【必填】
- **name：** Column 的名字。【必填】
- **type：** Column 的类型。【选填】
- **splitter：** 因为MongoDB 支持数组类型，但是Datax 框架本身不支持数组类型，所以mongoDB 读出来的数组类型要通过这个分隔符合并成字符串。【选填】

3. 执行

```bash
[vagrant@server01 datax]$ bin/datax.py job/mongdb2hdfs.json
```

4. 查看结果

#### 5.6.2 读取MongoDB的数据导入MySQL

1. 在MySQL 中创建表

```sql
mysql
>
create table vagrant
(
    name varchar(20),
    url  varchar(20)
);
```

2. 编写DataX 配置文件

```bash
[vagrant@server01 datax]$ vim job/mongodb2mysql.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mongodbreader",
          "parameter": {
            "address": [
              "127.0.0.1:27017"
            ],
            "collectionName": "vagrant",
            "column": [
              {
                "name": "name",
                "type": "string"
              },
              {
                "name": "url",
                "type": "string"
              }
            ],
            "dbName": "test"
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "column": [
              "*"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://server01:3306/test",
                "table": [
                  "vagrant"
                ]
              }
            ],
            "password": "000000",
            "username": "root",
            "writeMode": "insert"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

3. 执行

```bash
[vagrant@server01 datax]$ bin/datax.py job/mongodb2mysql.json
```

4. 查看结果

```sql
mysql
>
select *
from vagrant;
+---------+-----------------+
| name    | url             |
+---------+-----------------+
| vagrant | www.vagrant.com |
+---------+-----------------+
```

---

## 6. SQLServer

### 6.1 什么是SQLServer

美国Microsoft 公司推出的一种关系型数据库系统。SQL Server
是一个可扩展的、高性能的、为分布式客户机/服务器计算所设计的数据库管理系统，实现了与WindowsNT的有机结合，提供了基于事务的企业级信息管理系统方案。SQL
Server 的基本语法和MySQL 基本相同。

**特点：**

1. 高性能设计，可充分利用WindowsNT 的优势
2. 系统管理先进，支持Windows 图形化管理工具，支持本地和远程的系统管理和配置
3. 强壮的事务处理功能，采用各种方法保证数据的完整性
4. 支持对称多处理器结构、存储过程、ODBC，并具有自主的SQL 语言。 SQLServer 以其内置的数据复制功能、强大的管理工具、与Internet
   的紧密集成和开放的系统结构为广大的用户、开发人员和系统集成商提供了一个出众的数据库平台

### 6.2 安装

#### 6.2.1 安装要求

**系统要求：**

1. centos 或redhat7.0 以上系统
2. 内存2G 以上

**说明：**

linux 下安装sqlserver 数据库有2 种办法：

- 使用rpm 安装包安装
    - rpm 安装包地址：https://packages.microsoft.com/rhel/7/mssql-server-2017/
    - 安装时缺少什么依赖，就使用yum 进行安装补齐
- 使用yum 镜像安装

#### 6.2.2 安装步骤

1. 下载 Microsoft SQL Server 2017 Red Hat 存储库配置文件

```bash
sudo curl -o /etc/yum.repos.d/mssql-server.repo https://packages.microsoft.com/config/rhel/7/mssql-server-2017.repo
```

2. 执行安装

```bash
yum install -y mssql-server
```

3. 完毕之后运行做相关配置

```bash
sudo /opt/mssql/bin/mssql-conf setup
```

#### 6.2.3 安装配置

1.执行配置命令

```bash
sudo /opt/mssql/bin/mssql-conf setup
```

2.选择安装的版本

![image-20260101190116464](pics/image-20260101190116464.png)

3.接受许可条款

![image-20260101190155077](pics/image-20260101190155077.png)

4.选择语言

![image-20260101190236495](pics/image-20260101190236495.png)

5.配置系统管理员密码

![image-20260101190323682](pics/image-20260101190323682.png)

6.完成

![image-20260101190329993](pics/image-20260101190329993.png)

#### 6.2.4 安装命令行工具

1. 下载存储库配置文件

```bash
sudo curl -o /etc/yum.repos.d/msprod.repo https://packages.microsoft.com/config/rhel/7/prod.repo
```

2. 执行安装

```bash
sudo yum remove mssql-tools unixODBC-utf16-devel
sudo yum install mssql-tools unixODBC-devel
```

3. 配置环境变量

```bash
sudo vim /etc/profile.d/my_env.sh
```

添加环境变量：

```bash
export PATH="$PATH:/opt/mssql-tools/bin
```

使环境变量生效：

```bash
source /etc/profile.d/my_env.sh
```

4. 进入命令行

```bash
sqlcmd -S localhost -U SA -P 密码 # 用命令行连接
```

### 6.3 简单使用

#### 6.3.1 启停命令

```bash
#启动
systemctl start mssql-server
#重启
systemctl restart mssql-server
#停止
systemctl stop mssql-server
#查看状态
systemctl status mssql-server
#具体配置路径
/opt/mssql/bin/mssql-conf
```

#### 6.3.2 创建数据库

1. 建库

```sql
> create
database datax
> go
```

2. 看当前数据库列表

```sql
>
select *
from SysDatabases > go
```

3. 看当前数据表

```sql
> use 库名
>
select *
from sysobjects
where xtype = 'u'
          > go
```

4. 看表的内容

```sql
>
select *
from 表名;
> go
```

### 6.4 DataX导入导出案例

创建表并插入数据：

```sql
create table student
(
    id   int,
    name varchar(25)
) go
insert into student values(1,'zhangsan')
go
```

#### 6.4.1 读取SQLServer的数据导入到HDFS

1. 编写配置文件

```bash
[vagrant@server01 datax]$ vim job/sqlserver2hdfs.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "sqlserverreader",
          "parameter": {
            "column": [
              "id",
              "name"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:sqlserver://hadoop2:1433;DatabaseName=datax"
                ],
                "table": [
                  "student"
                ]
              }
            ],
            "username": "root",
            "password": "000000"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "defaultFS": "hdfs://server01:9000",
            "fieldDelimiter": "\t",
            "fileName": "sqlserver.txt",
            "fileType": "text",
            "path": "/",
            "writeMode": "append"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

#### 6.4.2 读取SQLServer的数据导入MySQL

```bash
[vagrant@server01 datax]$ vim job/sqlserver2mysql.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "sqlserverreader",
          "parameter": {
            "column": [
              "id",
              "name"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:sqlserver://hadoop2:1433;DatabaseName=datax"
                ],
                "table": [
                  "student"
                ]
              }
            ],
            "username": "root",
            "password": "000000"
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "column": [
              "*"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://server01:3306/datax",
                "table": [
                  "student"
                ]
              }
            ],
            "password": "000000",
            "username": "root",
            "writeMode": "insert"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

---

## 7. DB2

### 7.1 什么是DB2

DB2 是IBM 公司于1983 年研制的一种关系型数据库系统(Relational Database Management System)，主要应用于大型应用系统，具有较好的可伸缩性。DB2
是IBM 推出的第二个关系型数据库，所以称为db2。DB2
提供了高层次的数据利用性、完整性、安全性、并行性、可恢复性，以及小规模到大规模应用程序的执行能力，具有与平台无关的基本功能和SQL命令运行环境。可以同时在不同操作系统使用，包括Linux、UNIX
和 Windows。

### 7.2 DB2数据库对象关系

1. **instance：** 同一台机器上可以安装多个DB2 instance
2. **database：** 同一个instance 下面可以创建有多个database
3. **schema：** 同一个database 下面可以配置多个schema
4. **table：** 同一个schema 下可以创建多个table

### 7.3 安装前的准备

**安装依赖**

```bash
yum install -y bc binutils compat-libcap1 compat-libstdc++33 elfutils-libelf elfutils-libelf-devel fontconfig-devel glibc glibc-devel ksh libaio libaio-devel libX11 libXau libXi libXtst libXrender libXrender-devel libgcc libstdc++ libstdc++-devel libxcb make smartmontools sysstat kmod* gcc-c++ compat-libstdc++-33 libstdc++.so.6 kernel-devel pam-devel.i686 pam.i686 pam32*
```

**修改配置文件sysctl.conf**

```bash
[root@server01 module]# vim /etc/sysctl.conf
```

删除里面的内容，添加如下内容：

```
net.ipv4.ip_local_port_range = 9000 65500
fs.file-max = 6815744
kernel.shmall = 10523004
kernel.shmmax = 6465333657
kernel.shmmni = 4096
kernel.sem = 250 32000 100 128
net.core.rmem_default=262144
net.core.wmem_default=262144
net.core.rmem_max=4194304
net.core.wmem_max=1048576
fs.aio-max-nr = 1048576
```

**修改配置文件limits.conf**

```bash
[root@server01 module]# vim /etc/security/limits.conf
```

在文件末尾添加：

```
* soft nproc 65536
* hard nproc 65536
* soft nofile 65536
* hard nofile 65536
```

重启机器生效。

**上传安装包并解压**

```bash
[root@server01 software]# tar -zxvf v11.5.4_linuxx64_server_dec.tar.gz -C /opt/module/
[root@server01 module]# chmod 777 server_dec
```

### 7.4 安装

在root 用户下操作

**执行预检查命令**

```bash
./db2prereqcheck -l -s //检查环境
```

**执行安装**

```bash
./db2_install
```

1.接受许可条款（可能会出现两次询问是否接受条款，都选"是"即可）

![image-20260101190611845](pics/image-20260101190611845.png)

2.确认安装路径，默认

![image-20260101190629360](pics/image-20260101190629360.png)

3.选择安装SERVER

![image-20260101190640414](pics/image-20260101190640414.png)

4.不安装pureScale

![image-20260101190651518](pics/image-20260101190651518.png)

5.等待安装完成即可

6.查看许可

```bash
/opt/ibm/db2/V11.5/adm/db2licm -l
```

**添加组和用户**

```bash
groupadd -g 2000 db2iadm1
groupadd -g 2001 db2fadm1
useradd -m -g db2iadm1 -d /home/db2inst1 db2inst1
useradd -m -g db2fadm1 -d /home/db2fenc1 db2fenc1
passwd db2inst1
passwd db2fenc1
```

- **db2inst1：** 实例所有者
- **db2fenc1：** 受防护用户

**创建实例**

```bash
cd /opt/ibm/db2/V11.5/instance
./db2icrt -p 50000 -u db2fenc1 db2inst1
```

**创建样本数据库、开启服务**

```bash
su - db2inst1
db2sampl
db2start
```

**连接**

```bash
db2
conncet to sample #连接到某个数据库
select * from staff
```

**创建表、插入数据**

```sql
CREATE TABLE STUDENT
(
    ID   int,
    NAME varchar(20)
);
INSERT INTO STUDENT
VALUES (11, 'lisi');
commit;
```

### 7.5 DataX导入导出案例

#### 7.5.1 注册DB2驱动

datax 暂时没有独立插件支持db2，需要使用通用的使用rdbmsreader 或rdbmswriter。

1. 注册reader 的db2 驱动

```bash
[vagrant@server01 datax]$ vim /opt/module/datax/plugin/reader/rdbmsreader/plugin.json
```

在drivers 里添加db2 的驱动类：

```json
"drivers":["dm.jdbc.driver.DmDriver", "com.sybase.jdbc3.jdbc.SybDriver", "com.edb.Driver", "com.ibm.db2.jcc.DB2Driver"]
```

2. 注册writer 的db2 驱动

```bash
[vagrant@server01 datax]$ vim /opt/module/datax/plugin/writer/rdbmswriter/plugin.json
```

在drivers 里添加db2 的驱动类：

```json
"drivers":["dm.jdbc.driver.DmDriver", "com.sybase.jdbc3.jdbc.SybDriver", "com.edb.Driver", "com.ibm.db2.jcc.DB2Driver"]
```

#### 7.5.2 读取DB2的数据导入到HDFS

1. 编写配置文件

```bash
[vagrant@server01 datax]$ vim job/db2-2-hdfs.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "rdbmsreader",
          "parameter": {
            "column": [
              "ID",
              "NAME"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:db2://hadoop2:50000/sample"
                ],
                "table": [
                  "STUDENT"
                ]
              }
            ],
            "username": "db2inst1",
            "password": "vagrant"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "column": [
              {
                "name": "id",
                "type": "int"
              },
              {
                "name": "name",
                "type": "string"
              }
            ],
            "defaultFS": "hdfs://server01:9000",
            "fieldDelimiter": "\t",
            "fileName": "db2.txt",
            "fileType": "text",
            "path": "/",
            "writeMode": "append"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

#### 7.5.3 读取DB2的数据导入MySQL

```bash
[vagrant@server01 datax]$ vim job/db2-2-mysql.json
```

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "rdbmsreader",
          "parameter": {
            "column": [
              "ID",
              "NAME"
            ],
            "connection": [
              {
                "jdbcUrl": [
                  "jdbc:db2://hadoop2:50000/sample"
                ],
                "table": [
                  "STUDENT"
                ]
              }
            ],
            "username": "db2inst1",
            "password": "vagrant"
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "column": [
              "*"
            ],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://server01:3306/datax",
                "table": [
                  "student"
                ]
              }
            ],
            "password": "000000",
            "username": "root",
            "writeMode": "insert"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": "1"
      }
    }
  }
}
```

---

## 8. 执行流程源码分析

### 8.1 总体流程

![image-20260101190731337](pics/image-20260101190731337.png)

- **黄色：** Job 部分的执行阶段
- **蓝色：** Task 部分的执行阶段
- **绿色：** 框架执行阶段

### 8.2 程序入口

**datax.py**

```python
ENGINE_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} com.alibaba.datax.core.Engine -mode ${mode} -jobid ${jobid} -job ${job}" % (DEFAULT_PROPERTY_CONF, CLASS_PATH)
```

**Engine.java**

```java
public void start(Configuration allConf) {
    //JobContainer 会在schedule 后再行进行设置和调整值
    int channelNumber = 0;
    AbstractContainer container;
    long instanceId;
    int taskGroupId = -1;
    if (isJob) {
        allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, RUNTIME_MODE);
        container = new JobContainer(allConf);
        instanceId = allConf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
    } else {
        container = new TaskGroupContainer(allConf);
        instanceId = allConf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        taskGroupId = allConf.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
        channelNumber = allConf.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
    }
    container.start();
}
```

**JobContainer.java**

```java
/**
 * jobContainer 主要负责的工作全部在start()里面，包括init、prepare、split、scheduler、
 * post 以及destroy 和statistics
 */
@Override
public void start() {
    LOG.info("DataX jobContainer starts job.");
    boolean hasException = false;
    boolean isDryRun = false;
    try {
        this.startTimeStamp = System.currentTimeMillis();
        isDryRun = configuration.getBool(CoreConstant.DATAX_JOB_SETTING_DRYRUN, false);
        if (isDryRun) {
            LOG.info("jobContainer starts to do preCheck ...");
            this.preCheck();
        } else {
            userConf = configuration.clone();
            LOG.debug("jobContainer starts to do preHandle ...");
            //Job 前置操作
            this.preHandle();
            LOG.debug("jobContainer starts to do init ...");
            //初始化reader 和writer
            this.init();
            LOG.info("jobContainer starts to do prepare ...");
            //全局准备工作，比如odpswriter 清空目标表
            this.prepare();
            LOG.info("jobContainer starts to do split ...");
            //拆分Task
            this.totalStage = this.split();
            LOG.info("jobContainer starts to do schedule ...");
            this.schedule();
            LOG.debug("jobContainer starts to do post ...");
            this.post();
            LOG.debug("jobContainer starts to do postHandle ...");
            this.postHandle();
            LOG.info("DataX jobId [{}] completed successfully.", this.jobId);
            this.invokeHooks();
        }
    } catch (Exception e) {
        // 异常处理
    }
}
```

### 8.3 Task 切分逻辑

**JobContainer.java**

```java
private int split() {
    this.adjustChannelNumber();
    if (this.needChannelNumber <= 0) {
        this.needChannelNumber = 1;
    }
    List<Configuration> readerTaskConfigs = this.doReaderSplit(this.needChannelNumber);
    int taskNumber = readerTaskConfigs.size();
    List<Configuration> writerTaskConfigs = this.doWriterSplit(taskNumber);
    List<Configuration> transformerList = this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);
    LOG.debug("transformer configuration: " + JSON.toJSONString(transformerList));
    /**
     * 输入是reader 和writer 的parameter list，输出是content 下面元素的list
     */
    List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(readerTaskConfigs, writerTaskConfigs, transformerList);
    LOG.debug("contentConfig configuration: " + JSON.toJSONString(contentConfig));
    this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);
    return contentConfig.size();
}
```

#### 8.3.1 并发数的确定

```java
private void adjustChannelNumber() {
    int needChannelNumberByByte = Integer.MAX_VALUE;
    int needChannelNumberByRecord = Integer.MAX_VALUE;
    boolean isByteLimit = (this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 0) > 0);
    if (isByteLimit) {
        long globalLimitedByteSpeed = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 10 * 1024 * 1024);
        // 在byte 流控情况下，单个Channel 流量最大值必须设置，否则报错！
        Long channelLimitedByteSpeed = this.configuration.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
        if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "在有总bps 限速条件下，单个channel 的bps 值不能为空，也不能为非正数");
        }
        needChannelNumberByByte = (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
        needChannelNumberByByte = needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
        LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
    }
    boolean isRecordLimit = (this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 0)) > 0;
    if (isRecordLimit) {
        long globalLimitedRecordSpeed = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 100000);
        Long channelLimitedRecordSpeed = this.configuration.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
        if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "在有总tps 限速条件下，单个channel 的tps 值不能为空，也不能为非正数");
        }
        needChannelNumberByRecord = (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
        needChannelNumberByRecord = needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
        LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
    }
    // 取较小值
    this.needChannelNumber = needChannelNumberByByte < needChannelNumberByRecord ? needChannelNumberByByte : needChannelNumberByRecord;
    // 如果从byte 或record 上设置了needChannelNumber 则退出
    if (this.needChannelNumber < Integer.MAX_VALUE) {
        return;
    }
    boolean isChannelLimit = (this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL, 0) > 0);
    if (isChannelLimit) {
        this.needChannelNumber = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);
        LOG.info("Job set Channel-Number to " + this.needChannelNumber + " channels.");
        return;
    }
    throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "Job 运行速度必须设置");
}
```

### 8.4 调度

**JobContainer.java**

```java
private void schedule() {
    /**
     * 这里的全局speed 和每个channel 的速度设置为B/s
     */
    int channelsPerTaskGroup = this.configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, 5);
    int taskNumber = this.configuration.getList(CoreConstant.DATAX_JOB_CONTENT).size();
    //确定的channel 数和切分的task 数取最小值，避免浪费
    this.needChannelNumber = Math.min(this.needChannelNumber, taskNumber);
    PerfTrace.getInstance().setChannelNumber(needChannelNumber);
    /**
     * 通过获取配置信息得到每个taskGroup 需要运行哪些tasks 任务
     */
    List<Configuration> taskGroupConfigs = JobAssignUtil.assignFairly(this.configuration, this.needChannelNumber, channelsPerTaskGroup);
    LOG.info("Scheduler starts [{}] taskGroups.", taskGroupConfigs.size());
    ExecuteMode executeMode = null;
    AbstractScheduler scheduler;
    try {
        //可以看到3.0 进行了阉割，只有STANDALONE 模式
        executeMode = ExecuteMode.STANDALONE;
        scheduler = initStandaloneScheduler(this.configuration);
        //设置 executeMode
        for (Configuration taskGroupConfig : taskGroupConfigs) {
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, executeMode.getValue());
        }
        if (executeMode == ExecuteMode.LOCAL || executeMode == ExecuteMode.DISTRIBUTE) {
            if (this.jobId <= 0) {
                throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "在[ local | distribute ]模式下必须设置jobId，并且其值 > 0 .");
            }
        }
        LOG.info("Running by {} Mode.", executeMode);
        this.startTransferTimeStamp = System.currentTimeMillis();
        scheduler.schedule(taskGroupConfigs);
        this.endTransferTimeStamp = System.currentTimeMillis();
    } catch (Exception e) {
        LOG.error("运行scheduler 模式[{}]出错.", executeMode);
        this.endTransferTimeStamp = System.currentTimeMillis();
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
    }
    /**
     * 检查任务执行情况
     */
    this.checkLimit();
}
```

#### 8.4.1 确定组数和分组

**assignFairly 方法：**

1. 确定taskGroupNumber
2. 做分组分配
3. 做分组优化

```java
public static List<Configuration> assignFairly(Configuration configuration, int channelNumber, int channelsPerTaskGroup) {
    Validate.isTrue(configuration != null, "框架获得的 Job 不能为 null.");
    List<Configuration> contentConfig = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
    Validate.isTrue(contentConfig.size() > 0, "框架获得的切分后的 Job 无内容.");
    Validate.isTrue(channelNumber > 0 && channelsPerTaskGroup > 0, "每个channel 的平均task 数[averTaskPerChannel]，channel 数目[channelNumber]，每个taskGroup 的平均channel 数[channelsPerTaskGroup]都应该为正数");
    //TODO 确定taskgroup 的数量
    int taskGroupNumber = (int) Math.ceil(1.0 * channelNumber / channelsPerTaskGroup);
    Configuration aTaskConfig = contentConfig.get(0);
    String readerResourceMark = aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
    String writerResourceMark = aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
    boolean hasLoadBalanceResourceMark = StringUtils.isNotBlank(readerResourceMark) || StringUtils.isNotBlank(writerResourceMark);
    if (!hasLoadBalanceResourceMark) {
        // fake 一个固定的 key 作为资源标识（在 reader 或者 writer 上均可，此处选择在 reader 上进行 fake）
        for (Configuration conf : contentConfig) {
            conf.set(CoreConstant.JOB_READER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK, "aFakeResourceMarkForLoadBalance");
        }
        // 是为了避免某些插件没有设置 资源标识 而进行了一次随机打乱操作
        Collections.shuffle(contentConfig, new Random(System.currentTimeMillis()));
    }
    LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap = parseAndGetResourceMarkAndTaskIdMap(contentConfig);
    List<Configuration> taskGroupConfig = doAssign(resourceMarkAndTaskIdMap, configuration, taskGroupNumber);
    // 调整 每个 taskGroup 对应的 Channel 个数（属于优化范畴）
    adjustChannelNumPerTaskGroup(taskGroupConfig, channelNumber);
    return taskGroupConfig;
}
```

#### 8.4.2 调度实现

**AbstractScheduler.java**

```java
public void schedule(List<Configuration> configurations) {
    Validate.notNull(configurations, "scheduler 配置不能为空");
    int jobReportIntervalInMillSec = configurations.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 30000);
    int jobSleepIntervalInMillSec = configurations.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL, 10000);
    this.jobId = configurations.get(0).getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    errorLimit = new ErrorRecordChecker(configurations.get(0));
    /**
     * 给 taskGroupContainer 的 Communication 注册
     */
    this.containerCommunicator.registerCommunication(configurations);
    int totalTasks = calculateTaskCount(configurations);
    startAllTaskGroup(configurations);
    Communication lastJobContainerCommunication = new Communication();
    long lastReportTimeStamp = System.currentTimeMillis();
    try {
        while (true) {
            /**
             * step 1: collect job stat
             * step 2: getReport info, then report it
             * step 3: errorLimit do check
             * step 4: dealSucceedStat();
             * step 5: dealKillingStat();
             * step 6: dealFailedStat();
             * step 7: refresh last job stat, and then sleep for next while
             *
             * above steps, some ones should report info to DS
             */
            // 循环处理逻辑
        }
    } catch (Exception e) {
        // 异常处理
    }
}
```

**ProcessInnerScheduler.java**

```java
public void startAllTaskGroup(List<Configuration> configurations) {
    this.taskGroupContainerExecutorService = Executors.newFixedThreadPool(configurations.size());
    for (Configuration taskGroupConfiguration : configurations) {
        TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
        this.taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
    }
    this.taskGroupContainerExecutorService.shutdown();
}
```

### 8.5 数据传输

接8.3.2 丢到线程池执行

**TaskGroupContainer.start()**

```java
->taskExecutor.doStart()

可以看到调用插件的start 方法

public void doStart() {
    this.writerThread.start();
    // reader 没有起来，writer 不可能结束
    if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, this.taskCommunication.getThrowable());
    }
    this.readerThread.start();
    // ...
}
```

**可以看看generateRunner()**

**ReaderRunner.java**

```java
public void run() {
    try {
        channelWaitWrite.start();
        initPerfRecord.start();
        taskReader.init();
        initPerfRecord.end();
        preparePerfRecord.start();
        taskReader.prepare();
        preparePerfRecord.end();
        dataPerfRecord.start();
        taskReader.startRead(recordSender);
        recordSender.terminate();
        postPerfRecord.start();
        taskReader.post();
        postPerfRecord.end();
        // automatic flush
        // super.markSuccess(); 这里不能标记为成功，成功的标志由writerRunner 来标志（否则可能导致 reader 先结束，而 writer 还没有结束的严重bug）
    } catch (Throwable e) {
        LOG.error("Reader runner Received Exceptions:", e);
        super.markFail(e);
    } finally {
        LOG.debug("task reader starts to do destroy ...");
        PerfRecord desPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.READ_TASK_DESTROY);
        desPerfRecord.start();
        super.destroy();
        desPerfRecord.end();
        channelWaitWrite.end(super.getRunnerCommunication().getLongCounter(CommunicationTool.WAIT_WRITER_TIME));
        long transformerUsedTime = super.getRunnerCommunication().getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME);
        if (transformerUsedTime > 0) {
            PerfRecord transformerRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.TRANSFORMER_TIME);
            transformerRecord.start();
            transformerRecord.end(transformerUsedTime);
        }
    }
}
```

#### 8.5.1 限速的实现

比如看MysqlReader 的startReader 方法

- `CommonRdbmsReaderTask.startRead()`
- `transportOneRecord()`
- `sendToWriter()`
- `BufferedRecordExchanger.flush()`
- `Channel.pushAll()`
- `Channel.statPush()`

```java
private void statPush(long recordSize, long byteSize) {
    currentCommunication.increaseCounter(CommunicationTool.READ_SUCCEED_RECORDS, recordSize);
    currentCommunication.increaseCounter(CommunicationTool.READ_SUCCEED_BYTES, byteSize);
    //在读的时候进行统计waitCounter 即可，因为写（pull）的时候可能正在阻塞，但读的时候已经能读到这个阻塞的counter 数
    currentCommunication.setLongCounter(CommunicationTool.WAIT_READER_TIME, waitReaderTime);
    currentCommunication.setLongCounter(CommunicationTool.WAIT_WRITER_TIME, waitWriterTime);
    boolean isChannelByteSpeedLimit = (this.byteSpeed > 0);
    boolean isChannelRecordSpeedLimit = (this.recordSpeed > 0);
    if (!isChannelByteSpeedLimit && !isChannelRecordSpeedLimit) {
        return;
    }
    long lastTimestamp = lastCommunication.getTimestamp();
    long nowTimestamp = System.currentTimeMillis();
    long interval = nowTimestamp - lastTimestamp;
    if (interval - this.flowControlInterval >= 0) {
        long byteLimitSleepTime = 0;
        long recordLimitSleepTime = 0;
        if (isChannelByteSpeedLimit) {
            long currentByteSpeed = (CommunicationTool.getTotalReadBytes(currentCommunication) - CommunicationTool.getTotalReadBytes(lastCommunication)) * 1000 / interval;
            if (currentByteSpeed > this.byteSpeed) {
                // 计算根据byteLimit 得到的休眠时间
                byteLimitSleepTime = currentByteSpeed * interval / this.byteSpeed - interval;
            }
        }
        if (isChannelRecordSpeedLimit) {
            long currentRecordSpeed = (CommunicationTool.getTotalReadRecords(currentCommunication) - CommunicationTool.getTotalReadRecords(lastCommunication)) * 1000 / interval;
            if (currentRecordSpeed > this.recordSpeed) {
                // 计算根据recordLimit 得到的休眠时间
                recordLimitSleepTime = currentRecordSpeed * interval / this.recordSpeed - interval;
            }
        }
        // 休眠时间取较大值
        long sleepTime = byteLimitSleepTime < recordLimitSleepTime ? recordLimitSleepTime : byteLimitSleepTime;
        if (sleepTime > 0) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
```

---

## 9. DataX 使用优化

### 9.1 关键参数

- **job.setting.speed.channel：** channel 并发数
- **job.setting.speed.record：** 全局配置channel 的record 限速
- **job.setting.speed.byte：** 全局配置channel 的byte 限速
- **core.transport.channel.speed.record：** 单个channel 的record 限速
- **core.transport.channel.speed.byte：** 单个channel 的byte 限速

### 9.2 优化1：提升每个channel 的速度

在DataX 内部对每个Channel 会有严格的速度控制，分两种，一种是控制每秒同步的记录数，另外一种是每秒同步的字节数，默认的速度限制是1MB/s，可以根据具体硬件情况设置这个byte
速度或者record 速度，一般设置byte 速度，比如：我们可以把单个Channel 的速度上限配置为5MB。

### 9.3 优化2：提升DataX Job 内Channel 并发数

并发数 = taskGroup 的数量 * 每个TaskGroup 并发执行的Task 数 (默认为5)。

提升job 内Channel 并发有三种配置方式：

#### 9.3.1 配置全局Byte 限速以及单Channel Byte 限速

Channel 个数 = 全局Byte 限速 / 单Channel Byte 限速

```json
{
  "core": {
    "transport": {
      "channel": {
        "speed": {
          "byte": 1048576
        }
      }
    }
  },
  "job": {
    "setting": {
      "speed": {
        "byte": 5242880
      }
    }
  }
}
```

`core.transport.channel.speed.byte=1048576`，`job.setting.speed.byte=5242880`，所以Channel 个数 = 全局Byte 限速 / 单Channel
Byte 限速 = 5242880/1048576 = 5 个

#### 9.3.2 配置全局Record 限速以及单Channel Record 限速

Channel 个数 = 全局Record 限速 / 单Channel Record 限速

```json
{
  "core": {
    "transport": {
      "channel": {
        "speed": {
          "record": 100
        }
      }
    }
  },
  "job": {
    "setting": {
      "speed": {
        "record": 500
      }
    }
  }
}
```

`core.transport.channel.speed.record=100`，`job.setting.speed.record=500`，所以配置全局Record 限速以及单Channel Record
限速，Channel 个数 = 全局Record 限速 / 单Channel Record 限速 = 500/100 = 5

#### 9.3.3 直接配置Channel 个数

只有在上面两种未设置才生效，上面两个同时设置是取值小的作为最终的channel 数。

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 5
      }
    }
  }
}
```

直接配置`job.setting.speed.channel=5`，所以job 内Channel 并发=5 个

### 9.4 优化3：提高JVM 堆内存

当提升DataX Job 内Channel 并发数时，内存的占用会显著增加，因为DataX 作为数据交换通道，在内存中会缓存较多的数据。例如Channel
中会有一个Buffer，作为临时的数据交换的缓冲区，而在部分Reader 和Writer 的中，也会存在一些Buffer，为了防止OOM 等错误，调大JVM
的堆内存。

建议将内存设置为4G 或者8G，这个也可以根据实际情况来调整。

调整JVM xms xmx 参数的两种方式：一种是直接更改datax.py 脚本；另一种是在启动的时候，加上对应的参数，如下：

```bash
python datax/bin/datax.py --jvm="-Xms8G -Xmx8G" XXX.json
```

---

