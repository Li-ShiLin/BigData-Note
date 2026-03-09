<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [环境部署](#%E7%8E%AF%E5%A2%83%E9%83%A8%E7%BD%B2)
- [概念与教程](#%E6%A6%82%E5%BF%B5%E4%B8%8E%E6%95%99%E7%A8%8B)
- [示例模块](#%E7%A4%BA%E4%BE%8B%E6%A8%A1%E5%9D%97)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

本仓库汇总 Flink CDC 相关环境部署与示例代码文档

---

## 环境部署

| 文档 | 核心内容 |
|------|----------|
| [mysql-5.7.38.md](mysql-5.7.38.md) | MySQL 5.7.38 安装：下载、解压、用户/组、初始化、binlog 配置等 |
| [flink-1.13.0-deploy.md](flink-1.13.0-deploy.md) | Flink 1.13 Standalone 部署：Session/Per-Job/Application 模式、安装与启动 |
| [Doris-1.2.4-Deploy.md](Doris-1.2.4-Deploy.md) | Apache Doris 1.2.4 部署：FE/BE 配置、启动、扩缩容与基本使用 |
| [redis-6.2.6.md](redis-6.2.6.md) | Redis 6.2.6 部署：安装、配置与基础使用（缓存服务） |

---

## 概念与教程

| 文档 | 核心内容 |
|------|----------|
| [tutorial.md](tutorial.md) | CDC 概念、Flink CDC 简介、数据准备及 Flink CDC 使用教程 |

---

## 示例模块

| 文档 | 核心内容 |
|------|----------|
| [cdc-00-flink-cdc/README.md](cdc-00-flink-cdc/README.md) | 基础：DataStream API 与 SQL API 两种方式从 MySQL 读 CDC |
| [cdc-01-FlinkCDC-DataStream/README.md](cdc-01-FlinkCDC-DataStream/README.md) | DataStream：`initial()` / `latest()` / `earliest()` 三种启动模式对比 |
| [cdc-02-FlinkCDC-DataStream-Checkpoint/README.md](cdc-02-FlinkCDC-DataStream-Checkpoint/README.md) | DataStream + Checkpoint：状态与 binlog 位点持久化到 HDFS，断点续传 |
| [cdc-03-FlinkCDC-SQL/README.md](cdc-03-FlinkCDC-SQL/README.md) | Flink CDC SQL：`CREATE TABLE ... WITH ('connector'='mysql-cdc')` 声明式建表 |
| [cdc-04-FlinkCDC-SQL-Checkpoint/README.md](cdc-04-FlinkCDC-SQL-Checkpoint/README.md) | Flink CDC SQL + Checkpoint：SQL 方式配合 HDFS Checkpoint 断点续传 |
| [cdc-05-MySQL-to-Doris-Streaming-ETL-Implementation/README.md](cdc-05-MySQL-to-Doris-Streaming-ETL-Implementation/README.md) | Flink CDC 3.0：MySQL → Doris 流式 ETL（同步变更 / 路由变更） |
| [cdc-06-mysql-redis-cache-consistency/README.md](cdc-06-mysql-redis-cache-consistency/README.md) | MySQL → Redis 缓存一致性：Flink CDC 监听 `sakila.actor` 变更驱动缓存增删改 |
