1.Doris安装前置条件

> 已安装 JDK 8+（推荐 JDK 8）
> 已安装 MySQL 客户端（用于连接 Doris FE）
> 服务器内存建议 4G 以上，关闭防火墙 / 开放对应端口

2.下载 Doris 安装包

```bash
# 官网下载
# 官网地址:  https://doris.apache.org/zh-CN/download

# 1.上传安装包到Linux服务器
scp -i "E:\vm\vagrant\server01\.vagrant\machines\default\virtualbox\private_key" "C:\Users\22418\Downloads\apache-doris-1.2.4.1-bin-x86_64.tar.xz" vagrant@192.168.56.11:/home/vagrant/


# 2. 解压上传的安装包（注意你的包是.tar.xz格式，需用J参数）
# tar -Jxvf /home/vagrant/apache-doris-1.2.4.1-bin-x86_64.tar.xz -C /opt/module/
[vagrant@server01 ~]$ tar -Jxvf /home/vagrant/apache-doris-1.2.4.1-bin-x86_64.tar.xz -C /opt/module/


# 3. 重命名目录（简化后续操作，统一为doris）
[vagrant@server01 ~]$ mv /opt/module/apache-doris-1.2.4.1-bin-x86_64 /opt/module/doris

# 4. 验证解压结果
[vagrant@server01 ~]$ ll /opt/module/doris/
```

3.配置 FE（Frontend，前端节点）

```bash
# 0.查看JDK路径
# echo $JAVA_HOME
[vagrant@server01 ~]$ echo $JAVA_HOME
/opt/module/jdk1.8.0_144

# 1. 进入FE配置目录
[vagrant@server01 ~]$ cd /opt/module/doris/fe/conf/

# 2. 编辑FE核心配置文件（vim编辑，按i进入编辑模式）
[vagrant@server01 conf]$ vim fe.conf

# 3. 在文件中添加/修改以下配置（其余保持默认）
# -------------- 新增/修改的配置 --------------
# 指定JDK路径（根据实际JDK安装路径调整，先执行echo $JAVA_HOME查看）
JAVA_HOME=/opt/module/jdk1.8.0_144
# FE元数据存储目录（必须提前创建）
meta_dir=/opt/module/doris/fe/meta
# 取消注释并设置http端口（默认8030，确保未被占用）
http_port=8030
# MySQL客户端连接端口（默认9030）
query_port=9030
# -------------- 配置结束 --------------

# 4. 保存退出：按Esc → 输入:wq → 回车

# 5. 创建FE元数据目录
[vagrant@server01 conf]$ mkdir -p /opt/module/doris/fe/meta
# 修改权限
sudo chown -R vagrant:vagrant /opt/module/doris/fe/meta

# 6. 验证配置文件（可选，检查是否有语法错误）
[vagrant@server01 conf]$ cat fe.conf | grep -E "JAVA_HOME|meta_dir|http_port|query_port"

# 正常输出示例：
# JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
# meta_dir=/opt/module/doris/fe/meta
# http_port=8030
# query_port=9030
```

4.配置 BE（Backend，后端节点）

```bash
# 1. 进入BE配置目录
[vagrant@server01 ~]$ cd /opt/module/doris/be/conf/

# 2. 编辑BE核心配置文件
[vagrant@server01 conf]$ vim be.conf

# 3. 在文件中添加/修改以下配置
# -------------- 新增/修改的配置 --------------
# BE数据存储目录（必须提前创建）
storage_root_path=/opt/module/doris/be/storage
# BE服务端口（默认8040）
be_port = 8040
# BE网页端口
webserver_port = 8048
# 心跳端口（保留）
heartbeat_service_port = 9050
# BRPC端口（保留，和be_port不冲突）
brpc_port = 8060

# 添加priority_networks（绑定正确IP 192.168.56.11）：
# 匹配Host-Only网卡网段
priority_networks = 192.168.56.0/24


# -------------- 配置结束 --------------

# 4. 保存退出：Esc → :wq → 回车

# 5. 创建BE数据存储目录
[vagrant@server01 conf]$ mkdir -p /opt/module/doris/be/storage
# 赋予vagrant完整权限
[vagrant@server01 conf]$ sudo chown -R vagrant:vagrant /opt/module/doris/be/
# 验证目录权限
[vagrant@server01 conf]$ ls -ld /opt/module/doris/be/storage



# 6. 验证配置文件
[vagrant@server01 conf]$ cat be.conf | grep -E "storage_root_path|heartbeat_service_port|be_port"
```

5.启动 Doris 集群并验证

```bash
# ========== 第一步：启动FE ==========
# 1. 进入FE启动脚本目录
[vagrant@server01 ~]$ cd /opt/module/doris/fe/bin/

# 2. 启动FE（--daemon表示后台运行）
[vagrant@server01 bin]$ ./start_fe.sh --daemon

# 3. 验证FE启动状态
# 方法1：查找所有Doris FE相关进程（最可靠）
[vagrant@server01 bin]$ ps -ef | grep doris | grep fe

# 方法2：查看进程
[vagrant@server01 bin]$ jps | grep Frontend
# 正常输出示例：12345 Frontend（有进程号表示启动成功）

# 方法3：查看FE日志（无报错则正常）
[vagrant@server01 bin]$ tail -100 /opt/module/doris/fe/log/fe.log | grep -i error
# 无输出或无ERROR级日志则正常

# ========== 第二步：注册BE到FE ==========
# 1. 通过MySQL客户端连接FE（默认无密码）
[vagrant@server01 ~]$ mysql -uroot -P9030 -h127.0.0.1

# 2. 在MySQL终端执行注册BE命令（替换为你的服务器IP/主机名）
mysql> ALTER SYSTEM ADD BACKEND "server01:9050";
# 正常输出：Query OK, 0 rows affected (0.01 sec)

# 3. 退出MySQL终端
mysql> exit;




# ========== 第三步：启动BE ==========
# 1. 进入BE启动脚本目录
[vagrant@server01 ~]$ cd /opt/module/doris/be/bin/

# 2. 启动BE
# 可以使用前台启动，方便查看日志
# ./start_be.sh 
[vagrant@server01 bin]$ ./start_be.sh --daemon
# 2.1 报错
[vagrant@server01 bin]$ ./start_be.sh --daemon
Please set vm.max_map_count to be 2000000 under root using 'sysctl -w vm.max_map_count=2000000'.
# 2.2 报错解决
# 步骤 1：临时设置参数（立即生效）
# 切换到root用户（vagrant默认有sudo权限）
[vagrant@server01 bin]$ sudo -i
# 设置参数临时生效
[root@server01 ~]# sysctl -w vm.max_map_count=2000000
# 验证参数是否生效
[root@server01 ~]# sysctl vm.max_map_count
# 正常输出：vm.max_map_count = 2000000
# 退出root用户
[root@server01 ~]# exit

# 步骤 2：永久设置参数（重启服务器仍生效）
# 编辑sysctl.conf文件
[root@server01 ~]# vim /etc/sysctl.conf
# 在文件末尾添加以下内容（按i进入编辑模式）：
vm.max_map_count=2000000
# 保存退出：Esc → :wq → 回车
# 使配置立即生效（无需重启）
[root@server01 ~]# sysctl -p
# 再次验证
[root@server01 ~]# sysctl vm.max_map_count
# 输出：vm.max_map_count = 2000000

# 步骤 3：以 root 身份启动 BE（最终启动）
# 进入BE bin目录
[root@server01 ~]# cd /opt/module/doris/be/bin/
# 清理残留pid文件（避免启动冲突）
[root@server01 bin]# rm -rf be.pid
# 启动BE（daemon模式）
[root@server01 bin]# ./start_be.sh --daemon
# 检查BE进程（此时应能看到palo_be）
[root@server01 bin]# ps -ef | grep palo_be | grep -v grep
# 正常输出示例：
root      9000     1 20 18:00 ?        00:00:15 /opt/module/doris/be/lib/palo_be
# 退出root
[root@server01 bin]# exit





# 3. 验证BE启动状态
# 方法1：查看进程
[vagrant@server01 bin]$ jps | grep Backend
# 正常输出示例：67890 Backend

# 方法2：连接FE检查BE状态
[vagrant@server01 ~]$ mysql -uroot -P9030 -h127.0.0.1
mysql> SHOW BACKENDS;
# 关键检查：Alive列显示true，表示BE正常在线
# 输出示例：
# +-------------+---------------+----------+---------------+----------+-----------+
# | BackendId   | Host          | HeartbeatPort | BePort  | HttpPort | Alive     |
# +-------------+---------------+----------+---------------+----------+-----------+
# | 10001       | server01      | 9050     | 8040          | 8040     | true      |
# +-------------+---------------+----------+---------------+----------+-----------+

# ========== 第四步：设置Doris root密码 ==========
mysql> SET PASSWORD FOR 'root' = PASSWORD('000000');
mysql> exit;

# 验证密码生效
[vagrant@server01 ~]$ mysql -uroot -p000000 -P9030 -hserver01
# 能成功登录表示密码设置完成
```

6.创建数据库

```bash
[vagrant@server01 ~]$ mysql -uroot -p000000 -P9030 -hserver01
mysql> create database doris_test;
mysql> exit;
```





