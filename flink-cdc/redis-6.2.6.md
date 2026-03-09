### Redis 环境搭建

以下以 **Redis 6.2.6**、CentOS 7 为例，安装到 `/opt/module/redis-6.2.6`。

### 1.安装编译依赖

```bash
sudo yum install -y gcc wget
# Redis 6.x 需要 gcc 较新，若 make 报错可升级：
# sudo yum install -y centos-release-scl
# sudo yum install -y devtoolset-9-gcc
# scl enable devtoolset-9 bash
```

### 2.下载并解压

```bash
cd ~
# 若服务器可访问外网
wget https://download.redis.io/releases/redis-6.2.6.tar.gz

# 或使用你已有的包（如之前下载的）
# 解压
tar -xzf redis-6.2.6.tar.gz
cd redis-6.2.6
```

### 3.编译与安装

```bash
make
# 可选：指定安装目录
sudo make install PREFIX=/opt/module/redis-6.2.6
```

若未指定 `PREFIX`，默认会安装到 `/usr/local/bin`（redis-server、redis-cli 等）。以下按 **指定目录** 写法，便于与文档一致。

**复制可执行文件到安装目录**（若上面用了 PREFIX 则已安装好，否则）：

```bash
sudo mkdir -p /opt/module/redis-6.2.6/bin
sudo cp src/redis-server src/redis-cli /opt/module/redis-6.2.6/bin/
```

### 4.配置文件

```bash
sudo mkdir -p /opt/module/redis-6.2.6/conf /opt/module/redis-6.2.6/data
sudo cp redis.conf /opt/module/redis-6.2.6/conf/redis.conf
sudo vi /opt/module/redis-6.2.6/conf/redis.conf
```

建议修改或确认如下项：

```conf
# 绑定地址：允许本机及内网访问（按需改为 0.0.0.0 或本机 IP）
bind 0.0.0.0
port 6379
# 后台运行
daemonize yes
# 数据目录
dir /opt/module/redis-6.2.6/data
# 日志文件
logfile /opt/module/redis-6.2.6/data/redis.log
# 若 bind 0.0.0.0，建议设置密码（生产必设）
# requirepass yourpassword
# 允许外网访问时关闭保护模式（仅测试/内网可开，生产建议用密码）
protected-mode no
```

保存后，确保数据目录存在且权限合适：

```bash
sudo chown -R $(whoami) /opt/module/redis-6.2.6/data
# 或 sudo chown -R vagrant:vagrant /opt/module/redis-6.2.6
```

### 5.启动 Redis

```bash
/opt/module/redis-6.2.6/bin/redis-server /opt/module/redis-6.2.6/conf/redis.conf
```

验证：

```bash
/opt/module/redis-6.2.6/bin/redis-cli -h 127.0.0.1 -p 6379 ping
# 应返回 PONG
```

### 6.开放防火墙（如需远程访问）

```bash
sudo firewall-cmd --zone=public --add-port=6379/tcp --permanent
sudo firewall-cmd --reload
```

### 7.环境变量（可选）

```bash
echo 'export REDIS_HOME=/opt/module/redis-6.2.6' >> ~/.bashrc
echo 'export PATH=$PATH:$REDIS_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

之后可用 `redis-server`、`redis-cli` 直接调用。