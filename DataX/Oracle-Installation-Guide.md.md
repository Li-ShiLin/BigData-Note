<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [1.`Oracle`安装前环境准备](#1oracle%E5%AE%89%E8%A3%85%E5%89%8D%E7%8E%AF%E5%A2%83%E5%87%86%E5%A4%87)
- [2.静默安装 `Oracle 19c`](#2%E9%9D%99%E9%BB%98%E5%AE%89%E8%A3%85-oracle-19c)
- [3.静默配置Oracle监听（netca)](#3%E9%9D%99%E9%BB%98%E9%85%8D%E7%BD%AEoracle%E7%9B%91%E5%90%ACnetca)
- [4.静默创建 Oracle 数据库实例（dbca）](#4%E9%9D%99%E9%BB%98%E5%88%9B%E5%BB%BA-oracle-%E6%95%B0%E6%8D%AE%E5%BA%93%E5%AE%9E%E4%BE%8Bdbca)
- [5.验证 Oracle 数据库与监听可用性](#5%E9%AA%8C%E8%AF%81-oracle-%E6%95%B0%E6%8D%AE%E5%BA%93%E4%B8%8E%E7%9B%91%E5%90%AC%E5%8F%AF%E7%94%A8%E6%80%A7)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## 1.`Oracle`安装前环境准备

1.安装依赖

```bash
sudo yum install -y bc binutils compat-libcap1 compat-libstdc++33 elfutils-libelf elfutils-libelf-devel fontconfig-devel glibc glibc-devel ksh libaio libaio-devel libX11 libXau libXi libXtst libXrender libXrender-devel libgcc libstdc++ libstdc++-devel libxcb make smartmontools sysstat kmod* gcc-c++ compat-libstdc++-33
```

2.配置用户组

```bash
# Oracle 安装文件不允许通过root 用户启动，需要为oracle 配置一个专门的用户。
# 1.创建oracle_group用户组
[vagrant@server01 ~]$ sudo groupadd oracle_group

# 2.创建oracle_user用户并放入oracle_group组中
[vagrant@server01 ~]$ sudo useradd oracle_user -g oracle_group

# 3.修改oracle_user用户登录密码，输入密码后即可使用oracle_user用户登录系统
[vagrant@server01 ~]$ sudo passwd oracle_user
```

4.创建Oracle安装目录并设置权限

```bash
# 创建安装目录（ORACLE_HOME）
sudo mkdir -p /home/oracle_user/app/oracle/product/19.3.0/dbhome_1

# 修改目录所有者和组（关键：必须是oracle_user和oracle_group）
sudo chown -R oracle_user:oracle_group /home/oracle_user/app

# 验证权限（可选，确保输出显示所有者是oracle_user）
ls -ld /home/oracle_user/app
```

5.修改配置文件sysctl.conf

```bash
# 修改配置文件sysctl.conf
[vagrant@server01 module]$ sudo vim /etc/sysctl.conf
```

删除里面的内容，添加如下内容：

```properties
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

> **参数解析：**
>
> - **net.ipv4.ip_local_port_range：** 可使用的IPv4 端口范围
> - **fs.file-max：** 该参数表示文件句柄的最大数量。文件句柄设置表示在linux 系统中可以打开的文件数量
> - **kernel.shmall：** 该参数表示系统一次可以使用的共享内存总量（以页为单位）
> - **kernel.shmmax：** 该参数定义了共享内存段的最大尺寸（以字节为单位）
> - **kernel.shmmni：** 这个内核参数用于设置系统范围内共享内存段的最大数量
> - **kernel.sem：** 该参数表示设置的信号量
> - **net.core.rmem_default：** 默认的TCP 数据接收窗口大小（字节）
> - **net.core.wmem_default：** 默认的TCP 数据发送窗口大小（字节）
> - **net.core.rmem_max：** 最大的TCP 数据接收窗口（字节）
> - **net.core.wmem_max：** 最大的TCP 数据发送窗口（字节）
> - **fs.aio-max-nr：** 同时可以拥有的的异步IO 请求数目



6.修改配置文件limits.conf

```bash
# Oracle 用户需要足够的资源限制（如打开文件数、进程数），修改/etc/security/limits.conf
[root@server01 module] sudo vim /etc/security/limits.conf
```

在文件末尾添加：

```bash
oracle_user soft nproc 2047
oracle_user hard nproc 16384
oracle_user soft nofile 1024
oracle_user hard nofile 65536
```

重启机器生效。

```bash
# 生效配置（需重新登录oracle_user）
sudo su - oracle_user
```

7.配置 Oracle 用户的环境变量

```bash
# 切换到oracle_user用户，修改其环境变量文件.bash_profile（关键：环境变量错误会导致安装 / 启动失败）
# 切换到oracle_user
sudo su - oracle_user

# 编辑环境变量文件bash_profile
vi ~/.bash_profile
# 环境变量文件添加：
# ORACLE_HOME
export ORACLE_BASE=/home/oracle_user/app/oracle
export ORACLE_HOME=/home/oracle_user/app/oracle/product/19.3.0/dbhome_1
export PATH=$PATH:$ORACLE_HOME/bin
export ORACLE_SID=orcl
export NLS_LANG=AMERICAN_AMERICA.ZHS16GBK


# 使环境变量生效
source ~/.bash_profile

# 验证环境变量（可选，确保输出正确）
echo $ORACLE_HOME
echo $ORACLE_SID
```

3.上传安装包并解压

> **注意：** 19c 需要把软件包直接解压到ORACLE_HOME 的目录下

```bash
# 下载 Oracle 安装包
# 下载地址：https://www.oracle.com/database/technologies/oracle19c-linux-downloads.html

# win11上传安装包到Linux服务器
scp -i "D:\application\VM-Vagrant\server01\.vagrant\machines\default\virtualbox\private_key" "E:\dataX-sgg\DataX\software\LINUX.X64_193000_db_home.zip" vagrant@192.168.56.11:/home/vagrant/

# 软件包直接解压到ORACLE_HOME 的目录下
sudo unzip LINUX.X64_193000_db_home.zip -d /home/oracle_user/app/oracle/product/19.3.0/dbhome_1

# 修改目录所有者和组（关键：必须是oracle_user和oracle_group）
sudo chown -R oracle_user:oracle_group /home/oracle_user/app
```

## 2.静默安装 `Oracle 19c`

1.准备静默安装响应文件

```bash
# 切换到oracle用户
su - oracle_user

# 进入ORACLE_HOME目录
cd $ORACLE_HOME

# 复查看模板响应文件（Oracle自带模板）
# cat install/response/db_install.rsp 
cat install/response/db_install.rsp 


# 编辑响应文件（关键：修改以下参数）
# 创建包含详细注释的完整响应文件
cat > $ORACLE_HOME/db_install.rsp << 'EOF'
####################################################################
## Copyright(c) Oracle Corporation 1998,2019. All rights reserved.##
##                                                                ##
## Specify values for the variables listed below to customize     ##
## your installation.                                             ##
##                                                                ##
## IMPORTANT NOTE: This file contains plain text passwords and    ##
## should be secured to have read permission only by oracle user  ##
## or db administrator who owns this installation.                ##
##                                                                ##
####################################################################

#------------------------------------------------------------------------------
# Do not change the following system generated value.
#------------------------------------------------------------------------------
oracle.install.responseFileVersion=/oracle/install/rspfmt_dbinstall_response_schema_v19.0.0

# -------------------------- 19c 静默安装核心必填参数 --------------------------

# 安装选项配置
# INSTALL_DB_SWONLY - 仅安装数据库软件（不创建数据库）
# INSTALL_DB_AND_CONFIG - 安装数据库软件并创建数据库
# UPGRADE_DB - 升级数据库
oracle.install.option=INSTALL_DB_SWONLY

# 服务器主机名（执行 `hostname` 命令获取）
ORACLE_HOSTNAME=server01

# 安装语言：英文+简体中文（19c支持）
SELECTED_LANGUAGES=en,zh_CN

# Unix用户组名称，必须是之前创建的oracle_group
UNIX_GROUP_NAME=oracle_group

# Oracle Inventory目录位置，用于存储安装的清单信息
INVENTORY_LOCATION=/home/oracle_user/app/oraInventory

# Oracle主目录，软件安装的根目录
ORACLE_HOME=/home/oracle_user/app/oracle/product/19.3.0/dbhome_1

# Oracle基目录，存放所有Oracle相关文件的顶层目录
ORACLE_BASE=/home/oracle_user/app/oracle

# 数据库安装版本
# EE - Enterprise Edition（企业版，功能最全）
# SE2 - Standard Edition 2（标准版2）
oracle.install.db.InstallEdition=EE

# 操作系统DBA组（Database Administrator），用于授予SYSDBA权限的用户组
oracle.install.db.OSDBA_GROUP=oracle_group

# 操作系统OPER组（Operator），用于授予SYSOPER权限的用户组
oracle.install.db.OSOPER_GROUP=oracle_group

# 操作系统BACKUPDBA组（Backup Administrator），用于执行备份操作的用户组
oracle.install.db.OSBACKUPDBA_GROUP=oracle_group

# 操作系统DGDBA组（Data Guard Administrator），用于管理Data Guard的用户组
oracle.install.db.OSDGDBA_GROUP=oracle_group

# 操作系统KMDBA组（Key Management Administrator），用于管理加密密钥的用户组
oracle.install.db.OSKMDBA_GROUP=oracle_group

# 操作系统RACDBA组（RAC Administrator），用于管理RAC集群的用户组
# 注：即使是非RAC环境也需要设置此参数
oracle.install.db.OSRACDBA_GROUP=oracle_group

# 是否通过My Oracle Support接收安全更新
# true - 安装程序会尝试连接Oracle支持网站检查更新
# false - 跳过My Oracle Support检查（推荐离线环境使用）
SECURITY_UPDATES_VIA_MYORACLESUPPORT=false

# 是否拒绝安全更新
# true - 拒绝所有安全更新
# false - 接受安全更新（需要提供My Oracle Support账户）
DECLINE_SECURITY_UPDATES=true

#------------------------------------------------------------------------------
# 集群配置（单实例环境留空）
#------------------------------------------------------------------------------
oracle.install.db.CLUSTER_NODES=

#------------------------------------------------------------------------------
# 容器数据库配置
#------------------------------------------------------------------------------
oracle.install.db.ConfigureAsContainerDB=false
oracle.install.db.config.PDBName=

#------------------------------------------------------------------------------
# 数据库配置选项（仅软件安装时可不填）
#------------------------------------------------------------------------------
oracle.install.db.config.starterdb.type=
oracle.install.db.config.starterdb.globalDBName=
oracle.install.db.config.starterdb.SID=
oracle.install.db.config.starterdb.characterSet=
oracle.install.db.config.starterdb.memoryOption=true
oracle.install.db.config.starterdb.memoryLimit=2048
oracle.install.db.config.starterdb.installExampleSchemas=false

#------------------------------------------------------------------------------
# 密码配置（仅软件安装时可不填）
#------------------------------------------------------------------------------
oracle.install.db.config.starterdb.password.ALL=
oracle.install.db.config.starterdb.password.SYS=
oracle.install.db.config.starterdb.password.SYSTEM=
oracle.install.db.config.starterdb.password.DBSNMP=
oracle.install.db.config.starterdb.password.PDBADMIN=

#------------------------------------------------------------------------------
# 存储配置
#------------------------------------------------------------------------------
oracle.install.db.config.starterdb.storageType=FILE_SYSTEM_STORAGE
oracle.install.db.config.starterdb.fileSystemStorage.dataLocation=/home/oracle_user/app/oracle/oradata
oracle.install.db.config.starterdb.fileSystemStorage.recoveryLocation=/home/oracle_user/app/oracle/recovery_area

#------------------------------------------------------------------------------
# 恢复选项
#------------------------------------------------------------------------------
oracle.install.db.config.starterdb.enableRecovery=false

#------------------------------------------------------------------------------
# ASM存储配置（使用文件系统存储时留空）
#------------------------------------------------------------------------------
oracle.install.db.config.asm.diskGroup=
oracle.install.db.config.asm.ASMSNMPPassword=

#------------------------------------------------------------------------------
# 管理选项
#------------------------------------------------------------------------------
oracle.install.db.config.starterdb.managementOption=DEFAULT

#------------------------------------------------------------------------------
# 企业管理器配置（使用DEFAULT管理选项时留空）
#------------------------------------------------------------------------------
oracle.install.db.config.starterdb.omsHost=
oracle.install.db.config.starterdb.omsPort=
oracle.install.db.config.starterdb.emAdminUser=
oracle.install.db.config.starterdb.emAdminPassword=

#------------------------------------------------------------------------------
# Root脚本执行配置
# true - 自动执行root脚本
# false - 手动执行root脚本
#------------------------------------------------------------------------------
# 禁止安装程序自动执行root脚本（19c需同时设置以下2个参数）
oracle.install.db.rootconfig.executeRootScript=false
# 手动配置root脚本，不自动验证密码
oracle.install.db.rootconfig.configMethod=
oracle.install.db.rootconfig.sudoPath=
oracle.install.db.rootconfig.sudoUserName=

#------------------------------------------------------------------------------
# My Oracle Support凭证（拒绝安全更新时留空）
#------------------------------------------------------------------------------
MYORACLESUPPORT_USERNAME=
MYORACLESUPPORT_PASSWORD=

#------------------------------------------------------------------------------
# 代理设置（如需要）
#------------------------------------------------------------------------------
PROXY_HOST=
PROXY_PORT=
PROXY_USER=
PROXY_PWD=
PROXY_REALM=

#------------------------------------------------------------------------------
# 自动更新配置
#------------------------------------------------------------------------------
oracle.installer.autoupdates.option=SKIP_UPDATES
AUTOUPDATES_MYORACLESUPPORT_USERNAME=
AUTOUPDATES_MYORACLESUPPORT_PASSWORD=
COLLECTOR_SUPPORTHUB_URL=
EOF
```

2.执行静默安装

```bash
# 切换到ORACLE_HOME目录
cd $ORACLE_HOME

# 执行静默安装命令（指定响应文件，输出日志）关键：使用-ignorePrereq忽略先决条件检查
./runInstaller -silent -responseFile $ORACLE_HOME/db_install.rsp -ignorePrereq

# 或者，如果遇到其他问题，可以尝试使用-waitforcompletion参数
# ./runInstaller -silent -responseFile $ORACLE_HOME/db_install.rsp -ignorePrereq -waitforcompletion
```

> 故障排查，查看日志

```bash
# 查看安装日志
tail -f /home/oracle_user/app/oraInventory/logs/installActions*.log
tail -f $ORACLE_HOME/install/*.log
```

> 验证安装

```bash
# 查看Oracle版本
sqlplus -v
```

3.安装成功后手动执行 root 脚本

```bash
# 切换到root用户
su -
# 或者使用 sudo（如果您配置了sudo权限）
# sudo su -

# VirtualBox 虚拟机没有root用户解决方案：
# vagrant 用户默认拥有免密执行 sudo 的权限（这是 vagrant 环境的标配）
# 因此无需切换到 root 用户，直接用 vagrant 用户通过 sudo 执行这两个脚本
# 从 oracle_user 退出到 vagrant（按 Ctrl+D 或执行 exit）
exit
# 验证当前用户是 vagrant
whoami  # 输出应显示 "vagrant"


# 执行安装清单脚本
sudo /home/oracle_user/app/oraInventory/orainstRoot.sh

# 执行Oracle根目录脚本
sudo /home/oracle_user/app/oracle/product/19.3.0/dbhome_1/root.sh
```

## 3.静默配置Oracle监听（netca)

1.执行静默监听配置命令

```bash
# 1. 切换到oracle_user用户（必须使用该用户执行，避免权限问题）
su - oracle_user

# 2. 执行netca静默配置命令，指定响应文件模板（Oracle自带），输出日志
netca -silent -responseFile $ORACLE_HOME/assistants/netca/netca.rsp

# 命令说明：
# -silent：静默模式（无图形化界面）
# -responseFile：指定监听配置响应文件（默认路径无需修改）
```

2.验证监听配置是否成功

```bash
# 1. 查看监听状态（核心命令）
lsnrctl status

# 2. 正常输出特征：
# - 监听状态为 "READY"（就绪）
# - 监听端口为 1521（Oracle默认端口）
# - 关联的SID为 orcl（与你配置的ORACLE_SID一致）

# 3. 若监听未启动，手动启动监听
lsnrctl start
```

3.监听常见操作

```bash
# 停止监听
lsnrctl stop

# 重启监听
lsnrctl reload  # 无需停止，直接重载配置
```

## 4.静默创建 Oracle 数据库实例（dbca）

1.准备 dbca 静默响应文件（推荐方式）

```bash
# 切换到oracle_user用户，创建dbca响应文件
su - oracle_user
vi $ORACLE_HOME/dbca.rsp

# 复制以下内容到文件中（根据你的环境无需修改核心参数，与前面配置一致）
cat > $ORACLE_HOME/dbca.rsp << 'EOF'
responseFileVersion=/oracle/assistants/rspfmt_dbca_response_schema_v19.0.0
gdbName=orcl
sid=orcl
templateName=General_Purpose.dbc
sysPassword=Oracle@123456
systemPassword=Oracle@123456
datafileDestination=/home/oracle_user/app/oracle/oradata
recoveryAreaDestination=/home/oracle_user/app/oracle/recovery_area
characterSet=ZHS16GBK
nationalCharacterSet=AL16UTF16
memoryPercentage=40
automaticMemoryManagement=true
totalMemory=0
databaseType=MULTIPURPOSE
createAsContainerDatabase=false
emConfiguration=NONE
EOF
```

```bash
# 3. 验证响应文件是否创建成功
ls -l $ORACLE_HOME/dbca.rsp
```

2.执行静默创建数据库命令

```bash
# 1.切换到ORACLE_HOME目录
cd $ORACLE_HOME

# 2.执行dbca静默创建命令，指定响应文件，输出日志
dbca -silent -createDatabase -responseFile $ORACLE_HOME/dbca.rsp

# 命令说明：
# -silent：静默模式
# -createDatabase：显式指定“创建数据库”操作（19c 必需，无法通过响应文件隐式识别）
# -responseFile：指定自定义的dbca响应文件
# 执行时间：根据服务器性能，通常10-30分钟，请勿中断
# 执行过程中可能出现的提示（正常现象，无需干预）
# 提示 1："Prepare for db operation" → 准备数据库操作
# 提示 2："Creating and starting Oracle instance" → 创建并启动实例
# 提示 3："Completing Database Creation" → 完成数据库创建
# 提示 4："Database creation complete." → 数据库创建成功

# 3.验证参数文件是否生成（确认核心问题修复）
# 切换到 dbs 目录，查看实例专属参数文件
cd $ORACLE_HOME/dbs
ls -l
# 若输出中包含以下两个文件，说明参数文件已成功生成（核心验证）
# - spfileorcl.ora（二进制格式，Oracle 19c 优先读取的参数文件）
# - initorcl.ora（文本格式，spfile 的备份文件，可选但会自动生成）
# 示例正常输出：
# -rw-r----- 1 oracle_user oracle_group  3584 Dec 27 17:00 spfileorcl.ora
# -rw-r----- 1 oracle_user oracle_group   256 Dec 27 17:00 initorcl.ora
# -rw-r--r-- 1 oracle_user oracle_group  3079 May 14  2015 init.ora


# 4.验证 Oracle 实例是否能正常启动
# 以 sysdba 权限本地连接数据库
sqlplus / as sysdba
# 执行启动命令（此时参数文件已存在，直接执行 startup 即可）
SQL> startup
# 验证实例状态（正常输出为 OPEN，说明实例启动成功）
SQL> select status from v$instance;
# 正常输出：
# STATUS
# --------
# OPEN
# 验证实例名（正常输出为 orcl，与配置一致）
SQL> select instance_name from v$instance;
# 正常输出：
# INSTANCE_NAME
# ----------------
# orcl
# 退出 sqlplus
SQL> exit


# 5.验证监听与实例的关联（确保远程连接可用）
# 手动触发实例向监听注册（无需等待默认 60 秒延迟）
sqlplus / as sysdba
SQL> alter system register;
SQL> exit

# 查看监听状态，确认 orcl 服务已注册
lsnrctl status

# 正常输出特征（说明关联成功，可远程连接）
# Service "orcl" has 1 instance(s).
# Instance "orcl", status READY, has 1 handler(s) for this service...
```

3.查看数据库创建日志（故障排查用）

```bash
# 日志默认存储路径
tail -f /home/oracle_user/app/oracle/cfgtoollogs/dbca/orcl/orcl.log

# 若创建失败，可通过该日志定位问题（如权限不足、磁盘空间不足等）
```

 ## 5.验证 Oracle 数据库与监听可用性

1.本地连接数据库验证（核心验证步骤）

```bash
# 本地连接数据库（无需密码）
# 1. 切换到oracle_user用户
su - oracle_user

# 2. 使用sys用户以sysdba权限本地连接（无需密码，基于操作系统认证）
sqlplus / as sysdba

# 3. 连接成功后，验证数据库实例状态
SQL> select status from v$instance;

# 4. 正常输出：STATUS = OPEN（表示数据库已正常启动）

# 5. 验证数据库字符集（确保与环境变量一致）
SQL> select userenv('language') from dual;
# 正常输出：AMERICAN_AMERICA.ZHS16GBK

# 6. 退出sqlplus
SQL> exit
```

2.验证监听与数据库实例关联

```bash
# 1. 重启监听后查看状态
lsnrctl reload
lsnrctl status

# 2. 正常输出中应包含：
#   Service "orcl" has 1 instance(s).
#   Instance "orcl", status READY, has 1 handler(s) for this service...
# 表示监听已成功关联orcl数据库实例
```

3.可选：远程连接验证（客户端测试）

```bash
若需从远程机器连接，可使用 PL/SQL Developer、Navicat 等工具，连接信息如下：
主机名 / IP：CentOS 服务器 IP
端口：1521
SID：orcl
用户名：sys（或 system）
密码：在 dbca 响应文件中配置的密码（如 Oracle@123456）
连接身份：sysdba（sys 用户需选择，system 用户选择 normal）

# 验证数据库实例是否正常启动并打开
# 以 sysdba 权限本地连接数据库（无需密码，操作系统认证）
sqlplus / as sysdba

# 查看数据库实例状态
SQL> select status from v$instance;

# 关键输出分析：
# ① 正常状态应为 "OPEN"（数据库已完全打开，才能自动注册服务）
# ② 若状态为 "MOUNT" 或 "NOMOUNT" → 数据库未正常打开，需启动到 OPEN 状态
SQL> startup  # 若数据库未启动，执行此命令启动（完整启动到 OPEN 状态）
# 若已启动但未打开，执行：SQL> alter database open;

# 查看数据库实例名（确认 SID 与配置一致）
SQL> select instance_name from v$instance;
# 输出应等于 "orcl"（与你的 ORACLE_SID 配置一致）

# 退出 sqlplus
SQL> exit


# Oracle 密码：查看 / 重置 / 验证完整方案
# 1. 登录CentOS服务器（通过vagrant或root用户）
ssh vagrant@服务器IP  # 或本地直接操作

# 2. 切换到oracle_user用户
su - oracle_user

# 3. 查看dbca响应文件中的密码配置（核心命令）
cat $ORACLE_HOME/dbca.rsp | grep -E "SYSPASSWORD|SYSTEMPASSWORD"

# 输出示例（明文显示配置的密码）：
# SYSPASSWORD = "Oracle@123456"
# SYSTEMPASSWORD = "Oracle@123456"
```

4.`Oracle`数据库与监听的启动 / 停止方法

> 监听的启动 / 停止 / 重启

```bash
# 切换到oracle_user用户
su - oracle_user

# 启动监听
lsnrctl start

# 停止监听
lsnrctl stop

# 重启监听（无需停止，直接重载）
lsnrctl reload

# 查看监听状态
lsnrctl status
```

> 数据库实例的启动 / 停止

```bash
# 1. 切换到oracle_user用户
su - oracle_user

# 2. 以sysdba权限连接sqlplus
sqlplus / as sysdba

# 3. 启动数据库（正常启动模式）
SQL> startup

# 4. 停止数据库（推荐使用immediate模式，安全关闭，不丢失数据）
SQL> shutdown immediate

# 5. 其他启动模式（按需使用）
SQL> startup mount  # 挂载模式（仅加载控制文件，不打开数据文件）
SQL> startup nomount  # 非挂载模式（仅启动实例，不加载控制文件）

# 6. 强制关闭数据库（紧急情况使用，可能丢失数据）
SQL> shutdown abort
```

5.开放 CentOS 防火墙 1521 端口（远程连接必备）

```bash
# 1. 切换到root/vagrant用户（需管理员权限）
su - root  # 或 sudo -i（vagrant用户）

# 2. 开放1521端口（永久生效）
firewall-cmd --permanent --add-port=1521/tcp

# 3. 重载防火墙配置
firewall-cmd --reload

# 4. 验证端口是否开放
firewall-cmd --query-port=1521/tcp

# 5. 若禁用防火墙（测试环境可选）
systemctl stop firewalld
systemctl disable firewalld
```

6.配置数据库开机自启（可选）

```bash
# 1. 切换到root用户
su - root

# 2. 创建Oracle开机自启脚本
vi /etc/rc.d/init.d/oracle

# 3. 复制以下内容到脚本中
#!/bin/bash
# chkconfig: 345 99 10
# description: Oracle 19c auto start/stop script
# processname: oracle

ORACLE_USER=oracle_user
ORACLE_HOME=/home/oracle_user/app/oracle/product/19.3.0/dbhome_1
ORACLE_SID=orcl

case "$1" in
    start)
        su - $ORACLE_USER -c "$ORACLE_HOME/bin/lsnrctl start"
        su - $ORACLE_USER -c "$ORACLE_HOME/bin/sqlplus / as sysdba << EOF
startup
exit
EOF"
        echo "Oracle 19c started successfully"
        ;;
    stop)
        su - $ORACLE_USER -c "$ORACLE_HOME/bin/lsnrctl stop"
        su - $ORACLE_USER -c "$ORACLE_HOME/bin/sqlplus / as sysdba << EOF
shutdown immediate
exit
EOF"
        echo "Oracle 19c stopped successfully"
        ;;
    restart)
        $0 stop
        sleep 5
        $0 start
        ;;
    status)
        su - $ORACLE_USER -c "$ORACLE_HOME/bin/lsnrctl status"
        su - $ORACLE_USER -c "$ORACLE_HOME/bin/sqlplus / as sysdba << EOF
select status from v\$instance;
exit
EOF"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
esac
exit 0

# 4. 赋予脚本执行权限
chmod +x /etc/rc.d/init.d/oracle

# 5. 添加到系统服务并设置开机自启
chkconfig --add oracle
chkconfig oracle on

# 6. 测试服务启停
service oracle start
service oracle stop
```











