1.上传压缩包到虚拟机、解压

```bash
# win11上传安装包到Linux服务器
scp -i "D:\application\VM-Vagrant\server01\.vagrant\machines\default\virtualbox\private_key" "E:\dataX-sgg\DataX\software\mongodb-linux-x86_64-rhel70-5.0.2.tgz" vagrant@192.168.56.11:/home/vagrant/

# 解压
[vagrant@server01 ~]$ tar -zxvf mongodb-linux-x86_64-rhel70-5.0.2.tgz -C /opt/module/

# 重命名
[vagrant@server01 ~]$ cd /opt/module/
[vagrant@server01 module]$ ls
mongodb-linux-x86_64-rhel70-5.0.2
[vagrant@server01 module]$ mv mongodb-linux-x86_64-rhel70-5.0.2/ mongodb
[vagrant@server01 module]$ ls
mongodb
```

2.创建数据库目录

> MongoDB 的数据存储在data 目录的db 目录下，但是这个目录在安装过程不会自动创建，所以需要手动创建data 目录，并在data 目录中创建db
> 目录。

```bash
[vagrant@server01 module]$ sudo mkdir -p /data/db
[vagrant@server01 mongodb]$ sudo chmod 777 -R /data/db/
```

3.启动MongoDB 服务

```bash
[vagrant@server01 module]$ cd mongodb/
[vagrant@server01 mongodb]$ bin/mongod
```

4.重新打开一个连接窗口，进入shell 页面

```bash
[vagrant@server01 ~]$ cd /opt/module/mongodb
[vagrant@server01 mongodb]$ bin/mongo


```

