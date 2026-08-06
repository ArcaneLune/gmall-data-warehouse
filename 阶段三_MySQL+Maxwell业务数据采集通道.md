# 阶段三：业务数据采集通道（MySQL + Maxwell → Kafka）

> 对应文档：《电商数仓（业务数据采集平台）V6.0》
> 目标：部署 MySQL 8.0.39 + Maxwell 1.29.2，打通"业务数据生成 → MySQL binlog → Maxwell CDC → Kafka(topic_db)"链路

---

## 概述

本阶段安装 MySQL 和 Maxwell，打通第二条数据通道：

```
Mock JAR(非test模式)生成业务数据 → MySQL(gmall库, 36张表) → Maxwell(CDC, 监听binlog) → Kafka(topic_db)
```

**与阶段二的区别**：阶段二是用户行为日志（JSON 文件 → Flume → Kafka topic_log），阶段三是业务数据（MySQL binlog → Maxwell → Kafka topic_db）。两条通道独立运行。

**组件安装顺序**：

```
MySQL 8.0.39 → 建库建表 → Maxwell 1.29.2 → 业务数据模拟 → 通道测试
```

---

## 1. MySQL 8.0.39 安装

**部署节点**：hadoop100（单机）

**下载地址**：[RPM Bundle](https://downloads.mysql.com/archives/community/) | [JDBC 驱动](https://downloads.mysql.com/archives/c-j/)

### 1.1 上传安装包

将 `mysql-8.0.39-1.el7.x86_64.rpm-bundle.tar` 上传到 **hadoop100** 的 `/opt/software/`。

### 1.2 解压 RPM 包

```bash
sudo mkdir /opt/software/mysql
sudo chown -R hadoop:hadoop /opt/software/
tar -xvf /opt/software/mysql-8.0.39-1.el7.x86_64.rpm-bundle.tar -C /opt/software/mysql/
```

### 1.3 卸载 mariadb-libs

CentOS 7.9 自带 mariadb-libs，与 MySQL 冲突，必须先卸载：

```bash
rpm -qa | grep mariadb
sudo yum remove mariadb-libs -y
```

### 1.4 安装前检查

```bash
# 检查 /tmp 目录权限
chmod -R 777 /tmp

# 检查依赖
rpm -qa | grep libaio
rpm -qa | grep net-tools
# 若缺失：sudo yum install -y libaio net-tools
```

### 1.5 安装 RPM

切换到 root 用户，按顺序安装 7 个包（顺序不能乱——common 必须在 libs 之前，libs 必须在 client/server 之前）：

```bash
su - root
cd /opt/software/mysql

rpm -ivh mysql-community-common-8.0.39-1.el7.x86_64.rpm
rpm -ivh mysql-community-client-plugins-8.0.39-1.el7.x86_64.rpm
rpm -ivh mysql-community-libs-8.0.39-1.el7.x86_64.rpm
rpm -ivh mysql-community-libs-compat-8.0.39-1.el7.x86_64.rpm
rpm -ivh mysql-community-client-8.0.39-1.el7.x86_64.rpm
rpm -ivh mysql-community-icu-data-files-8.0.39-1.el7.x86_64.rpm
rpm -ivh mysql-community-server-8.0.39-1.el7.x86_64.rpm
```

> **报错**：`Failed dependencies: libaio` → `yum install -y libaio`
> **报错**：`Failed dependencies: net-tools` → `yum install -y net-tools`

验证安装：

```bash
mysql --version
rpm -qa | grep -i mysql
```

### 1.6 初始化与启动

```bash
# 初始化（生成临时 root 密码）
mysqld --initialize --user=mysql

# 查看临时密码
cat /var/log/mysqld.log
# 找到：A temporary password is generated for root@localhost: xxxxxxxx

# 启动
systemctl start mysqld
systemctl status mysqld
systemctl enable mysqld   # 开机自启
```

### 1.7 登录并修改 root 密码

```bash
mysql -uroot -p
# 粘贴临时密码
```

```sql
ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';
```

### 1.8 设置远程登录（mysql_native_password 兼容性更好）

```sql
UPDATE mysql.user SET host = '%' WHERE user = 'root';
FLUSH PRIVILEGES;
ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'root';
FLUSH PRIVILEGES;
```

> 使用 `mysql_native_password` 而非默认的 `caching_sha2_password`，Flink CDC、DataX 等工具兼容性更好。

### 1.9 创建 gmall 业务数据库

```sql
CREATE DATABASE gmall DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
```

### 1.10 导入建表脚本

将 `gmall.sql`（36 张表的 DDL）上传到 hadoop100，然后导入：

```bash
mysql -uroot -p'root' gmall < /opt/software/gmall.sql
```

验证：

```bash
mysql -uroot -p'root' -e "USE gmall; SHOW TABLES;" | wc -l
```

**预期**：输出 37（36 张表 + 1 行表头）。

### 1.11 保存 JDBC 驱动 JAR

将下载的 `mysql-connector-j-8.0.39.jar` 保存到 `/opt/software/`，后续 Hive、DataX 等组件需要用到。

### 1.12 开启 MySQL binlog（Maxwell 必需）

```bash
sudo vim /etc/my.cnf
```

在 `[mysqld]` 段添加：

```ini
[mysqld]
server-id=1
log-bin=mysql-bin
binlog_format=ROW
binlog-do-db=gmall
```

重启 MySQL：

```bash
sudo systemctl restart mysqld
```

验证：

```bash
mysql -uroot -p'root' -e "SHOW VARIABLES LIKE 'log_bin';"
```

**预期**：`log_bin = ON`。

---

## 2. Maxwell 1.29.2 安装

**部署节点**：hadoop100

> 注意：Maxwell 1.30.0 及以上版本不再支持 JDK 1.8，必须使用 1.29.2。

### 2.1 下载解压

```bash
# 下载（或 Windows 下载后 XFTP 上传到 /opt/software/）
wget -P /opt/software/ https://github.com/zendesk/maxwell/releases/download/v1.29.2/maxwell-1.29.2.tar.gz

tar -zxvf /opt/software/maxwell-1.29.2.tar.gz -C /opt/module/
mv /opt/module/maxwell-1.29.2 /opt/module/maxwell
```

### 2.2 创建 Maxwell 元数据库及用户

Maxwell 需要在 MySQL 中存储 binlog 同步断点位置（支持断点续传），需要单独的数据库和用户：

```sql
CREATE DATABASE maxwell;
CREATE USER 'maxwell'@'%' IDENTIFIED BY 'maxwell';
GRANT ALL ON maxwell.* TO 'maxwell'@'%';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'%';
FLUSH PRIVILEGES;
```

### 2.3 配置 Maxwell

```bash
cd /opt/module/maxwell
cp config.properties.example config.properties
vim config.properties
```

写入：

```ini
# Maxwell 数据发送目的地：Kafka
producer=kafka

# Kafka 集群地址
kafka.bootstrap.servers=hadoop100:9092,hadoop101:9092,hadoop102:9092

# Kafka topic（业务数据统一发到 topic_db）
kafka_topic=topic_db

# ====== MySQL 连接配置 ======
host=hadoop100
user=maxwell
password=maxwell
jdbc_options=useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true

# 过滤 gmall.z_log 表（日志数据备份表，无须采集）
filter=exclude:gmall.z_log

# 按主键分组进入不同 Kafka 分区，避免数据倾斜
producer_partition_by=primary_key

# 教学环境：修正时间戳日期与业务日期一致
# mock_date=2026-06-18
```

### 2.4 环境变量

```bash
sudo vim /etc/profile.d/my_env.sh
```

追加：

```bash
# MAXWELL_HOME
export MAXWELL_HOME=/opt/module/maxwell
export PATH=$PATH:$MAXWELL_HOME/bin
```

```bash
source /etc/profile
```

### 2.5 Maxwell 启停脚本

```bash
sudo vim /home/hadoop/bin/mxw.sh
```

```bash
#!/bin/bash

MAXWELL_HOME=/opt/module/maxwell

status_maxwell(){
    result=`ps -ef | grep com.zendesk.maxwell.Maxwell | grep -v grep | wc -l`
    return $result
}

start_maxwell(){
    status_maxwell
    if [[ $? -lt 1 ]]; then
        echo "启动 Maxwell"
        $MAXWELL_HOME/bin/maxwell --config $MAXWELL_HOME/config.properties --daemon
    else
        echo "Maxwell 正在运行"
    fi
}

stop_maxwell(){
    status_maxwell
    if [[ $? -gt 0 ]]; then
        echo "停止 Maxwell"
        ps -ef | grep com.zendesk.maxwell.Maxwell | grep -v grep | awk '{print $2}' | xargs kill -9
    else
        echo "Maxwell 未在运行"
    fi
}

case $1 in
    start )
        start_maxwell
    ;;
    stop )
        stop_maxwell
    ;;
    restart )
        stop_maxwell
        start_maxwell
    ;;
    * )
        echo "Usage: mxw.sh start|stop|restart"
    ;;
esac
```

```bash
sudo chmod 777 /home/hadoop/bin/mxw.sh
```

> 比原始文档增加：`status_maxwell()` 检查进程是否已在运行；支持 `restart`；使用 `com.zendesk.maxwell.Maxwell` 精确匹配进程名。

### 2.6 启动 Maxwell

```bash
# 确保 Kafka 和 MySQL 均已启动
mxw.sh start
```

验证：

```bash
jps | grep Maxwell
```

hadoop100 上应有 `Maxwell` 进程。

### 2.7 Maxwell 常见报错

| 报错 | 原因 | 解决 |
|------|------|------|
| `Could not find first log file name in binary log index file` | MySQL binlog 未开启或格式不对 | 检查 `SHOW VARIABLES LIKE 'log_bin'` 和 `binlog_format` |
| `Access denied for user 'maxwell'` | Maxwell 用户权限不足 | 执行 `GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'%'` |
| `Unable to connect to MySQL` | MySQL 未启动或 bind-address 限制 | `sudo systemctl status mysqld`；检查 `/etc/my.cnf` 中 `bind-address` |
| `Producer closed with exception: TopicExistsException` | topic_db 残留删除标记 | 参考阶段二的 ZK 清理方法 |
| `caching_sha2_password` 相关报错 | MySQL 8.0 默认认证插件 | Maxwell 1.29.2 已支持，如仍报错，将 maxwell 用户的认证方式改为 `mysql_native_password` |

---

## 3. 业务数据模拟

### 3.1 与阶段二日志模拟的区别

| 模式 | 命令 | 生成什么 | MySQL 必需？ |
|------|------|---------|:----------:|
| 测试模式（阶段二） | `java -jar xxx.jar test 10` | 只生成日志文件 | 否 |
| 完整模式（阶段三） | `java -jar xxx.jar` 或 `lg.sh 100 2022-06-08` | 日志 + 业务数据写入 MySQL | **是** |

### 3.2 重新生成日志脚本（兼容 MySQL）

之前 lg.sh 只在 hadoop100 和 hadoop101 生成日志，现在改用完整模式，同时生成日志和业务数据。

先确保 `application.yml` 中 MySQL 连接配置正确：

```bash
vim /opt/module/applog/application.yml
```

关键配置：

```yaml
spring:
  datasource:
    druid:
      url: jdbc:mysql://hadoop100:3306/gmall?characterEncoding=utf-8&allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=GMT%2B8
      username: root
      password: "root"
```

> 记得改密码！之前阶段二是 `root`，现在改成 `Root@123456!`。

然后生成业务数据：

```bash
# 在 hadoop100 上直接执行（先测试能否成功）
cd /opt/module/applog/
java -jar gmall-remake-mock-2023-05-15-3.jar 100 2026-06-18
```

验证 MySQL 中有数据：

```bash
mysql -uroot -p'Root@123456!' -e "SELECT COUNT(*) FROM gmall.order_info;"
```

**预期**：返回 >0 的行数。

---

## 4. 通道测试

### 4.1 完整操作流程速览

```
1. 确保 MySQL 已启动 + binlog 已开启
2. zk.sh start         →  启动 Zookeeper
3. kf.sh start         →  启动 Kafka（等15秒）
4. kafka-topics --create → 手动创建 topic_db
5. kafka-console-consumer → 另开终端，准备观察数据
6. mxw.sh start        →  启动 Maxwell
7. java -jar 完整模式   →  生成业务数据
8. 观察消费者终端       →  看到 Maxwell JSON 即成功
```

### 4.2 手动创建 topic_db

```bash
kafka-topics.sh --bootstrap-server hadoop100:9092 --create --topic topic_db --partitions 3 --replication-factor 1
```

### 4.3 启动 Maxwell + 消费者

```bash
# 终端1：启动 Maxwell
mxw.sh start

# 终端2：启动 Kafka 消费者
kafka-console-consumer.sh --bootstrap-server hadoop100:9092 --topic topic_db
```

### 4.4 生成业务数据并观察

```bash
# 终端3：在 hadoop100 上生成业务数据
cd /opt/module/applog/
lg.sh
```

在终端2中应该看到 Maxwell CDC JSON 数据持续输出，类似：

```json
{
  "database": "gmall",
  "table": "order_info",
  "type": "insert",
  "ts": 1654646400,
  "data": {
    "id": "10001",
    "consignee": "张三",
    "total_amount": 999.00,
    "order_status": "1001",
    "user_id": "485"
  }
}
```

**如果终端2有数据输出，业务数据通道打通成功。**

### 4.5 排障：没收到数据怎么办

```
① Maxwell 进程在吗？
   xcall jps | grep Maxwell   → hadoop100 上应该有

② topic_db 创建了吗？
   kafka-topics.sh --bootstrap-server hadoop100:9092 --list

③ MySQL binlog 开启了吗？
   mysql -e "SHOW VARIABLES LIKE 'log_bin'"   → 应为 ON

④ MySQL 有新数据吗？
   mysql -e "SELECT COUNT(*) FROM gmall.order_info"   → 应 >0

⑤ Maxwell 报什么错？
   查看日志：tail -100 /opt/module/maxwell/logs/Maxwell.log
   或者前台启动（去掉 --daemon）看实时输出
```

---

## 5. 阶段验证清单

| # | 验证项 | 命令 | 预期 |
|---|--------|------|------|
| 1 | MySQL 已启动 | `sudo systemctl status mysqld` | active (running) |
| 2 | MySQL binlog 已开启 | `mysql -e "SHOW VARIABLES LIKE 'log_bin'"` | ON |
| 3 | gmall 库 36 张表 | `mysql -e "USE gmall; SHOW TABLES" \| wc -l` | 37（含表头） |
| 4 | Maxwell 进程正常 | `xcall jps \| grep Maxwell` | hadoop100 有 Maxwell |
| 5 | topic_db 已创建 | `kafka-topics.sh --list --bootstrap-server hadoop100:9092` | 含 topic_db |
| 6 | 业务数据已生成 | `mysql -e "SELECT COUNT(*) FROM gmall.order_info"` | >0 |
| 7 | 消费者收到 Maxwell JSON | console-consumer 有数据输出 | 通道打通 ✓ |

---

> **阶段三完成** | 下一阶段：数据同步策略 + Hive on Spark（Flume消费Kafka→HDFS + DataX全量同步 + Hive安装）
