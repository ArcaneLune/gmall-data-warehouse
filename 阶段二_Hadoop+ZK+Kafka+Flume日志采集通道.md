# 阶段二：Hadoop + Zookeeper + Kafka + Flume 日志采集通道

> 对应文档：《电商数仓（用户行为采集平台）V6.0》第3章3.5节 + 第4章
> 目标：部署 Hadoop/ZK/Kafka/Flume，打通"日志生成 → Flume采集 → Kafka"链路

---

## 概述

本阶段在阶段一的纯净集群上安装四个核心组件，并打通第一条数据通道：

```
Mock JAR生成日志文件 → Flume(TaildirSrc+KafkaChannel) → Kafka(topic_log)
```

**组件安装顺序**（依赖关系决定，不可颠倒）：

```
Hadoop(HDFS+YARN) → Zookeeper → Kafka → Flume → 数据模拟+通道测试
```

---

## 1. Hadoop 3.3.6 安装

### 1.1 节点角色分配

| 进程 | hadoop100 | hadoop101 | hadoop102 |
|------|:--------:|:--------:|:--------:|
| NameNode | ✓ | | |
| DataNode | ✓ | ✓ | ✓ |
| SecondaryNameNode | | | ✓ |
| ResourceManager | | ✓ | |
| NodeManager | ✓ | ✓ | ✓ |

> 内存紧张（每台 4GB）：NameNode 和 ResourceManager 各自独立部署，避免 hadoop100 负载过重。

### 1.2 下载并解压

在 **hadoop100** 上操作：

```bash
# 下载 Hadoop 3.3.6
wget -P /opt/software/ https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

# 校验文件完整性（可选但推荐）
# wget -P /opt/software/ https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz.sha512
# cd /opt/software && sha512sum -c hadoop-3.3.6.tar.gz.sha512

# 解压
tar -zxvf /opt/software/hadoop-3.3.6.tar.gz -C /opt/module/
mv /opt/module/hadoop-3.3.6 /opt/module/hadoop
```

> **报错**：`wget` 下载慢或失败 → 用 Windows 下载后 XFTP 上传到 `/opt/software/`，后续步骤不变。

### 1.3 配置环境变量

```bash
sudo vim /etc/profile.d/my_env.sh
```

追加以下内容：

```bash
# HADOOP_HOME
export HADOOP_HOME=/opt/module/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

生效并验证：

```bash
source /etc/profile
hadoop version
```

预期输出：`Hadoop 3.3.6`

### 1.4 配置 Hadoop

> 以下所有配置文件均在 `/opt/module/hadoop/etc/hadoop/` 下。

#### 1.4.1 核心配置：core-site.xml

```bash
vim /opt/module/hadoop/etc/hadoop/core-site.xml
```

内容如下（`<configuration>` 标签内）：

```xml
<!-- 指定 NameNode 地址 -->
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://hadoop100:8020</value>
</property>

<!-- 指定 Hadoop 临时目录 -->
<property>
    <name>hadoop.tmp.dir</name>
    <value>/opt/module/hadoop/data</value>
</property>

<!-- 配置 HDFS 网页登录使用的静态用户 -->
<property>
    <name>hadoop.http.staticuser.user</name>
    <value>hadoop</value>
</property>

<!-- 配置 hadoop(superUser) 允许通过代理访问的主机节点 -->
<property>
    <name>hadoop.proxyuser.hadoop.hosts</name>
    <value>*</value>
</property>

<!-- 配置 hadoop(superUser) 允许通过代理用户所属组 -->
<property>
    <name>hadoop.proxyuser.hadoop.groups</name>
    <value>*</value>
</property>

<!-- 配置 hadoop(superUser) 允许通过代理的用户 -->
<property>
    <name>hadoop.proxyuser.hadoop.users</name>
    <value>*</value>
</property>
```

> **proxyuser 的作用**：后续 DolphinScheduler、HiveServer2、Spark 等组件可能以不同用户身份提交任务到 HDFS。`proxyuser.*` 允许 hadoop 超级用户代理任意用户访问 HDFS，不配的话会出现 `Permission denied: user=xxx, access=WRITE` 类错误。

#### 1.4.2 HDFS 配置：hdfs-site.xml

```bash
vim /opt/module/hadoop/etc/hadoop/hdfs-site.xml
```

```xml
<!-- NameNode web 界面地址 -->
<property>
    <name>dfs.namenode.http-address</name>
    <value>hadoop100:9870</value>
</property>

<!-- SecondaryNameNode web 界面地址 -->
<property>
    <name>dfs.namenode.secondary.http-address</name>
    <value>hadoop102:9868</value>
</property>

<!-- 测试环境指定 HDFS 副本数量为 1（生产环境应为 3） -->
<property>
    <name>dfs.replication</name>
    <value>1</value>
</property>
```

> ⚠️ `dfs.replication` 设为 1，学习环境每台 VM 只有一块虚拟磁盘，副本多了没意义且浪费空间。
>
> 📝 **备用**：如果后续 DataX/DolphinScheduler 等组件以不同用户运行时遇到 `Permission denied`，追加以下配置关闭 HDFS 权限检查：
> ```xml
> <property>
>     <name>dfs.permissions.enabled</name>
>     <value>false</value>
> </property>
> ```

#### 1.4.3 YARN 配置：yarn-site.xml

```bash
vim /opt/module/hadoop/etc/hadoop/yarn-site.xml
```

```xml
<!-- 指定 MR 走 shuffle -->
<property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
</property>

<!-- 指定 ResourceManager 地址（部署在 hadoop101） -->
<property>
    <name>yarn.resourcemanager.hostname</name>
    <value>hadoop101</value>
</property>

<!-- 环境变量继承（避免 classpath 问题） -->
<property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
</property>

<!-- ====== 4GB 节点内存调优（必须配置） ====== -->

<!-- YARN 单个容器允许分配的最小/最大内存 -->
<property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>512</value>
</property>
<property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>2048</value>
</property>

<!-- YARN 每节点可用物理内存（4GB 总内存，留 ~2GB 给 OS/ZK/Kafka 等） -->
<property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>2048</value>
</property>

<!-- YARN 每节点可用 CPU 核心数 -->
<property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>2</value>
</property>

<!-- 关闭物理内存检查（4GB 节点必须关，否则容器会被 YARN 误杀） -->
<property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
</property>

<!-- 关闭虚拟内存检查 -->
<property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
</property>

<!-- 开启日志聚集 -->
<property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
</property>

<!-- 日志聚集服务器地址 -->
<property>
    <name>yarn.log.server.url</name>
    <value>http://hadoop100:19888/jobhistory/logs</value>
</property>

<!-- 日志保留 7 天 -->
<property>
    <name>yarn.log-aggregation.retain-seconds</name>
    <value>604800</value>
</property>
```

> 📝 **内存配置说明**：`yarn.nodemanager.resource.memory-mb=2048` 告知 YARN 每节点可用 2GB 作为容器计算资源。4GB 总内存中，OS 约 500MB + ZK 256MB + Kafka 512MB + Hadoop 守护进程约 500MB（含 NN/DN/NM/RM/SNN），剩余约 2GB 留给 YARN 容器。后续安装更多组件（MySQL/Maxwell/Flume/DS）会进一步挤压 YARN 可用资源，届时可能需降至 1536MB。
> 
> **为什么必须关 pmem-check**：YARN 默认会监控容器实际物理内存使用量，如果超过申请值就杀容器。但在 4GB 低内存环境下，JVM 堆外内存和各种 overhead 很容易让实际使用量超过申请值。关闭后 YARN 不再检查，避免无辜杀容器。

#### 1.4.4 MapReduce 配置：mapred-site.xml

```bash
vim /opt/module/hadoop/etc/hadoop/mapred-site.xml
```

```xml
<!-- 指定 MapReduce 运行在 YARN 上 -->
<property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
</property>

<!-- Map 任务内存（4GB 节点约束） -->
<property>
    <name>mapreduce.map.memory.mb</name>
    <value>512</value>
</property>

<!-- Reduce 任务内存 -->
<property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>512</value>
</property>

<!-- Map 任务 JVM 堆（为 overhead 留余量，JVM 堆一般设为 container 内存的 75%） -->
<property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx384m</value>
</property>

<!-- Reduce 任务 JVM 堆 -->
<property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx384m</value>
</property>

<!-- 历史服务器地址 -->
<property>
    <name>mapreduce.jobhistory.address</name>
    <value>hadoop100:10020</value>
</property>

<!-- 历史服务器 web 地址 -->
<property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>hadoop100:19888</value>
</property>
```

#### 1.4.5 配置 workers

```bash
vim /opt/module/hadoop/etc/hadoop/workers
```

删除原有内容（通常是 `localhost`），写入（**每行一个，无空行**）：

```
hadoop100
hadoop101
hadoop102
```

> `workers` 文件决定了 DataNode 和 NodeManager 在哪些节点启动。

#### 1.4.6 配置 hadoop-env.sh（JVM 堆 + JAVA_HOME）

```bash
vim /opt/module/hadoop/etc/hadoop/hadoop-env.sh
```

找到 `export JAVA_HOME=` 行，修改并追加以下内容：

```bash
export JAVA_HOME=/opt/module/jdk-1.8.0

# ====== JVM 堆限制（4GB 内存环境必须配置，全组件累计不能超 4GB） ======

# NameNode: 512MB（存储元数据，不能太小）
export HADOOP_NAMENODE_OPTS="-Xmx512m -Xms256m $HADOOP_NAMENODE_OPTS"

# SecondaryNameNode: 512MB（checkpoint 合并镜像时需要较大堆）
export HADOOP_SECONDARYNAMENODE_OPTS="-Xmx512m -Xms256m $HADOOP_SECONDARYNAMENODE_OPTS"

# DataNode: 256MB（主要是 I/O，不需要大堆）
export HADOOP_DATANODE_OPTS="-Xmx256m -Xms128m $HADOOP_DATANODE_OPTS"

# NodeManager: 256MB（管理容器生命周期，不跑计算）
export YARN_NODEMANAGER_OPTS="-Xmx256m -Xms128m $YARN_NODEMANAGER_OPTS"

# ResourceManager: 512MB（集群调度大脑，需要较大堆）
export YARN_RESOURCEMANAGER_OPTS="-Xmx512m -Xms256m $YARN_RESOURCEMANAGER_OPTS"

# 解决 "JAVA_HOME is not set" 警告
export HADOOP_OS_TYPE=${HADOOP_OS_TYPE:-$(uname -s)}
```

> 📝 **各节点 JVM 堆的内存占用分析**（全组件就绪后预估）：
>
> | 节点 | 常驻 JVM 进程（堆大小） | JVM 堆合计 | 非 JVM 进程 | OS 预留 | 实际可用 |
> |------|------------------------|-----------|-----------|--------|:------:|
> | hadoop100 | NN(512) + DN(256) + NM(256) + ZK(256) + Kafka(256) + HMS(512) | ~2.05GB | — | ~500MB | ~**1.45GB** |
> | hadoop101 | DN(256) + RM(512) + NM(256) + ZK(256) + Kafka(256) + MySQL(~512) | ~2.33GB | ~30MB | ~500MB | ~**1.14GB** |
> | hadoop102 | SNN(512) + DN(256) + NM(256) + ZK(256) + Kafka(256) + DS(1152) + Superset(512) | ~3.20GB | — | ~500MB | ~**0.30GB** |
>
> ⚠️ **hadoop102 内存最紧张**。DolphinScheduler 四个服务合计 1.15GB（Master 384 + Worker 384 + API 256 + Alert 128），加上其他 JVM 进程后仅剩约 300MB 物理内存。虽然关了 `pmem-check-enabled`（YARN 不会因此杀容器），但 OS 的 OOM Killer 仍可能介入。**建议**：YARN 容器尽量不要调度到 hadoop102，或将 DS 的 Master/Worker 堆降至 256MB。

#### 1.4.7 配置 capacity-scheduler.xml（AM 资源比例）

```bash
vim /opt/module/hadoop/etc/hadoop/capacity-scheduler.xml
```

在 `<configuration>` 标签内添加：

```xml
<property>
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
    <value>0.5</value>
    <description>
        AM 资源占比上限。YARN 容量调度器默认限制 ApplicationMaster 
        最多使用队列总资源的 10%。学习集群资源少（每节点 2GB），默认值太保守。
        0.5 = 集群总资源的 50%，约 3GB，够 Flink JM + Spark Driver 同时存在。
    </description>
</property>
```

> ⚠️ **必须修改**。默认 10%（约 600MB）不够 Flink JM（申请 1GB）提交到 YARN。设为 0.5 后 AM 上限约 3GB，Flink JM 1GB + Spark Driver 512MB 可同时运行。生产环境保持默认 0.1 即可。

### 1.5 分发 Hadoop

```bash
xsync /opt/module/hadoop
xsync /etc/profile.d/my_env.sh
```

在 hadoop101、hadoop102 上执行：

```bash
source /etc/profile
```

### 1.6 格式化并启动

#### 1.6.1 格式化 NameNode（仅在 hadoop100，只执行一次）

```bash
hdfs namenode -format
```

> **关键**：看到 `successfully formatted` 才算成功。**千万不要重复执行 format**，否则 NameNode 的 clusterID 与 DataNode 不一致，导致 DataNode 无法启动。
>
> **报错**：`ERROR common.Util: Invalid dfs.namenode.name.dir` → 检查 `/opt/module/hadoop/data` 目录是否存在且有写权限。

#### 1.6.2 启动 HDFS

```bash
# 在 hadoop100 上执行
start-dfs.sh
```

#### 1.6.3 启动 YARN + 历史服务器

```bash
# 在 hadoop101 上执行（ResourceManager 所在节点）
start-yarn.sh

# 在 hadoop100 上启动历史服务器
mapred --daemon start historyserver
```

#### 1.6.4 验证

```bash
# 在 hadoop100 上查看所有 Java 进程
xcall jps
```

**预期进程分布**：

| 节点 | 应有进程 |
|------|---------|
| hadoop100 | NameNode, DataNode, NodeManager, JobHistoryServer |
| hadoop101 | DataNode, NodeManager, ResourceManager |
| hadoop102 | DataNode, SecondaryNameNode, NodeManager |

#### 1.6.5 Web 界面验证

浏览器访问：

| 界面 | 地址 |
|------|------|
| HDFS NameNode | http://192.168.100.130:9870 |
| YARN ResourceManager | http://192.168.100.131:8088 |
| JobHistory Server | http://192.168.100.100:19888 |

> 如果无法访问：1) 检查防火墙是否已关闭；2) 检查 Windows 能否 ping 通虚拟机 IP；3) 如果用了 NAT 网络，需要在 VMware 虚拟网络编辑器中做端口转发。

### 1.7 Hadoop 启停脚本

创建一键启停脚本：

```bash
sudo vim /home/hadoop/bin/hdp.sh
```

```bash
#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input..."
    exit ;
fi
case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="
        echo " --------------- 启动 hdfs ---------------"
        ssh hadoop100 "/opt/module/hadoop/sbin/start-dfs.sh"
        echo " --------------- 启动 yarn ---------------"
        ssh hadoop101 "/opt/module/hadoop/sbin/start-yarn.sh"
        echo " --------------- 启动 historyserver ---------------"
        ssh hadoop100 "/opt/module/hadoop/bin/mapred --daemon start historyserver"
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="
        echo " --------------- 关闭 historyserver ---------------"
        ssh hadoop100 "/opt/module/hadoop/bin/mapred --daemon stop historyserver"
        echo " --------------- 关闭 yarn ---------------"
        ssh hadoop101 "/opt/module/hadoop/sbin/stop-yarn.sh"
        echo " --------------- 关闭 hdfs ---------------"
        ssh hadoop100 "/opt/module/hadoop/sbin/stop-dfs.sh"
;;
*)
    echo "Input Args Error..."
;;
esac

```

```bash
sudo chmod 777 /home/hadoop/bin/hdp.sh
```

> **停顺序与启顺序相反**：停止时先停历史服务器 → YARN → HDFS，避免 NameNode 已关但 NodeManager 还在写数据。

### 1.8 Hadoop 常见报错

| 报错 | 原因 | 解决 |
|------|------|------|
| `DataNode 无法启动` | 多次 format NameNode 导致 clusterID 不一致 | 删除所有节点 `/opt/module/hadoop/data` 后重新 format |
| `NameNode 启动后自动挂掉` | 磁盘满或 tmp 目录权限问题 | `df -h` 检查磁盘；`chown -R hadoop:hadoop /opt/module/hadoop/data` |
| `jps 后没有 NameNode` | format 失败或配置错误 | 检查 core-site.xml 的 `fs.defaultFS` 地址 |
| `NodeManager 没有启动` | YARN 配置错误或内存不足 | 检查 yarn-site.xml 中 RM 主机名；`free -h` 查看内存 |
| `YARN 容器一直处于 ACCEPTED 状态` | AM 资源比例限制 | 确认 capacity-scheduler.xml 中 `maximum-am-resource-percent` 已设为 0.5 |
| `Permission denied: user=xxx` | proxyuser 未配置 | 确认 core-site.xml 中 `hadoop.proxyuser.hadoop.*` 三项已配 |
| `Container killed by YARN for exceeding memory limits` | pmem-check 开启导致误杀 | 确认 `yarn.nodemanager.pmem-check-enabled` 为 false |
| `Cannot set priority of xxx process` | hadoop 用户资源限制 | 通常无害，可以忽略 |

---

## 2. Zookeeper 3.7.1 安装

**部署节点**：hadoop100 / hadoop101 / hadoop102

**下载地址**：https://mirrors.huaweicloud.com/apache/zookeeper/zookeeper-3.7.1/

| 节点 | myid |
|------|:----:|
| hadoop100 | 2 |
| hadoop101 | 3 |
| hadoop102 | 4 |

> myid 编号只需与 zoo.cfg 中的 `server.X` 一致即可，不必从 1 开始。

### 2.1 下载解压

在 **hadoop100** 上操作：

```bash
# 下载（或 Windows 下载后 XFTP 上传到 /opt/software/）
wget -P /opt/software/ https://mirrors.huaweicloud.com/apache/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz

# 解压
tar -zxvf /opt/software/apache-zookeeper-3.7.1-bin.tar.gz -C /opt/module/

# 重命名
cd /opt/module
mv apache-zookeeper-3.7.1-bin/ zookeeper-3.7.1/
```

### 2.2 配置环境变量

```bash
sudo vim /etc/profile.d/my_env.sh
```

末尾添加：

```bash
# ZOOKEEPER_HOME
export ZOOKEEPER_HOME=/opt/module/zookeeper-3.7.1
export PATH=$PATH:$ZOOKEEPER_HOME/bin
```

刷新：

```bash
source /etc/profile
```

### 2.3 创建数据目录与 myid 文件

```bash
# 创建数据目录
sudo mkdir /opt/module/zookeeper-3.7.1/zkData
sudo chown -R hadoop:hadoop /opt/module/zookeeper-3.7.1/

# 写入 myid（hadoop100 的编号为 2）
sudo vim /opt/module/zookeeper-3.7.1/zkData/myid
```

文件内容：`2`

### 2.4 配置 zoo.cfg

```bash
cd /opt/module/zookeeper-3.7.1
mv conf/zoo_sample.cfg conf/zoo.cfg
vim conf/zoo.cfg
```

修改 `dataDir`：

```ini
dataDir=/opt/module/zookeeper-3.7.1/zkData
```

末尾添加集群配置：

```ini
#######################cluster##########################
server.2=hadoop100:2888:3888
server.3=hadoop101:2888:3888
server.4=hadoop102:2888:3888
```

> 参数说明：`server.A=B:C:D`
> - A = myid 编号，与 zkData/myid 文件内容一致
> - B = 服务器地址
> - C = Follower 与 Leader 通信端口
> - D = Leader 选举端口

### 2.5 分发到集群

```bash
xsync /opt/module/zookeeper-3.7.1
xsync /etc/profile.d/my_env.sh
```

分发后，在 hadoop101 和 hadoop102 上分别执行：

```bash
source /etc/profile
```

### 2.6 修改其他节点的 myid

```bash
# hadoop101 上执行
sudo vim /opt/module/zookeeper-3.7.1/zkData/myid
# 内容改为：3

# hadoop102 上执行
sudo vim /opt/module/zookeeper-3.7.1/zkData/myid
# 内容改为：4
```

> `myid` 与 `zoo.cfg` 中 `server.X` 的数字一一对应，写错会导致该节点无法加入集群。

### 2.7 启动集群

```bash
# 三台分别执行
zkServer.sh start
```

验证：

```bash
xcall "zkServer.sh status"
```

**预期**：一台为 `Mode: leader`，另外两台为 `Mode: follower`。

### 2.8 ZK 启停脚本

```bash
sudo vim /home/hadoop/bin/zk.sh
```

```bash
#!/bin/bash

case $1 in
"start")
    for i in hadoop100 hadoop101 hadoop102
    do
        echo ---------- zookeeper $i 启动 ------------
        ssh $i "/opt/module/zookeeper-3.7.1/bin/zkServer.sh start"
    done
    ;;
"stop")
    for i in hadoop100 hadoop101 hadoop102
    do
        echo ---------- zookeeper $i 停止 ------------
        ssh $i "/opt/module/zookeeper-3.7.1/bin/zkServer.sh stop"
    done
    ;;
"status")
    for i in hadoop100 hadoop101 hadoop102
    do
        echo ---------- zookeeper $i 状态 ------------
        ssh $i "/opt/module/zookeeper-3.7.1/bin/zkServer.sh status"
    done
    ;;
*)
    echo "Usage: zk.sh start|stop|status"
    ;;
esac
```

```bash
chmod 777 /home/hadoop/bin/zk.sh
```

> **报错**：`Error contacting service. It is probably not running.`
> 原因：ZK 启动需要时间，或者 myid 不匹配 / 数据目录被破坏
> 解决：先 `jps` 确认 QuorumPeerMain 进程在跑，再 `zkServer.sh status`；如果 myid 错了，改完后清空 `zkData` 目录下除 myid 以外的文件，重启。

---

## 3. Kafka 3.3.1 安装

**部署节点**：hadoop100 / hadoop101 / hadoop102

> 注意：`kafka_2.12-3.3.1` 中 `2.12` 是 Scala 版本，`3.3.1` 是 Kafka 版本。

### 3.1 下载解压

在 **hadoop100** 上操作：

```bash
# 下载（或 Windows 下载后 XFTP 上传到 /opt/software/）
wget -P /opt/software/ https://archive.apache.org/dist/kafka/3.3.1/kafka_2.12-3.3.1.tgz

tar -zxvf /opt/software/kafka_2.12-3.3.1.tgz -C /opt/module/
mv /opt/module/kafka_2.12-3.3.1 /opt/module/kafka
```

### 3.2 配置 server.properties

```bash
cd /opt/module/kafka/config
vim server.properties
```

完整配置如下（**每台的 broker.id 和 advertised.listeners 不同，分发后单独修改**）：

```ini
# ====== 每台不同，分发后分别修改 ======
# broker 全局唯一编号（hadoop100=0, hadoop101=1, hadoop102=2）
broker.id=0
advertised.listeners=PLAINTEXT://hadoop100:9092

# ====== 网络线程（4GB/低核 VM 适当降低） ======
num.network.threads=3
num.io.threads=4
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# ====== 日志存储 ======
log.dirs=/opt/module/kafka/datas
num.partitions=3
default.replication.factor=1
num.recovery.threads.per.data.dir=1

# ====== 内部 Topic 副本（单节点集群必须设为 1） ======
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

# ====== 保留策略（50GB 磁盘，3 天即可） ======
log.retention.hours=72
log.segment.bytes=536870912
log.retention.check.interval.ms=300000

# ====== Zookeeper ======
zookeeper.connect=hadoop100:2181,hadoop101:2181,hadoop102:2181/kafka
```

> `/kafka` 后缀是 ZK 中的 chroot 路径，Kafka 会在 ZK 中该节点下创建自己的元数据，与其他组件隔离开，避免污染 ZK 根路径。

### 3.3 分发 + 修改 broker.id

```bash
xsync /opt/module/kafka
```

在 **hadoop101** 上：

```bash
vim /opt/module/kafka/config/server.properties
# 改两处：
# broker.id=1
# advertised.listeners=PLAINTEXT://hadoop101:9092
```

在 **hadoop102** 上：

```bash
vim /opt/module/kafka/config/server.properties
# 改三处：
# broker.id=2
# advertised.listeners=PLAINTEXT://hadoop102:9092
```

> broker.id 必须唯一，整个集群不能重复。

### 3.4 环境变量 + 启动

```bash
sudo vim /etc/profile.d/my_env.sh
```

追加：

```bash
# KAFKA_HOME
export KAFKA_HOME=/opt/module/kafka
export PATH=$PATH:$KAFKA_HOME/bin
```

分发并刷新：

```bash
xsync /etc/profile.d/my_env.sh
xcall "source /etc/profile"
```

启动（**必须先启动 ZK 集群，再启动 Kafka**）：

```bash
# 每台分别执行
bin/kafka-server-start.sh -daemon config/server.properties
```

验证：

```bash
xcall jps | grep Kafka
```

三台都应该有 `Kafka` 进程。

### 3.5 Kafka 启停脚本

```bash
sudo vim /home/hadoop/bin/kf.sh
```

```bash
#!/bin/bash

case $1 in
"start")
    for i in hadoop100 hadoop101 hadoop102
    do
        echo " --------启动 $i Kafka-------"
        ssh $i "/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties"
    done
    ;;
"stop")
    for i in hadoop100 hadoop101 hadoop102
    do
        echo " --------停止 $i Kafka-------"
        ssh $i "/opt/module/kafka/bin/kafka-server-stop.sh"
    done
    ;;
*)
    echo "Usage: kf.sh start|stop"
    ;;
esac
```

```bash
chmod 777 /home/hadoop/bin/kf.sh
```

> **重要**：停止 Kafka 集群时，一定要等 Kafka 所有节点进程全部停止后再停止 Zookeeper 集群。因为 ZK 中记录了 Kafka 集群信息，ZK 先停会导致 Kafka 无法正常关闭，只能手动 kill 进程。

---

## 4. Flume 1.10.1 安装（日志采集端）

### 4.1 需求分析

Flume 在 hadoop100 和 hadoop101 上采集本地日志文件，推送到 Kafka：

- **Source**：TaildirSource（断点续传、多目录，比 ExecSource 可靠）
- **Channel**：KafkaChannel（省掉 Sink，直接对接 Kafka，效率高）
- **目标**：Kafka topic `topic_log`

### 4.2 下载解压

在 **hadoop100** 上操作：

```bash
# 下载（或 Windows 下载后 XFTP 上传到 /opt/software/）
wget -P /opt/software/ https://archive.apache.org/dist/flume/1.10.1/apache-flume-1.10.1-bin.tar.gz

# 解压
tar -zxf /opt/software/apache-flume-1.10.1-bin.tar.gz -C /opt/module/

# 重命名
mv /opt/module/apache-flume-1.10.1-bin /opt/module/flume
```

### 4.3 配置 Flume 日志（log4j2.xml）

Flume 默认日志输出路径不直观，改为输出到 `/opt/module/flume/log`，方便排查问题：

```bash
vim /opt/module/flume/conf/log4j2.xml
```

将 `<Property name="LOG_DIR">` 的值改为：

```xml
<Property name="LOG_DIR">/opt/module/flume/log</Property>
```

同时在 Root Logger 的 AppenderRef 中**追加 Console 输出**（控制台也能看到日志，便于学习调试）：

```xml
<Root level="INFO">
    <AppenderRef ref="LogFile" />
    <AppenderRef ref="Console" />
</Root>
```

> `Console` 输出到 `SYSTEM_ERR`（标准错误流），启动 Flume 时前台运行会直接在终端看到日志，便于观察组件初始化过程。

### 4.4 分发 Flume

```bash
xsync /opt/module/flume
```

> Flume 同时也部署到 hadoop101（日志也会在 hadoop101 上生成）；hadoop102 不需要采集端 Flume。

### 4.5 配置 Flume Job

```bash
mkdir /opt/module/flume/job
vim /opt/module/flume/job/file_to_kafka.conf
```

写入：

```properties
# ====== 定义组件 ======
a1.sources = r1
a1.channels = c1

# ====== Source: Taildir Source ======
a1.sources.r1.type = TAILDIR
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = /opt/module/applog/log/app.*
a1.sources.r1.positionFile = /opt/module/flume/data/taildir_position.json

# ====== Channel: Kafka Channel ======
a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = hadoop100:9092,hadoop101:9092
a1.channels.c1.kafka.topic = topic_log
a1.channels.c1.parseAsFlumeEvent = false

# ====== 组装 ======
a1.sources.r1.channels = c1
```

> 关键参数说明：
> - `filegroups.f1` — 监控 `/opt/module/applog/log/` 下所有 `app.*` 文件
> - `positionFile` — 记录每个文件读取到了哪个位置，重启后不丢数据
> - `parseAsFlumeEvent = false` — 直接将文件行内容写入 Kafka，不包装 Flume Event Header，消费者拿到的是纯 JSON

### 4.6 Flume 采集端启停脚本

```bash
sudo vim /home/hadoop/bin/f1.sh
```

```bash
#!/bin/bash

case $1 in
"start")
    echo "========== 启动 hadoop100 日志采集 Flume =========="
    ssh hadoop100 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/file_to_kafka.conf >/dev/null 2>&1 &"

    echo "========== 启动 hadoop101 日志采集 Flume =========="
    ssh hadoop101 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/file_to_kafka.conf >/dev/null 2>&1 &"
    ;;
"stop")
    echo "========== 停止 hadoop100 日志采集 Flume =========="
    ssh hadoop100 "ps -ef | grep file_to_kafka | grep -v grep | awk '{print \$2}' | xargs -r kill -9"

    echo "========== 停止 hadoop101 日志采集 Flume =========="
    ssh hadoop101 "ps -ef | grep file_to_kafka | grep -v grep | awk '{print \$2}' | xargs -r kill -9"
    ;;
*)
    echo "Usage: f1.sh start|stop"
    ;;
esac
```

```bash
sudo chmod 777 /home/hadoop/bin/f1.sh
```

---

## 5. 数据模拟

### 5.1 创建 applog 目录

```bash
# hadoop100 和 hadoop101 都执行
xcall "mkdir -p /opt/module/applog"
```

### 5.2 上传模拟器文件

将以下文件通过 XFTP 上传到 hadoop100 的 `/opt/module/applog/`：

| 文件 | 说明 |
|------|------|
| `application.yml` | 数据生成配置（mock日期、session数、行为概率等） |
| `gmall-remake-mock-2023-05-15-3.jar` | 数据模拟器 JAR |
| `path.json` | 用户访问路径配置 |
| `logback.xml` | 日志输出配置 |

### 5.3 修改 application.yml

```bash
vim /opt/module/applog/application.yml
```

需要确认/修改的关键配置：

```yaml
# 日志输出方式：file（写文件）
mock:
  log:
    type: "file"

# MySQL 连接（mock JAR 同时生成业务数据到 MySQL）
spring:
  datasource:
    druid:
      url: jdbc:mysql://hadoop100:3306/gmall?characterEncoding=utf-8&allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=GMT%2B8
      username: root
      password: "root"

# 业务日期
mock.date: "2026-06-18"

# 首次运行：重置业务数据 + 用户
mock.clear.busi: 1
mock.clear.user: 1
mock.new.user: 100

# 日志不写入数据库 z_log 表
mock.log.db.enable: 0

# session 数量（测试用 50-100，正式 200+）
mock.user-session.count: 100
```

> 此时 MySQL 还没装，mock JAR 连不上 MySQL 会报错，但不影响日志文件生成。报错可暂时忽略，阶段三装好 MySQL 后会解决。

### 5.4 修改 logback.xml

```bash
vim /opt/module/applog/logback.xml
```

确认日志输出路径：

```xml
<property name="LOG_HOME" value="/opt/module/applog/log" />
```

### 5.5 分发模拟器到 hadoop101

```bash
xsync /opt/module/applog
```

### 5.6 创建日志生成脚本

```bash
sudo vim /home/hadoop/bin/lg.sh
```

```bash
#!/bin/bash

echo "========== hadoop100 生成数据 =========="
ssh hadoop100 "cd /opt/module/applog/ && java -jar /opt/module/applog/gmall-remake-mock-2023-05-15-3.jar  \$1 \$2 \$3 >/dev/null 2>&1 &"
echo "========== hadoop101 生成数据 =========="
ssh hadoop101 "cd /opt/module/applog/ && java -jar /opt/module/applog/gmall-remake-mock-2023-05-15-3.jar  \$1 \$2 \$3 >/dev/null 2>&1 &"
```

```bash
sudo chmod 777 /home/hadoop/bin/lg.sh
```

---

## 6. 通道测试

### 6.0 完整操作流程速览（按顺序执行）

```
1. 确保4个文件已上传到 /opt/module/applog/（jar + yml + json + xml）
2. zk.sh start          →  启动 Zookeeper
3. hdp.sh start         →  启动 Hadoop
4. kf.sh start          →  启动 Kafka（等15秒）
5. f1.sh start          →  启动 Flume 采集
6. kafka-topics --create →  手动创建 topic_log
7. kafka-console-consumer → 另开终端，准备观察数据
8. lg.sh test 10        →  生成测试数据
9. 观察消费者终端       →  看到 JSON 即成功
```

> **关键检查点**：Flume 启动后会在 Kafka 自动创建 `topic_log`。如果 `topic_log` 没出现，说明 Flume → Kafka 不通。如果 topic 有了但消费者没数据，说明 Flume 没读到日志文件（检查 `/opt/module/applog/log/` 下有没有 `app.log`）。

### 6.1 启动各组件

```bash
# 第1步：启动 Zookeeper
zk.sh start
# 验证：xcall jps | grep QuorumPeerMain（三台都有）

# 第2步：启动 Hadoop
hdp.sh start
# 验证：浏览器访问 http://192.168.100.130:9870 能看到 HDFS 界面

# 第3步：启动 Kafka（等 ZK 稳定后再启，10-15秒）
kf.sh start
# 验证：xcall jps | grep Kafka（三台都有）

# 第4步：启动 Flume 日志采集
f1.sh start
# 验证：xcall jps | grep Application（hadoop100 和 hadoop101 各一个 Application 进程）
```

### 6.2 手动创建 topic（推荐）

Flume 连接 Kafka 时 topic 不会自动出现，需要手动创建或等第一条数据写入时自动创建。为可控起见，建议先手动创建：

```bash
kafka-topics.sh --bootstrap-server hadoop100:9092 --create --topic topic_log --partitions 3 --replication-factor 1
```

验证：

```bash
kafka-topics.sh --bootstrap-server hadoop100:9092 --list
```

**预期**：输出包含 `topic_log`。

> 参数说明：`--partitions 3`（3个分区对应3个broker并行消费）、`--replication-factor 1`（单副本，学习环境）。

### 6.3 启动消费者（监控 Kafka 是否有数据到达）

在 hadoop100 另开一个终端：

```bash
kafka-console-consumer.sh --bootstrap-server hadoop100:9092 --topic topic_log
```

### 6.4 生成模拟数据

在 hadoop100 回到原终端：

```bash
# 生成 10 个 session（约 50 条日志）
lg.sh test 10
```

### 6.5 观察结果

在消费者终端中应该能看到 JSON 格式的日志数据持续输出，类似：

```json
{"common":{"ar":"15","ba":"iPhone","ch":"Appstore",...},"page":{"page_id":"home",...},"ts":1654646400000}
```

**如果消费者终端有数据输出，通道打通成功。**

### 6.6 排障：没收到数据怎么办

按顺序排查，一条命令定位问题：

```
① topic_log 创建了吗？
   kafka-topics.sh --bootstrap-server hadoop100:9092 --list
   没看到 → Flume 没连上 Kafka，检查 f1.sh 是否正常启动、Kafka 是否正常

② 日志文件生成了吗？
   ls /opt/module/applog/log/
   没有 *.log 文件 → lg.sh 没执行成功，手动跑一次 java -jar 命令确认

③ Flume 读到文件了吗？
   查看 Flume 日志：tail -f /opt/module/flume/log/flume.log
   看有没有读取 app.log 的记录，positionFile 是否在 /opt/module/flume/data/ 下

④ 消费者连对了吗？
   确认 --bootstrap-server 地址和 --topic 名称与 Flume 配置一致
```

### 6.6 停止测试

```bash
# 停止顺序与启动相反
f1.sh stop
kf.sh stop
zk.sh stop
```

---

## 7. 阶段验证清单

| # | 验证项 | 命令 | 预期 |
|---|--------|------|------|
| 1 | ZK 集群正常 | `zk.sh status` | 1 leader + 2 follower |
| 2 | HDFS 正常 | `hdfs dfs -ls /` | 列出根目录，无报错 |
| 3 | YARN 正常 + 3 活跃 NM | 访问 http://192.168.100.131:8088 | RM 界面，Active Nodes = 3 |
| 4 | JobHistory 正常 | 访问 http://192.168.100.100:19888 | 历史服务器界面 |
| 5 | HDFS 副本数正确 | `hdfs getconf -confKey dfs.replication` | 输出 `1` |
| 6 | Kafka 正常 | `kafka-topics.sh --bootstrap-server hadoop100:9092 --list` | 无报错 |
| 7 | Flume 正常启动 | `xcall jps` 中 hadoop100/101 有 Application | 无报错退出 |
| 8 | topic_log 已创建 | `kafka-topics.sh --list --bootstrap-server hadoop100:9092` | 看到 topic_log |
| 9 | 日志生成正常 | `ls /opt/module/applog/log/` | 有 `app.log` 文件 |
| 10 | 消费者能收到数据 | console-consumer 有 JSON 输出 | 通道打通 ✓ |

---

## 8. 高频报错速查

| 报错 | 原因 | 解决 |
|------|------|------|
| `DataNode 无法启动` | 重复 format NameNode | 删除 data 目录后重 format |
| `Kafka: java.net.UnknownHostException` | hosts 中 127.0.0.1 绑定主机名 | 注释掉 /etc/hosts 中相关行 |
| `Flume: No channel found for source r1` | Flume 配置中组件名拼写不一致 | 仔细检查 `file_to_kafka.conf` 中 a1/r1/c1 三处命名 |
| `Flume: Kafka Channel: Failed to create topic` | Kafka 集群未启动或 ZK 连不上 | 先确认 `zk.sh status` 正常，再确认 `jps` 中有 Kafka |
| `Flume 启动后立刻退出` | 无法读取监控目录 | 检查 `/opt/module/applog/log/` 是否存在 |
| `Consumer 没有数据` | Flume 的 positionFile 记录偏移导致跳过已读文件、或 lg.sh 在 Flume 之后的节点执行 | 删除 `/opt/module/flume/data/taildir_position.json` 后重启 Flume；确保 lg.sh 和 Flume 在同一节点 |
| `Connection refused: hadoop100:9092` | Kafka 未完全启动 | 等 10-15 秒后重试 |
| `NoClassDefFoundError: org/apache/flume/channel/kafka/KafkaChannel` | Flume 缺少 Kafka 客户端 jar | Flume 1.10.1 自带的 Kafka jar 版本过旧，替换为 Kafka 3.3.1 的 jar（详见下文） |

### 8.1 Kafka 客户端 Jar 兼容性处理（概率问题）

Flume 1.10.1 内置的 Kafka 客户端 jar 版本为 2.7.x，与 Kafka 3.3.1 broker 通信理论兼容，如果遇到 `NoClassDefFoundError` 或 `ClassNotFoundException`，执行以下操作：

```bash
# 删除 Flume 自带的旧 Kafka jar
rm -f /opt/module/flume/lib/kafka-clients-*.jar
rm -f /opt/module/flume/lib/kafka_2.12-*.jar

# 拷贝 Kafka 3.3.1 的客户端 jar 到 Flume lib
cp /opt/module/kafka/libs/kafka-clients-3.3.1.jar /opt/module/flume/lib/
cp /opt/module/kafka/libs/kafka_2.12-3.3.1.jar /opt/module/flume/lib/

# 分发
xsync /opt/module/flume/lib
```

---

> **阶段二完成** | 下一阶段：业务数据采集通道（MySQL + Maxwell → Kafka）
