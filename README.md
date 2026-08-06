# 电商离线数据仓库（GMall Data Warehouse）

> 从零搭建的完整电商大数据离线数仓系统，覆盖数据采集 → 数仓建模 → 任务调度 → 可视化报表全链路。

## 项目概述

基于 3 节点 CentOS 7 虚拟机集群，构建电商平台（GMall）离线数据仓库。将用户行为日志和业务数据库中的交易数据，经 Flume + Maxwell + Kafka 实时采集至 HDFS，通过 Hive on Spark 完成 ODS → DIM → DWD → DWS → ADS 五层建模，最终导入 Apache Doris 实现毫秒级 OLAP 查询，并通过 DolphinScheduler 编排全链路定时调度，Superset 输出 16 张业务报表的可视化仪表盘。

## 技术架构

```
行为日志(Mock)  →  Flume  →  Kafka  →  Flume  →  HDFS
                                                       ↘
MySQL 业务数据  →  Maxwell  →  Kafka  →  Flume  →  HDFS  →  Hive on Spark
                                                                      ↓
                                              ODS(30表) → DIM(9表) → DWD(10表) → DWS(15表) → ADS(16表)
                                                                                                      ↓
                                                                              Doris 2.0 (Broker Load) → Superset 可视化
                                                                                                      ↑
                                                                              DolphinScheduler 2.0 (全链路 DAG 调度)
```

## 技术栈

| 类别 | 组件 | 版本 |
|------|------|------|
| 数据采集 | Flume, Maxwell | 1.10.1 / 1.29.2 |
| 消息队列 | Kafka | 2.12-3.3.1 |
| 数据同步 | DataX | 202309 |
| 分布式存储 | Hadoop HDFS | 3.3.6 |
| 计算引擎 | Spark (Hive on Spark) | 3.3.1 |
| 数据仓库 | Hive | 3.1.3 |
| 协调服务 | Zookeeper | 3.7.1 |
| 关系数据库 | MySQL | 8.0.39 |
| OLAP 引擎 | Apache Doris | 2.0.3 |
| 任务调度 | DolphinScheduler | 2.0.5 |
| 数据可视化 | Apache Superset | 2.0.0 |

## 集群规划

| 服务 | hadoop100 (4GB) | hadoop101 (4GB) | hadoop102 (8GB) |
|------|:---:|:---:|:---:|
| NameNode / RM / SNN | NN | RM | SNN |
| Hive / Spark / DataX | ✓ | | |
| Flume 采集 | ✓ | ✓ | |
| Flume 消费 | | | f2 + f3 |
| Maxwell CDC | ✓ | | |
| MySQL 元数据库 | ✓ | | |
| Doris FE + BE | | | ✓ |
| DolphinScheduler | | | ✓ (Standalone) |
| Superset | | ✓ | |

## 数仓分层

```
ADS  (应用层) — 16张报表，面向 Doris/Superset
DWS  (汇总层) — 15张汇总表，1日/7日/30日/历史至今
DWD  (明细层) — 10张事实表，事务型/周期快照/累积快照
DIM  (维度层) — 9张维度表，星型模型，含拉链表
ODS  (贴源层) — 30张表，1:1 映射源数据
```

## 项目结构

```
├── 电商离线数仓V6.0_项目介绍.md        # 项目总览
├── 阶段一_服务器基础环境准备.md          # JDK, SSH, NTP, 集群脚本
├── 阶段二_Hadoop+ZK+Kafka+Flume日志采集通道.md
├── 阶段三_MySQL+Maxwell业务数据采集通道.md
├── 阶段四_数据同步策略+Hive_on_Spark环境.md
├── 阶段五_数据模拟+数仓分层建模_ODS层.md # ODS + DIM + DWD + DWS + ADS
├── 阶段六_Doris数据迁移.md              # Doris 部署 + Broker Load 导入
├── 阶段七_DolphinScheduler工作流调度.md  # DS 部署 + 全链路 DAG
├── 阶段八_Superset可视化.md             # Superset 部署 + Doris 连接
├── 集群启停速查手册.md                  # 所有组件启停命令
├── gmall.sql                            # MySQL 业务库建表脚本
├── *.sh                                 # 每日装载脚本（10个）
└── outputs/                             # 各阶段实施文档
```

## 快速开始

### 1. 启动集群
```bash
ssh hadoop100 "zk.sh start && start-dfs.sh"
ssh hadoop101 "start-yarn.sh"
ssh hadoop100 "kf.sh start"
```

### 2. 启动数据采集
```bash
# 日志采集
ssh hadoop100 "f1.sh start"
ssh hadoop102 "f2.sh start"

# 业务数据采集
ssh hadoop100 "mxw.sh start"
ssh hadoop102 "f3.sh start"
```

### 3. 执行数仓任务（DolphinScheduler 调度或手动）
```bash
# ODS → DIM → DWD → DWS → ADS → Doris
dws_to_ads.sh all 2026-06-18
ads_broker_load.sh 2026-06-18
```

### 4. 查看可视化
- **DolphinScheduler**：http://hadoop102:12345/dolphinscheduler
- **Superset**：http://hadoop101:8787

## 关键指标

| 指标 | 数值 |
|------|------|
| 数据表总数 | 36 (MySQL) + 30 (ODS) + 9 (DIM) + 10 (DWD) + 15 (DWS) + 16 (ADS) = 116 张 |
| ADS 报表 | 16 张，覆盖流量/用户/商品/交易/优惠券五大主题 |
| 每日装载脚本 | 10 个，Hive SQL 自动切 MR 引擎避免 4GB OOM |
| 调度 DAG | 7 步，DolphinScheduler Standalone 编排 |
| 集群规模 | 3 节点 CentOS 7.9，8GB + 4GB + 4GB |

## License

MIT
