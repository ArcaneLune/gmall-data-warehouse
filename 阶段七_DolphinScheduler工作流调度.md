# 阶段七：DolphinScheduler 工作流调度

> 目标：在 hadoop102 上 Standalone 模式部署 Apache DolphinScheduler 2.0.5，将数仓各层装载脚本编排为 DAG 工作流，实现全链路自动化调度

---

## 概述

前六个阶段已打通 "日志/业务数据采集 → ODS → DIM → DWD → DWS → ADS → Doris" 全链路。各层装载脚本散落执行，效率低且易遗漏。本阶段引入 DolphinScheduler 工作流调度系统，将所有脚本串成 DAG，一键触发全链路跑批。

选择 **hadoop102 + Standalone 单机模式**：
- 一个 JVM 进程包含 Master、Worker、API、Alert 全部功能，无需额外部署 MySQL/PostgreSQL 元数据库
- 内置内存数据库 H2 和内置 ZooKeeper，零外部依赖
- 轻量省资源（~1GB 堆），适合本项目 < 20 个 DAG 工作流的规模
- 控制台地址：`http://hadoop102:12345/dolphinscheduler`

---

## 1. 前置准备

| 组件 | 位置 | 状态 |
|---|---|---|
| JDK 1.8 | /opt/module/jdk-1.8.0，全节点 | ✅ |
| psmisc | hadoop102 | 需安装 |

```bash
# 在 hadoop102 上执行
sudo yum install -y psmisc
```

---

## 2. 清理旧部署 + 重新安装

### 2.1 清理旧部署残留

```bash
# 1. 停止当前所有 DS 进程（在 hadoop102 上）
cd /opt/module/dolphinscheduler
bin/dolphinscheduler-daemon.sh stop standalone-server 2>/dev/null
bin/stop-all.sh 2>/dev/null

# 2. 强制杀残留
pkill -f dolphinscheduler 2>/dev/null
pkill -f StandaloneServer 2>/dev/null

# 3. 删除旧安装目录和临时文件
rm -rf /opt/module/dolphinscheduler
rm -rf /opt/software/apache-dolphinscheduler-2.0.5-bin
rm -rf /tmp/dolphinscheduler

# 4. 清理 HDFS 上的 DS 资源目录
hdfs dfs -rm -r -f /dolphinscheduler 2>/dev/null

# 5. 清理 MySQL 中的 DS 元数据库（在 hadoop100 上）
mysql -u root -p -e "DROP DATABASE IF EXISTS dolphinscheduler; DROP USER IF EXISTS 'dolphinscheduler'@'%';"
```

### 2.2 解压并启动

```bash
# 在 hadoop102 上操作
cd /opt/software/
tar -zxvf apache-dolphinscheduler-2.0.5-bin.tar.gz
mv apache-dolphinscheduler-2.0.5-bin /opt/module/dolphinscheduler

# 启动
cd /opt/module/dolphinscheduler
bin/dolphinscheduler-daemon.sh start standalone-server
```

验证：

```bash
jps | grep Standalone   # 应输出：StandaloneServer
```

---

## 3. 访问 UI 与用户配置

浏览器打开 `http://hadoop102:12345/dolphinscheduler`

| 用户名 | 密码 |
|---|---|
| admin | dolphinscheduler123 |

### 3.1 创建租户

首次登录后，进入 **安全中心 → 租户管理 → 创建租户**：

| 字段 | 值 |
|---|---|
| 租户编码 | hadoop |
| 租户名称 | hadoop |
| 队列 | default |

> DS 租户对应 Linux 用户。Worker 执行任务时会 `sudo -u hadoop` 切换身份，因此租户编码必须与 Linux 用户名一致。

### 3.2 创建普通用户

进入 **安全中心 → 用户管理 → 创建用户**：

| 字段 | 值 |
|---|---|
| 用户名 | hadoop |
| 密码 | hadoop123 |
| 租户 | hadoop（上一步创建的） |
| 邮箱 | hadoop@localhost |
| 手机 | 可选 |

### 3.3 切换普通用户

退出 admin 登录，使用 **hadoop / hadoop123** 重新登录。

> 后续所有项目管理、工作流创建、任务调度均在 hadoop 普通用户下操作，不直接使用 admin 管理员账号。

---

## 4. 启停命令与资源配置

Standalone 模式默认资源中心指向 HDFS，单机部署需改为本地文件系统。

### 4.1 配置资源中心使用 HDFS

```bash
vim /opt/module/dolphinscheduler/standalone-server/conf/common.properties
```

修改：

```properties
resource.storage.type=HDFS
resource.storage.upload.base.path=/dolphinscheduler
fs.defaultFS=hdfs://hadoop100:8020
hdfs.root.user=hadoop
```

> Standalone 模式下只需改 `standalone-server/conf/common.properties` 这一个文件。
> 确保 HDFS 路径有写权限：`hdfs dfs -chmod -R 777 /dolphinscheduler`

### 4.2 启停命令

| 操作 | 命令 |
|---|---|
| 启动 | `bin/dolphinscheduler-daemon.sh start standalone-server` |
| 停止 | `bin/dolphinscheduler-daemon.sh stop standalone-server` |
| 状态 | `bin/dolphinscheduler-daemon.sh status standalone-server` |
| 日志 | `tail -f logs/dolphinscheduler-standalone-server-hadoop102.out` |

> Standalone 模式使用内存数据库 H2，进程重启后工作流定义等元数据会丢失。生产环境需切换为伪集群/集群模式并外接 MySQL。

---

## 5. 工作流设计

> DS Worker 运行在 hadoop102，通过 hadoop 用户的 SSH 免密通道（阶段一已配置）远程操作 hadoop100 和 hadoop101。

### 5.1 上传脚本到资源中心

> 集群始终保持运行状态（不包含启停），仅上传**每日装载**脚本。首日装载已手动执行完毕。

以 **admin** 身份登录 → 资源中心 → 创建目录 `scripts` → 上传以下 10 个脚本：

| # | 脚本 | 用途 |
|---|------|------|
| 1 | `ods_to_dwd.sh` | DWD 层每日装载 |
| 2 | `ods_to_dim.sh` | DIM 层每日装载 |
| 3 | `mysql_to_hdfs_full.sh` | MySQL 全量表同步到 HDFS |
| 4 | `hdfs_to_ods_log.sh` | ODS 日志表装载 |
| 5 | `hdfs_to_ods_db.sh` | ODS 业务表装载 |
| 6 | `ads_broker_load.sh` | ADS→Doris |
| 7 | `dws_to_ads.sh` | ADS 层装载 |
| 8 | `dws_1d_to_dws_td.sh` | 历史至今汇总表每日 |
| 9 | `dws_1d_to_dws_nd.sh` | n日汇总表 |
| 10 | `dwd_to_dws_1d.sh` | DWS 1日汇总表每日 |

### 5.2 创建环境（admin 操作）

安全中心 → 环境管理 → 创建环境：

| 字段 | 值 |
|---|---|
| 环境名称 | gmall |
| 环境配置 | 见下方 |

```bash
export HADOOP_HOME=/opt/module/hadoop
export HADOOP_CONF_DIR=/opt/module/hadoop/etc/hadoop
export SPARK_HOME=/opt/module/spark
export JAVA_HOME=/opt/module/jdk-1.8.0
export HIVE_HOME=/opt/module/hive
export DATAX_HOME=/opt/module/datax
export PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin:$JAVA_HOME/bin:$HIVE_HOME/bin:$DATAX_HOME/bin
```

> 给 Shell 任务指定 `gmall` 环境后，脚本自动继承上述环境变量，无需在每个任务中重复 export。

### 5.3 创建工作流（hadoop 用户操作）

退出 admin，以 **hadoop / hadoop123** 登录。

项目管理 → 创建项目 `gmall` → 进入项目 → 工作流定义 → 创建工作流。

所有节点选择 **Shell 类型**，脚本引用资源中心 `scripts/xxx.sh`，环境选择 `gmall`：

```
┌──────────────────────┐
│ ① mysql_to_hdfs_full   │  mysql_to_hdfs_full.sh all ${dt}
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ ② hdfs_to_ods          │  hdfs_to_ods_log.sh ${dt}
│                        │  hdfs_to_ods_db.sh all ${dt}
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ ③ DIM + DWD            │  ods_to_dim.sh all ${dt}
│                        │  ods_to_dwd.sh all ${dt}
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ ④ DWS 1日              │  dwd_to_dws_1d.sh all ${dt}
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ ⑤ DWS nd + td          │  dws_1d_to_dws_nd.sh all ${dt}
│                        │  dws_1d_to_dws_td.sh all ${dt}
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ ⑥ ADS                  │  dws_to_ads.sh all ${dt}
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ ⑦ ADS→Doris            │  ads_broker_load.sh ${dt}
└──────────────────────┘
```

### 5.4 任务节点配置

每个任务节点的通用设置如下：

| 配置项 | 值 |
|---|---|
| 节点类型 | Shell |
| Worker 分组 | default |
| 环境名称 | gmall |
| 失败重试间隔 | 1 分 |
| 资源 | scripts/xxx.sh |
| 脚本代码 | `bash scripts/xxx.sh <参数>` |

具体配置：

| 步骤 | 节点名称 | 脚本代码 |
|---|---|---|
| ① | mysql_to_hdfs_full | `bash scripts/mysql_to_hdfs_full.sh all ${dt}` |
| ② | hdfs_to_ods_log | `bash scripts/hdfs_to_ods_log.sh ${dt}` |
| ② | hdfs_to_ods_db | `bash scripts/hdfs_to_ods_db.sh all ${dt}` |
| ③ | ods_to_dim | `bash scripts/ods_to_dim.sh all ${dt}` |
| ③ | ods_to_dwd | `bash scripts/ods_to_dwd.sh all ${dt}` |
| ④ | dwd_to_dws_1d | `bash scripts/dwd_to_dws_1d.sh all ${dt}` |
| ⑤ | dws_1d_to_dws_nd | `bash scripts/dws_1d_to_dws_nd.sh all ${dt}` |
| ⑤ | dws_1d_to_dws_td | `bash scripts/dws_1d_to_dws_td.sh all ${dt}` |
| ⑥ | dws_to_ads | `bash scripts/dws_to_ads.sh all ${dt}` |
| ⑦ | ads_broker_load | `bash scripts/ads_broker_load.sh ${dt}` |

### 5.5 保存并上线工作流

所有节点连线完成后，点击右上角 **保存**：

| 字段 | 值 |
|---|---|
| DAG 图名称 | gmall |
| 租户 | hadoop |
| 全局参数 | `dt`（value 不填，实际工作中每日手动填入 T-1 日期） |

> 保存后进入工作流定义页面，点击 **上线** 按钮。上线后即可在 **工作流实例** 中手动触发运行，或配置定时调度自动执行。

---

## 6. 阶段验证清单

| # | 验证项 | 操作 | 预期 |
|---|--------|------|------|
| 1 | DS 进程运行 | `jps \| grep Standalone` | `StandaloneServer` |
| 2 | UI 可访问 | `http://hadoop102:12345/dolphinscheduler` | 登录页 |
| 3 | 租户/用户创建 | 按 3.1~3.3 操作 | 可用 hadoop/hadoop123 登录 |
| 4 | 环境创建 | admin → 环境管理 → gmall | 环境配置保存成功 |
| 5 | 资源上传 | 资源中心 → scripts → 上传 10 个脚本 | 全部上传成功 |
| 6 | Shell 任务可执行 | 创建测试工作流，运行 `hostname` | 绿色（成功） |
| 7 | 全链路 DAG 运行 | 按 5.3 的 7 步 DAG 提交 | 全部绿色 |

> **脚本调用格式**（注意 `$1` 和 `$2` 的区别）：
> - `all <date>` 格式：`mysql_to_hdfs_full.sh all 2026-06-19`、`ods_to_dim.sh all 2026-06-19`、`ods_to_dwd.sh all 2026-06-19`、`dwd_to_dws_1d.sh all 2026-06-19`、`dws_1d_to_dws_nd.sh all 2026-06-19`、`dws_1d_to_dws_td.sh all 2026-06-19`、`dws_to_ads.sh all 2026-06-19`、`hdfs_to_ods_db.sh all 2026-06-19`
> - `date only` 格式：`hdfs_to_ods_log.sh 2026-06-19`、`ads_broker_load.sh 2026-06-19`
> - DS 全局参数 `dt=$[yyyy-MM-dd-1]`，脚本中使用 `${dt}` 代换
