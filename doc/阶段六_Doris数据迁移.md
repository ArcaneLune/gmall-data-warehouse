# 阶段六：Doris 部署 + ADS 数据迁移

> 目标：单机部署 Apache Doris 2.0.3，将 Hive ADS 层 16 张报表迁移至 Doris，为看板可视化做准备

---

## 概述

ADS 层报表数据量小（百~万行级），但需支持低延迟交互查询和看板可视化。Hive on Spark 走 MR/Spark 批处理，查询延迟高（秒~分钟级），不适合直接接入 BI 看板。因此选择 Apache Doris（OLAP 数据库）作为 ADS 层的查询加速层。

Doris 兼容 MySQL 协议，BI 工具（如 Superset、DataEase）可通过标准 MySQL JDBC 直连，查询延迟低至毫秒级。

---

## 1. 版本选择与部署规划

### 1.1 为什么选 Doris 2.0.3

| 候选版本 | 说明 |
|---|---|
| Doris 1.2.7 | 最后一个 1.x 版本，极轻量，但已停止主line维护 |
| Doris 2.0.3 | 2.0.x 最新稳定版，Compute-Storage 分离架构成熟，倒排索引/半结构化等新特性，社区活跃 |
| Doris 2.1.x | 更新，但对系统资源要求更高，单机 4G 部署不够 |

**结论：Apache Doris 2.0.3**。低版本不推荐（功能少/已停维），高版本吃资源，2.0.3 在功能性和资源占用之间取最佳平衡点。

### 1.2 单机部署架构

集群共 3 台 VM，hadoop102 已升级到 8GB（原 4GB），其余两台仍为 4GB。hadoop100 和 hadoop101 负载较重（NN/RM/Hive/Spark/Maxwell），只有 hadoop102 相对空闲（仅 SNN + Flume 消费端）。因此将 Doris 单机部署在 **hadoop102**：

```
┌─────────────────────────────────────┐
│ hadoop102 (192.168.100.132, 8GB)    │
│                                     │
│  ┌──────────┐    ┌─────────────┐    │
│  │ Doris FE │    │  Doris BE   │    │
│  │ (Frontend)│    │ (Backend)   │    │
│  │ 端口:9030 │    │ 端口:9060   │    │
│  │ 堆内存:2G │    │ 堆内存:2G   │    │
│  │ HTTP:8030│    │ 内存上限:45% │    │
│  └──────────┘    └─────────────┘    │
│                                     │
│  BI 工具通过 MySQL 协议连接 FE:9030 │
└─────────────────────────────────────┘
```

| 组件 | 节点 | 内存 | 端口 | 说明 |
|---|---|---|---|---|
| FE (Frontend) | hadoop102 | 堆 2GB | 9030/8030 | 元数据管理、查询解析、MySQL协议入口 |
| BE (Backend) | hadoop102 | 堆 2GB + mem_limit 45%(≈3.6GB) | 9060/8040 | 数据存储、查询执行 |

> hadoop102 总内存 8GB：OS ~1GB + SNN/Flume ~1GB + FE 堆 2GB + BE ~3.6GB ≈ 7.6GB，留 0.4GB 缓冲。

---

## 2. 环境准备

### 2.1 系统限制调优

```bash
# 在 hadoop102 上执行
sudo vim /etc/security/limits.conf
```

追加：

```
hadoop  soft    nofile  65536
hadoop  hard    nofile  65536
hadoop  soft    nproc   65536
hadoop  hard    nproc   65536
```

生效（注意：limits.conf 仅对新登录会话生效，当前终端需手动执行 ulimit）：

```bash
# 当前终端立即生效（Doris BE 要求 ≥ 60000）
ulimit -n 65536
ulimit -u 65536

# 验证
ulimit -n
```

### 2.2 关闭 swap（Doris 强烈建议）

```bash
# 临时关闭
sudo swapoff -a

# 永久关闭
sudo sed -i '/swap/s/^/#/' /etc/fstab
```

### 2.3 设置 vm.max_map_count（Doris BE 必需）

```bash
# 临时生效
sudo sysctl -w vm.max_map_count=2000000

# 永久生效
echo 'vm.max_map_count=2000000' | sudo tee -a /etc/sysctl.conf
```

> Doris BE 使用 mmap 映射大量文件，默认值 65530 不够，BE 启动时会直接报错 `Please set vm.max_map_count to be 2000000`。

### 2.4 禁用 THP（透明大页）

```bash
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
```

---

## 3. 下载并解压 Doris 2.0.3

```bash
# 在 hadoop102 上以 hadoop 用户操作
cd /opt/software/

# 下载二进制包（x64, 无avx2 版本兼容旧CPU）
wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-2.0.3-bin-x64.tar.gz

# 解压
tar -zxvf apache-doris-2.0.3-bin-x64.tar.gz -C /opt/module/

# 重命名
mv /opt/module/apache-doris-2.0.3-bin-x64 /opt/module/doris
```

### 3.1 配置环境变量

```bash
sudo vim /etc/profile.d/doris.sh
```

```bash
# DORIS_HOME
export DORIS_HOME=/opt/module/doris
export PATH=$PATH:$DORIS_HOME/fe/bin:$DORIS_HOME/be/bin
```

```bash
source /etc/profile.d/doris.sh
```

---

## 4. FE（Frontend）部署

### 4.1 配置 fe.conf

```bash
vim /opt/module/doris/fe/conf/fe.conf
```

默认配置文件关键字段及优化（适配 4GB 单机）：

```properties
CUR_DATE=`date +%Y%m%d-%H%M%S`

# ===== 日志 =====
LOG_DIR = ${DORIS_HOME}/log
sys_log_level = INFO
sys_log_mode = NORMAL

# ===== JVM 堆内存：hadoop102 升级到 8GB，FE 调至 2G =====
# ⚠️ 必须写在一行内！start_fe.sh 用 eval 解析配置，换行会报"语法错误: 未预期的文件结尾"
JAVA_OPTS="-Djavax.security.auth.useSubjectCredsOnly=false -Xss2m -Xmx2048m -Xms2048m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:$DORIS_HOME/log/fe.gc.log.$CUR_DATE"

# JDK 9+ 使用 JAVA_OPTS_FOR_JDK_9（本项目用 JDK 8，此段保留不动，同样写在一行）
JAVA_OPTS_FOR_JDK_9="-Djavax.security.auth.useSubjectCredsOnly=false -Xss2m -Xmx2048m -Xms2048m -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xlog:gc*:$DORIS_HOME/log/fe.gc.log.$CUR_DATE:time"

# ===== 元数据目录（取消注释）=====
meta_dir = ${DORIS_HOME}/doris-meta

# ===== 网络端口 =====
http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010

# ===== 单机部署优化 =====
# FE 最大连接数（降低资源消耗）
qe_max_connection = 256
```

> **改动说明**：
> - `Xmx8192m → 2048m`：默认 8G 堆，hadoop102 8GB 下调至 2G（FE 堆）
> - `CMS → G1GC`：CMS 在低内存下容易触发 Concurrent Mode Failure，G1 更平滑
> - `Xss4m → 2m`：减少线程栈占用
> - `default_replication_num = 1`：单节点不需要三副本
> - 其余端口、日志路径保持默认即可

### 4.2 启动 FE

```bash
cd /opt/module/doris/fe/bin
./start_fe.sh --daemon
```

验证（等待约 30 秒）：

```bash
curl http://hadoop102:8030/api/bootstrap
```

返回 `{"status":"OK","msg":"Success"}` 即启动成功。

### 4.3 通过 MySQL 客户端连接 FE

> Doris FE 的 9030 端口实现了 MySQL 网络协议，使用标准 MySQL 客户端即可连接。
> MySQL 客户端安装在 hadoop100（与 MySQL 8.0.39 一同），因此从 hadoop100 远程连接 hadoop102 上的 Doris FE。

```bash
# 在 hadoop100 上执行
mysql -h hadoop102 -P 9030 -u root
```

首次登录无密码，进入后设置密码：

```sql
SET PASSWORD FOR 'root' = PASSWORD('root');
```

创建 gmall 数据库：

```sql
CREATE DATABASE gmall;
SHOW DATABASES;
```

---

## 5. BE（Backend）部署

### 5.1 配置 be.conf

```bash
vim /opt/module/doris/be/conf/be.conf
```

默认配置文件及优化（适配 4GB 单机部署）：

```properties
CUR_DATE=`date +%Y%m%d-%H%M%S`

PPROF_TMPDIR="$DORIS_HOME/log/"

# ===== JVM 堆内存：hadoop102 升级到 8GB，BE 调至 2G =====
JAVA_OPTS="-Xmx2048m -DlogPath=$DORIS_HOME/log/jni.log -Xloggc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000"

# JDK 9+（本项目用 JDK 8，保留不动）
JAVA_OPTS_FOR_JDK_9="-Xmx2048m -DlogPath=$DORIS_HOME/log/jni.log -Xlog:gc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000"

# ===== JDK 路径（取消注释，指定实际路径）=====
JAVA_HOME=/opt/module/jdk-1.8.0

# ===== jemalloc 内存分配器（默认配置，不动）=====
JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:15000,dirty_decay_ms:15000,oversize_threshold:0,lg_tcache_max:20,prof:false,lg_prof_interval:32,lg_prof_sample:19,prof_gdump:false,prof_accum:false,prof_leak:false,prof_final:false"
JEMALLOC_PROF_PRFIX=""

# ===== 日志 =====
sys_log_level = INFO

# ===== 网络端口（默认值，不动）=====
be_port = 9060
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060

# HTTPS 关闭
enable_https = false
ssl_certificate_path = "$DORIS_HOME/conf/cert.pem"
ssl_private_key_path = "$DORIS_HOME/conf/key.pem"

# 关闭 BE 端认证
enable_auth = false

# ===== 以下为手动添加的 4GB 优化参数（默认文件中不存在）=====

# 存储目录
storage_root_path = ${DORIS_HOME}/storage

# 内存限制：hadoop102 升级到 8GB，45% ≈ 3.6GB（OS + SNN + Flume 留足余量）
mem_limit = 45%

# 单 Tablet 写入 buffer 从默认 200MB 降至 64MB
write_buffer_size = 67108864
```

> **与默认文件的差异**：
> - `JAVA_HOME` 取消注释，指向 JDK 安装路径
> - 末尾追加 `storage_root_path` / `mem_limit` / `write_buffer_size` 三个参数，**默认文件不含这些**

> `mem_limit = 45%` 表示 BE 最多使用系统内存的 45%（8GB × 45% ≈ 3.6GB），给 OS + SNN + Flume + DS 留足余量。

### 5.2 启动 BE

```bash
cd /opt/module/doris/be/bin
./start_be.sh --daemon
```

### 5.3 将 BE 加入 FE 集群

通过 MySQL 客户端连接到 **Doris FE**（端口 9030），执行 Doris SQL：

```bash
# 在 hadoop100 上执行（mysql 客户端装在 hadoop100）
mysql -h hadoop102 -P 9030 -u root -proot
```

进入 Doris 命令行后：

```sql
ALTER SYSTEM ADD BACKEND "hadoop102:9050";
```

查看 BE 状态：

```sql
SHOW BACKENDS\G
```

等待 `Alive` 变为 `true` 且 `SystemDecommissioned` 为 `false`。

> 如果 `Alive` 一直为 `false`，查看 BE 日志：
> ```bash
> tail -100 /opt/module/doris/be/log/be.WARNING
> ```

---

## 6. Doris 中创建 ADS 报表表

> ADS 层 16 张表均需在 Doris 中建立结构一致的副本。Doris 表模型选择：
> - **明细模型（DUPLICATE KEY）**：用于不需要聚合的报表，如路径分析、漏斗分析、Top3 排名
> - **聚合模型（AGGREGATE KEY）**：用于需要聚合的报表（本项目 ADS 层已在 Hive 侧完成聚合，用 DUPLICATE 即可）
> - 所有表使用 `DUPLICATE KEY` 模型，以 `dt` 作为排序键首列

### 6.1 流量主题（2张）

```sql
-- 各渠道流量统计
CREATE TABLE IF NOT EXISTS gmall.ads_traffic_stats_by_channel (
    dt               VARCHAR(10)   COMMENT '统计日期',
    recent_days      BIGINT        COMMENT '最近天数',
    channel          VARCHAR(32)   COMMENT '渠道',
    uv_count         BIGINT        COMMENT '访客人数',
    avg_duration_sec BIGINT        COMMENT '会话平均停留时长',
    avg_page_count   BIGINT        COMMENT '会话平均浏览页面数',
    sv_count         BIGINT        COMMENT '会话数',
    bounce_rate      DECIMAL(16,2) COMMENT '跳出率'
) ENGINE=OLAP
DUPLICATE KEY(dt, recent_days, channel)
COMMENT '各渠道流量统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 页面路径分析
CREATE TABLE IF NOT EXISTS gmall.ads_page_path (
    dt          VARCHAR(10) COMMENT '统计日期',
    source      VARCHAR(255) COMMENT '跳转起始页面ID',
    target      VARCHAR(255) COMMENT '跳转终到页面ID',
    path_count  BIGINT      COMMENT '跳转次数'
) ENGINE=OLAP
DUPLICATE KEY(dt, source, target)
COMMENT '页面浏览路径分析'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
```

### 6.2 用户主题（6张）

```sql
-- 用户变动统计
CREATE TABLE IF NOT EXISTS gmall.ads_user_change (
    dt               VARCHAR(10) COMMENT '统计日期',
    user_churn_count BIGINT      COMMENT '流失用户数',
    user_back_count  BIGINT      COMMENT '回流用户数'
) ENGINE=OLAP
DUPLICATE KEY(dt)
COMMENT '用户变动统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 用户留存率
CREATE TABLE IF NOT EXISTS gmall.ads_user_retention (
    dt              VARCHAR(10)   COMMENT '统计日期',
    create_date     VARCHAR(10)   COMMENT '用户新增日期',
    retention_day   INT           COMMENT '截至当前日期留存天数',
    retention_count BIGINT        COMMENT '留存用户数量',
    new_user_count  BIGINT        COMMENT '新增用户数量',
    retention_rate  DECIMAL(16,2) COMMENT '留存率'
) ENGINE=OLAP
DUPLICATE KEY(dt, create_date, retention_day)
COMMENT '用户留存率'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 用户新增活跃统计
CREATE TABLE IF NOT EXISTS gmall.ads_user_stats (
    dt                VARCHAR(10) COMMENT '统计日期',
    recent_days       BIGINT      COMMENT '最近n日',
    new_user_count    BIGINT      COMMENT '新增用户数',
    active_user_count BIGINT      COMMENT '活跃用户数'
) ENGINE=OLAP
DUPLICATE KEY(dt, recent_days)
COMMENT '用户新增活跃统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 用户行为漏斗分析
CREATE TABLE IF NOT EXISTS gmall.ads_user_action (
    dt                VARCHAR(10) COMMENT '统计日期',
    home_count        BIGINT      COMMENT '浏览首页人数',
    good_detail_count BIGINT      COMMENT '浏览商品详情页人数',
    cart_count        BIGINT      COMMENT '加购人数',
    order_count       BIGINT      COMMENT '下单人数',
    payment_count     BIGINT      COMMENT '支付人数'
) ENGINE=OLAP
DUPLICATE KEY(dt)
COMMENT '用户行为漏斗分析'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 新增下单用户统计
CREATE TABLE IF NOT EXISTS gmall.ads_new_order_user_stats (
    dt                   VARCHAR(10) COMMENT '统计日期',
    recent_days          BIGINT      COMMENT '最近天数',
    new_order_user_count BIGINT      COMMENT '新增下单人数'
) ENGINE=OLAP
DUPLICATE KEY(dt, recent_days)
COMMENT '新增下单用户统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 最近7日内连续3日下单用户数
CREATE TABLE IF NOT EXISTS gmall.ads_order_continuously_user_count (
    dt                            VARCHAR(10) COMMENT '统计日期',
    recent_days                   BIGINT      COMMENT '最近天数',
    order_continuously_user_count BIGINT      COMMENT '连续3日下单用户数'
) ENGINE=OLAP
DUPLICATE KEY(dt, recent_days)
COMMENT '最近7日内连续3日下单用户数统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
```

### 6.3 商品主题（5张）

```sql
-- 最近30日各品牌复购率
CREATE TABLE IF NOT EXISTS gmall.ads_repeat_purchase_by_tm (
    dt                VARCHAR(10)   COMMENT '统计日期',
    recent_days       BIGINT        COMMENT '最近天数',
    tm_id             VARCHAR(32)   COMMENT '品牌ID',
    tm_name           VARCHAR(128)  COMMENT '品牌名称',
    order_repeat_rate DECIMAL(16,2) COMMENT '复购率'
) ENGINE=OLAP
DUPLICATE KEY(dt, recent_days, tm_id)
COMMENT '最近30日各品牌复购率统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 各品牌商品下单统计
CREATE TABLE IF NOT EXISTS gmall.ads_order_stats_by_tm (
    dt               VARCHAR(10)  COMMENT '统计日期',
    recent_days      BIGINT       COMMENT '最近天数',
    tm_id            VARCHAR(32)  COMMENT '品牌ID',
    tm_name          VARCHAR(128) COMMENT '品牌名称',
    order_count      BIGINT       COMMENT '下单数',
    order_user_count BIGINT       COMMENT '下单人数'
) ENGINE=OLAP
DUPLICATE KEY(dt, recent_days, tm_id)
COMMENT '各品牌商品下单统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 各品类商品下单统计
CREATE TABLE IF NOT EXISTS gmall.ads_order_stats_by_cate (
    dt               VARCHAR(10)  COMMENT '统计日期',
    recent_days      BIGINT       COMMENT '最近天数',
    category1_id     VARCHAR(32)  COMMENT '一级品类ID',
    category1_name   VARCHAR(128) COMMENT '一级品类名称',
    category2_id     VARCHAR(32)  COMMENT '二级品类ID',
    category2_name   VARCHAR(128) COMMENT '二级品类名称',
    category3_id     VARCHAR(32)  COMMENT '三级品类ID',
    category3_name   VARCHAR(128) COMMENT '三级品类名称',
    order_count      BIGINT       COMMENT '下单数',
    order_user_count BIGINT       COMMENT '下单人数'
) ENGINE=OLAP
DUPLICATE KEY(dt)
COMMENT '各品类商品下单统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 各品类商品购物车存量Top3
CREATE TABLE IF NOT EXISTS gmall.ads_sku_cart_num_top3_by_cate (
    dt             VARCHAR(10)  COMMENT '统计日期',
    category1_id   VARCHAR(32)  COMMENT '一级品类ID',
    category1_name VARCHAR(128) COMMENT '一级品类名称',
    category2_id   VARCHAR(32)  COMMENT '二级品类ID',
    category2_name VARCHAR(128) COMMENT '二级品类名称',
    category3_id   VARCHAR(32)  COMMENT '三级品类ID',
    category3_name VARCHAR(128) COMMENT '三级品类名称',
    sku_id         VARCHAR(32)  COMMENT 'SKU_ID',
    sku_name       VARCHAR(256) COMMENT 'SKU名称',
    cart_num       BIGINT       COMMENT '购物车中商品数量',
    rk             BIGINT       COMMENT '排名'
) ENGINE=OLAP
DUPLICATE KEY(dt)
COMMENT '各品类商品购物车存量Top3'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 各品牌商品收藏次数Top3
CREATE TABLE IF NOT EXISTS gmall.ads_sku_favor_count_top3_by_tm (
    dt          VARCHAR(10)  COMMENT '统计日期',
    tm_id       VARCHAR(32)  COMMENT '品牌ID',
    tm_name     VARCHAR(128) COMMENT '品牌名称',
    sku_id      VARCHAR(32)  COMMENT 'SKU_ID',
    sku_name    VARCHAR(256) COMMENT 'SKU名称',
    favor_count BIGINT       COMMENT '被收藏次数',
    rk          BIGINT       COMMENT '排名'
) ENGINE=OLAP
DUPLICATE KEY(dt)
COMMENT '各品牌商品收藏次数Top3'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
```

### 6.4 交易主题（2张）

```sql
-- 下单到支付时间间隔平均值
CREATE TABLE IF NOT EXISTS gmall.ads_order_to_pay_interval_avg (
    dt                        VARCHAR(10) COMMENT '统计日期',
    order_to_pay_interval_avg BIGINT      COMMENT '下单到支付时间间隔平均值,秒'
) ENGINE=OLAP
DUPLICATE KEY(dt)
COMMENT '下单到支付时间间隔平均值统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 各省份交易统计
CREATE TABLE IF NOT EXISTS gmall.ads_order_by_province (
    dt                 VARCHAR(10)   COMMENT '统计日期',
    recent_days        BIGINT        COMMENT '最近天数',
    province_id        VARCHAR(32)   COMMENT '省份ID',
    province_name      VARCHAR(128)  COMMENT '省份名称',
    area_code          VARCHAR(32)   COMMENT '地区编码',
    iso_code           VARCHAR(32)   COMMENT '旧版ISO编码',
    iso_code_3166_2    VARCHAR(32)   COMMENT '新版ISO编码',
    order_count        BIGINT        COMMENT '订单数',
    order_total_amount DECIMAL(16,2) COMMENT '订单金额'
) ENGINE=OLAP
DUPLICATE KEY(dt, recent_days, province_id)
COMMENT '各省份交易统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
```

### 6.5 优惠券主题（1张）

```sql
-- 优惠券使用统计
CREATE TABLE IF NOT EXISTS gmall.ads_coupon_stats (
    dt              VARCHAR(10)  COMMENT '统计日期',
    coupon_id       VARCHAR(32)  COMMENT '优惠券ID',
    coupon_name     VARCHAR(128) COMMENT '优惠券名称',
    used_count      BIGINT       COMMENT '使用次数',
    used_user_count BIGINT       COMMENT '使用人数'
) ENGINE=OLAP
DUPLICATE KEY(dt, coupon_id)
COMMENT '优惠券使用统计'
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
```

---


## 7. 数据迁移方案选型

Doris 支持多种方式接入 Hive 数据，本项目的 ADS 报表数据量小（日增 < 10 万行），同时 4GB VM 磁盘空间紧张。以下对比三种主流方案：

| 维度 | Multi-Catalog | Broker Load | Stream Load（原方案） |
|---|---|---|---|
| 原理 | 建 Hive 外部 Catalog，Doris 实时查 HDFS | Doris BE 从 HDFS 批量拉取数据文件灌入本地存储 | curl HTTP 推送本地 TSV 文件到 BE |
| 数据存储 | **不存**，零冗余 | 存一份 Doris 副本 | 存一份 Doris 副本 |
| 查询延迟 | 依赖 Hive/HDFS，百毫秒~秒级 | **毫秒级** | **毫秒级** |
| Hive 依赖 | 必须在线 | 导入后可离线 | 导入后可离线 |
| 运维复杂度 | 一条 SQL 建 Catalog，零维护 | 需配置 Broker + 定时 LOAD | 需先导出到本地再 curl |
| 生产推荐度 | 轻量验证 / 临时查询 | **🥇 中大厂标配** | 🥉 小数据兜底 |
| 磁盘占用 | 无 | 少量（ADS 数据 < 100MB） | 少量 |

**本项目采用策略：Multi-Catalog 用于快速验证（建完即查），Broker Load 作为正式调度导入方案（对齐生产环境）。**

---

## 8. 方案一：Multi-Catalog 联邦查询（快速验证）

> 适用场景：建完 Doris FE/BE 后，一条 SQL 即可查询 Hive ADS 数据，无需任何导入流程。

### 8.1 创建 Hive Catalog

通过 MySQL 客户端连接 Doris FE：

```sql
mysql -h hadoop102 -P 9030 -u root -proot
```

创建指向 Hive MetaStore 的 Catalog：

```sql
CREATE CATALOG hive_catalog PROPERTIES (
    "type" = "hms",
    "hive.metastore.uris" = "thrift://hadoop100:9083"
);
```

> `hadoop100:9083` 是 Hive MetaStore 的 Thrift 端口，确认 Hive Metastore 服务已启动：
> ```bash
> ssh hadoop100 "ps aux | grep metastore | grep -v grep"
> ```

### 8.2 查询验证

```sql
-- 切换 Catalog
SWITCH hive_catalog;

-- 查看 Hive 库
SHOW DATABASES;

-- 直查 Hive ADS 表
SELECT * FROM gmall.ads_traffic_stats_by_channel WHERE dt='2026-06-18' LIMIT 10;
SELECT COUNT(*) FROM gmall.ads_user_action WHERE dt='2026-06-18';
SELECT * FROM gmall.ads_order_by_province WHERE dt='2026-06-18';
```

### 8.3 在 Doris 内部库中创建视图（可选）

如果 BI 工具需要固定连接某个 Doris 库，可以在 Doris 内部库中创建视图指向 Hive 表：

```sql
-- 回到 Doris 内部库
SWITCH internal;
USE gmall;

-- 创建视图（注意：Doris 2.0 视图跨 Catalog 引用需使用 catalog.database.table 格式）
CREATE VIEW ads_traffic_stats_by_channel_v AS
SELECT * FROM hive_catalog.gmall.ads_traffic_stats_by_channel;
```

> **局限**：视图方式不支持复杂聚合下推，大查询会退化为全表扫描再在 Doris 侧过滤。建议仅用于小数据量场景或临时验证。

---

## 9. 方案二：Broker Load 批量导入（生产方案）

> 适用场景：每日定时将 ADS 数据导入 Doris 本地存储，查询性能最优，适合接入 BI 看板。

### 9.1 原理

```
Hive ADS 表 (HDFS 文件)
        │
        │ Broker Load 命令 (Doris FE 发起)
        ▼
   Doris BE ──直接读取 HDFS──→ 解析 ORC/TSV → 写入 Doris 本地 Tablet
```

Broker Load 是异步导入：FE 下发任务后，每个 BE 直接从 HDFS 并行拉取数据文件，不经过中间 JVM 转发，吞吐可达 **37-42 万行/秒**。

### 9.2 前置条件

**① 确认 Hive ADS 表文件路径**

```bash
hive -e "DESC FORMATTED gmall.ads_traffic_stats_by_channel" 2>/dev/null | grep "Location:"
# 输出示例：hdfs://hadoop100:8020/warehouse/gmall/ads/ads_traffic_stats_by_channel
```

**② Doris 2.0 起 BE 内置 HDFS 支持，使用 `WITH HDFS` 语法，无需额外部署 Broker 进程。**

### 9.3 逐表 Broker Load

连接 Doris FE 后执行 `LOAD LABEL` 语句。ADS 表在 Hive 侧存储为 TSV（`\t` 分隔），Doris 侧对应解析。

```sql
-- 1. 各渠道流量统计
LOAD LABEL gmall.ads_traffic_stats_by_channel_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_traffic_stats_by_channel/*")
    INTO TABLE ads_traffic_stats_by_channel
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES (
    "timeout" = "300",
    "max_filter_ratio" = "0.1"
);

-- 2. 页面路径分析
LOAD LABEL gmall.ads_page_path_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_page_path/*")
    INTO TABLE ads_page_path
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 3. 用户变动统计
LOAD LABEL gmall.ads_user_change_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_user_change/*")
    INTO TABLE ads_user_change
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 4. 用户留存率
LOAD LABEL gmall.ads_user_retention_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_user_retention/*")
    INTO TABLE ads_user_retention
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 5. 用户新增活跃统计
LOAD LABEL gmall.ads_user_stats_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_user_stats/*")
    INTO TABLE ads_user_stats
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 6. 漏斗分析
LOAD LABEL gmall.ads_user_action_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_user_action/*")
    INTO TABLE ads_user_action
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 7. 新增下单用户
LOAD LABEL gmall.ads_new_order_user_stats_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_new_order_user_stats/*")
    INTO TABLE ads_new_order_user_stats
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 8. 连续3日下单
LOAD LABEL gmall.ads_order_continuously_user_count_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_order_continuously_user_count/*")
    INTO TABLE ads_order_continuously_user_count
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 9. 品牌复购率
LOAD LABEL gmall.ads_repeat_purchase_by_tm_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_repeat_purchase_by_tm/*")
    INTO TABLE ads_repeat_purchase_by_tm
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 10. 各品牌下单统计
LOAD LABEL gmall.ads_order_stats_by_tm_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_order_stats_by_tm/*")
    INTO TABLE ads_order_stats_by_tm
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 11. 各品类下单统计
LOAD LABEL gmall.ads_order_stats_by_cate_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_order_stats_by_cate/*")
    INTO TABLE ads_order_stats_by_cate
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 12. 购物车Top3
LOAD LABEL gmall.ads_sku_cart_num_top3_by_cate_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/*")
    INTO TABLE ads_sku_cart_num_top3_by_cate
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 13. 收藏Top3
LOAD LABEL gmall.ads_sku_favor_count_top3_by_tm_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_sku_favor_count_top3_by_tm/*")
    INTO TABLE ads_sku_favor_count_top3_by_tm
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 14. 下单到支付间隔
LOAD LABEL gmall.ads_order_to_pay_interval_avg_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_order_to_pay_interval_avg/*")
    INTO TABLE ads_order_to_pay_interval_avg
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 15. 各省份交易统计
LOAD LABEL gmall.ads_order_by_province_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_order_by_province/*")
    INTO TABLE ads_order_by_province
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");

-- 16. 优惠券使用统计
LOAD LABEL gmall.ads_coupon_stats_20260618
(
    DATA INFILE("hdfs://hadoop100:8020/warehouse/gmall/ads/ads_coupon_stats/*")
    INTO TABLE ads_coupon_stats
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "csv"
)
WITH HDFS
(
    "fs.defaultFS" = "hdfs://hadoop100:8020",
    "hadoop.username" = "hadoop"
)
PROPERTIES ("timeout" = "300", "max_filter_ratio" = "0.1");
```

### 9.4 查看导入状态

```sql
-- 按时间倒序查看最近的任务
SHOW LOAD FROM gmall ORDER BY createtime DESC LIMIT 16\G

-- 查看单个任务详情
SHOW LOAD FROM gmall WHERE label = 'gmall.ads_coupon_stats_20260618'\G
```

状态字段关注：
- `State`: `FINISHED` 表示成功，`CANCELLED` 表示失败
- `LoadBytes`: 实际导入的字节数
- `LoadRows`: 实际导入的行数

### 9.5 Broker Load 调度脚本

```bash
sudo vim /home/hadoop/bin/ads_broker_load.sh
```

```bash
#!/bin/bash
# 功能：通过 Broker Load 将 Hive ADS 层 16 张表导入 Doris
# 用法: ads_broker_load.sh 2026-06-18
# 注意：此脚本通过 mysql 客户端连接 Doris FE 执行 LOAD LABEL

DORIS_HOST="hadoop102"
DORIS_PORT="9030"
DORIS_USER="root"
DORIS_PASS="root"
HDFS_PREFIX="hdfs://hadoop100:8020/warehouse/gmall/ads"

if [ -n "$1" ]; then
    do_date=$1
else
    do_date=`date -d "-1 day" +%F`
fi

# 表清单
TABLES=(
    "ads_traffic_stats_by_channel"
    "ads_page_path"
    "ads_user_change"
    "ads_user_retention"
    "ads_user_stats"
    "ads_user_action"
    "ads_new_order_user_stats"
    "ads_order_continuously_user_count"
    "ads_repeat_purchase_by_tm"
    "ads_order_stats_by_tm"
    "ads_order_stats_by_cate"
    "ads_sku_cart_num_top3_by_cate"
    "ads_sku_favor_count_top3_by_tm"
    "ads_order_to_pay_interval_avg"
    "ads_order_by_province"
    "ads_coupon_stats"
)

for table in "${TABLES[@]}"; do
    label="${table}_${do_date//-/}"
    echo -n "[${table}] "

    result=$(mysql -h ${DORIS_HOST} -P ${DORIS_PORT} -u ${DORIS_USER} -p${DORIS_PASS} -e "
    LOAD LABEL gmall.${label}
    (
        DATA INFILE('${HDFS_PREFIX}/${table}/*')
        INTO TABLE ${table}
        COLUMNS TERMINATED BY '\t'
        FORMAT AS 'csv'
    )
    WITH HDFS
    (
        'fs.defaultFS' = 'hdfs://hadoop100:8020',
        'hadoop.username' = 'hadoop'
    )
    PROPERTIES (
        'timeout' = '300',
        'max_filter_ratio' = '0.1'
    );" 2>&1)

    if echo "$result" | grep -q "success"; then
        echo "OK (label: ${label})"
    else
        echo "FAILED: $result"
    fi
done

echo "===== Broker Load 全部提交完成 ====="
echo "查看状态: mysql -h ${DORIS_HOST} -P ${DORIS_PORT} -u ${DORIS_USER} -p -e 'SHOW LOAD FROM gmall ORDER BY createtime DESC LIMIT 16\G'"
```

```bash
sudo chmod +x /home/hadoop/bin/ads_broker_load.sh
# 用法: ads_broker_load.sh 2026-06-18
```

---

## 10. 两种方案对比总结

| | Multi-Catalog 联邦查询 | Broker Load 批量导入 |
|---|---|---|
| **首次可用时间** | 建 Catalog 后立即可用（< 1 分钟） | 需表结构就绪 + 首次 LOAD（~5 分钟） |
| **查询性能** | 百毫秒~秒级 | **毫秒级** |
| **Hive 依赖** | 强依赖，Hive 挂了 Doris 也查不了 | 弱依赖，仅导入时需要 Hadoop 集群在线 |
| **磁盘占用** | 无 | ADS 16 表 < 100MB |
| **生产对齐度** | 验证/临时查询 | **✅ 中大厂生产标配** |
| **推荐使用方式** | Doris 部署完成后立即验证数据可查 | 每日 `dws_to_ads.sh` 跑完后调度执行 |

> **建议**：Doris 安装完成后先用 Multi-Catalog 快速验证所有 ADS 表数据是否正确可见，确认无误后切换到 Broker Load 作为正式每日导入方案。

---

## 11. 验证

### 11.1 Multi-Catalog 验证

```sql
-- Doris 内执行
SWITCH hive_catalog;
SELECT 'ads_traffic_stats_by_channel', COUNT(*) FROM gmall.ads_traffic_stats_by_channel WHERE dt='2026-06-18' UNION ALL
SELECT 'ads_user_action', COUNT(*) FROM gmall.ads_user_action WHERE dt='2026-06-18' UNION ALL
SELECT 'ads_order_by_province', COUNT(*) FROM gmall.ads_order_by_province WHERE dt='2026-06-18';
-- ... 16 表全查
```

### 11.2 Broker Load 验证

```bash
# 提交导入
ads_broker_load.sh 2026-06-18

# 检查状态（等待 ~30 秒）
mysql -h hadoop102 -P 9030 -u root -proot -e "SHOW LOAD FROM gmall ORDER BY createtime DESC LIMIT 16\G" | grep State

# 对比 Hive 与 Doris 行数
mysql -h hadoop102 -P 9030 -u root -proot -e "
SELECT 'ads_coupon_stats', COUNT(*) FROM gmall.ads_coupon_stats UNION ALL
SELECT 'ads_order_by_province', COUNT(*) FROM gmall.ads_order_by_province UNION ALL
SELECT 'ads_user_action', COUNT(*) FROM gmall.ads_user_action;
"

hive -e "SELECT 'ads_coupon_stats', COUNT(*) FROM gmall.ads_coupon_stats WHERE dt='2026-06-18' UNION ALL
SELECT 'ads_order_by_province', COUNT(*) FROM gmall.ads_order_by_province WHERE dt='2026-06-18' UNION ALL
SELECT 'ads_user_action', COUNT(*) FROM gmall.ads_user_action WHERE dt='2026-06-18';"
```

---

## 12. 阶段验证清单

| # | 验证项 | 操作 | 预期 |
|---|--------|------|------|
| 1 | Doris FE 启动 | `curl http://hadoop102:8030/api/bootstrap` | `"status":"OK"` |
| 2 | Doris BE Alive | `SHOW BACKENDS\G` | `Alive: true` |
| 3 | gmall 库存在 | `SHOW DATABASES` | 包含 gmall |
| 4 | 16 张 ADS 表存在 | `SHOW TABLES IN gmall` | 16 rows |
| 5 | Multi-Catalog 可用 | `SWITCH hive_catalog; SELECT COUNT(*) FROM gmall.ads_user_action` | 返回行数 > 0 |
| 6 | Broker Load 提交成功 | `ads_broker_load.sh 2026-06-18` | 16 表全部 `OK` |
| 7 | Broker Load 状态 | `SHOW LOAD FROM gmall` | 全部 `FINISHED` |
| 8 | Hive 与 Doris 行数一致 | 对比验证 SQL | 16 表全部一致 |
