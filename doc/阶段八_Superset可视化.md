# 阶段八：Superset 可视化

> 目标：在 hadoop101 上部署 Apache Superset 2.0.0，连接 hadoop102 上的 Doris 进行 ADS 报表数据可视化

---

## 概述

选择 **hadoop101** 部署 Superset（非 hadoop102），理由：
- hadoop102 已运行 Doris FE+BE（~5.6GB）+ DolphinScheduler（~1GB），内存饱和
- hadoop101 运行 YARN ResourceManager，负载中等，4GB 尚有余量
- Superset Gunicorn 进程约 500MB~1GB，hadoop101 完全够用

| 组件 | 位置 | 状态 |
|---|---|---|
| Python 3.8 | Miniconda，安装在 hadoop101 | 待安装 |
| Superset 2.0.0 | hadoop101:8787 | 待部署 |
| Superset 元数据库 | hadoop100:3306（复用已有 MySQL 8.0.39） | ✅ |
| 数据源 Doris | hadoop102:9030 | ✅ |

---

## 1. 在 hadoop101 上安装 Python 环境

```bash
# 下载 Miniconda（CentOS 7 GLIBC 2.17 兼容版）
ssh hadoop101 "cd /opt/software && wget https://repo.anaconda.com/miniconda/Miniconda3-py38_23.1.0-1-Linux-x86_64.sh"

# 静默安装到 /opt/module/miniconda3
ssh hadoop101 "bash /opt/software/Miniconda3-py38_23.1.0-1-Linux-x86_64.sh -b -p /opt/module/miniconda3"

# 初始化 conda + 禁止自动激活 base
ssh hadoop101 "/opt/module/miniconda3/bin/conda init bash"
ssh hadoop101 "source ~/.bashrc && conda config --set auto_activate_base false"

# 创建 superset 环境
ssh hadoop101 "source ~/.bashrc && conda create --name superset python=3.8.16 -y"
```

---

## 2. 安装 Superset 2.0.0

```bash
# 系统依赖
ssh hadoop101 "sudo yum install -y gcc gcc-c++ libffi-devel python-devel python-pip python-wheel openssl-devel cyrus-sasl-devel openldap-devel"

# 安装 Superset
ssh hadoop101 "source ~/.bashrc && conda activate superset && pip install --upgrade pip"
ssh hadoop101 "cd && wget https://raw.githubusercontent.com/apache/superset/2.0.0/requirements/base.txt"
ssh hadoop101 "pip install apache-superset==2.0.0 -i https://pypi.tuna.tsinghua.edu.cn/simple -r base.txt"
```

---

## 3. 配置元数据库

```bash
# 在 hadoop100 上创建元数据库
mysql -h hadoop100 -u root -p -e "
CREATE DATABASE superset DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;
CREATE USER 'superset'@'%' IDENTIFIED WITH mysql_native_password BY 'superset';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, REFERENCES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER ON superset.* TO 'superset'@'%';
FLUSH PRIVILEGES;"

# 修改 Superset 配置（指向 hadoop100:3306）
vim /opt/module/miniconda3/envs/superset/lib/python3.8/site-packages/superset/config.py
# 查找并修改为以下信息
SQLALCHEMY_DATABASE_URI = 'mysql://superset:superset@hadoop100:3306/superset?charset=utf8'

# 安装 MySQL 驱动 + 初始化
ssh hadoop101 "source ~/.bashrc && conda activate superset && conda install mysqlclient -y && export FLASK_APP=superset && superset db upgrade"

# 创建管理员
ssh hadoop101 "source ~/.bashrc && conda activate superset && export FLASK_APP=superset && superset fab create-admin"
# 用户名: admin, 密码: admin123

# 初始化
ssh hadoop101 "source ~/.bashrc && conda activate superset && superset init"
```

---

## 4. 启动 Superset

```bash
# 安装 Gunicorn + 启动
ssh hadoop101 "source ~/.bashrc && conda activate superset && pip install gunicorn && gunicorn --workers 5 --timeout 120 --bind hadoop101:8787 'superset.app:create_app()' --daemon"
```

访问 `http://hadoop101:8787`，账号 admin / admin123。

---

## 5. 启停脚本

```bash
sudo vim /home/hadoop/bin/superset.sh   # 在 hadoop101 上
```

```bash
#!/bin/bash
superset_status(){
    result=`ps -ef | awk '/gunicorn/ && !/awk/{print $2}' | wc -l`
    if [[ $result -eq 0 ]]; then return 0; else return 1; fi
}
superset_start(){
    source ~/.bashrc
    superset_status >/dev/null 2>&1
    if [[ $? -eq 0 ]]; then
        conda activate superset ; gunicorn --workers 5 --timeout 120 --bind hadoop101:8787 --daemon 'superset.app:create_app()'
    else
        echo "superset正在运行"
    fi
}
superset_stop(){
    superset_status >/dev/null 2>&1
    if [[ $? -eq 0 ]]; then
        echo "superset未在运行"
    else
        ps -ef | awk '/gunicorn/ && !/awk/{print $2}' | xargs kill -9
    fi
}
case $1 in
    start   ) superset_start ;;
    stop    ) superset_stop ;;
    restart ) superset_stop ; superset_start ;;
    status  ) superset_status >/dev/null 2>&1 && echo "superset未在运行" || echo "superset正在运行" ;;
esac
```

```bash
ssh hadoop101 "chmod +x /home/hadoop/bin/superset.sh"
```

---

## 6. 连接 Doris + 导入数据集

### 6.1 安装驱动 + 添加数据库

```bash
ssh hadoop101 "source ~/.bashrc && conda activate superset && pip install mysql-connector-python"
```

登录 Superset → **Data** → **Databases** → **+ DATABASE**：

| 字段 | 值 |
|---|---|
| Database | Doris |
| SQLAlchemy URI | `mysql+mysqlconnector://root:root@hadoop102:9030/gmall?charset=utf8` |

点击 **CONNECT**，确认连接成功。

### 6.2 导入 ADS 数据集

进入 **Data** → **Datasets** → **+ DATASET**，依次添加 16 张 ADS 报表表：

| 表名 | 说明 |
|---|---|
| `gmall.ads_traffic_stats_by_channel` | 各渠道流量统计 |
| `gmall.ads_page_path` | 页面路径分析 |
| `gmall.ads_user_change` | 用户变动统计 |
| `gmall.ads_user_retention` | 用户留存率 |
| `gmall.ads_user_stats` | 用户新增活跃统计 |
| `gmall.ads_user_action` | 用户行为漏斗 |
| `gmall.ads_new_order_user_stats` | 新增下单用户 |
| `gmall.ads_order_continuously_user_count` | 连续3日下单 |
| `gmall.ads_repeat_purchase_by_tm` | 品牌复购率 |
| `gmall.ads_order_stats_by_tm` | 各品牌下单统计 |
| `gmall.ads_order_stats_by_cate` | 各品类下单统计 |
| `gmall.ads_sku_cart_num_top3_by_cate` | 购物车Top3 |
| `gmall.ads_sku_favor_count_top3_by_tm` | 收藏Top3 |
| `gmall.ads_order_to_pay_interval_avg` | 下单支付间隔 |
| `gmall.ads_order_by_province` | 各省份交易 |
| `gmall.ads_coupon_stats` | 优惠券使用 |

每张表选择 Doris 数据库后，**Table** 填写 `gmall.<表名>`，点击 **SAVE**。

---

## 7. 创建仪表盘

进入 **Dashboards** → **+ DASHBOARD**：

| 字段 | 值 |
|---|---|
| Title | 电商离线数仓看板 |

点击 **SAVE**。

---

## 8. 创建图表

进入 **Charts** → **+ CHART**，选择数据集 → 选择图表类型 → **CREATE NEW CHART**。

以 `ads_traffic_stats_by_channel` 为例：

- **Dataset**：`gmall.ads_traffic_stats_by_channel`
- **Chart Type**：Bar Chart
- **Metrics**：`uv_count`、`sv_count`
- **Filters**：`dt = '2026-06-18'`（测试用，实际看板可不设）
- **Series**：`channel`
- 配置完成后 **SAVE** → **ADD TO DASHBOARD** → 选择"电商离线数仓看板"

其他表的图表创建同理，全部添加到同一个仪表盘即可。

---

## 9. 阶段验证清单

| # | 验证项 | 操作 | 预期 |
|---|--------|------|------|
| 1 | Python 环境 | `ssh hadoop101 "source ~/.bashrc && conda activate superset && python -V"` | Python 3.8.16 |
| 2 | Superset 启动 | `ssh hadoop101 "superset.sh start"` | 无报错 |
| 3 | UI 可访问 | `http://hadoop101:8787` | 登录页 |
| 4 | Doris 连接 | Test Connection | "Connection looks good!" |
