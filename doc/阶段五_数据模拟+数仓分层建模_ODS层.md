# 阶段五：数据模拟 + 数仓分层建模（ODS层）

> 对应文档：《电商数据仓库系统》V6.0 第6-7章
> 目标：模拟历史数据 → 构建ODS层30张表 → ODS数据装载

---

## 概述

本阶段正式开始数仓建模。先模拟4天历史数据（2026-06-18至06-21），再构建ODS（贴源层）30张表，完成数据装载。

**流程**：

```
清理HDFS → 生成4天历史数据 → 生成第5天正式数据 → 全量同步 → 增量首日全量
→ ODS建表(1张日志表 + 17张全量表 + 12张增量表) → 数据装载到ODS
```

---

## 第一部分：模拟历史数据准备

> 假定数仓上线日期为 **2026-06-18**，需模拟业务历史数据（用户行为日志无历史数据，只需当天）。

### 1.1 清理 HDFS 旧数据

```bash
hadoop fs -rm -r -f /origin_data/gmall
```

### 1.2 启动采集通道

```bash
# 启动所有组件
cluster.sh start

# 停止 Maxwell（数据生成时不需要）
mxw.sh stop
```

### 1.3 生成 4 天历史数据 + 1 天正式数据

#### 第1天：2026-06-18（历史数据，重置业务+用户）

修改 hadoop100 的 `/opt/module/applog/application.yml`：

```yaml
#业务日期
mock.date: "2026-06-18"
#是否重置业务数据
mock.clear.busi: 1
#是否重置用户数据
mock.clear.user: 1
# 批量生成新用户数量
mock.new.user: 100
# 日志是否写入数据库一份  写入z_log表中
mock.log.db.enable: 0
```

生成数据：

```bash
cd /opt/module/applog/
lg.sh
```

#### 第2天：2026-06-19

修改 `application.yml`：

```yaml
mock.date: "2026-06-19"
mock.clear.busi: 0
mock.clear.user: 0
mock.new.user: 0
```

```bash
lg.sh
```

#### 第3天：2026-06-20

```yaml
mock.date: "2026-06-20"
```

```bash
lg.sh
```

#### 第4天：2026-06-21

```yaml
mock.date: "2026-06-21"
```

```bash
lg.sh
```

#### 第5天：2026-06-18（正式上线数据）

先删除旧日志：

```bash
hadoop fs -rm -r -f /origin_data/gmall/log
```

修改 `application.yml`：

```yaml
mock.date: "2026-06-18"
```

```bash
lg.sh
```

### 1.4 全量表同步

```bash
mysql_to_hdfs_full.sh all 2026-06-18
hdfs dfs -ls /origin_data/gmall/db/ | grep _full    # 预期 17 个目录
```

### 1.5 增量表首日全量同步

清除 Maxwell 断点记录（MySQL 中）：

```sql
DROP TABLE maxwell.bootstrap;
DROP TABLE maxwell.columns;
DROP TABLE maxwell.databases;
DROP TABLE maxwell.heartbeats;
DROP TABLE maxwell.positions;
DROP TABLE maxwell.schemas;
DROP TABLE maxwell.tables;
```

修改 Maxwell 配置文件：

```bash
vim /opt/module/maxwell/config.properties
# mock_date=2026-06-18
```

启动 Maxwell 并执行首日全量同步：

```bash
mxw.sh start
mysql_to_kafka_inc_init.sh all
hdfs dfs -ls /origin_data/gmall/db/ | grep _inc    # 预期 13 个目录
```

---

## 第二部分：ODS 层（贴源层）构建

> ODS 层设计要点：
> - 表结构依托于从业务系统同步过来的数据结构
> - 保存全部历史数据，压缩格式选 gzip
> - 命名规范：`ods_表名_单分区增量/全量标识(inc/full)`

### 2.1 日志表：ods_log_inc

表结构使用 Hive STRUCT 和 ARRAY 类型映射 JSON 日志：

```sql
DROP TABLE IF EXISTS ods_log_inc;
CREATE EXTERNAL TABLE ods_log_inc
(
    `common` STRUCT<ar :STRING,
        ba :STRING,
        ch :STRING,
        is_new :STRING,
        md :STRING,
        mid :STRING,
        os :STRING,
        sid :STRING,
        uid :STRING,
        vc :STRING> COMMENT '公共信息',
    `page` STRUCT<during_time :STRING,
        item :STRING,
        item_type :STRING,
        last_page_id :STRING,
        page_id :STRING,
        from_pos_id :STRING,
        from_pos_seq :STRING,
        refer_id :STRING> COMMENT '页面信息',
    `actions` ARRAY<STRUCT<action_id:STRING,
        item:STRING,
        item_type:STRING,
        ts:BIGINT>> COMMENT '动作信息',
    `displays` ARRAY<STRUCT<display_type :STRING,
        item :STRING,
        item_type :STRING,
        `pos_seq` :STRING,
        pos_id :STRING>> COMMENT '曝光信息',
    `start` STRUCT<entry :STRING,
        first_open :BIGINT,
        loading_time :BIGINT,
        open_ad_id :BIGINT,
        open_ad_ms :BIGINT,
        open_ad_skip_ms :BIGINT> COMMENT '启动信息',
    `err` STRUCT<error_code:BIGINT,
            msg:STRING> COMMENT '错误信息',
    `ts` BIGINT  COMMENT '时间戳'
) COMMENT '用户行为日志表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_log_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 数据装载

```sql
LOAD DATA INPATH '/origin_data/gmall/log/topic_log/2026-06-18' 
INTO TABLE ods_log_inc PARTITION(dt='2026-06-18');
```

#### 每日装载脚本

```bash
sudo vim /home/hadoop/bin/hdfs_to_ods_log.sh
```

```bash
#!/bin/bash

APP=gmall

if [ -n "$1" ] ;then
   do_date=$1
else
   do_date=`date -d "-1 day" +%F`
fi

echo ================== 日志日期为 $do_date ==================
sql="load data inpath '/origin_data/$APP/log/topic_log/$do_date' into table ${APP}.ods_log_inc partition(dt='$do_date');"

hive -e "$sql"
```

```bash
sudo chmod 777 /home/hadoop/bin/hdfs_to_ods_log.sh
```

测试：

```bash
hdfs_to_ods_log.sh 2026-06-18
```

### 2.2 业务全量表（17张）

> 全量表来自 DataX 同步到 HDFS 的 `_full` 目录。字段分隔符为 `\t`。

#### 2.2.1 活动信息表

```sql
DROP TABLE IF EXISTS ods_activity_info_full;
CREATE EXTERNAL TABLE ods_activity_info_full
(
    `id`              STRING COMMENT '活动id',
    `activity_name` STRING COMMENT '活动名称',
    `activity_type` STRING COMMENT '活动类型',
    `activity_desc` STRING COMMENT '活动描述',
    `start_time`     STRING COMMENT '开始时间',
    `end_time`        STRING COMMENT '结束时间',
    `create_time`    STRING COMMENT '创建时间',
    `operate_time`   STRING COMMENT '修改时间'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_activity_info_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

```

#### 2.2.2 活动规则表

```sql
DROP TABLE IF EXISTS ods_activity_rule_full;
CREATE EXTERNAL TABLE ods_activity_rule_full
(
    `id`               STRING COMMENT '编号',
    `activity_id`      STRING COMMENT '活动ID',
    `activity_type`    STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
    `condition_num`    BIGINT COMMENT '满减件数',
    `benefit_amount`   DECIMAL(16,2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
    `benefit_level`    STRING COMMENT '优惠级别',
    `create_time`      STRING COMMENT '创建时间',
    `operate_time`     STRING COMMENT '修改时间'
)
COMMENT '活动规则表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_activity_rule_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.3 一级品类表

```sql
DROP TABLE IF EXISTS ods_base_category1_full;
CREATE EXTERNAL TABLE ods_base_category1_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '分类名称',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT '一级品类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_category1_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.4 二级品类表

```sql
CREATE EXTERNAL TABLE ods_base_category2_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '二级分类名称',
    `category1_id` STRING COMMENT '一级分类编号',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT '二级品类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_category2_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.5 三级品类表

```sql
CREATE EXTERNAL TABLE ods_base_category3_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '三级分类名称',
    `category2_id` STRING COMMENT '二级分类编号',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT '三级品类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_category3_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.6 编码字典表

```sql
CREATE EXTERNAL TABLE ods_base_dic_full
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编号',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
)
COMMENT '编码字典表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_dic_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.7 省份表

```sql
CREATE EXTERNAL TABLE ods_base_province_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '省份名称',
    `region_id`    STRING COMMENT '地区ID',
    `area_code`    STRING COMMENT '地区编码',
    `iso_code`     STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`   STRING COMMENT '新版国际标准地区编码',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT '省份表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_province_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.8 地区表

```sql
CREATE EXTERNAL TABLE ods_base_region_full
(
    `id`           STRING COMMENT '地区ID',
    `region_name`  STRING COMMENT '地区名称',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT '地区表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_region_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.9 品牌表

```sql
CREATE EXTERNAL TABLE ods_base_trademark_full
(
    `id`           STRING COMMENT '编号',
    `tm_name`      STRING COMMENT '品牌名称',
    `logo_url`     STRING COMMENT '品牌LOGO的图片路径',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT '品牌表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_trademark_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.10 购物车全量表

```sql
CREATE EXTERNAL TABLE ods_cart_info_full
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户ID',
    `sku_id`       STRING COMMENT 'SKU_ID',
    `cart_price`   DECIMAL(16,2) COMMENT '放入购物车时价格',
    `sku_num`      BIGINT COMMENT '数量',
    `img_url`      BIGINT COMMENT '商品图片地址',
    `sku_name`     STRING COMMENT 'SKU名称(冗余)',
    `is_checked`   STRING COMMENT '是否被选中',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered`   STRING COMMENT '是否已经下单',
    `order_time`   STRING COMMENT '下单时间'
)
COMMENT '购物车全量表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_cart_info_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.11 优惠券信息表

```sql
CREATE EXTERNAL TABLE ods_coupon_info_full
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type`      STRING COMMENT '购物券类型 1现金券 2折扣券 3满减券 4满件打折券',
    `condition_amount` DECIMAL(16,2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16,2) COMMENT '减免金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '折扣',
    `create_time`      STRING COMMENT '创建时间',
    `range_type`       STRING COMMENT '范围类型 1商品(SPUID) 2品类 3品牌',
    `limit_num`        BIGINT COMMENT '最多领用次数',
    `taken_count`      BIGINT COMMENT '已领用次数',
    `start_time`       STRING COMMENT '开始时间',
    `end_time`         STRING COMMENT '结束时间',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间',
    `range_desc`       STRING COMMENT '范围描述'
)
COMMENT '优惠券信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_coupon_info_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.12 商品平台属性表

```sql
CREATE EXTERNAL TABLE ods_sku_attr_value_full
(
    `id`           STRING COMMENT '编号',
    `attr_id`      STRING COMMENT '平台属性ID',
    `value_id`     STRING COMMENT '平台属性值ID',
    `sku_id`       STRING COMMENT 'SKU_ID',
    `attr_name`    STRING COMMENT '平台属性名称',
    `value_name`   STRING COMMENT '平台属性值名称',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT '商品平台属性表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_sku_attr_value_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.13 商品表

```sql
CREATE EXTERNAL TABLE ods_sku_info_full
(
    `id`              STRING COMMENT 'SKU_ID',
    `spu_id`          STRING COMMENT 'SPU_ID',
    `price`           DECIMAL(16,2) COMMENT '价格',
    `sku_name`        STRING COMMENT 'SKU名称',
    `sku_desc`        STRING COMMENT 'SKU规格描述',
    `weight`          DECIMAL(16,2) COMMENT '重量',
    `tm_id`           STRING COMMENT '品牌ID',
    `category3_id`    STRING COMMENT '三级品类ID',
    `sku_default_img` STRING COMMENT '默认显示图片地址',
    `is_sale`         STRING COMMENT '是否在售',
    `create_time`     STRING COMMENT '创建时间',
    `operate_time`    STRING COMMENT '修改时间'
)
COMMENT '商品表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_sku_info_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.14 商品销售属性值表

```sql
CREATE EXTERNAL TABLE ods_sku_sale_attr_value_full
(
    `id`                    STRING COMMENT '编号',
    `sku_id`               STRING COMMENT 'SKU_ID',
    `spu_id`               STRING COMMENT 'SPU_ID',
    `sale_attr_value_id`   STRING COMMENT '销售属性值ID',
    `sale_attr_id`         STRING COMMENT '销售属性ID',
    `sale_attr_name`       STRING COMMENT '销售属性名称',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称',
    `create_time`          STRING COMMENT '创建时间',
    `operate_time`         STRING COMMENT '修改时间'
)
COMMENT '商品销售属性值表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_sku_sale_attr_value_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.15 SPU表

```sql
CREATE EXTERNAL TABLE ods_spu_info_full
(
    `id`           STRING COMMENT 'SPU_ID',
    `spu_name`     STRING COMMENT 'SPU名称',
    `description`  STRING COMMENT '描述信息',
    `category3_id` STRING COMMENT '三级品类ID',
    `tm_id`        STRING COMMENT '品牌ID',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT 'SPU表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_spu_info_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.16 营销坑位表

```sql
CREATE EXTERNAL TABLE ods_promotion_pos_full
(
    `id`             STRING COMMENT '营销坑位ID',
    `pos_location`   STRING COMMENT '营销坑位位置',
    `pos_type`       STRING COMMENT '营销坑位类型',
    `promotion_type` STRING COMMENT '营销类型',
    `create_time`    STRING COMMENT '创建时间',
    `operate_time`   STRING COMMENT '修改时间'
)
COMMENT '营销坑位表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_promotion_pos_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.2.17 营销渠道表

```sql
CREATE EXTERNAL TABLE ods_promotion_refer_full
(
    `id`           STRING COMMENT '外部营销渠道ID',
    `refer_name`   STRING COMMENT '外部营销渠道名称',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
)
COMMENT '营销渠道表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_promotion_refer_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

### 2.3 业务增量表（12张）

> 增量表来自 Maxwell → Flume → HDFS 的 `_inc` 目录，JSON 格式。

#### 2.3.1 购物车增量表

```sql
CREATE EXTERNAL TABLE ods_cart_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<
        id :STRING, user_id :STRING, sku_id :STRING, cart_price :DECIMAL(16,2),
        sku_num :BIGINT, img_url :STRING, sku_name :STRING,
        is_checked :STRING, create_time :STRING, operate_time :STRING,
        is_ordered :STRING, order_time:STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '购物车增量表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_cart_info_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.2 评论表

```sql
CREATE EXTERNAL TABLE ods_comment_info_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, user_id:STRING, nick_name:STRING,
        head_img:STRING, sku_id:STRING, spu_id:STRING, order_id:STRING,
        appraise:STRING, comment_txt:STRING, create_time:STRING,
        operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '评论表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_comment_info_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.3 优惠券领用表

```sql
CREATE EXTERNAL TABLE ods_coupon_use_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, coupon_id:STRING, user_id:STRING,
        order_id:STRING, coupon_status:STRING, get_time:STRING,
        using_time:STRING, used_time:STRING, expire_time:STRING,
        create_time:STRING, operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '优惠券领用表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_coupon_use_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.4 收藏表

```sql
CREATE EXTERNAL TABLE ods_favor_info_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, user_id:STRING, sku_id:STRING,
        spu_id:STRING, is_cancel:STRING, create_time:STRING,
        operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '收藏表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_favor_info_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.5 订单明细表

```sql
CREATE EXTERNAL TABLE ods_order_detail_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, order_id:STRING, sku_id:STRING,
        sku_name:STRING, img_url:STRING, order_price:DECIMAL(16,2),
        sku_num:BIGINT, create_time:STRING, source_type:STRING,
        source_id:STRING, split_total_amount:DECIMAL(16,2),
        split_activity_amount:DECIMAL(16,2),
        split_coupon_amount:DECIMAL(16,2), operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '订单明细表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_order_detail_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.6 订单明细活动关联表

```sql
CREATE EXTERNAL TABLE ods_order_detail_activity_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, order_id:STRING, order_detail_id:STRING,
        activity_id:STRING, activity_rule_id:STRING, sku_id:STRING,
        create_time:STRING, operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_order_detail_activity_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.7 订单明细优惠券关联表

```sql
CREATE EXTERNAL TABLE ods_order_detail_coupon_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, order_id:STRING, order_detail_id:STRING,
        coupon_id:STRING, coupon_use_id:STRING, sku_id:STRING,
        create_time:STRING, operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.8 订单表

```sql
CREATE EXTERNAL TABLE ods_order_info_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, consignee:STRING, consignee_tel:STRING,
        total_amount:DECIMAL(16,2), order_status:STRING, user_id:STRING,
        payment_way:STRING, delivery_address:STRING, order_comment:STRING,
        out_trade_no:STRING, trade_body:STRING, create_time:STRING,
        operate_time:STRING, expire_time:STRING, process_status:STRING,
        tracking_no:STRING, parent_order_id:STRING, img_url:STRING,
        province_id:STRING, activity_reduce_amount:DECIMAL(16,2),
        coupon_reduce_amount:DECIMAL(16,2),
        original_total_amount:DECIMAL(16,2),
        freight_fee:DECIMAL(16,2),
        freight_fee_reduce:DECIMAL(16,2),
        refundable_time:DECIMAL(16,2)> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '订单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_order_info_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.9 退单表

```sql
CREATE EXTERNAL TABLE ods_order_refund_info_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, user_id:STRING, order_id:STRING,
        sku_id:STRING, refund_type:STRING, refund_num:BIGINT,
        refund_amount:DECIMAL(16,2), refund_reason_type:STRING,
        refund_reason_txt:STRING, refund_status:STRING,
        create_time:STRING, operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '退单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_order_refund_info_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.10 订单状态流水表

```sql
CREATE EXTERNAL TABLE ods_order_status_log_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, order_id:STRING, order_status:STRING,
        create_time:STRING, operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '订单状态流水表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_order_status_log_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.11 支付表

```sql
CREATE EXTERNAL TABLE ods_payment_info_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, out_trade_no:STRING, order_id:STRING,
        user_id:STRING, payment_type:STRING, trade_no:STRING,
        total_amount:DECIMAL(16,2), subject:STRING,
        payment_status:STRING, create_time:STRING,
        callback_time:STRING, callback_content:STRING,
        operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '支付表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_payment_info_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

#### 2.3.12 退款表 + 用户表

```sql
-- 退款表
CREATE EXTERNAL TABLE ods_refund_payment_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, out_trade_no:STRING, order_id:STRING,
        sku_id:STRING, payment_type:STRING, trade_no:STRING,
        total_amount:DECIMAL(16,2), subject:STRING,
        refund_status:STRING, create_time:STRING,
        callback_time:STRING, callback_content:STRING,
        operate_time:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '退款表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_refund_payment_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

-- 用户表
CREATE EXTERNAL TABLE ods_user_info_inc
(
    `type` STRING, `ts` BIGINT,
    `data` STRUCT<id:STRING, login_name:STRING, nick_name:STRING,
        passwd:STRING, name:STRING, phone_num:STRING, email:STRING,
        head_img:STRING, user_level:STRING, birthday:STRING,
        gender:STRING, create_time:STRING, operate_time:STRING,
        status:STRING> COMMENT '数据',
    `old` MAP<STRING,STRING> COMMENT '旧值'
)
COMMENT '用户表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_user_info_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
```

### 2.4 ODS 层业务表装载脚本

```bash
sudo vim /home/hadoop/bin/hdfs_to_ods_db.sh
```

```bash
#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
   do_date=$2
else
   do_date=`date -d '-1 day' +%F`
fi

load_data(){
    sql=""
    for i in $*; do
        hadoop fs -test -e /origin_data/$APP/db/${i:4}/$do_date
        if [[ $? = 0 ]]; then
            sql=$sql"load data inpath '/origin_data/$APP/db/${i:4}/$do_date' OVERWRITE into table ${APP}.$i partition(dt='$do_date');"
        fi
    done
    hive -e "$sql"
}

case $1 in
    "ods_activity_info_full")       load_data "ods_activity_info_full" ;;
    "ods_activity_rule_full")       load_data "ods_activity_rule_full" ;;
    "ods_base_category1_full")      load_data "ods_base_category1_full" ;;
    "ods_base_category2_full")      load_data "ods_base_category2_full" ;;
    "ods_base_category3_full")      load_data "ods_base_category3_full" ;;
    "ods_base_dic_full")            load_data "ods_base_dic_full" ;;
    "ods_base_province_full")       load_data "ods_base_province_full" ;;
    "ods_base_region_full")         load_data "ods_base_region_full" ;;
    "ods_base_trademark_full")      load_data "ods_base_trademark_full" ;;
    "ods_cart_info_full")           load_data "ods_cart_info_full" ;;
    "ods_coupon_info_full")         load_data "ods_coupon_info_full" ;;
    "ods_sku_attr_value_full")      load_data "ods_sku_attr_value_full" ;;
    "ods_sku_info_full")            load_data "ods_sku_info_full" ;;
    "ods_sku_sale_attr_value_full") load_data "ods_sku_sale_attr_value_full" ;;
    "ods_spu_info_full")            load_data "ods_spu_info_full" ;;
    "ods_promotion_pos_full")       load_data "ods_promotion_pos_full" ;;
    "ods_promotion_refer_full")     load_data "ods_promotion_refer_full" ;;
    "ods_cart_info_inc")            load_data "ods_cart_info_inc" ;;
    "ods_comment_info_inc")         load_data "ods_comment_info_inc" ;;
    "ods_coupon_use_inc")           load_data "ods_coupon_use_inc" ;;
    "ods_favor_info_inc")           load_data "ods_favor_info_inc" ;;
    "ods_order_detail_inc")         load_data "ods_order_detail_inc" ;;
    "ods_order_detail_activity_inc") load_data "ods_order_detail_activity_inc" ;;
    "ods_order_detail_coupon_inc")  load_data "ods_order_detail_coupon_inc" ;;
    "ods_order_info_inc")           load_data "ods_order_info_inc" ;;
    "ods_order_refund_info_inc")    load_data "ods_order_refund_info_inc" ;;
    "ods_order_status_log_inc")     load_data "ods_order_status_log_inc" ;;
    "ods_payment_info_inc")         load_data "ods_payment_info_inc" ;;
    "ods_refund_payment_inc")       load_data "ods_refund_payment_inc" ;;
    "ods_user_info_inc")            load_data "ods_user_info_inc" ;;
    "all")
        load_data "ods_activity_info_full" "ods_activity_rule_full" "ods_base_category1_full" "ods_base_category2_full" "ods_base_category3_full" "ods_base_dic_full" "ods_base_province_full" "ods_base_region_full" "ods_base_trademark_full" "ods_cart_info_full" "ods_coupon_info_full" "ods_sku_attr_value_full" "ods_sku_info_full" "ods_sku_sale_attr_value_full" "ods_spu_info_full" "ods_promotion_pos_full" "ods_promotion_refer_full" "ods_cart_info_inc" "ods_comment_info_inc" "ods_coupon_use_inc" "ods_favor_info_inc" "ods_order_detail_inc" "ods_order_detail_activity_inc" "ods_order_detail_coupon_inc" "ods_order_info_inc" "ods_order_refund_info_inc" "ods_order_status_log_inc" "ods_payment_info_inc" "ods_refund_payment_inc" "ods_user_info_inc"
        ;;
esac
```

```bash
sudo chmod 777 /home/hadoop/bin/hdfs_to_ods_db.sh
```

测试：

```bash
# 执行表建表语句后，装载数据
hdfs_to_ods_db.sh all 2026-06-18

# 验证
hive -e "USE gmall; SELECT COUNT(*) FROM ods_order_info_inc;"
```

---

> **阶段五 ODS 层完成** | 继续下一部分：DIM 层维度表构建


> 对应文档：《电商数据仓库系统》V6.0 第8-9章
> 目标：9张维度表(DIM) + 10张事实表(DWD)，维度建模核心

---

## 第三部分：DIM 层（维度层）构建

- 依据维度建模理论，存储维度模型的维度表
- 存储格式：ORC + Snappy 压缩
- 命名规范：`dim_表名_full`（全量快照表）或 `dim_表名_zip`（拉链表）
- 设计参考：业务总线矩阵（阶段5文档附录）

---

### 3.1 商品维度表（核心）

> 最复杂的维度表——关联 8 张 ODS 表

#### 3.1.1 建表

```sql
DROP TABLE IF EXISTS dim_sku_full;
CREATE EXTERNAL TABLE dim_sku_full
(
    `id`                   STRING COMMENT 'SKU_ID',
    `price`                DECIMAL(16, 2) COMMENT '商品价格',
    `sku_name`             STRING COMMENT '商品名称',
    `sku_desc`             STRING COMMENT '商品描述',
    `weight`               DECIMAL(16, 2) COMMENT '重量',
    `is_sale`              BOOLEAN COMMENT '是否在售',
    `spu_id`               STRING COMMENT 'SPU编号',
    `spu_name`             STRING COMMENT 'SPU名称',
    `category3_id`         STRING COMMENT '三级品类ID',
    `category3_name`       STRING COMMENT '三级品类名称',
    `category2_id`         STRING COMMENT '二级品类id',
    `category2_name`       STRING COMMENT '二级品类名称',
    `category1_id`         STRING COMMENT '一级品类ID',
    `category1_name`       STRING COMMENT '一级品类名称',
    `tm_id`                  STRING COMMENT '品牌ID',
    `tm_name`               STRING COMMENT '品牌名称',
    `sku_attr_values`      ARRAY<STRUCT<attr_id :STRING,
        value_id :STRING,
        attr_name :STRING,
        value_name:STRING>> COMMENT '平台属性',
    `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id :STRING,
        sale_attr_value_id :STRING,
        sale_attr_name :STRING,
        sale_attr_value_name:STRING>> COMMENT '销售属性',
    `create_time`          STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_sku_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

#### 3.1.2 数据装载

> 思路：以 `ods_sku_info_full` 为主维表，left join 关联 spu_info、三级品类、品牌、平台属性、销售属性等 7 张表。left join 优于 join，避免因从表数据丢失导致主表整行被丢弃。

```sql
with
sku as
(
    select
        id,
        price,
        sku_name,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        category3_id,
        tm_id,
        create_time
    from ods_sku_info_full
    where dt='2026-06-18'
),
spu as
(
    select
        id,
        spu_name
    from ods_spu_info_full
    where dt='2026-06-18'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ods_base_category3_full
    where dt='2026-06-18'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ods_base_category2_full
    where dt='2026-06-18'
),
c1 as
(
    select
        id,
        name
    from ods_base_category1_full
    where dt='2026-06-18'
),
tm as
(
    select
        id,
        tm_name
    from ods_base_trademark_full
    where dt='2026-06-18'
),
attr as
(
    select
        sku_id,
        collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
    from ods_sku_attr_value_full
    where dt='2026-06-18'
    group by sku_id
),
sale_attr as
(
    select
        sku_id,
        collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
    from ods_sku_sale_attr_value_full
    where dt='2026-06-18'
    group by sku_id
)
insert overwrite table dim_sku_full partition(dt='2026-06-18')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;

```

---

### 3.2 优惠券维度表
#### 3.2.1 建表


```sql
DROP TABLE IF EXISTS dim_coupon_full;
CREATE EXTERNAL TABLE dim_coupon_full
(
    `id`                  STRING COMMENT '优惠券编号',
    `coupon_name`       STRING COMMENT '优惠券名称',
    `coupon_type_code` STRING COMMENT '优惠券类型编码',
    `coupon_type_name` STRING COMMENT '优惠券类型名称',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`     BIGINT COMMENT '满件数',
    `activity_id`       STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减免金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`       STRING COMMENT '创建时间',
    `range_type_code`  STRING COMMENT '优惠范围类型编码',
    `range_type_name`  STRING COMMENT '优惠范围类型名称',
    `limit_num`         BIGINT COMMENT '最多领取次数',
    `taken_count`       BIGINT COMMENT '已领取次数',
    `start_time`        STRING COMMENT '可以领取的开始时间',
    `end_time`          STRING COMMENT '可以领取的结束时间',
    `operate_time`      STRING COMMENT '修改时间',
    `expire_time`       STRING COMMENT '过期时间'
) COMMENT '优惠券维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_coupon_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

```

#### 3.2.2 数据装载

```sql
SET hive.execution.engine=mr;
insert overwrite table dim_coupon_full partition(dt='2026-06-18')
select
    id,
    coupon_name,
    coupon_type,
    coupon_dic.dic_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type
        when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3202' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3203' then concat('减',benefit_amount,'元')
    end benefit_rule,
    create_time,
    range_type,
    range_dic.dic_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from
(
    select
        id,
        coupon_name,
        coupon_type,
        condition_amount,
        condition_num,
        activity_id,
        benefit_amount,
        benefit_discount,
        create_time,
        range_type,
        limit_num,
        taken_count,
        start_time,
        end_time,
        operate_time,
        expire_time
    from ods_coupon_info_full
    where dt='2026-06-18'
)ci
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic_full
    where dt='2026-06-18'
    and parent_code='32'
)coupon_dic
on ci.coupon_type=coupon_dic.dic_code
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic_full
    where dt='2026-06-18'
    and parent_code='33'
)range_dic
on ci.range_type=range_dic.dic_code;


```

### 3.3 活动维度表
#### 3.3.1 建表
```sql
DROP TABLE IF EXISTS dim_activity_full;
CREATE EXTERNAL TABLE dim_activity_full
(
    `activity_rule_id`   STRING COMMENT '活动规则ID',
    `activity_id`         STRING COMMENT '活动ID',
    `activity_name`       STRING COMMENT '活动名称',
    `activity_type_code` STRING COMMENT '活动类型编码',
    `activity_type_name` STRING COMMENT '活动类型名称',
    `activity_desc`       STRING COMMENT '活动描述',
    `start_time`           STRING COMMENT '开始时间',
    `end_time`             STRING COMMENT '结束时间',
    `create_time`          STRING COMMENT '创建时间',
    `condition_amount`    DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`       BIGINT COMMENT '满减件数',
    `benefit_amount`      DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_rule`        STRING COMMENT '优惠规则',
    `benefit_level`       STRING COMMENT '优惠级别'
) COMMENT '活动维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_activity_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

```

#### 3.3.2 数据装载

```sql
SET hive.execution.engine=mr;
insert overwrite table dim_activity_full partition(dt='2026-06-18')
select
    rule.id,
    info.id,
    activity_name,
    rule.activity_type,
    dic.dic_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3102' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3103' then concat('打', benefit_discount,'折')
    end benefit_rule,
    benefit_level
from
(
    select
        id,
        activity_id,
        activity_type,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        benefit_level
    from ods_activity_rule_full
    where dt='2026-06-18'
)rule
left join
(
    select
        id,
        activity_name,
        activity_type,
        activity_desc,
        start_time,
        end_time,
        create_time
    from ods_activity_info_full
    where dt='2026-06-18'
)info
on rule.activity_id=info.id
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic_full
    where dt='2026-06-18'
    and parent_code='31'
)dic
on rule.activity_type=dic.dic_code;


```

### 3.4 地区维度表
#### 3.4.1 建表
```sql
DROP TABLE IF EXISTS dim_province_full;
CREATE EXTERNAL TABLE dim_province_full
(
    `id`              STRING COMMENT '省份ID',
    `province_name` STRING COMMENT '省份名称',
    `area_code`     STRING COMMENT '地区编码',
    `iso_code`      STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2`    STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `region_id`     STRING COMMENT '地区ID',
    `region_name`   STRING COMMENT '地区名称'
) COMMENT '地区维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_province_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


```

#### 3.4.2 数据装载

```sql
SET hive.execution.engine=mr;
insert overwrite table dim_province_full partition(dt='2026-06-18')
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
(
    select
        id,
        name,
        region_id,
        area_code,
        iso_code,
        iso_3166_2
    from ods_base_province_full
    where dt='2026-06-18'
)province
left join
(
    select
        id,
        region_name
    from ods_base_region_full
    where dt='2026-06-18'
)region
on province.region_id=region.id;


```
### 3.5 营销坑位维度表
#### 3.5.1 建表
```sql
DROP TABLE IF EXISTS dim_promotion_pos_full;
CREATE EXTERNAL TABLE dim_promotion_pos_full
(
    `id`                 STRING COMMENT '营销坑位ID',
    `pos_location`     STRING COMMENT '营销坑位位置',
    `pos_type`          STRING COMMENT '营销坑位类型 ',
    `promotion_type`   STRING COMMENT '营销类型',
    `create_time`       STRING COMMENT '创建时间',
    `operate_time`      STRING COMMENT '修改时间'
) COMMENT '营销坑位维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_promotion_pos_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```
#### 3.5.2 数据装载
```sql
SET hive.execution.engine=mr;
insert overwrite table dim_promotion_pos_full partition(dt='2026-06-18')
select
    `id`,         
    `pos_location`,
    `pos_type`,
    `promotion_type`,
    `create_time`,
    `operate_time`
from ods_promotion_pos_full 
where dt='2026-06-18';
```
### 3.6 营销渠道维度表

#### 3.6.1 建表

```sql
DROP TABLE IF EXISTS dim_promotion_refer_full;
CREATE EXTERNAL TABLE dim_promotion_refer_full
(
    `id`                    STRING COMMENT '营销渠道ID',
    `refer_name`          STRING COMMENT '营销渠道名称',
    `create_time`         STRING COMMENT '创建时间',
    `operate_time`        STRING COMMENT '修改时间'
) COMMENT '营销渠道维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_promotion_refer_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```
#### 3.6.2 数据装载
```sql
SET hive.execution.engine=mr;
insert overwrite table dim_promotion_refer_full partition(dt='2026-06-18')
select
    `id`, 
    `refer_name`,
    `create_time`,
    `operate_time`   
from ods_promotion_refer_full 
where dt='2026-06-18';
```
### 3.7 日期维度表

日期维度表是静态表，一次性导入 2026-2027 两年数据即可。

#### 3.7.1 建表

```sql
DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date
(
    `date_id`    STRING COMMENT '日期ID',
    `week_id`    STRING COMMENT '周ID,一年中的第几周',
    `week_day`   STRING COMMENT '周几',
    `day`         STRING COMMENT '每月的第几天',
    `month`       STRING COMMENT '一年中的第几月',
    `quarter`    STRING COMMENT '一年中的第几季度',
    `year`        STRING COMMENT '年份',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '日期维度表'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_date/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

```

#### 3.7.2 数据装载

```sql
-- 创建临时表
DROP TABLE IF EXISTS tmp_dim_date_info;
CREATE EXTERNAL TABLE tmp_dim_date_info (
    `date_id`    STRING COMMENT '日',
    `week_id`    STRING COMMENT '周ID',
    `week_day`   STRING COMMENT '周几',
    `day`        STRING COMMENT '每月的第几天',
    `month`      STRING COMMENT '第几月',
    `quarter`    STRING COMMENT '第几季度',
    `year`       STRING COMMENT '年',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/tmp/tmp_dim_date_info/';
```

将 `date_info.txt`（2026-2027两年数据，730行，已生成在 outputs 目录）上传到 HDFS：

```bash
hadoop fs -put /opt/software/date_info.txt /warehouse/gmall/tmp/tmp_dim_date_info/
```

导入日期维度表：

```sql
INSERT OVERWRITE TABLE dim_date SELECT * FROM tmp_dim_date_info;
```

验证：

```sql
SELECT * FROM dim_date LIMIT 5;
```

示例数据格式：

```
2026-04-05	14	7	5	4	2	2026	0	清明节
2026-04-06	15	1	6	4	2	2026	0	清明节
2026-06-18	25	4	18	6	2	2026	1	\N
```
#### 3.8.1 建表
### 3.8 用户维度表（拉链表）

```sql
DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户ID',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

### 首日装载
```sql
SET hive.execution.engine=mr;
insert overwrite table dim_user_zip partition (dt = '9999-12-31')
select data.id,
       concat(substr(data.name, 1, 1), '*')                name,
       if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
          concat(substr(data.phone_num, 1, 3), '*'), null) phone_num,
       if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
          concat('*@', split(data.email, '@')[1]), null)   email,
       data.user_level,
       data.birthday,
       data.gender,
       data.create_time,
       data.operate_time,
       '2026-06-18'                                        start_date,
       '9999-12-31'                                        end_date
from ods_user_info_inc
where dt = '2026-06-18'
  and type = 'bootstrap-insert';
```

### 每日装载
```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_zip partition (dt)
select id,
       name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       start_date,
       if(rn = 2, date_sub('2026-06-19', 1), end_date)     end_date,
       if(rn = 1, '9999-12-31', date_sub('2026-06-19', 1)) dt
from (
         select id,
                name,
                phone_num,
                email,
                user_level,
                birthday,
                gender,
                create_time,
                operate_time,
                start_date,
                end_date,
                row_number() over (partition by id order by start_date desc) rn
         from (
                  select id,
                         name,
                         phone_num,
                         email,
                         user_level,
                         birthday,
                         gender,
                         create_time,
                         operate_time,
                         start_date,
                         end_date
                  from dim_user_zip
                  where dt = '9999-12-31'
                  union
                  select id,
                         concat(substr(name, 1, 1), '*')                name,
                         if(phone_num regexp
                            '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
                            concat(substr(phone_num, 1, 3), '*'), null) phone_num,
                         if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
                            concat('*@', split(email, '@')[1]), null)   email,
                         user_level,
                         birthday,
                         gender,
                         create_time,
                         operate_time,
                         '2026-06-19'                                   start_date,
                         '9999-12-31'                                   end_date
                  from (
                           select data.id,
                                  data.name,
                                  data.phone_num,
                                  data.email,
                                  data.user_level,
                                  data.birthday,
                                  data.gender,
                                  data.create_time,
                                  data.operate_time,
                                  row_number() over (partition by data.id order by ts desc) rn
                           from ods_user_info_inc
                           where dt = '2026-06-19'
                       ) t1
                  where rn = 1
              ) t2
     ) t3;

```
> 拉链表的 `start_date` 和 `end_date` 标记每一行数据的有效期，`end_date='9999-12-31'` 表示当前仍然有效。后续通过封闭链（更新旧记录的 end_date）+ 新增开链记录实现缓慢变化维。

---

### 3.9 DIM 层装载脚本

#### 3.9.1 首日装载脚本

```bash
sudo vim /home/hadoop/bin/ods_to_dim_init.sh
```

```bash
#!/bin/bash
APP=gmall
if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 
dim_user_zip="
insert overwrite table ${APP}.dim_user_zip partition (dt = '9999-12-31')
select data.id,
       concat(substr(data.name, 1, 1), '*')                name,
       if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
          concat(substr(data.phone_num, 1, 3), '*'), null) phone_num,
       if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
          concat('*@', split(data.email, '@')[1]), null)   email,
       data.user_level,
       data.birthday,
       data.gender,
       data.create_time,
       data.operate_time,
       '$do_date'                                        start_date,
       '9999-12-31'                                        end_date
from ${APP}.ods_user_info_inc
where dt = '$do_date'
  and type = 'bootstrap-insert';
"
dim_sku_full="
with sku as (select id,price,sku_name,sku_desc,weight,is_sale,spu_id,category3_id,tm_id,create_time from ${APP}.ods_sku_info_full where dt='$do_date'),
spu as (select id,spu_name from ${APP}.ods_spu_info_full where dt='$do_date'),
c3 as (select id,name,category2_id from ${APP}.ods_base_category3_full where dt='$do_date'),
c2 as (select id,name,category1_id from ${APP}.ods_base_category2_full where dt='$do_date'),
c1 as (select id,name from ${APP}.ods_base_category1_full where dt='$do_date'),
tm as (select id,tm_name from ${APP}.ods_base_trademark_full where dt='$do_date'),
attr as (select sku_id,collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs from ${APP}.ods_sku_attr_value_full where dt='$do_date' group by sku_id),
sale_attr as (select sku_id,collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs from ${APP}.ods_sku_sale_attr_value_full where dt='$do_date' group by sku_id)
insert overwrite table ${APP}.dim_sku_full partition(dt='$do_date')
select sku.id,sku.price,sku.sku_name,sku.sku_desc,sku.weight,sku.is_sale,sku.spu_id,spu.spu_name,
       sku.category3_id,c3.name,c3.category2_id,c2.name,c2.category1_id,c1.name,
       sku.tm_id,tm.tm_name,attr.attrs,sale_attr.sale_attrs,sku.create_time
from sku left join spu on sku.spu_id=spu.id left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
"
dim_province_full="
insert overwrite table ${APP}.dim_province_full partition(dt='$do_date')
select province.id,province.name,province.area_code,province.iso_code,province.iso_3166_2,
       region_id,region_name
from (select id,name,region_id,area_code,iso_code,iso_3166_2 from ${APP}.ods_base_province_full where dt='$do_date') province
left join (select id,region_name from ${APP}.ods_base_region_full where dt='$do_date') region
on province.region_id=region.id;
"
dim_coupon_full="
insert overwrite table ${APP}.dim_coupon_full partition(dt='$do_date')
select ci.id,ci.coupon_name,ci.coupon_type,coupon_dic.dic_name,
       ci.condition_amount,ci.condition_num,ci.activity_id,
       ci.benefit_amount,ci.benefit_discount,
       case ci.coupon_type when '3201' then concat('满',ci.condition_amount,'元减',ci.benefit_amount,'元')
            when '3202' then concat('满',ci.condition_num,'件打',ci.benefit_discount,'折')
            when '3203' then concat('减',ci.benefit_amount,'元') end benefit_rule,
       ci.create_time,ci.range_type,range_dic.dic_name,
       ci.limit_num,ci.taken_count,ci.start_time,ci.end_time,
       ci.operate_time,ci.expire_time
from (select * from ${APP}.ods_coupon_info_full where dt='$do_date') ci
left join (select dic_code,dic_name from ${APP}.ods_base_dic_full where dt='$do_date' and parent_code='32') coupon_dic on ci.coupon_type=coupon_dic.dic_code
left join (select dic_code,dic_name from ${APP}.ods_base_dic_full where dt='$do_date' and parent_code='33') range_dic on ci.range_type=range_dic.dic_code;
"
dim_activity_full="
insert overwrite table ${APP}.dim_activity_full partition(dt='$do_date')
select rule.id,info.id,activity_name,rule.activity_type,dic.dic_name,
       activity_desc,start_time,end_time,create_time,
       condition_amount,condition_num,benefit_amount,benefit_discount,
       case rule.activity_type when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
            when '3102' then concat('满',condition_num,'件打',benefit_discount,'折')
            when '3103' then concat('打',benefit_discount,'折') end benefit_rule,
       benefit_level
from (select * from ${APP}.ods_activity_rule_full where dt='$do_date') rule
left join (select * from ${APP}.ods_activity_info_full where dt='$do_date') info on rule.activity_id=info.id
left join (select dic_code,dic_name from ${APP}.ods_base_dic_full where dt='$do_date' and parent_code='31') dic on rule.activity_type=dic.dic_code;
"
dim_promotion_pos_full="
insert overwrite table ${APP}.dim_promotion_pos_full partition(dt='$do_date')
select id,pos_location,pos_type,promotion_type,create_time,operate_time
from ${APP}.ods_promotion_pos_full where dt='$do_date';
"
dim_promotion_refer_full="
insert overwrite table ${APP}.dim_promotion_refer_full partition(dt='$do_date')
select id,refer_name,create_time,operate_time
from ${APP}.ods_promotion_refer_full where dt='$do_date';
"
case $1 in
"dim_user_zip")       hive -e "$dim_user_zip" ;;
"dim_sku_full")       hive -e "$dim_sku_full" ;;
"dim_province_full")  hive -e "$dim_province_full" ;;
"dim_coupon_full")    hive -e "$dim_coupon_full" ;;
"dim_activity_full")  hive -e "$dim_activity_full" ;;
"dim_promotion_pos_full")    hive -e "$dim_promotion_pos_full" ;;
"dim_promotion_refer_full")  hive -e "$dim_promotion_refer_full" ;;
"all")
    hive -e "SET hive.execution.engine=mr; SET hive.exec.parallel=false; $dim_user_zip$dim_sku_full$dim_province_full$dim_coupon_full$dim_activity_full$dim_promotion_refer_full$dim_promotion_pos_full SET hive.execution.engine=spark; SET hive.exec.parallel=true;"
    ;;
esac
```

```bash
sudo chmod +x /home/hadoop/bin/ods_to_dim_init.sh
```

用法：

```bash
ods_to_dim_init.sh all 2026-06-18
```

#### 3.9.2 每日装载脚本

```bash
sudo vim /home/hadoop/bin/ods_to_dim.sh
```

```bash
#!/bin/bash
APP=gmall
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi
dim_user_zip="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dim_user_zip partition (dt)
select id,name,phone_num,email,user_level,birthday,gender,create_time,operate_time,
       start_date,
       if(rn = 2, date_sub('$do_date', 1), end_date)     end_date,
       if(rn = 1, '9999-12-31', date_sub('$do_date', 1)) dt
from (
    select id,name,phone_num,email,user_level,birthday,gender,create_time,operate_time,
           start_date,end_date,
           row_number() over (partition by id order by start_date desc) rn
    from (
        select id,name,phone_num,email,user_level,birthday,gender,create_time,operate_time,start_date,end_date
        from ${APP}.dim_user_zip where dt = '9999-12-31'
        union
        select id,
               concat(substr(name, 1, 1), '*') name,
               if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
                  concat(substr(phone_num, 1, 3), '*'), null) phone_num,
               if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
                  concat('*@', split(email, '@')[1]), null) email,
               user_level,birthday,gender,create_time,operate_time,
               '$do_date' start_date,'9999-12-31' end_date
        from (select data.id,data.name,data.phone_num,data.email,data.user_level,data.birthday,data.gender,
                     data.create_time,data.operate_time,
                     row_number() over (partition by data.id order by ts desc) rn
              from ${APP}.ods_user_info_inc where dt = '$do_date') t1
        where rn = 1
    ) t2
) t3;
"
# 以下 dim_sku_full/dim_province_full/dim_coupon_full/dim_activity_full/dim_promotion_pos_full/dim_promotion_refer_full 的 SQL 与首日完全相同
dim_sku_full="
with sku as (select id,price,sku_name,sku_desc,weight,is_sale,spu_id,category3_id,tm_id,create_time from ${APP}.ods_sku_info_full where dt='$do_date'),
spu as (select id,spu_name from ${APP}.ods_spu_info_full where dt='$do_date'),
c3 as (select id,name,category2_id from ${APP}.ods_base_category3_full where dt='$do_date'),
c2 as (select id,name,category1_id from ${APP}.ods_base_category2_full where dt='$do_date'),
c1 as (select id,name from ${APP}.ods_base_category1_full where dt='$do_date'),
tm as (select id,tm_name from ${APP}.ods_base_trademark_full where dt='$do_date'),
attr as (select sku_id,collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs from ${APP}.ods_sku_attr_value_full where dt='$do_date' group by sku_id),
sale_attr as (select sku_id,collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs from ${APP}.ods_sku_sale_attr_value_full where dt='$do_date' group by sku_id)
insert overwrite table ${APP}.dim_sku_full partition(dt='$do_date')
select sku.id,sku.price,sku.sku_name,sku.sku_desc,sku.weight,sku.is_sale,sku.spu_id,spu.spu_name,
       sku.category3_id,c3.name,c3.category2_id,c2.name,c2.category1_id,c1.name,
       sku.tm_id,tm.tm_name,attr.attrs,sale_attr.sale_attrs,sku.create_time
from sku left join spu on sku.spu_id=spu.id left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
"
dim_province_full="
insert overwrite table ${APP}.dim_province_full partition(dt='$do_date')
select province.id,province.name,province.area_code,province.iso_code,province.iso_3166_2,region_id,region_name
from (select id,name,region_id,area_code,iso_code,iso_3166_2 from ${APP}.ods_base_province_full where dt='$do_date') province
left join (select id,region_name from ${APP}.ods_base_region_full where dt='$do_date') region
on province.region_id=region.id;
"
dim_coupon_full="
insert overwrite table ${APP}.dim_coupon_full partition(dt='$do_date')
select ci.id,ci.coupon_name,ci.coupon_type,coupon_dic.dic_name,
       ci.condition_amount,ci.condition_num,ci.activity_id,
       ci.benefit_amount,ci.benefit_discount,
       case ci.coupon_type when '3201' then concat('满',ci.condition_amount,'元减',ci.benefit_amount,'元')
            when '3202' then concat('满',ci.condition_num,'件打',ci.benefit_discount,'折')
            when '3203' then concat('减',ci.benefit_amount,'元') end benefit_rule,
       ci.create_time,ci.range_type,range_dic.dic_name,
       ci.limit_num,ci.taken_count,ci.start_time,ci.end_time,
       ci.operate_time,ci.expire_time
from (select * from ${APP}.ods_coupon_info_full where dt='$do_date') ci
left join (select dic_code,dic_name from ${APP}.ods_base_dic_full where dt='$do_date' and parent_code='32') coupon_dic on ci.coupon_type=coupon_dic.dic_code
left join (select dic_code,dic_name from ${APP}.ods_base_dic_full where dt='$do_date' and parent_code='33') range_dic on ci.range_type=range_dic.dic_code;
"
dim_activity_full="
insert overwrite table ${APP}.dim_activity_full partition(dt='$do_date')
select rule.id,info.id,activity_name,rule.activity_type,dic.dic_name,
       activity_desc,start_time,end_time,create_time,
       condition_amount,condition_num,benefit_amount,benefit_discount,
       case rule.activity_type when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
            when '3102' then concat('满',condition_num,'件打',benefit_discount,'折')
            when '3103' then concat('打',benefit_discount,'折') end benefit_rule,
       benefit_level
from (select * from ${APP}.ods_activity_rule_full where dt='$do_date') rule
left join (select * from ${APP}.ods_activity_info_full where dt='$do_date') info on rule.activity_id=info.id
left join (select dic_code,dic_name from ${APP}.ods_base_dic_full where dt='$do_date' and parent_code='31') dic on rule.activity_type=dic.dic_code;
"
dim_promotion_pos_full="
insert overwrite table ${APP}.dim_promotion_pos_full partition(dt='$do_date')
select id,pos_location,pos_type,promotion_type,create_time,operate_time
from ${APP}.ods_promotion_pos_full where dt='$do_date';
"
dim_promotion_refer_full="
insert overwrite table ${APP}.dim_promotion_refer_full partition(dt='$do_date')
select id,refer_name,create_time,operate_time
from ${APP}.ods_promotion_refer_full where dt='$do_date';
"
case $1 in
"dim_user_zip")       hive -e "$dim_user_zip" ;;
"dim_sku_full")       hive -e "$dim_sku_full" ;;
"dim_province_full")  hive -e "$dim_province_full" ;;
"dim_coupon_full")    hive -e "$dim_coupon_full" ;;
"dim_activity_full")  hive -e "$dim_activity_full" ;;
"dim_promotion_pos_full")    hive -e "$dim_promotion_pos_full" ;;
"dim_promotion_refer_full")  hive -e "$dim_promotion_refer_full" ;;
"all")
    hive -e "SET hive.execution.engine=mr; SET hive.exec.parallel=false; $dim_user_zip$dim_sku_full$dim_province_full$dim_coupon_full$dim_activity_full$dim_promotion_refer_full$dim_promotion_pos_full SET hive.execution.engine=spark; SET hive.exec.parallel=true;"
    ;;
esac
```

> 每日脚本中用户维度（dim_user_zip）的逻辑与首日不同：采用拉链表更新策略——将当日变更的用户数据与历史拉链表 union，通过窗口函数 `row_number() over(partition by id order by start_date desc)` 标记每条记录的最新版本，更新旧记录的 `end_date`（封闭链）并新增开链记录。

```bash
sudo chmod +x /home/hadoop/bin/ods_to_dim.sh
```

用法：

```bash
ods_to_dim.sh all 2026-06-19
```

---

## 第四部分：DWD 层（明细层）构建

- 事实表存储在 DWD 层
- 格式：ORC + Snappy
- 命名：`dwd_数据域_业务过程_事实表类型(inc/full/snapshot)`
- 三类事实表：事务型（inc）、周期快照型（snapshot）、累积快照型（accumulating）

---

### 4.1 加购事务事实表


```sql
DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`                  STRING COMMENT '编号',
    `user_id`            STRING COMMENT '用户ID',
    `sku_id`             STRING COMMENT 'SKU_ID',
    `date_id`            STRING COMMENT '日期ID',
    `create_time`        STRING COMMENT '加购时间',
    `sku_num`            BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

#### 4.1.1 数据装载
#### 4.1.2 首日装载
```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time,
    data.sku_num,
    date_format(data.create_time, 'yyyy-MM-dd')
from ods_cart_info_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert';

```
#### 4.1.3 每日装载
```sql
SET hive.execution.engine=mr;
insert overwrite table dwd_trade_cart_add_inc partition (dt = '2026-06-19')
select data.id,
       data.user_id,
       data.sku_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')                          date_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')                 create_time,
       if(type = 'insert', data.sku_num, cast(data.sku_num as int) - cast(old['sku_num'] as int)) sku_num
from ods_cart_info_inc
where dt = '2026-06-19'
  and (type = 'insert'
    or (type = 'update' and old['sku_num'] is not null and cast(data.sku_num as int) > cast(old['sku_num'] as int)));

```

### 4.2 下单事务事实表

```sql
DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                     STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT '商品ID',
    `province_id`          STRING COMMENT '省份ID',
    `activity_id`          STRING COMMENT '参与活动ID',
    `activity_rule_id`    STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `date_id`               STRING COMMENT '下单日期ID',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`                BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

```

#### 4.2.1 数据装载

#### 4.2.2 首日装载
```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.create_time,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods_order_detail_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert'
) od
left join
(
    select
        data.id,
        data.user_id,
        data.province_id
    from ods_order_info_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods_order_detail_activity_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods_order_detail_coupon_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert'
) cou
on od.id = cou.order_detail_id;


```
#### 4.2.3 每日装载
```sql
SET hive.execution.engine=mr;
insert overwrite table dwd_trade_order_detail_inc partition (dt='2026-06-19')
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        date_format(data.create_time, 'yyyy-MM-dd') date_id,
        data.create_time,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods_order_detail_inc
    where dt = '2026-06-19'
    and type = 'insert'
) od
left join
(
    select
        data.id,
        data.user_id,
        data.province_id
    from ods_order_info_inc
    where dt = '2026-06-19'
    and type = 'insert'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods_order_detail_activity_inc
    where dt = '2026-06-19'
    and type = 'insert'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods_order_detail_coupon_inc
    where dt = '2026-06-19'
    and type = 'insert'
) cou
on od.id = cou.order_detail_id;


```

#### 4.2.4 交易域购物车周期快照事实表

```sql
DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`         STRING COMMENT '编号',
    `user_id`   STRING COMMENT '用户ID',
    `sku_id`    STRING COMMENT 'SKU_ID',
    `sku_name`  STRING COMMENT '商品名称',
    `sku_num`   BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

#### 4.2.10 数据装载

```sql
SET hive.execution.engine=mr;
insert overwrite table dwd_trade_cart_full partition(dt='2026-06-18')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info_full
where dt='2026-06-18'
and is_ordered='0';

```

#### 4.2.5 交易域交易流程累积快照事实表

```sql
DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
(
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `province_id`           STRING COMMENT '省份ID',
    `order_date_id`         STRING COMMENT '下单日期ID',
    `order_time`             STRING COMMENT '下单时间',
    `payment_date_id`        STRING COMMENT '支付日期ID',
    `payment_time`           STRING COMMENT '支付时间',
    `finish_date_id`         STRING COMMENT '确认收货日期ID',
    `finish_time`             STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    `payment_amount`         DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
TBLPROPERTIES ('orc.compress' = 'snappy');
```

#### 4.2.11 首日装载

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition(dt)
select
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    date_format(finish_time,'yyyy-MM-dd'),
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    nvl(date_format(finish_time,'yyyy-MM-dd'),'9999-12-31')
from
(
    select
        data.id,
        data.user_id,
        data.province_id,
        data.create_time,
        data.original_total_amount,
        data.activity_reduce_amount,
        data.coupon_reduce_amount,
        data.total_amount
    from ods_order_info_inc
    where dt='2026-06-18'
    and type='bootstrap-insert'
)oi
left join
(
    select
        data.order_id,
        data.callback_time,
        data.total_amount payment_amount
    from ods_payment_info_inc
    where dt='2026-06-18'
    and type='bootstrap-insert'
    and data.payment_status='1602'
)pi
on oi.id=pi.order_id
left join
(
    select
        data.order_id,
        data.create_time finish_time
    from ods_order_status_log_inc
    where dt='2026-06-18'
    and type='bootstrap-insert'
    and data.order_status='1004'
)log
on oi.id=log.order_id;

```

#### 4.2.12 每日装载

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition(dt)
select
    oi.order_id,
    user_id,
    province_id,
    order_date_id,
    order_time,
    nvl(oi.payment_date_id,pi.payment_date_id),
    nvl(oi.payment_time,pi.payment_time),
    nvl(oi.finish_date_id,log.finish_date_id),
    nvl(oi.finish_time,log.finish_time),
    order_original_amount,
    order_activity_amount,
    order_coupon_amount,
    order_total_amount,
    nvl(oi.payment_amount,pi.payment_amount),
    nvl(nvl(oi.finish_time,log.finish_time),'9999-12-31')
from
(
    select
        order_id,
        user_id,
        province_id,
        order_date_id,
        order_time,
        payment_date_id,
        payment_time,
        finish_date_id,
        finish_time,
        order_original_amount,
        order_activity_amount,
        order_coupon_amount,
        order_total_amount,
        payment_amount
    from dwd_trade_trade_flow_acc
    where dt='9999-12-31'
    union all
    select
        data.id,
        data.user_id,
        data.province_id,
        date_format(data.create_time,'yyyy-MM-dd') order_date_id,
        data.create_time,
        null payment_date_id,
        null payment_time,
        null finish_date_id,
        null finish_time,
        data.original_total_amount,
        data.activity_reduce_amount,
        data.coupon_reduce_amount,
        data.total_amount,
        null payment_amount
    from ods_order_info_inc
    where dt='2026-06-19'
    and type='insert'
)oi
left join
(
    select
        data.order_id,
        date_format(data.callback_time,'yyyy-MM-dd') payment_date_id,
        data.callback_time payment_time,
        data.total_amount payment_amount
    from ods_payment_info_inc
    where dt='2026-06-19'
    and type='update'
    and array_contains(map_keys(old),'payment_status')
    and data.payment_status='1602'
)pi
on oi.order_id=pi.order_id
left join
(
    select
        data.order_id,
        date_format(data.create_time,'yyyy-MM-dd') finish_date_id,
        data.create_time finish_time
    from ods_order_status_log_inc
    where dt='2026-06-19'
    and type='insert'
    and data.order_status='1004'
)log
on oi.order_id=log.order_id;

```

#### 4.2.6 工具域优惠券使用事务事实表

```sql
DROP TABLE IF EXISTS dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_used_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
```

#### 4.2.13 首日装载

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_tool_coupon_used_inc partition(dt)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time,'yyyy-MM-dd') date_id,
    data.used_time,
    date_format(data.used_time,'yyyy-MM-dd')
from ods_coupon_use_inc
where dt='2026-06-18'
and type='bootstrap-insert'
and data.used_time is not null;

```

#### 4.2.14 每日装载

```sql
SET hive.execution.engine=mr;
insert overwrite table dwd_tool_coupon_used_inc partition(dt='2026-06-19')
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(data.used_time,'yyyy-MM-dd') date_id,
    data.used_time
from ods_coupon_use_inc
where dt='2026-06-19'
and type='update'
and array_contains(map_keys(old),'used_time');

```

#### 4.2.7 互动域收藏商品事务事实表

```sql
DROP TABLE IF EXISTS dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
```

#### 4.2.15 首日装载

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add_inc partition(dt)
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time,
    date_format(data.create_time,'yyyy-MM-dd')
from ods_favor_info_inc
where dt='2026-06-18'
and type = 'bootstrap-insert';

```

#### 4.2.16 每日装载

```sql
SET hive.execution.engine=mr;
insert overwrite table dwd_interaction_favor_add_inc partition(dt='2026-06-19')
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time
from ods_favor_info_inc
where dt='2026-06-19'
and type = 'insert';

```

#### 4.2.8 用户域用户注册事务事实表

```sql
DROP TABLE IF EXISTS dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`          STRING COMMENT '用户ID',
    `date_id`          STRING COMMENT '日期ID',
    `create_time`     STRING COMMENT '注册时间',
    `channel`          STRING COMMENT '应用下载渠道',
    `province_id`     STRING COMMENT '省份ID',
    `version_code`    STRING COMMENT '应用版本',
    `mid_id`           STRING COMMENT '设备ID',
    `brand`            STRING COMMENT '设备品牌',
    `model`            STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
```

#### 4.2.17 首日装载

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_user_register_inc partition(dt)
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        data.id user_id,
        data.create_time
    from ods_user_info_inc
    where dt='2026-06-18'
    and type='bootstrap-insert'
)ui
left join
(
    select
        common.ar province_id,
        common.ba brand,
        common.ch channel,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code
    from ods_log_inc
    where dt='2026-06-18'
    and page.page_id='register'
    and common.uid is not null
)log
on ui.user_id=log.user_id;

```

#### 4.2.18 每日装载

```sql
SET hive.execution.engine=mr;
insert overwrite table dwd_user_register_inc partition(dt='2026-06-19')
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
(
    select
        data.id user_id,
        data.create_time
    from ods_user_info_inc
    where dt='2026-06-19'
    and type='insert'
)ui
left join
(
    select
        common.ar province_id,
        common.ba brand,
        common.ch channel,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code
    from ods_log_inc
    where dt='2026-06-19'
    and page.page_id='register'
    and common.uid is not null
)log
on ui.user_id=log.user_id;

```

#### 4.2.9 用户域用户登录事务事实表

```sql
DROP TABLE IF EXISTS dwd_user_login_inc;
CREATE EXTERNAL TABLE dwd_user_login_inc
(
    `user_id`         STRING COMMENT '用户ID',
    `date_id`         STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`         STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`          STRING COMMENT '设备ID',
    `brand`           STRING COMMENT '设备品牌',
    `model`           STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_login_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
```

#### 4.2.19 数据装载

```sql
SET hive.execution.engine=mr;
insert overwrite table dwd_user_login_inc partition (dt = '2026-06-18')
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select user_id,
                channel,
                province_id,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts
         from (select common.uid user_id,
                      common.ch  channel,
                      common.ar  province_id,
                      common.vc  version_code,
                      common.mid mid_id,
                      common.ba  brand,
                      common.md  model,
                      common.os  operate_system,
                      ts,
                      row_number() over (partition by common.sid order by ts) rn
               from ods_log_inc
               where dt = '2026-06-18'
                 and page is not null
                 and common.uid is not null) t1
         where rn = 1
     ) t2;

```

### 4.3 支付成功事务事实表

```sql
DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                      STRING COMMENT '编号',
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `sku_id`                 STRING COMMENT 'SKU_ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`     STRING COMMENT '参与活动规则ID',
    `coupon_id`              STRING COMMENT '使用优惠券ID',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`                STRING COMMENT '支付日期ID',
    `callback_time`         STRING COMMENT '支付成功时间',
    `sku_num`                 BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

```

#### 4.3.1 数据装载

#### 4.3.2 首日装载
```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    date_format(callback_time,'yyyy-MM-dd')
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods_order_detail_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert'
) od
join
(
    select
        data.user_id,
        data.order_id,
        data.payment_type,
        data.callback_time
    from ods_payment_info_inc
    where dt='2026-06-18'
    and type='bootstrap-insert'
    and data.payment_status='1602'
) pi
on od.order_id=pi.order_id
left join
(
    select
        data.id,
        data.province_id
    from ods_order_info_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert'
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods_order_detail_activity_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods_order_detail_coupon_inc
    where dt = '2026-06-18'
    and type = 'bootstrap-insert'
) cou
on od.id = cou.order_detail_id
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic_full
    where dt='2026-06-18'
    and parent_code='11'
) pay_dic
on pi.payment_type=pay_dic.dic_code;

```
#### 4.3.3 每日装载
```sql
SET hive.execution.engine=mr;
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt='2026-06-19')
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods_order_detail_inc
    where (dt = '2026-06-19' or dt = date_add('2026-06-19',-1))
    and (type = 'insert' or type = 'bootstrap-insert')
) od
join
(
    select
        data.user_id,
        data.order_id,
        data.payment_type,
        data.callback_time
    from ods_payment_info_inc
    where dt='2026-06-19'
    and type='update'
    and array_contains(map_keys(old),'payment_status')
    and data.payment_status='1602'
) pi
on od.order_id=pi.order_id
left join
(
    select
        data.id,
        data.province_id
    from ods_order_info_inc
    where (dt = '2026-06-19' or dt = date_add('2026-06-19',-1))
    and (type = 'insert' or type = 'bootstrap-insert')
) oi
on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods_order_detail_activity_inc
    where (dt = '2026-06-19' or dt = date_add('2026-06-19',-1))
    and (type = 'insert' or type = 'bootstrap-insert')
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods_order_detail_coupon_inc
    where (dt = '2026-06-19' or dt = date_add('2026-06-19',-1))
    and (type = 'insert' or type = 'bootstrap-insert')
) cou
on od.id = cou.order_detail_id
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic_full
    where dt='2026-06-19'
    and parent_code='11'
) pay_dic
on pi.payment_type=pay_dic.dic_code;

```

### 4.4 页面浏览事务事实表（流量域）

```sql
DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`       STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页ID',
    `page_id`          STRING COMMENT '页面ID ',
    `from_pos_id`     STRING COMMENT '点击坑位ID',
    `from_pos_seq`    STRING COMMENT '点击坑位位置',
    `refer_id`         STRING COMMENT '营销渠道ID',
    `date_id`          STRING COMMENT '日期ID',
    `view_time`       STRING COMMENT '跳入时间',
    `session_id`      STRING COMMENT '所属会话ID',
    `during_time`     BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

> 页面浏览事实表从 ODS 层 `ods_log_inc` 的 actions 数组展开（lateral view explode），同时关联 common 和 page 结构体中的环境信息。

#### 4.4.1 数据装载
```sql
SET hive.execution.engine=mr;
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_page_view_inc partition (dt='2026-06-18')
select
    common.ar province_id,
    common.ba brand,
    common.ch channel,
    common.is_new is_new,
    common.md model,
    common.mid mid_id,
    common.os operate_system,
    common.uid user_id,
    common.vc version_code,
    page.item page_item,
    page.item_type page_item_type,
    page.last_page_id,
    page.page_id,
    page.from_pos_id,
    page.from_pos_seq,
    page.refer_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    common.sid session_id,
    page.during_time
from ods_log_inc
where dt='2026-06-18'
and page is not null;
set hive.cbo.enable=true;

```
---

### 4.5 DWD 层装载脚本

#### 4.5.1 首日装载脚本（ods_to_dwd_init.sh）

```bash
sudo vim /home/hadoop/bin/ods_to_dwd_init.sh
```

```bash
#!/bin/bash
APP=gmall
if [ -n "$2" ] ;then do_date=$2; else echo "请传入日期参数"; exit; fi

dwd_trade_cart_add_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_trade_cart_add_inc partition (dt)
select data.id, data.user_id, data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time, data.sku_num,
       date_format(data.create_time, 'yyyy-MM-dd')
from ${APP}.ods_cart_info_inc
where dt = '$do_date' and type = 'bootstrap-insert';
"
dwd_trade_order_detail_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_trade_order_detail_inc partition (dt)
select od.id, order_id, user_id, sku_id, province_id,
       activity_id, activity_rule_id, coupon_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time, sku_num, split_original_amount,
       nvl(split_activity_amount,0.0), nvl(split_coupon_amount,0.0),
       split_total_amount, date_format(create_time,'yyyy-MM-dd')
from (
    select data.id, data.order_id, data.sku_id, data.create_time, data.sku_num,
           data.sku_num * data.order_price split_original_amount,
           data.split_total_amount, data.split_activity_amount, data.split_coupon_amount
    from ${APP}.ods_order_detail_inc where dt = '$do_date' and type = 'bootstrap-insert'
) od
left join (
    select data.id, data.user_id, data.province_id
    from ${APP}.ods_order_info_inc where dt = '$do_date' and type = 'bootstrap-insert'
) oi on od.order_id = oi.id
left join (
    select data.order_detail_id, data.activity_id, data.activity_rule_id
    from ${APP}.ods_order_detail_activity_inc where dt = '$do_date' and type = 'bootstrap-insert'
) act on od.id = act.order_detail_id
left join (
    select data.order_detail_id, data.coupon_id
    from ${APP}.ods_order_detail_coupon_inc where dt = '$do_date' and type = 'bootstrap-insert'
) cou on od.id = cou.order_detail_id;
"
dwd_trade_pay_detail_suc_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_trade_pay_detail_suc_inc partition (dt)
select od.id, od.order_id, od.user_id, od.sku_id, od.province_id,
       od.activity_id, od.activity_rule_id, od.coupon_id,
       payment_type_code, payment_type_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time, od.sku_num,
       od.split_original_amount, od.split_activity_amount,
       od.split_coupon_amount, od.split_total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (
    select data.id, data.order_id, data.payment_type payment_type_code,
           data.callback_time, data.total_amount
    from ${APP}.ods_payment_info_inc
    where dt = '$do_date' and type = 'bootstrap-insert' and data.payment_status='1602'
) pi
left join ${APP}.dwd_trade_order_detail_inc od on pi.order_id = od.order_id and od.dt = '$do_date'
left join (
    select dic_code, dic_name payment_type_name
    from ${APP}.ods_base_dic_full where dt='$do_date' and parent_code='11'
) dic on pi.payment_type_code = dic.dic_code;
"
dwd_trade_cart_full="
insert overwrite table ${APP}.dwd_trade_cart_full partition(dt='$do_date')
select id, user_id, sku_id, sku_name, sku_num
from ${APP}.ods_cart_info_full
where dt='$do_date' and is_ordered='0';
"
dwd_trade_trade_flow_acc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_trade_trade_flow_acc partition(dt)
select oi.id, user_id, province_id,
       date_format(create_time,'yyyy-MM-dd'), create_time,
       date_format(callback_time,'yyyy-MM-dd'), callback_time,
       date_format(finish_time,'yyyy-MM-dd'), finish_time,
       original_total_amount, activity_reduce_amount, coupon_reduce_amount, total_amount,
       nvl(payment_amount,0.0), nvl(date_format(finish_time,'yyyy-MM-dd'),'9999-12-31')
from (
    select data.id, data.user_id, data.province_id, data.create_time,
           data.original_total_amount, data.activity_reduce_amount,
           data.coupon_reduce_amount, data.total_amount
    from ${APP}.ods_order_info_inc where dt='$do_date' and type='bootstrap-insert'
) oi
left join (
    select data.order_id, data.callback_time, data.total_amount payment_amount
    from ${APP}.ods_payment_info_inc
    where dt='$do_date' and type='bootstrap-insert' and data.payment_status='1602'
) pi on oi.id=pi.order_id
left join (
    select data.order_id, data.create_time finish_time
    from ${APP}.ods_order_status_log_inc
    where dt='$do_date' and type='bootstrap-insert' and data.order_status='1004'
) log on oi.id=log.order_id;
"
dwd_tool_coupon_used_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_tool_coupon_used_inc partition(dt)
select data.id, data.coupon_id, data.user_id, data.order_id,
       date_format(data.used_time,'yyyy-MM-dd') date_id, data.used_time,
       date_format(data.used_time,'yyyy-MM-dd')
from ${APP}.ods_coupon_use_inc
where dt='$do_date' and type='bootstrap-insert' and data.used_time is not null;
"
dwd_interaction_favor_add_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_interaction_favor_add_inc partition(dt)
select data.id, data.user_id, data.sku_id,
       date_format(data.create_time,'yyyy-MM-dd') date_id, data.create_time,
       date_format(data.create_time,'yyyy-MM-dd')
from ${APP}.ods_favor_info_inc
where dt='$do_date' and type='bootstrap-insert';
"
dwd_user_register_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_user_register_inc partition(dt)
select ui.user_id, date_format(create_time,'yyyy-MM-dd') date_id, create_time,
       channel, province_id, version_code, mid_id, brand, model, operate_system,
       date_format(create_time,'yyyy-MM-dd')
from (
    select data.id user_id, data.create_time
    from ${APP}.ods_user_info_inc where dt='$do_date' and type='bootstrap-insert'
) ui
left join (
    select common.ar province_id, common.ba brand, common.ch channel,
           common.md model, common.mid mid_id, common.os operate_system,
           common.uid user_id, common.vc version_code
    from ${APP}.ods_log_inc
    where dt='$do_date' and page.page_id='register' and common.uid is not null
) log on ui.user_id=log.user_id;
"
dwd_user_login_inc="
insert overwrite table ${APP}.dwd_user_login_inc partition(dt='$do_date')
select user_id,
       date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
       date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') login_time,
       channel, province_id, version_code, mid_id, brand, model, operate_system
from (
    select user_id, channel, province_id, version_code, mid_id, brand, model, operate_system, ts
    from (
        select common.uid user_id, common.ch channel, common.ar province_id,
               common.vc version_code, common.mid mid_id, common.ba brand,
               common.md model, common.os operate_system, ts,
               row_number() over (partition by common.sid order by ts) rn
        from ${APP}.ods_log_inc
        where dt='$do_date' and page is not null and common.uid is not null
    ) t1 where rn=1
) t2;
"
dwd_traffic_page_view_inc="
set hive.cbo.enable=false;
insert overwrite table ${APP}.dwd_traffic_page_view_inc partition(dt='$do_date')
select common.ar province_id, common.ba brand, common.ch channel, common.is_new is_new,
       common.md model, common.mid mid_id, common.os operate_system, common.uid user_id,
       common.vc version_code, page.item page_item, page.item_type page_item_type,
       page.last_page_id, page.page_id, page.from_pos_id, page.from_pos_seq, page.refer_id,
       date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
       date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
       common.sid session_id, page.during_time
from ${APP}.ods_log_inc
where dt='$do_date' and page is not null;
set hive.cbo.enable=true;
"

case $1 in
    "dwd_trade_cart_add_inc")       hive -e "$dwd_trade_cart_add_inc" ;;
    "dwd_trade_order_detail_inc")   hive -e "$dwd_trade_order_detail_inc" ;;
    "dwd_trade_cart_full")          hive -e "$dwd_trade_cart_full" ;;
    "dwd_trade_trade_flow_acc")     hive -e "$dwd_trade_trade_flow_acc" ;;
    "dwd_tool_coupon_used_inc")     hive -e "$dwd_tool_coupon_used_inc" ;;
    "dwd_interaction_favor_add_inc") hive -e "$dwd_interaction_favor_add_inc" ;;
    "dwd_user_register_inc")        hive -e "$dwd_user_register_inc" ;;
    "dwd_user_login_inc")           hive -e "$dwd_user_login_inc" ;;
    "dwd_trade_pay_detail_suc_inc") hive -e "$dwd_trade_pay_detail_suc_inc" ;;
    "dwd_traffic_page_view_inc")    hive -e "$dwd_traffic_page_view_inc" ;;
    "all") hive -e "SET hive.execution.engine=mr; SET hive.exec.parallel=false; $dwd_trade_cart_add_inc$dwd_trade_order_detail_inc$dwd_trade_cart_full$dwd_trade_trade_flow_acc$dwd_tool_coupon_used_inc$dwd_interaction_favor_add_inc$dwd_user_register_inc$dwd_user_login_inc$dwd_trade_pay_detail_suc_inc$dwd_traffic_page_view_inc SET hive.execution.engine=spark; SET hive.exec.parallel=true;" ;;
esac
```

```bash
sudo chmod +x /home/hadoop/bin/ods_to_dwd_init.sh
# 用法: ods_to_dwd_init.sh all 2026-06-18
```

#### 4.5.2 每日装载脚本（ods_to_dwd.sh）

```bash
sudo vim /home/hadoop/bin/ods_to_dwd.sh
```

```bash
#!/bin/bash
APP=gmall
if [ -n "$2" ] ;then do_date=$2; else do_date=`date -d "-1 day" +%F`; fi

dwd_trade_cart_add_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_trade_cart_add_inc partition(dt)
select data.id, data.user_id, data.sku_id,
       date_format(data.create_time,'yyyy-MM-dd'), data.create_time, data.sku_num,
       date_format(data.create_time,'yyyy-MM-dd')
from ${APP}.ods_cart_info_inc where dt='$do_date' and type='insert';
"
dwd_trade_order_detail_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_trade_order_detail_inc partition(dt)
select od.id, order_id, user_id, sku_id, province_id,
       activity_id, activity_rule_id, coupon_id,
       date_format(create_time,'yyyy-MM-dd'), create_time, sku_num,
       split_original_amount, nvl(split_activity_amount,0.0),
       nvl(split_coupon_amount,0.0), split_total_amount,
       date_format(create_time,'yyyy-MM-dd')
from (
    select data.id, data.order_id, data.sku_id, data.create_time, data.sku_num,
           data.sku_num*data.order_price split_original_amount,
           data.split_total_amount, data.split_activity_amount, data.split_coupon_amount
    from ${APP}.ods_order_detail_inc where dt='$do_date' and type='insert'
) od
left join (
    select data.id, data.user_id, data.province_id
    from ${APP}.ods_order_info_inc where dt='$do_date' and type='insert'
) oi on od.order_id=oi.id
left join (
    select data.order_detail_id, data.activity_id, data.activity_rule_id
    from ${APP}.ods_order_detail_activity_inc where dt='$do_date' and type='insert'
) act on od.id=act.order_detail_id
left join (
    select data.order_detail_id, data.coupon_id
    from ${APP}.ods_order_detail_coupon_inc where dt='$do_date' and type='insert'
) cou on od.id=cou.order_detail_id;
"
dwd_trade_pay_detail_suc_inc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_trade_pay_detail_suc_inc partition(dt)
select od.id, od.order_id, od.user_id, od.sku_id, od.province_id,
       od.activity_id, od.activity_rule_id, od.coupon_id,
       payment_type_code, payment_type_name,
       date_format(callback_time,'yyyy-MM-dd'), callback_time, od.sku_num,
       od.split_original_amount, od.split_activity_amount,
       od.split_coupon_amount, od.split_total_amount,
       date_format(callback_time,'yyyy-MM-dd')
from (
    select data.id, data.order_id, data.payment_type payment_type_code,
           data.callback_time, data.total_amount
    from ${APP}.ods_payment_info_inc
    where dt='$do_date' and type='update'
      and array_contains(map_keys(old),'payment_status')
      and data.payment_status='1602'
) pi
left join ${APP}.dwd_trade_order_detail_inc od on pi.order_id=od.order_id and od.dt='$do_date'
left join (
    select dic_code, dic_name payment_type_name
    from ${APP}.ods_base_dic_full where dt='$do_date' and parent_code='11'
) dic on pi.payment_type_code=dic.dic_code;
"
dwd_trade_trade_flow_acc="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dwd_trade_trade_flow_acc partition(dt)
select oi.order_id, user_id, province_id,
       order_date_id, order_time,
       nvl(oi.payment_date_id,pi.payment_date_id),
       nvl(oi.payment_time,pi.payment_time),
       nvl(oi.finish_date_id,log.finish_date_id),
       nvl(oi.finish_time,log.finish_time),
       order_original_amount, order_activity_amount, order_coupon_amount, order_total_amount,
       nvl(oi.payment_amount,pi.payment_amount),
       nvl(nvl(oi.finish_time,log.finish_time),'9999-12-31')
from (
    select order_id, user_id, province_id, order_date_id, order_time,
           payment_date_id, payment_time, finish_date_id, finish_time,
           order_original_amount, order_activity_amount, order_coupon_amount,
           order_total_amount, payment_amount
    from ${APP}.dwd_trade_trade_flow_acc where dt='9999-12-31'
    union all
    select data.id, data.user_id, data.province_id,
           date_format(data.create_time,'yyyy-MM-dd') order_date_id, data.create_time,
           null payment_date_id, null payment_time,
           null finish_date_id, null finish_time,
           data.original_total_amount, data.activity_reduce_amount,
           data.coupon_reduce_amount, data.total_amount, null payment_amount
    from ${APP}.ods_order_info_inc where dt='$do_date' and type='insert'
) oi
left join (
    select data.order_id,
           date_format(data.callback_time,'yyyy-MM-dd') payment_date_id,
           data.callback_time payment_time, data.total_amount payment_amount
    from ${APP}.ods_payment_info_inc
    where dt='$do_date' and type='update'
      and array_contains(map_keys(old),'payment_status')
      and data.payment_status='1602'
) pi on oi.order_id=pi.order_id
left join (
    select data.order_id,
           date_format(data.create_time,'yyyy-MM-dd') finish_date_id,
           data.create_time finish_time
    from ${APP}.ods_order_status_log_inc
    where dt='$do_date' and type='insert' and data.order_status='1004'
) log on oi.order_id=log.order_id;
"
dwd_tool_coupon_used_inc="
insert overwrite table ${APP}.dwd_tool_coupon_used_inc partition(dt='$do_date')
select data.id, data.coupon_id, data.user_id, data.order_id,
       date_format(data.used_time,'yyyy-MM-dd') date_id, data.used_time
from ${APP}.ods_coupon_use_inc
where dt='$do_date' and type='update'
  and array_contains(map_keys(old),'used_time');
"
dwd_interaction_favor_add_inc="
insert overwrite table ${APP}.dwd_interaction_favor_add_inc partition(dt='$do_date')
select data.id, data.user_id, data.sku_id,
       date_format(data.create_time,'yyyy-MM-dd') date_id, data.create_time
from ${APP}.ods_favor_info_inc
where dt='$do_date' and type='insert';
"
dwd_user_register_inc="
insert overwrite table ${APP}.dwd_user_register_inc partition(dt='$do_date')
select ui.user_id, date_format(create_time,'yyyy-MM-dd') date_id, create_time,
       channel, province_id, version_code, mid_id, brand, model, operate_system
from (
    select data.id user_id, data.create_time
    from ${APP}.ods_user_info_inc where dt='$do_date' and type='insert'
) ui
left join (
    select common.ar province_id, common.ba brand, common.ch channel,
           common.md model, common.mid mid_id, common.os operate_system,
           common.uid user_id, common.vc version_code
    from ${APP}.ods_log_inc
    where dt='$do_date' and page.page_id='register' and common.uid is not null
) log on ui.user_id=log.user_id;
"

case $1 in
    "dwd_trade_cart_add_inc")       hive -e "$dwd_trade_cart_add_inc" ;;
    "dwd_trade_order_detail_inc")   hive -e "$dwd_trade_order_detail_inc" ;;
    "dwd_trade_trade_flow_acc")     hive -e "$dwd_trade_trade_flow_acc" ;;
    "dwd_tool_coupon_used_inc")     hive -e "$dwd_tool_coupon_used_inc" ;;
    "dwd_interaction_favor_add_inc") hive -e "$dwd_interaction_favor_add_inc" ;;
    "dwd_user_register_inc")        hive -e "$dwd_user_register_inc" ;;
    "dwd_trade_pay_detail_suc_inc") hive -e "$dwd_trade_pay_detail_suc_inc" ;;
    "all") hive -e "SET hive.execution.engine=mr; SET hive.exec.parallel=false; $dwd_trade_cart_add_inc$dwd_trade_order_detail_inc$dwd_trade_trade_flow_acc$dwd_tool_coupon_used_inc$dwd_interaction_favor_add_inc$dwd_user_register_inc$dwd_trade_pay_detail_suc_inc SET hive.execution.engine=spark; SET hive.exec.parallel=true;" ;;
esac
```

```bash
sudo chmod +x /home/hadoop/bin/ods_to_dwd.sh
# 用法: ods_to_dwd.sh all 2026-06-19
```

---

## 第五部分：阶段验证清单

| # | 验证项 | SQL | 预期 |
|---|--------|-----|------|
| 1 | 商品维度表有数据 | `SELECT COUNT(*) FROM dim_sku_full` | >0 |
| 2 | 优惠券维度表有数据 | `SELECT COUNT(*) FROM dim_coupon_full` | >0 |
| 3 | 加购事实表有数据 | `SELECT COUNT(*) FROM dwd_trade_cart_add_inc` | >0 |
| 4 | 下单事实表有数据 | `SELECT COUNT(*) FROM dwd_trade_order_detail_inc` | >0 |
| 5 | 支付事实表有数据 | `SELECT COUNT(*) FROM dwd_trade_pay_detail_suc_inc` | >0 |

---

---

> **DIM + DWD 完成** | DWS 汇总层见下文

## 第六部分：DWS 层（汇总层）构建

> 对应文档：《电商数据仓库系统》V6.0 第10章
> 目标：9张1日汇总表 + 2张n日汇总表 + 2张历史至今汇总表

---

**DWS 层设计要点：**

- 参考指标体系设计，基于 DWD 层事实表 + DIM 层维度表汇总
- 存储格式：ORC + Snappy 压缩
- 命名规范：`dws_数据域_统计粒度_业务过程_统计周期（1d/nd/td）`
  - 1d：最近1日，7d/30d：最近n日，td：历史至今

---

### 6.1 最近1日汇总表

#### 6.1.1 交易域用户商品粒度订单最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_trade_user_sku_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_1d
(
    `user_id`                   STRING COMMENT '用户ID',
    `sku_id`                    STRING COMMENT 'SKU_ID',
    `sku_name`                  STRING COMMENT 'SKU名称',
    `category1_id`              STRING COMMENT '一级品类ID',
    `category1_name`            STRING COMMENT '一级品类名称',
    `category2_id`              STRING COMMENT '二级品类ID',
    `category2_name`            STRING COMMENT '二级品类名称',
    `category3_id`              STRING COMMENT '三级品类ID',
    `category3_name`            STRING COMMENT '三级品类名称',
    `tm_id`                     STRING COMMENT '品牌ID',
    `tm_name`                   STRING COMMENT '品牌名称',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

数据装载——从 `dwd_trade_order_detail_inc` 聚合后 JOIN `dim_sku_full` 补充维度：

**首日装载**

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
insert overwrite table dws_trade_user_sku_order_1d partition(dt)
select
    user_id, id, sku_name,
    category1_id, category1_name, category2_id, category2_name,
    category3_id, category3_name, tm_id, tm_name,
    order_count_1d, order_num_1d,
    order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d, dt
from
(
    select
        dt, user_id, sku_id,
        count(*) order_count_1d,
        sum(sku_num) order_num_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0.0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0.0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from dwd_trade_order_detail_inc
    group by dt,user_id,sku_id
) od
left join
(
    select id, sku_name, category1_id, category1_name,
           category2_id, category2_name, category3_id, category3_name,
           tm_id, tm_name
    from dim_sku_full
    where dt='2026-06-18'
) sku on od.sku_id=sku.id;
set hive.vectorized.execution.enabled = true;
```

**每日装载**

```sql
SET hive.execution.engine=mr;
set hive.vectorized.execution.enabled = false;
insert overwrite table dws_trade_user_sku_order_1d partition(dt='2026-06-19')
select
    user_id, id, sku_name,
    category1_id, category1_name, category2_id, category2_name,
    category3_id, category3_name, tm_id, tm_name,
    order_count_1d, order_num_1d,
    order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d
from
(
    select
        user_id, sku_id,
        count(*) order_count_1d,
        sum(sku_num) order_num_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from dwd_trade_order_detail_inc
    where dt='2026-06-19'
    group by user_id,sku_id
) od
left join
(
    select id, sku_name, category1_id, category1_name,
           category2_id, category2_name, category3_id, category3_name,
           tm_id, tm_name
    from dim_sku_full
    where dt='2026-06-19'
) sku on od.sku_id=sku.id;
set hive.vectorized.execution.enabled = true;
```

#### 6.1.2 交易域用户粒度订单最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_trade_user_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

**首日装载**

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_order_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount),
    dt
from dwd_trade_order_detail_inc
group by user_id,dt;
```

**每日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_trade_user_order_1d partition(dt='2026-06-19')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount)
from dwd_trade_order_detail_inc
where dt='2026-06-19'
group by user_id;
```

#### 6.1.3 交易域用户粒度加购最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_trade_user_cart_add_1d;
CREATE EXTERNAL TABLE dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) COMMENT '交易域用户粒度加购最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_cart_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

**首日装载**

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_cart_add_1d partition(dt)
select
    user_id,
    count(*),
    sum(sku_num),
    dt
from dwd_trade_cart_add_inc
group by user_id,dt;
```

**每日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_trade_user_cart_add_1d partition(dt='2026-06-19')
select
    user_id,
    count(*),
    sum(sku_num)
from dwd_trade_cart_add_inc
where dt='2026-06-19'
group by user_id;
```

#### 6.1.4 交易域用户粒度支付最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_trade_user_payment_1d;
CREATE EXTERNAL TABLE dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '交易域用户粒度支付最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

**首日装载**

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_payment_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount),
    dt
from dwd_trade_pay_detail_suc_inc
group by user_id,dt;
```

**每日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_trade_user_payment_1d partition(dt='2026-06-19')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount)
from dwd_trade_pay_detail_suc_inc
where dt='2026-06-19'
group by user_id;
```

#### 6.1.5 交易域省份粒度订单最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_trade_province_order_1d;
CREATE EXTERNAL TABLE dws_trade_province_order_1d
(
    `province_id`               STRING COMMENT '省份ID',
    `province_name`             STRING COMMENT '省份名称',
    `area_code`                 STRING COMMENT '地区编码',
    `iso_code`                  STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                STRING COMMENT '新版国际标准地区编码',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域省份粒度订单最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

**首日装载**

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_province_order_1d partition(dt)
select
    province_id, province_name, area_code, iso_code, iso_3166_2,
    order_count_1d, order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d, dt
from
(
    select
        province_id,
        count(distinct(order_id)) order_count_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d,
        dt
    from dwd_trade_order_detail_inc
    group by province_id,dt
) o
left join
(
    select id, province_name, area_code, iso_code, iso_3166_2
    from dim_province_full
    where dt='2026-06-18'
) p on o.province_id=p.id;
```

**每日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_trade_province_order_1d partition(dt='2026-06-19')
select
    province_id, province_name, area_code, iso_code, iso_3166_2,
    order_count_1d, order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d
from
(
    select
        province_id,
        count(distinct(order_id)) order_count_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from dwd_trade_order_detail_inc
    where dt='2026-06-19'
    group by province_id
) o
left join
(
    select id, province_name, area_code, iso_code, iso_3166_2
    from dim_province_full
    where dt='2026-06-19'
) p on o.province_id=p.id;
```

#### 6.1.6 工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_tool_user_coupon_coupon_used_1d;
CREATE EXTERNAL TABLE dws_tool_user_coupon_coupon_used_1d
(
    `user_id`          STRING COMMENT '用户ID',
    `coupon_id`        STRING COMMENT '优惠券ID',
    `coupon_name`      STRING COMMENT '优惠券名称',
    `coupon_type_code` STRING COMMENT '优惠券类型编码',
    `coupon_type_name` STRING COMMENT '优惠券类型名称',
    `benefit_rule`     STRING COMMENT '优惠规则',
    `used_count_1d`    BIGINT COMMENT '使用(支付)次数'
) COMMENT '工具域用户优惠券粒度优惠券使用(支付)最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_tool_user_coupon_coupon_used_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

**首日装载**

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition(dt)
select
    user_id, coupon_id, coupon_name, coupon_type_code,
    coupon_type_name, benefit_rule, used_count, dt
from
(
    select
        dt, user_id, coupon_id, count(*) used_count
    from dwd_tool_coupon_used_inc
    group by dt,user_id,coupon_id
) t1
left join
(
    select id, coupon_name, coupon_type_code, coupon_type_name, benefit_rule
    from dim_coupon_full
    where dt='2026-06-18'
) t2 on t1.coupon_id=t2.id;
```

**每日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_tool_user_coupon_coupon_used_1d partition(dt='2026-06-19')
select
    user_id, coupon_id, coupon_name, coupon_type_code,
    coupon_type_name, benefit_rule, used_count
from
(
    select
        user_id, coupon_id, count(*) used_count
    from dwd_tool_coupon_used_inc
    where dt='2026-06-19'
    group by user_id,coupon_id
) t1
left join
(
    select id, coupon_name, coupon_type_code, coupon_type_name, benefit_rule
    from dim_coupon_full
    where dt='2026-06-19'
) t2 on t1.coupon_id=t2.id;
```

#### 6.1.7 互动域商品粒度收藏商品最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_interaction_sku_favor_add_1d;
CREATE EXTERNAL TABLE dws_interaction_sku_favor_add_1d
(
    `sku_id`             STRING COMMENT 'SKU_ID',
    `sku_name`           STRING COMMENT 'SKU名称',
    `category1_id`       STRING COMMENT '一级品类ID',
    `category1_name`     STRING COMMENT '一级品类名称',
    `category2_id`       STRING COMMENT '二级品类ID',
    `category2_name`     STRING COMMENT '二级品类名称',
    `category3_id`       STRING COMMENT '三级品类ID',
    `category3_name`     STRING COMMENT '三级品类名称',
    `tm_id`              STRING COMMENT '品牌ID',
    `tm_name`            STRING COMMENT '品牌名称',
    `favor_add_count_1d` BIGINT COMMENT '商品被收藏次数'
) COMMENT '互动域商品粒度收藏商品最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_interaction_sku_favor_add_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

**首日装载**

```sql
SET hive.execution.engine=mr;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_interaction_sku_favor_add_1d partition(dt)
select
    sku_id, sku_name, category1_id, category1_name,
    category2_id, category2_name, category3_id, category3_name,
    tm_id, tm_name, favor_add_count, dt
from
(
    select
        dt, sku_id, count(*) favor_add_count
    from dwd_interaction_favor_add_inc
    group by dt,sku_id
) favor
left join
(
    select id, sku_name, category1_id, category1_name,
           category2_id, category2_name, category3_id, category3_name,
           tm_id, tm_name
    from dim_sku_full
    where dt='2026-06-18'
) sku on favor.sku_id=sku.id;
```

**每日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_interaction_sku_favor_add_1d partition(dt='2026-06-19')
select
    sku_id, sku_name, category1_id, category1_name,
    category2_id, category2_name, category3_id, category3_name,
    tm_id, tm_name, favor_add_count
from
(
    select
        sku_id, count(*) favor_add_count
    from dwd_interaction_favor_add_inc
    where dt='2026-06-19'
    group by sku_id
) favor
left join
(
    select id, sku_name, category1_id, category1_name,
           category2_id, category2_name, category3_id, category3_name,
           tm_id, tm_name
    from dim_sku_full
    where dt='2026-06-19'
) sku on favor.sku_id=sku.id;
```

#### 6.1.8 流量域会话粒度页面浏览最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_traffic_session_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_session_page_view_1d
(
    `session_id`     STRING COMMENT '会话ID',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '手机品牌',
    `model`          STRING COMMENT '手机型号',
    `operate_system` STRING COMMENT '操作系统',
    `version_code`   STRING COMMENT 'APP版本号',
    `channel`        STRING COMMENT '渠道',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `page_count_1d`  BIGINT COMMENT '最近1日浏览页面数'
) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_session_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

数据装载（流量表无历史分区，每次全量写入当日分区）：

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_traffic_session_page_view_1d partition(dt='2026-06-18')
select
    session_id, mid_id, brand, model, operate_system,
    version_code, channel,
    sum(during_time), count(*)
from dwd_traffic_page_view_inc
where dt='2026-06-18'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
```

#### 6.1.9 流量域访客页面粒度页面浏览最近1日汇总表

```sql
DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
(
    `mid_id`         STRING COMMENT '访客ID',
    `brand`          STRING COMMENT '手机品牌',
    `model`          STRING COMMENT '手机型号',
    `operate_system` STRING COMMENT '操作系统',
    `page_id`        STRING COMMENT '页面ID',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

数据装载：

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_traffic_page_visitor_page_view_1d partition(dt='2026-06-18')
select
    mid_id, brand, model, operate_system, page_id,
    sum(during_time), count(*)
from dwd_traffic_page_view_inc
where dt='2026-06-18'
group by mid_id,brand,model,operate_system,page_id;
```

---

### 6.2 最近n日汇总表

> n日汇总表基于对应的1日汇总表聚合，复用1日表的统计结果，减少重复计算。首日和每日逻辑相同——筛选最近30日1日表数据，通过 `sum(if(dt>=date_add('$do_date',-6),...))` 统计7日指标，`sum()` 统计30日指标。

#### 6.2.1 交易域用户商品粒度订单最近n日汇总表

```sql
DROP TABLE IF EXISTS dws_trade_user_sku_order_nd;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_nd
(
    `user_id`                     STRING COMMENT '用户ID',
    `sku_id`                      STRING COMMENT 'SKU_ID',
    `sku_name`                    STRING COMMENT 'SKU名称',
    `category1_id`               STRING COMMENT '一级品类ID',
    `category1_name`             STRING COMMENT '一级品类名称',
    `category2_id`               STRING COMMENT '二级品类ID',
    `category2_name`             STRING COMMENT '二级品类名称',
    `category3_id`               STRING COMMENT '三级品类ID',
    `category3_name`             STRING COMMENT '三级品类名称',
    `tm_id`                       STRING COMMENT '品牌ID',
    `tm_name`                     STRING COMMENT '品牌名称',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

数据装载：

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_trade_user_sku_order_nd partition(dt='2026-06-18')
select
    user_id, sku_id, sku_name,
    category1_id, category1_name, category2_id, category2_name,
    category3_id, category3_name, tm_id, tm_name,
    sum(if(dt>=date_add('2026-06-18',-6),order_count_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),order_num_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),order_total_amount_1d,0)),
    sum(order_count_1d), sum(order_num_1d),
    sum(order_original_amount_1d), sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d), sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where dt>=date_add('2026-06-18',-29)
group by user_id,sku_id,sku_name,category1_id,category1_name,
         category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
```

#### 6.2.2 交易域省份粒度订单最近n日汇总表

```sql
DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '省份ID',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                 STRING COMMENT '新版国际标准地区编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

数据装载：

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_trade_province_order_nd partition(dt='2026-06-18')
select
    province_id, province_name, area_code, iso_code, iso_3166_2,
    sum(if(dt>=date_add('2026-06-18',-6),order_count_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2026-06-18',-6),order_total_amount_1d,0)),
    sum(order_count_1d), sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d), sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
where dt>=date_add('2026-06-18',-29) and dt<='2026-06-18'
group by province_id,province_name,area_code,iso_code,iso_3166_2;
```

---

### 6.3 历史至今汇总表

> 历史至今（td）汇总表首日装载与每日装载逻辑不同。首日从1日表全量聚合，每日从td表昨日分区 + 1日表当日分区 full outer join 增量更新。

#### 6.3.1 交易域用户粒度订单历史至今汇总表

```sql
DROP TABLE IF EXISTS dws_trade_user_order_td;
CREATE EXTERNAL TABLE dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_date_first`          STRING COMMENT '历史至今首次下单日期',
    `order_date_last`           STRING COMMENT '历史至今末次下单日期',
    `order_count_td`            BIGINT COMMENT '历史至今下单次数',
    `order_num_td`              BIGINT COMMENT '历史至今购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '历史至今下单原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '历史至今下单活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '历史至今下单优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '历史至今下单最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

**首日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_trade_user_order_td partition(dt='2026-06-18')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count_td,
    sum(order_num_1d) order_num_td,
    sum(order_original_amount_1d) original_amount_td,
    sum(activity_reduce_amount_1d) activity_reduce_amount_td,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_td,
    sum(order_total_amount_1d) total_amount_td
from dws_trade_user_order_1d
group by user_id;
```

**每日装载（full outer join 实现）**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_trade_user_order_td partition(dt='2026-06-19')
select nvl(old.user_id, new.user_id),
       if(old.user_id is not null, old.order_date_first, '2026-06-19'),
       if(new.user_id is not null, '2026-06-19', old.order_date_last),
       nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
       nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
       nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
       nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
       nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
       nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
from (
    select user_id, order_date_first, order_date_last,
           order_count_td, order_num_td, original_amount_td,
           activity_reduce_amount_td, coupon_reduce_amount_td, total_amount_td
    from dws_trade_user_order_td
    where dt = date_add('2026-06-19', -1)
) old
full outer join
(
    select user_id, order_count_1d, order_num_1d,
           order_original_amount_1d, activity_reduce_amount_1d,
           coupon_reduce_amount_1d, order_total_amount_1d
    from dws_trade_user_order_1d
    where dt = '2026-06-19'
) new
on old.user_id = new.user_id;
```

#### 6.3.2 用户域用户粒度登录历史至今汇总表

```sql
DROP TABLE IF EXISTS dws_user_user_login_td;
CREATE EXTERNAL TABLE dws_user_user_login_td
(
    `user_id`          STRING COMMENT '用户ID',
    `login_date_last`  STRING COMMENT '历史至今末次登录日期',
    `login_date_first` STRING COMMENT '历史至今首次登录日期',
    `login_count_td`   BIGINT COMMENT '历史至今累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');
```

**首日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_user_user_login_td partition(dt='2026-06-18')
select u.id                                                         user_id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) login_date_last,
       date_format(create_time, 'yyyy-MM-dd')                       login_date_first,
       nvl(login_count_td, 1)                                       login_count_td
from (
    select id, create_time
    from dim_user_zip
    where dt = '9999-12-31'
) u
left join
(
    select user_id, max(dt) login_date_last, count(*) login_count_td
    from dwd_user_login_inc
    group by user_id
) l on u.id = l.user_id;
```

**每日装载**

```sql
SET hive.execution.engine=mr;
insert overwrite table dws_user_user_login_td partition(dt='2026-06-19')
select nvl(old.user_id, new.user_id)                                        user_id,
       if(new.user_id is null, old.login_date_last, '2026-06-19')           login_date_last,
       if(old.login_date_first is null, '2026-06-19', old.login_date_first) login_date_first,
       nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0)              login_count_td
from (
    select user_id, login_date_last, login_date_first, login_count_td
    from dws_user_user_login_td
    where dt = date_add('2026-06-19', -1)
) old
full outer join
(
    select user_id, count(*) login_count_1d
    from dwd_user_login_inc
    where dt = '2026-06-19'
    group by user_id
) new
on old.user_id = new.user_id;
```
---

### 6.4 DWS 层装载脚本

> DWS 层仅保留两个入口脚本：首日装载（含1d/n日/历史至今首日）和每日装载（含1d/n日/历史至今每日）。所有 SQL 已内置 MR 引擎切换，直接执行即可。

#### 6.4.1 首日装载脚本（dws_init.sh）

```bash
sudo vim /home/hadoop/bin/dws_init.sh
```

```bash
#!/bin/bash
APP=gmall
if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi

# ===== 1日汇总表（首日）=====
dws_trade_user_sku_order_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
insert overwrite table ${APP}.dws_trade_user_sku_order_1d partition(dt)
select
    user_id, id, sku_name,
    category1_id, category1_name, category2_id, category2_name,
    category3_id, category3_name, tm_id, tm_name,
    order_count_1d, order_num_1d,
    order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d, dt
from
(
    select
        dt, user_id, sku_id,
        count(*) order_count_1d,
        sum(sku_num) order_num_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0.0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0.0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from ${APP}.dwd_trade_order_detail_inc
    group by dt,user_id,sku_id
) od
left join
(
    select id, sku_name, category1_id, category1_name,
           category2_id, category2_name, category3_id, category3_name,
           tm_id, tm_name
    from ${APP}.dim_sku_full
    where dt='$do_date'
) sku on od.sku_id=sku.id;
set hive.vectorized.execution.enabled = true;
"
dws_trade_user_order_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_trade_user_order_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount),
    dt
from ${APP}.dwd_trade_order_detail_inc
group by user_id,dt;
"
dws_trade_user_cart_add_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_trade_user_cart_add_1d partition(dt)
select
    user_id, count(*), sum(sku_num), dt
from ${APP}.dwd_trade_cart_add_inc
group by user_id,dt;
"
dws_trade_user_payment_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_trade_user_payment_1d partition(dt)
select
    user_id, count(distinct(order_id)), sum(sku_num),
    sum(split_payment_amount), dt
from ${APP}.dwd_trade_pay_detail_suc_inc
group by user_id,dt;
"
dws_trade_province_order_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_trade_province_order_1d partition(dt)
select
    province_id, province_name, area_code, iso_code, iso_3166_2,
    order_count_1d, order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d, dt
from
(
    select
        province_id,
        count(distinct(order_id)) order_count_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d,
        dt
    from ${APP}.dwd_trade_order_detail_inc
    group by province_id,dt
) o
left join
(
    select id, province_name, area_code, iso_code, iso_3166_2
    from ${APP}.dim_province_full
    where dt='$do_date'
) p on o.province_id=p.id;
"
dws_tool_user_coupon_coupon_used_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_tool_user_coupon_coupon_used_1d partition(dt)
select
    user_id, coupon_id, coupon_name, coupon_type_code,
    coupon_type_name, benefit_rule, used_count, dt
from
(
    select
        dt, user_id, coupon_id, count(*) used_count
    from ${APP}.dwd_tool_coupon_used_inc
    group by dt,user_id,coupon_id
) t1
left join
(
    select id, coupon_name, coupon_type_code, coupon_type_name, benefit_rule
    from ${APP}.dim_coupon_full
    where dt='$do_date'
) t2 on t1.coupon_id=t2.id;
"
dws_interaction_sku_favor_add_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_interaction_sku_favor_add_1d partition(dt)
select
    sku_id, sku_name, category1_id, category1_name,
    category2_id, category2_name, category3_id, category3_name,
    tm_id, tm_name, favor_add_count, dt
from
(
    select
        dt, sku_id, count(*) favor_add_count
    from ${APP}.dwd_interaction_favor_add_inc
    group by dt,sku_id
) favor
left join
(
    select id, sku_name, category1_id, category1_name,
           category2_id, category2_name, category3_id, category3_name,
           tm_id, tm_name
    from ${APP}.dim_sku_full
    where dt='$do_date'
) sku on favor.sku_id=sku.id;
"
dws_traffic_session_page_view_1d="
insert overwrite table ${APP}.dws_traffic_session_page_view_1d partition(dt='$do_date')
select
    session_id, mid_id, brand, model, operate_system,
    version_code, channel,
    sum(during_time), count(*)
from ${APP}.dwd_traffic_page_view_inc
where dt='$do_date'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
"
dws_traffic_page_visitor_page_view_1d="
insert overwrite table ${APP}.dws_traffic_page_visitor_page_view_1d partition(dt='$do_date')
select
    mid_id, brand, model, operate_system, page_id,
    sum(during_time), count(*)
from ${APP}.dwd_traffic_page_view_inc
where dt='$do_date'
group by mid_id,brand,model,operate_system,page_id;
"

# ===== n日汇总表 =====
dws_trade_user_sku_order_nd="
insert overwrite table ${APP}.dws_trade_user_sku_order_nd partition(dt='$do_date')
select
    user_id, sku_id, sku_name,
    category1_id, category1_name, category2_id, category2_name,
    category3_id, category3_name, tm_id, tm_name,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_num_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d), sum(order_num_1d),
    sum(order_original_amount_1d), sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d), sum(order_total_amount_1d)
from ${APP}.dws_trade_user_sku_order_1d
where dt>=date_add('$do_date',-29)
group by user_id,sku_id,sku_name,category1_id,category1_name,
         category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
"
dws_trade_province_order_nd="
insert overwrite table ${APP}.dws_trade_province_order_nd partition(dt='$do_date')
select
    province_id, province_name, area_code, iso_code, iso_3166_2,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d), sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d), sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from ${APP}.dws_trade_province_order_1d
where dt>=date_add('$do_date',-29) and dt<='$do_date'
group by province_id,province_name,area_code,iso_code,iso_3166_2;
"

# ===== 历史至今汇总表（首日）=====
dws_trade_user_order_td="
insert overwrite table ${APP}.dws_trade_user_order_td partition(dt='$do_date')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count_td,
    sum(order_num_1d) order_num_td,
    sum(order_original_amount_1d) original_amount_td,
    sum(activity_reduce_amount_1d) activity_reduce_amount_td,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_td,
    sum(order_total_amount_1d) total_amount_td
from ${APP}.dws_trade_user_order_1d
group by user_id;
"
dws_user_user_login_td="
insert overwrite table ${APP}.dws_user_user_login_td partition(dt='$do_date')
select u.id                                                         user_id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) login_date_last,
       date_format(create_time, 'yyyy-MM-dd')                       login_date_first,
       nvl(login_count_td, 1)                                       login_count_td
from (
    select id, create_time
    from ${APP}.dim_user_zip
    where dt = '9999-12-31'
) u
left join
(
    select user_id, max(dt) login_date_last, count(*) login_count_td
    from ${APP}.dwd_user_login_inc
    group by user_id
) l on u.id = l.user_id;
"

case $1 in
    "all")
        hive -e "SET hive.execution.engine=mr; SET hive.exec.parallel=false; $dws_trade_user_sku_order_1d$dws_trade_user_order_1d$dws_trade_user_cart_add_1d$dws_trade_user_payment_1d$dws_trade_province_order_1d$dws_tool_user_coupon_coupon_used_1d$dws_interaction_sku_favor_add_1d$dws_traffic_session_page_view_1d$dws_traffic_page_visitor_page_view_1d$dws_trade_user_sku_order_nd$dws_trade_province_order_nd$dws_trade_user_order_td$dws_user_user_login_td SET hive.execution.engine=spark; SET hive.exec.parallel=true;" ;;
esac
```

```bash
sudo chmod +x /home/hadoop/bin/dws_init.sh
# 用法: dws_init.sh all 2026-06-18
```

#### 6.4.2 每日装载脚本（dws_daily.sh）

```bash
sudo vim /home/hadoop/bin/dws_daily.sh
```

```bash
#!/bin/bash
APP=gmall
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

# ===== 1日汇总表（每日）=====
dws_trade_user_sku_order_1d="
set hive.vectorized.execution.enabled = false;
insert overwrite table ${APP}.dws_trade_user_sku_order_1d partition(dt='$do_date')
select
    user_id, id, sku_name,
    category1_id, category1_name, category2_id, category2_name,
    category3_id, category3_name, tm_id, tm_name,
    order_count_1d, order_num_1d,
    order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d
from
(
    select
        user_id, sku_id,
        count(*) order_count_1d,
        sum(sku_num) order_num_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from ${APP}.dwd_trade_order_detail_inc
    where dt='$do_date'
    group by user_id,sku_id
) od
left join
(
    select id, sku_name, category1_id, category1_name,
           category2_id, category2_name, category3_id, category3_name,
           tm_id, tm_name
    from ${APP}.dim_sku_full
    where dt='$do_date'
) sku on od.sku_id=sku.id;
set hive.vectorized.execution.enabled = true;
"
dws_trade_user_order_1d="
insert overwrite table ${APP}.dws_trade_user_order_1d partition(dt='$do_date')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_original_amount),
    sum(nvl(split_activity_amount,0)),
    sum(nvl(split_coupon_amount,0)),
    sum(split_total_amount)
from ${APP}.dwd_trade_order_detail_inc
where dt='$do_date'
group by user_id;
"
dws_trade_user_cart_add_1d="
insert overwrite table ${APP}.dws_trade_user_cart_add_1d partition(dt='$do_date')
select
    user_id, count(*), sum(sku_num)
from ${APP}.dwd_trade_cart_add_inc
where dt='$do_date'
group by user_id;
"
dws_trade_user_payment_1d="
insert overwrite table ${APP}.dws_trade_user_payment_1d partition(dt='$do_date')
select
    user_id, count(distinct(order_id)), sum(sku_num),
    sum(split_payment_amount)
from ${APP}.dwd_trade_pay_detail_suc_inc
where dt='$do_date'
group by user_id;
"
dws_trade_province_order_1d="
insert overwrite table ${APP}.dws_trade_province_order_1d partition(dt='$do_date')
select
    province_id, province_name, area_code, iso_code, iso_3166_2,
    order_count_1d, order_original_amount_1d, activity_reduce_amount_1d,
    coupon_reduce_amount_1d, order_total_amount_1d
from
(
    select
        province_id,
        count(distinct(order_id)) order_count_1d,
        sum(split_original_amount) order_original_amount_1d,
        sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
        sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
        sum(split_total_amount) order_total_amount_1d
    from ${APP}.dwd_trade_order_detail_inc
    where dt='$do_date'
    group by province_id
) o
left join
(
    select id, province_name, area_code, iso_code, iso_3166_2
    from ${APP}.dim_province_full
    where dt='$do_date'
) p on o.province_id=p.id;
"
dws_tool_user_coupon_coupon_used_1d="
insert overwrite table ${APP}.dws_tool_user_coupon_coupon_used_1d partition(dt='$do_date')
select
    user_id, coupon_id, coupon_name, coupon_type_code,
    coupon_type_name, benefit_rule, used_count
from
(
    select
        user_id, coupon_id, count(*) used_count
    from ${APP}.dwd_tool_coupon_used_inc
    where dt='$do_date'
    group by user_id,coupon_id
) t1
left join
(
    select id, coupon_name, coupon_type_code, coupon_type_name, benefit_rule
    from ${APP}.dim_coupon_full
    where dt='$do_date'
) t2 on t1.coupon_id=t2.id;
"
dws_interaction_sku_favor_add_1d="
insert overwrite table ${APP}.dws_interaction_sku_favor_add_1d partition(dt='$do_date')
select
    sku_id, sku_name, category1_id, category1_name,
    category2_id, category2_name, category3_id, category3_name,
    tm_id, tm_name, favor_add_count
from
(
    select
        sku_id, count(*) favor_add_count
    from ${APP}.dwd_interaction_favor_add_inc
    where dt='$do_date'
    group by sku_id
) favor
left join
(
    select id, sku_name, category1_id, category1_name,
           category2_id, category2_name, category3_id, category3_name,
           tm_id, tm_name
    from ${APP}.dim_sku_full
    where dt='$do_date'
) sku on favor.sku_id=sku.id;
"
dws_traffic_session_page_view_1d="
insert overwrite table ${APP}.dws_traffic_session_page_view_1d partition(dt='$do_date')
select
    session_id, mid_id, brand, model, operate_system,
    version_code, channel,
    sum(during_time), count(*)
from ${APP}.dwd_traffic_page_view_inc
where dt='$do_date'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
"
dws_traffic_page_visitor_page_view_1d="
insert overwrite table ${APP}.dws_traffic_page_visitor_page_view_1d partition(dt='$do_date')
select
    mid_id, brand, model, operate_system, page_id,
    sum(during_time), count(*)
from ${APP}.dwd_traffic_page_view_inc
where dt='$do_date'
group by mid_id,brand,model,operate_system,page_id;
"

# ===== n日汇总表（每日与首日逻辑相同）=====
dws_trade_user_sku_order_nd="
insert overwrite table ${APP}.dws_trade_user_sku_order_nd partition(dt='$do_date')
select
    user_id, sku_id, sku_name,
    category1_id, category1_name, category2_id, category2_name,
    category3_id, category3_name, tm_id, tm_name,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_num_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d), sum(order_num_1d),
    sum(order_original_amount_1d), sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d), sum(order_total_amount_1d)
from ${APP}.dws_trade_user_sku_order_1d
where dt>=date_add('$do_date',-29)
group by user_id,sku_id,sku_name,category1_id,category1_name,
         category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
"
dws_trade_province_order_nd="
insert overwrite table ${APP}.dws_trade_province_order_nd partition(dt='$do_date')
select
    province_id, province_name, area_code, iso_code, iso_3166_2,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d), sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d), sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from ${APP}.dws_trade_province_order_1d
where dt>=date_add('$do_date',-29) and dt<='$do_date'
group by province_id,province_name,area_code,iso_code,iso_3166_2;
"

# ===== 历史至今汇总表（每日）=====
dws_trade_user_order_td="
insert overwrite table ${APP}.dws_trade_user_order_td partition(dt='$do_date')
select nvl(old.user_id, new.user_id),
       if(old.user_id is not null, old.order_date_first, '$do_date'),
       if(new.user_id is not null, '$do_date', old.order_date_last),
       nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
       nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
       nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
       nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
       nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
       nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
from (
    select user_id, order_date_first, order_date_last,
           order_count_td, order_num_td, original_amount_td,
           activity_reduce_amount_td, coupon_reduce_amount_td, total_amount_td
    from ${APP}.dws_trade_user_order_td
    where dt = date_add('$do_date', -1)
) old
full outer join
(
    select user_id, order_count_1d, order_num_1d,
           order_original_amount_1d, activity_reduce_amount_1d,
           coupon_reduce_amount_1d, order_total_amount_1d
    from ${APP}.dws_trade_user_order_1d
    where dt = '$do_date'
) new
on old.user_id = new.user_id;
"
dws_user_user_login_td="
insert overwrite table ${APP}.dws_user_user_login_td partition(dt='$do_date')
select nvl(old.user_id, new.user_id)                                        user_id,
       if(new.user_id is null, old.login_date_last, '$do_date')           login_date_last,
       if(old.login_date_first is null, '$do_date', old.login_date_first) login_date_first,
       nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0)              login_count_td
from (
    select user_id, login_date_last, login_date_first, login_count_td
    from ${APP}.dws_user_user_login_td
    where dt = date_add('$do_date', -1)
) old
full outer join
(
    select user_id, count(*) login_count_1d
    from ${APP}.dwd_user_login_inc
    where dt = '$do_date'
    group by user_id
) new
on old.user_id = new.user_id;
"

case $1 in
    "all")
        hive -e "SET hive.execution.engine=mr; SET hive.exec.parallel=false; $dws_trade_user_sku_order_1d$dws_trade_user_order_1d$dws_trade_user_cart_add_1d$dws_trade_user_payment_1d$dws_trade_province_order_1d$dws_tool_user_coupon_coupon_used_1d$dws_interaction_sku_favor_add_1d$dws_traffic_session_page_view_1d$dws_traffic_page_visitor_page_view_1d$dws_trade_user_sku_order_nd$dws_trade_province_order_nd$dws_trade_user_order_td$dws_user_user_login_td SET hive.execution.engine=spark; SET hive.exec.parallel=true;" ;;
esac
```

```bash
sudo chmod +x /home/hadoop/bin/dws_daily.sh
# 用法: dws_daily.sh all 2026-06-19
```

## 第七部分：ADS 层（应用层）构建

> 对应文档：《电商数据仓库系统》V6.0 第11章
> 目标：16张应用层报表，覆盖流量/用户/商品/交易/优惠券五大主题

---

**ADS 层设计要点：**

- 基于 DWS 层汇总数据，面向具体业务需求产出报表
- 存储格式：ROW FORMAT DELIMITED，\t 分隔（方便导出到 MySQL）
- ADS 层报表均为非分区表，通过 `dt` 字段区分统计日期
- 每日装载采用 `insert overwrite ... union select * from ads_xxx` 幂等写入

---

### 7.1 流量主题

#### 7.1.1 各渠道流量统计

```sql
DROP TABLE IF EXISTS ads_traffic_stats_by_channel;
CREATE EXTERNAL TABLE ads_traffic_stats_by_channel
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '各渠道流量统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_traffic_stats_by_channel/';
```

数据装载——通过 `lateral view explode(array(1,7,30))` 将数据膨胀三倍，一次 SQL 完成 1/7/30 日统计：

```sql
insert overwrite table ads_traffic_stats_by_channel
select * from ads_traffic_stats_by_channel
union
select
    '2026-06-18' dt,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_add('2026-06-18',-recent_days+1)
group by recent_days,channel;
```

#### 7.1.2 路径分析

```sql
DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`          STRING COMMENT '统计日期',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_page_path/';
```

数据装载——通过 `lead() + row_number()` 窗口函数提取页面跳转路径：

```sql
insert overwrite table ads_page_path
select * from ads_page_path
union
select
    '2026-06-18' dt,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            page_id,
            lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
            row_number() over (partition by session_id order by view_time) rn
        from dwd_traffic_page_view_inc
        where dt='2026-06-18'
    ) t1
) t2
group by source,target;
```

---

### 7.2 用户主题

#### 7.2.1 用户变动统计

```sql
DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_change/';
```

数据装载——流失用户：7日前活跃但最近7日未活跃；回流用户：流失后当日又活跃：

```sql
insert overwrite table ads_user_change
select * from ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '2026-06-18' dt,
        count(*) user_churn_count
    from dws_user_user_login_td
    where dt='2026-06-18'
    and login_date_last=date_add('2026-06-18',-7)
) churn
join
(
    select
        '2026-06-18' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from dws_user_user_login_td
        where dt='2026-06-18'
        and login_date_last = '2026-06-18'
    ) t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from dws_user_user_login_td
        where dt=date_add('2026-06-18',-1)
    ) t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
) back
on churn.dt=back.dt;
```

#### 7.2.2 用户留存率

```sql
DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_retention/';
```

数据装载——统计前7天至前1天的新增用户当日留存率：

```sql
insert overwrite table ads_user_retention
select * from ads_user_retention
union
select '2026-06-18' dt,
       login_date_first create_date,
       datediff('2026-06-18', login_date_first) retention_day,
       sum(if(login_date_last = '2026-06-18', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '2026-06-18', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
    select user_id, login_date_last, login_date_first
    from dws_user_user_login_td
    where dt = '2026-06-18'
      and login_date_first >= date_add('2026-06-18', -7)
      and login_date_first < '2026-06-18'
) t1
group by login_date_first;
```

#### 7.2.3 用户新增活跃统计

```sql
DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_stats/';
```

数据装载：

```sql
insert overwrite table ads_user_stats
select * from ads_user_stats
union
select '2026-06-18' dt,
       recent_days,
       sum(if(login_date_first >= date_add('2026-06-18', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '2026-06-18'
  and login_date_last >= date_add('2026-06-18', -recent_days + 1)
group by recent_days;
```

#### 7.2.4 用户行为漏斗分析

```sql
DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE ads_user_action
(
    `dt`                STRING COMMENT '统计日期',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加购人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '用户行为漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_action/';
```

数据装载——通过 `recent_days` 伪字段 JOIN 四个子查询：

```sql
insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '2026-06-18' dt,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
(
    select
        1 recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from dws_traffic_page_visitor_page_view_1d
    where dt='2026-06-18'
    and page_id in ('home','good_detail')
) page
join
(
    select 1 recent_days, count(*) cart_count
    from dws_trade_user_cart_add_1d
    where dt='2026-06-18'
) cart on page.recent_days=cart.recent_days
join
(
    select 1 recent_days, count(*) order_count
    from dws_trade_user_order_1d
    where dt='2026-06-18'
) ord on page.recent_days=ord.recent_days
join
(
    select 1 recent_days, count(*) payment_count
    from dws_trade_user_payment_1d
    where dt='2026-06-18'
) pay on page.recent_days=pay.recent_days;
```

#### 7.2.5 新增下单用户统计

```sql
DROP TABLE IF EXISTS ads_new_order_user_stats;
CREATE EXTERNAL TABLE ads_new_order_user_stats
(
    `dt`                   STRING COMMENT '统计日期',
    `recent_days`          BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count` BIGINT COMMENT '新增下单人数'
) COMMENT '新增下单用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_new_order_user_stats/';
```

数据装载：

```sql
insert overwrite table ads_new_order_user_stats
select * from ads_new_order_user_stats
union
select
    '2026-06-18' dt,
    recent_days,
    count(*) new_order_user_count
from dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
where dt='2026-06-18'
and order_date_first>=date_add('2026-06-18',-recent_days+1)
group by recent_days;
```

#### 7.2.6 最近7日内连续3日下单用户数

```sql
DROP TABLE IF EXISTS ads_order_continuously_user_count;
CREATE EXTERNAL TABLE ads_order_continuously_user_count
(
    `dt`                            STRING COMMENT '统计日期',
    `recent_days`                   BIGINT COMMENT '最近天数,7:最近7天',
    `order_continuously_user_count` BIGINT COMMENT '连续3日下单用户数'
) COMMENT '最近7日内连续3日下单用户数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_continuously_user_count/';
```

数据装载——`lead(dt,2)` 与当前日期差为2即连续3日：

```sql
insert overwrite table ads_order_continuously_user_count
select * from ads_order_continuously_user_count
union
select
    '2026-06-18',
    7,
    count(distinct(user_id))
from
(
    select
        user_id,
        datediff(lead(dt,2,'9999-12-31') over(partition by user_id order by dt),dt) diff
    from dws_trade_user_order_1d
    where dt>=date_add('2026-06-18',-6)
) t1
where diff=2;
```

---

### 7.3 商品主题

#### 7.3.1 最近30日各品牌复购率

```sql
DROP TABLE IF EXISTS ads_repeat_purchase_by_tm;
CREATE EXTERNAL TABLE ads_repeat_purchase_by_tm
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,30:最近30天',
    `tm_id`             STRING COMMENT '品牌ID',
    `tm_name`           STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '最近30日各品牌复购率统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_repeat_purchase_by_tm/';
```

数据装载：

```sql
insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '2026-06-18',
    30,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
(
    select
        user_id,
        tm_id,
        tm_name,
        sum(order_count_30d) order_count
    from dws_trade_user_sku_order_nd
    where dt='2026-06-18'
    group by user_id, tm_id,tm_name
) t1
group by tm_id,tm_name;
```

#### 7.3.2 各品牌商品下单统计

```sql
DROP TABLE IF EXISTS ads_order_stats_by_tm;
CREATE EXTERNAL TABLE ads_order_stats_by_tm
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`            STRING COMMENT '品牌ID',
    `tm_name`          STRING COMMENT '品牌名称',
    `order_count`      BIGINT COMMENT '下单数',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '各品牌商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_stats_by_tm/';
```

数据装载——1日指标取自 1d 表，7/30取自 nd 表，union all 合并：

```sql
insert overwrite table ads_order_stats_by_tm
select * from ads_order_stats_by_tm
union
select
    '2026-06-18' dt,
    recent_days,
    tm_id,
    tm_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        tm_id,
        tm_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where dt='2026-06-18'
    group by tm_id,tm_name
    union all
    select
        recent_days,
        tm_id,
        tm_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            tm_id,
            tm_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='2026-06-18'
    ) t1
    group by recent_days,tm_id,tm_name
) odr;
```

#### 7.3.3 各品类商品下单统计

```sql
DROP TABLE IF EXISTS ads_order_stats_by_cate;
CREATE EXTERNAL TABLE ads_order_stats_by_cate
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`     STRING COMMENT '一级品类ID',
    `category1_name`   STRING COMMENT '一级品类名称',
    `category2_id`     STRING COMMENT '二级品类ID',
    `category2_name`   STRING COMMENT '二级品类名称',
    `category3_id`     STRING COMMENT '三级品类ID',
    `category3_name`   STRING COMMENT '三级品类名称',
    `order_count`      BIGINT COMMENT '下单数',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '各品类商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_stats_by_cate/';
```

数据装载：

```sql
insert overwrite table ads_order_stats_by_cate
select * from ads_order_stats_by_cate
union
select
    '2026-06-18' dt,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where dt='2026-06-18'
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    select
        recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='2026-06-18'
    ) t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
) odr;
```

#### 7.3.4 各品类商品购物车存量Top3

```sql
DROP TABLE IF EXISTS ads_sku_cart_num_top3_by_cate;
CREATE EXTERNAL TABLE ads_sku_cart_num_top3_by_cate
(
    `dt`             STRING COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `category2_id`   STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category3_id`   STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `sku_id`         STRING COMMENT 'SKU_ID',
    `sku_name`       STRING COMMENT 'SKU名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
) COMMENT '各品类商品购物车存量Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/';
```

数据装载——从 `dwd_trade_cart_full` 聚合购物车存量，按品类分组 rank 取 Top3：

```sql
set hive.mapjoin.optimized.hashtable=false;
insert overwrite table ads_sku_cart_num_top3_by_cate
select * from ads_sku_cart_num_top3_by_cate
union
select
    '2026-06-18' dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
(
    select
        sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        cart_num,
        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
        select
            sku_id,
            sum(sku_num) cart_num
        from dwd_trade_cart_full
        where dt='2026-06-18'
        group by sku_id
    ) cart
    left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name
        from dim_sku_full
        where dt='2026-06-18'
    ) sku
    on cart.sku_id=sku.id
) t1
where rk<=3;
set hive.mapjoin.optimized.hashtable=true;
```

#### 7.3.5 各品牌商品收藏次数Top3

```sql
DROP TABLE IF EXISTS ads_sku_favor_count_top3_by_tm;
CREATE EXTERNAL TABLE ads_sku_favor_count_top3_by_tm
(
    `dt`          STRING COMMENT '统计日期',
    `tm_id`       STRING COMMENT '品牌ID',
    `tm_name`     STRING COMMENT '品牌名称',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `sku_name`    STRING COMMENT 'SKU名称',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `rk`          BIGINT COMMENT '排名'
) COMMENT '各品牌商品收藏次数Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_favor_count_top3_by_tm/';
```

数据装载：

```sql
insert overwrite table ads_sku_favor_count_top3_by_tm
select * from ads_sku_favor_count_top3_by_tm
union
select
    '2026-06-18' dt,
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_add_count_1d,
    rk
from
(
    select
        tm_id,
        tm_name,
        sku_id,
        sku_name,
        favor_add_count_1d,
        rank() over (partition by tm_id order by favor_add_count_1d desc) rk
    from dws_interaction_sku_favor_add_1d
    where dt='2026-06-18'
) t1
where rk<=3;
```

---

### 7.4 交易主题

#### 7.4.1 下单到支付时间间隔平均值

```sql
DROP TABLE IF EXISTS ads_order_to_pay_interval_avg;
CREATE EXTERNAL TABLE ads_order_to_pay_interval_avg
(
    `dt`                        STRING COMMENT '统计日期',
    `order_to_pay_interval_avg` BIGINT COMMENT '下单到支付时间间隔平均值,单位为秒'
) COMMENT '下单到支付时间间隔平均值统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_to_pay_interval_avg/';
```

数据装载——基于累积快照表 `dwd_trade_trade_flow_acc`，直接用 `payment_time - order_time`，无需大表 JOIN：

```sql
insert overwrite table ads_order_to_pay_interval_avg
select * from ads_order_to_pay_interval_avg
union
select
    '2026-06-18',
    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
from dwd_trade_trade_flow_acc
where dt in ('9999-12-31','2026-06-18')
and payment_date_id='2026-06-18';
```

#### 7.4.2 各省份交易统计

```sql
DROP TABLE IF EXISTS ads_order_by_province;
CREATE EXTERNAL TABLE ads_order_by_province
(
    `dt`                 STRING COMMENT '统计日期',
    `recent_days`        BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`        STRING COMMENT '省份ID',
    `province_name`      STRING COMMENT '省份名称',
    `area_code`          STRING COMMENT '地区编码',
    `iso_code`           STRING COMMENT '旧版国际标准地区编码',
    `iso_code_3166_2`    STRING COMMENT '新版国际标准地区编码',
    `order_count`        BIGINT COMMENT '订单数',
    `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额'
) COMMENT '各省份交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_by_province/';
```

数据装载：

```sql
insert overwrite table ads_order_by_province
select * from ads_order_by_province
union
select
    '2026-06-18' dt,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from dws_trade_province_order_1d
where dt='2026-06-18'
union
select
    '2026-06-18' dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
    end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
    end order_total_amount
from dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='2026-06-18';
```

---

### 7.5 优惠券主题

#### 7.5.1 优惠券使用统计

```sql
DROP TABLE IF EXISTS ads_coupon_stats;
CREATE EXTERNAL TABLE ads_coupon_stats
(
    `dt`              STRING COMMENT '统计日期',
    `coupon_id`       STRING COMMENT '优惠券ID',
    `coupon_name`     STRING COMMENT '优惠券名称',
    `used_count`      BIGINT COMMENT '使用次数',
    `used_user_count` BIGINT COMMENT '使用人数'
) COMMENT '优惠券使用统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_coupon_stats/';
```

数据装载：

```sql
insert overwrite table ads_coupon_stats
select * from ads_coupon_stats
union
select
    '2026-06-18' dt,
    coupon_id,
    coupon_name,
    cast(sum(used_count_1d) as bigint),
    cast(count(*) as bigint)
from dws_tool_user_coupon_coupon_used_1d
where dt='2026-06-18'
group by coupon_id,coupon_name;
```

---

### 7.6 ADS 层装载脚本

> ADS 层无首日/每日之分，所有报表均为 `insert overwrite ... union select * from ads_xxx` 幂等写入，每日执行即可。

#### 7.6.1 每日装载脚本（dws_to_ads.sh）

```bash
sudo vim /home/hadoop/bin/dws_to_ads.sh
```

```bash
#!/bin/bash
APP=gmall
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

ads_traffic_stats_by_channel="
insert overwrite table ${APP}.ads_traffic_stats_by_channel
select * from ${APP}.ads_traffic_stats_by_channel
union
select
    '$do_date' dt,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from ${APP}.dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_add('$do_date',-recent_days+1)
group by recent_days,channel;
"
ads_page_path="
insert overwrite table ${APP}.ads_page_path
select * from ${APP}.ads_page_path
union
select
    '$do_date' dt,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            page_id,
            lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
            row_number() over (partition by session_id order by view_time) rn
        from ${APP}.dwd_traffic_page_view_inc
        where dt='$do_date'
    ) t1
) t2
group by source,target;
"
ads_user_change="
insert overwrite table ${APP}.ads_user_change
select * from ${APP}.ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '$do_date' dt,
        count(*) user_churn_count
    from ${APP}.dws_user_user_login_td
    where dt='$do_date'
    and login_date_last=date_add('$do_date',-7)
) churn
join
(
    select
        '$do_date' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from ${APP}.dws_user_user_login_td
        where dt='$do_date'
        and login_date_last = '$do_date'
    ) t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from ${APP}.dws_user_user_login_td
        where dt=date_add('$do_date',-1)
    ) t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
) back
on churn.dt=back.dt;
"
ads_user_retention="
insert overwrite table ${APP}.ads_user_retention
select * from ${APP}.ads_user_retention
union
select '$do_date' dt,
       login_date_first create_date,
       datediff('$do_date', login_date_first) retention_day,
       sum(if(login_date_last = '$do_date', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '$do_date', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
    select user_id, login_date_last, login_date_first
    from ${APP}.dws_user_user_login_td
    where dt = '$do_date'
      and login_date_first >= date_add('$do_date', -7)
      and login_date_first < '$do_date'
) t1
group by login_date_first;
"
ads_user_stats="
insert overwrite table ${APP}.ads_user_stats
select * from ${APP}.ads_user_stats
union
select '$do_date' dt,
       recent_days,
       sum(if(login_date_first >= date_add('$do_date', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from ${APP}.dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '$do_date'
  and login_date_last >= date_add('$do_date', -recent_days + 1)
group by recent_days;
"
ads_user_action="
insert overwrite table ${APP}.ads_user_action
select * from ${APP}.ads_user_action
union
select
    '$do_date' dt,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
(
    select
        1 recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from ${APP}.dws_traffic_page_visitor_page_view_1d
    where dt='$do_date'
    and page_id in ('home','good_detail')
) page
join
(
    select 1 recent_days, count(*) cart_count
    from ${APP}.dws_trade_user_cart_add_1d
    where dt='$do_date'
) cart on page.recent_days=cart.recent_days
join
(
    select 1 recent_days, count(*) order_count
    from ${APP}.dws_trade_user_order_1d
    where dt='$do_date'
) ord on page.recent_days=ord.recent_days
join
(
    select 1 recent_days, count(*) payment_count
    from ${APP}.dws_trade_user_payment_1d
    where dt='$do_date'
) pay on page.recent_days=pay.recent_days;
"
ads_new_order_user_stats="
insert overwrite table ${APP}.ads_new_order_user_stats
select * from ${APP}.ads_new_order_user_stats
union
select
    '$do_date' dt,
    recent_days,
    count(*) new_order_user_count
from ${APP}.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
where dt='$do_date'
and order_date_first>=date_add('$do_date',-recent_days+1)
group by recent_days;
"
ads_order_continuously_user_count="
insert overwrite table ${APP}.ads_order_continuously_user_count
select * from ${APP}.ads_order_continuously_user_count
union
select
    '$do_date',
    7,
    count(distinct(user_id))
from
(
    select
        user_id,
        datediff(lead(dt,2,'9999-12-31') over(partition by user_id order by dt),dt) diff
    from ${APP}.dws_trade_user_order_1d
    where dt>=date_add('$do_date',-6)
) t1
where diff=2;
"
ads_repeat_purchase_by_tm="
insert overwrite table ${APP}.ads_repeat_purchase_by_tm
select * from ${APP}.ads_repeat_purchase_by_tm
union
select
    '$do_date',
    30,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
(
    select
        user_id,
        tm_id,
        tm_name,
        sum(order_count_30d) order_count
    from ${APP}.dws_trade_user_sku_order_nd
    where dt='$do_date'
    group by user_id, tm_id,tm_name
) t1
group by tm_id,tm_name;
"
ads_order_stats_by_tm="
insert overwrite table ${APP}.ads_order_stats_by_tm
select * from ${APP}.ads_order_stats_by_tm
union
select
    '$do_date' dt,
    recent_days,
    tm_id,
    tm_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        tm_id,
        tm_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from ${APP}.dws_trade_user_sku_order_1d
    where dt='$do_date'
    group by tm_id,tm_name
    union all
    select
        recent_days,
        tm_id,
        tm_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            tm_id,
            tm_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from ${APP}.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    ) t1
    group by recent_days,tm_id,tm_name
) odr;
"
ads_order_stats_by_cate="
insert overwrite table ${APP}.ads_order_stats_by_cate
select * from ${APP}.ads_order_stats_by_cate
union
select
    '$do_date' dt,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from ${APP}.dws_trade_user_sku_order_1d
    where dt='$do_date'
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    select
        recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from ${APP}.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    ) t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
) odr;
"
ads_sku_cart_num_top3_by_cate="
set hive.mapjoin.optimized.hashtable=false;
insert overwrite table ${APP}.ads_sku_cart_num_top3_by_cate
select * from ${APP}.ads_sku_cart_num_top3_by_cate
union
select
    '$do_date' dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
(
    select
        sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        cart_num,
        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
        select
            sku_id,
            sum(sku_num) cart_num
        from ${APP}.dwd_trade_cart_full
        where dt='$do_date'
        group by sku_id
    ) cart
    left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name
        from ${APP}.dim_sku_full
        where dt='$do_date'
    ) sku
    on cart.sku_id=sku.id
) t1
where rk<=3;
set hive.mapjoin.optimized.hashtable=true;
"
ads_sku_favor_count_top3_by_tm="
insert overwrite table ${APP}.ads_sku_favor_count_top3_by_tm
select * from ${APP}.ads_sku_favor_count_top3_by_tm
union
select
    '$do_date' dt,
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_add_count_1d,
    rk
from
(
    select
        tm_id,
        tm_name,
        sku_id,
        sku_name,
        favor_add_count_1d,
        rank() over (partition by tm_id order by favor_add_count_1d desc) rk
    from ${APP}.dws_interaction_sku_favor_add_1d
    where dt='$do_date'
) t1
where rk<=3;
"
ads_order_to_pay_interval_avg="
insert overwrite table ${APP}.ads_order_to_pay_interval_avg
select * from ${APP}.ads_order_to_pay_interval_avg
union
select
    '$do_date',
    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
from ${APP}.dwd_trade_trade_flow_acc
where dt in ('9999-12-31','$do_date')
and payment_date_id='$do_date';
"
ads_order_by_province="
insert overwrite table ${APP}.ads_order_by_province
select * from ${APP}.ads_order_by_province
union
select
    '$do_date' dt,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from ${APP}.dws_trade_province_order_1d
where dt='$do_date'
union
select
    '$do_date' dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
    end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
    end order_total_amount
from ${APP}.dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='$do_date';
"
ads_coupon_stats="
insert overwrite table ${APP}.ads_coupon_stats
select * from ${APP}.ads_coupon_stats
union
select
    '$do_date' dt,
    coupon_id,
    coupon_name,
    cast(sum(used_count_1d) as bigint),
    cast(count(*) as bigint)
from ${APP}.dws_tool_user_coupon_coupon_used_1d
where dt='$do_date'
group by coupon_id,coupon_name;
"

case $1 in
    "all")
        hive -e "SET hive.execution.engine=mr; SET hive.exec.parallel=false; $ads_traffic_stats_by_channel$ads_page_path$ads_user_change$ads_user_retention$ads_user_stats$ads_user_action$ads_new_order_user_stats$ads_order_continuously_user_count$ads_repeat_purchase_by_tm$ads_order_stats_by_tm$ads_order_stats_by_cate$ads_sku_cart_num_top3_by_cate$ads_sku_favor_count_top3_by_tm$ads_order_to_pay_interval_avg$ads_order_by_province$ads_coupon_stats SET hive.execution.engine=spark; SET hive.exec.parallel=true;" ;;
esac
```

```bash
sudo chmod +x /home/hadoop/bin/dws_to_ads.sh
# 用法: dws_to_ads.sh all 2026-06-18
```
