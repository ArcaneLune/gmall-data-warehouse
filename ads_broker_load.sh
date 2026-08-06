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
