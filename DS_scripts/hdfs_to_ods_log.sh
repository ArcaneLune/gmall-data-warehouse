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
