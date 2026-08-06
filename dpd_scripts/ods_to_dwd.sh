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
