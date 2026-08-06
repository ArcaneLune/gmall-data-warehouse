#!/bin/bash

DATAX_HOME=/opt/module/datax

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

handle_targetdir() {
  hadoop fs -test -e $1
  if [[ $? -eq 1 ]]; then
    echo "路径 $1 不存在，正在创建......"
    hadoop fs -mkdir -p $1
  else
    echo "路径 $1 已经存在"
  fi
}

import_data() {
  datax_config=$1
  target_dir=$2
  handle_targetdir $target_dir
  python $DATAX_HOME/bin/datax.py -p"-Dtargetdir=$target_dir" $datax_config
}

case $1 in
"activity_info")
  import_data /opt/module/datax/job/import/gmall.activity_info.json /origin_data/gmall/db/activity_info_full/$do_date ;;
"activity_rule")
  import_data /opt/module/datax/job/import/gmall.activity_rule.json /origin_data/gmall/db/activity_rule_full/$do_date ;;
"base_category1")
  import_data /opt/module/datax/job/import/gmall.base_category1.json /origin_data/gmall/db/base_category1_full/$do_date ;;
"base_category2")
  import_data /opt/module/datax/job/import/gmall.base_category2.json /origin_data/gmall/db/base_category2_full/$do_date ;;
"base_category3")
  import_data /opt/module/datax/job/import/gmall.base_category3.json /origin_data/gmall/db/base_category3_full/$do_date ;;
"base_dic")
  import_data /opt/module/datax/job/import/gmall.base_dic.json /origin_data/gmall/db/base_dic_full/$do_date ;;
"base_province")
  import_data /opt/module/datax/job/import/gmall.base_province.json /origin_data/gmall/db/base_province_full/$do_date ;;
"base_region")
  import_data /opt/module/datax/job/import/gmall.base_region.json /origin_data/gmall/db/base_region_full/$do_date ;;
"base_trademark")
  import_data /opt/module/datax/job/import/gmall.base_trademark.json /origin_data/gmall/db/base_trademark_full/$do_date ;;
"cart_info")
  import_data /opt/module/datax/job/import/gmall.cart_info.json /origin_data/gmall/db/cart_info_full/$do_date ;;
"coupon_info")
  import_data /opt/module/datax/job/import/gmall.coupon_info.json /origin_data/gmall/db/coupon_info_full/$do_date ;;
"sku_attr_value")
  import_data /opt/module/datax/job/import/gmall.sku_attr_value.json /origin_data/gmall/db/sku_attr_value_full/$do_date ;;
"sku_info")
  import_data /opt/module/datax/job/import/gmall.sku_info.json /origin_data/gmall/db/sku_info_full/$do_date ;;
"sku_sale_attr_value")
  import_data /opt/module/datax/job/import/gmall.sku_sale_attr_value.json /origin_data/gmall/db/sku_sale_attr_value_full/$do_date ;;
"spu_info")
  import_data /opt/module/datax/job/import/gmall.spu_info.json /origin_data/gmall/db/spu_info_full/$do_date ;;
"promotion_pos")
  import_data /opt/module/datax/job/import/gmall.promotion_pos.json /origin_data/gmall/db/promotion_pos_full/$do_date ;;
"promotion_refer")
  import_data /opt/module/datax/job/import/gmall.promotion_refer.json /origin_data/gmall/db/promotion_refer_full/$do_date ;;
"all")
  for table in activity_info activity_rule base_category1 base_category2 base_category3 base_dic base_province base_region base_trademark cart_info coupon_info sku_attr_value sku_info sku_sale_attr_value spu_info promotion_pos promotion_refer; do
    import_data /opt/module/datax/job/import/gmall.${table}.json /origin_data/gmall/db/${table}_full/$do_date
  done
  ;;
esac
