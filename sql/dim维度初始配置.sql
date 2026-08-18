DROP TABLE IF EXISTS `table_process_dim`;
CREATE TABLE `table_process_dim` (
    `source_table` varchar(200) NOT NULL COMMENT '来源表',
    `sink_table` varchar(200) NOT NULL COMMENT '输出表',
    `sink_family` varchar(200) COMMENT '当输出类型是 dim 的时候,输出到 hbase 的列族',
    `sink_columns` varchar(2000) COMMENT '输出字段',
    `sink_row_key` varchar(200) COMMENT '当输出类型是dim时的rowkey',
    PRIMARY KEY (`sink_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('activity_info', 'dim_activity_info', 'info','id,activity_name,activity_type,activity_desc,start_time,end_time,create_time', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('activity_rule', 'dim_activity_rule', 'info','id,activity_id,activity_type,condition_amount,condition_num,benefit_amount,benefit_discount,benefit_level', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('activity_sku', 'dim_activity_sku', 'info','id,activity_id,sku_id,create_time', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('base_category1', 'dim_base_category1', 'info','id,name', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('base_category2', 'dim_base_category2', 'info','id,name,category1_id', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('base_category3', 'dim_base_category3', 'info','id,name,category2_id', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('base_province', 'dim_base_province', 'info','id,name,region_id,area_code,iso_code,iso_3166_2', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('base_region', 'dim_base_region', 'info','id,region_name', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('base_trademark', 'dim_base_trademark', 'info','id,tm_name', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('coupon_info', 'dim_coupon_info', 'info', 'id,coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('coupon_range', 'dim_coupon_range', 'info','id,coupon_id,range_type,range_id', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('financial_sku_cost', 'dim_financial_sku_cost', 'info','id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('sku_info', 'dim_sku_info', 'info','id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('spu_info', 'dim_spu_info', 'info','id,spu_name,description,category3_id,tm_id','id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('user_info', 'dim_user_info', 'info','id,login_name,name,user_level,birthday,gender,create_time,operate_time', 'id');
INSERT INTO `table_process_dim`(`source_table`, `sink_table`, `sink_family`, `sink_columns`, `sink_row_key`) VALUES ('base_dic', 'dim_base_dic', 'info','dic_code,dic_name', 'dic_code');