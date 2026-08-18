

DROP TABLE IF EXISTS `table_process_dwd`;
CREATE TABLE `table_process_dwd` (
    `source_table` varchar(200) NOT NULL COMMENT '来源表',
    `source_type` varchar(200) NOT NULL COMMENT '来源操作类型',
    `sink_table` varchar(200) NOT NULL COMMENT '输出表',
    `sink_columns` varchar(2000) COMMENT '输出字段',
    PRIMARY KEY (`sink_table`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `table_process_dwd` */
INSERT INTO `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`) VALUES ('coupon_use', 'insert', 'dwd_tool_coupon_get',  'id,coupon_id,user_id,get_time,coupon_status');
INSERT INTO `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`) VALUES ('coupon_use', 'update', 'dwd_tool_coupon_use', 'id,coupon_id,user_id,order_id,using_time,used_time,coupon_status');
INSERT INTO `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`) VALUES ('favor_info', 'insert', 'dwd_interaction_favor_add',  'id,user_id,sku_id,create_time');
INSERT INTO `table_process_dwd`(`source_table`, `source_type`, `sink_table`, `sink_columns`) VALUES ('user_info', 'insert', 'dwd_user_register',  'id,create_time');