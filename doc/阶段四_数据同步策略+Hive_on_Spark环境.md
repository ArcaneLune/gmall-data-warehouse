# 阶段四：数据同步策略 + Hive on Spark 环境

> 目标：用户行为数据落地HDFS → 业务数据落地HDFS(增量+全量) → Hive on Spark

---

## 概述

本阶段分两大块，先搞用户行为数据，再搞业务数据：

```
【用户行为数据同步】
Kafka(topic_log) → Flume(f2, TimestampInterceptor) → HDFS /origin_data/gmall/log/topic_log/

【业务数据同步】
Kafka(topic_db) → Flume(f3, TimestampAndTableNameInterceptor) → HDFS /origin_data/gmall/db/{table}_inc/
MySQL(gmall) → DataX → HDFS /origin_data/gmall/db/{table}_full/
MySQL(gmall) → Maxwell bootstrap → Kafka → Flume(f3) → HDFS（增量表首日全量）
```

**部署节点新增**：hadoop102 部署两个消费 Flume。

---

## 第一部分：用户行为数据同步

> 目标：Kafka(topic_log) → Flume(f2) → HDFS，解决零点漂移问题

### 1. Flume f2 配置（日志消费）

在 **hadoop102** 上操作：

```bash
cd /opt/module/flume
mkdir -p job checkpoint/behavior1 data/behavior1
vim job/kafka_to_hdfs_log.conf
```

```properties
# ====== 定义组件 ======
a1.sources = r1
a1.channels = c1
a1.sinks = k1

# ====== Source: Kafka Source ======
a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.r1.batchSize = 5000
a1.sources.r1.batchDurationMillis = 2000
a1.sources.r1.kafka.bootstrap.servers = hadoop100:9092,hadoop101:9092,hadoop102:9092
a1.sources.r1.kafka.topics = topic_log
# 拦截器：解决零点漂移
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = com.hadoop.gmall.flume.interceptor.TimestampInterceptor$Builder

# ====== Channel: File Channel ======
a1.channels.c1.type = file
a1.channels.c1.checkpointDir = /opt/module/flume/checkpoint/behavior1
a1.channels.c1.dataDirs = /opt/module/flume/data/behavior1
a1.channels.c1.maxFileSize = 2146435071
a1.channels.c1.capacity = 1000000
a1.channels.c1.keep-alive = 6

# ====== Sink: HDFS Sink ======
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /origin_data/gmall/log/topic_log/%Y-%m-%d
a1.sinks.k1.hdfs.filePrefix = log
a1.sinks.k1.hdfs.round = false
a1.sinks.k1.hdfs.rollInterval = 10
a1.sinks.k1.hdfs.rollSize = 134217728
a1.sinks.k1.hdfs.rollCount = 0
a1.sinks.k1.hdfs.fileType = CompressedStream
a1.sinks.k1.hdfs.codeC = gzip

# ====== 组装 ======
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

### 2. 零点漂移问题 + TimestampInterceptor

**什么是零点漂移**：日志在接近午夜（23:59:59）时生成，但由于 Flume 传输延迟，实际写入 HDFS 的时间已经是第二天 00:00:01。Flume HDFSSink 默认用系统当前时间确定文件路径 `%Y-%m-%d`，导致本该属于 6月8日的日志被写进了 6月9日的分区。

**解决方案**：用拦截器从日志 JSON 的 `ts` 字段（日志生成时间戳）覆盖 Flume header 的 timestamp，让 HDFSSink 按日志的实际时间分区。

#### 2.1 在 IDEA 中创建 Maven 项目

- 项目名：`gmall`
- GroupId：`com.hadoop`
- ArtifactId：`gmall`
- JDK：1.8

#### 2.2 pom.xml

```xml
    <dependencies>
        <dependency>
            <groupId>org.apache.flume</groupId>
            <artifactId>flume-ng-core</artifactId>
            <version>1.10.1</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.62</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>2.3.2</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```

#### 2.3 TimestampInterceptor

包路径：`com.hadoop.gmall.flume.interceptor`

```java
package com.hadoop.gmall.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {

    @Override
    public void initialize() {}

    @Override
    public Event intercept(Event event) {
        // 1、获取 header 和 body 的数据
        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        try {
            // 2、将 body 转成 JSONObject（方便取值）
            JSONObject jsonObject = JSONObject.parseObject(log);

            // 3、header 中 timestamp 替换成日志生成的时间戳（解决零点漂移）
            String ts = jsonObject.getString("ts");
            headers.put("timestamp", ts);

            return event;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()) {
            Event event = iterator.next();
            if (intercept(event) == null) {
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {}

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }
        @Override
        public void configure(Context context) {}
    }
}
```

#### 2.4 打包 + 部署

IDEA 中执行 `mvn clean package`，生成的 jar：`gmall-1.0-SNAPSHOT-jar-with-dependencies.jar`

上传到 hadoop102 的 `/opt/module/flume/lib/`：

```bash
# 在 hadoop102 上执行（先删旧版再拷贝新版）
cd /opt/module/flume/lib/
rm -f gmall-1.0-SNAPSHOT-jar-with-dependencies.jar
# 从 Windows 用 XFTP 上传到此目录
```

> 注意：拦截器只在 hadoop102（消费端）需要。

### 3. f2 启停脚本

```bash
sudo vim /home/hadoop/bin/f2.sh
```

```bash
#!/bin/bash

case $1 in
"start")
    echo " --------启动 hadoop102 日志消费 Flume-------"
    ssh hadoop102 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/kafka_to_hdfs_log.conf >/dev/null 2>&1 &"
    ;;
"stop")
    echo " --------停止 hadoop102 日志消费 Flume-------"
    ssh hadoop102 "ps -ef | grep kafka_to_hdfs_log | grep -v grep | awk '{print \$2}' | xargs -r kill"
    ;;
*)
    echo "Usage: f2.sh start|stop"
    ;;
esac
```

```bash
sudo chmod 777 /home/hadoop/bin/f2.sh
```

### 4. 用户行为数据同步测试

```bash
# 第1步：启动 Zookeeper
zk.sh start

# 第2步：启动 Kafka
kf.sh start

# 第3步：启动 Hadoop
hdp.sh start

# 第4步：启动日志采集 Flume（hadoop100/101）
f1.sh start

# 第5步：启动 hadoop102 的日志消费 Flume（首次建议前台运行看报错）
ssh hadoop102 "/opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/kafka_to_hdfs_log.conf"

# 第6步：另开终端，生成日志模拟数据
lg.sh test 10

# 第7步：观察 HDFS 是否出现数据
hdfs dfs -ls /origin_data/gmall/log/topic_log/
```

**预期**：`/origin_data/gmall/log/topic_log/` 下出现日期分区目录，目录内有 gzip 压缩文件。

**出现数据即用户行为数据同步成功。**

> 测试通过后把第5步改为：`f2.sh start`（后台运行）

---

## 第二部分：业务数据同步（先全量，后增量）

> 数据流：MySQL → DataX → HDFS（全量），Kafka → Flume(f3) → HDFS（增量），MySQL → Maxwell → Kafka → Flume → HDFS（增量首日）

### 全量同步

> DataX 将 MySQL 17张全量表批量同步到 HDFS


### 5. DataX 安装与配置生成器

#### 5.1 DataX 安装

在 hadoop100 上操作：

```bash
# 下载（或 Windows 下载后上传）
wget -P /opt/software/ https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202309/datax.tar.gz

tar -zxvf /opt/software/datax.tar.gz -C /opt/module/
```

```bash
# 自检
python /opt/module/datax/bin/datax.py /opt/module/datax/job/job.json
```

#### 5.2 DataX 配置文件生成器

> 在 Windows IDEA 中创建 Maven 项目，自动生成 17 张全量表的 DataX 同步配置。

**创建 Maven 项目**：
- 项目名：`datax-config-generator`
- GroupId：`com.datax`
- ArtifactId：`datax-config-generator`
- JDK：1.8

> 具体项目信息和代码可见项目根目录下的datax-config-generator目录

**打包**：

IDEA 执行 `mvn clean package`，target 目录生成：
`datax-config-generator-1.0-SNAPSHOT-jar-with-dependencies.jar`

#### 5.3 上传并部署到服务器

将 `jar` 包和 `configuration.properties` 上传到 `/opt/module/gen_datax_config/`：

```bash
mkdir -p /opt/module/gen_datax_config
# XFTP 上传两个文件到此目录
```

修改服务器端 `configuration.properties`：

```bash
vim /opt/module/gen_datax_config/configuration.properties
```

```properties
mysql.username=root
mysql.password=root
mysql.host=hadoop100
mysql.port=3306
mysql.database.import=gmall
mysql.tables.import=activity_info,activity_rule,base_trademark,cart_info,base_category1,base_category2,base_category3,coupon_info,sku_attr_value,sku_sale_attr_value,base_dic,sku_info,base_province,spu_info,base_region,promotion_pos,promotion_refer
is.seperated.tables=0
hdfs.uri=hdfs://hadoop100:8020
import_out_dir=/opt/module/datax/job/import
```

> 注意改 `mysql.password` 为你的实际密码。

执行生成器：

```bash
cd /opt/module/gen_datax_config
java -jar datax-config-generator-1.0-SNAPSHOT-jar-with-dependencies.jar
```

验证：

```bash
ls /opt/module/datax/job/import/
# 应有 17 个 gmall.xxx.json 文件
```

### 6. DataX 全量表同步脚本（mysql_to_hdfs_full.sh）

```bash
sudo vim /home/hadoop/bin/mysql_to_hdfs_full.sh
```

```bash
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
```

```bash
sudo chmod 777 /home/hadoop/bin/mysql_to_hdfs_full.sh
```

#### 6.1 测试 DataX 同步

```bash
# 第1步：创建目标路径（DataX 要求路径提前存在）
hadoop fs -mkdir -p /origin_data/gmall/db/activity_info_full/2026-06-18

# 第2步：执行 DataX 同步
python /opt/module/datax/bin/datax.py \
  -p"-Dtargetdir=/origin_data/gmall/db/activity_info_full/2026-06-18" \
  /opt/module/datax/job/import/gmall.activity_info.json

# 第3步：观察 HDFS 是否出现数据
hdfs dfs -ls /origin_data/gmall/db/activity_info_full/2026-06-18/
```

**如果出现数据**，则 DataX 配置文件和通道可用。

#### 6.2 测试全量同步脚本

```bash
mysql_to_hdfs_full.sh all 2026-06-18
hdfs dfs -ls /origin_data/gmall/db/ | grep _full    # 预期 17 个目录
```

### 增量同步

> Flume(f3)消费Kafka topic_db → HDFS，Maxwell监听MySQL binlog增量变化

### 7. Flume f3 部署

> Flume 需要将 Kafka 中 topic_db 主题的数据传输到 HDFS。HDFSSink 需要将不同 MySQL 业务表的数据写到不同的路径，路径中包含一层日期区分每天的数据。

在 **hadoop102** 上操作：

```bash
mkdir -p /opt/module/flume/checkpoint/behavior2 /opt/module/flume/data/behavior2
vim /opt/module/flume/job/kafka_to_hdfs_db.conf
```

```properties
a1.sources = r1
a1.channels = c1
a1.sinks = k1

a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.r1.batchSize = 5000
a1.sources.r1.batchDurationMillis = 2000
a1.sources.r1.kafka.bootstrap.servers = hadoop100:9092,hadoop101:9092,hadoop102:9092
a1.sources.r1.kafka.topics = topic_db
a1.sources.r1.kafka.consumer.group.id = flume
a1.sources.r1.setTopicHeader = true
a1.sources.r1.topicHeader = topic
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = com.hadoop.gmall.flume.interceptor.TimestampAndTableNameInterceptor$Builder

a1.channels.c1.type = file
a1.channels.c1.checkpointDir = /opt/module/flume/checkpoint/behavior2
a1.channels.c1.dataDirs = /opt/module/flume/data/behavior2/
a1.channels.c1.maxFileSize = 2146435071
a1.channels.c1.capacity = 1000000
a1.channels.c1.keep-alive = 6

## sink1
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /origin_data/gmall/db/%{tableName}_inc/%Y-%m-%d
a1.sinks.k1.hdfs.filePrefix = db
a1.sinks.k1.hdfs.round = false
a1.sinks.k1.hdfs.rollInterval = 10
a1.sinks.k1.hdfs.rollSize = 134217728
a1.sinks.k1.hdfs.rollCount = 0
a1.sinks.k1.hdfs.fileType = CompressedStream
a1.sinks.k1.hdfs.codeC = gzip

## 拼装
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

### 8. TimestampAndTableNameInterceptor

在同一个 Maven 项目 `gmall` 中，`com.hadoop.gmall.flume.interceptor` 包下创建：

```java
package com.hadoop.gmall.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimestampAndTableNameInterceptor implements Interceptor {

    @Override
    public void initialize() {}

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        // Maxwell 输出的 ts 单位为秒，Flume HDFSSink 要求毫秒
        Long ts = jsonObject.getLong("ts");
        String timeMills = String.valueOf(ts * 1000);

        String tableName = jsonObject.getString("table");

        headers.put("timestamp", timeMills);
        headers.put("tableName", tableName);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {}

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimestampAndTableNameInterceptor();
        }
        @Override
        public void configure(Context context) {}
    }
}
```

重新打包后部署到 hadoop102：

```bash
# 在 hadoop102 上执行
cd /opt/module/flume/lib/
rm -f gmall-1.0-SNAPSHOT-jar-with-dependencies.jar
# XFTP 上传新的 jar 到此目录
ls | grep gmall    # 确认 jar 已到位
```

> 两个拦截器已在同一个 Maven 项目 `gmall` 中，一次打包同时生成，替换一次即可。

### 9. 业务数据通道测试

```bash
# 第1步：启动 Zookeeper、Kafka集群及 Maxwell
zk.sh start
kf.sh start
hdp.sh start
mxw.sh start

# 第2步：启动 hadoop102 的 Flume（首次前台运行，观察报错）
ssh hadoop102 "/opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/kafka_to_hdfs_db.conf"

# 第3步：另开终端，生成模拟数据（确保 Maxwell 正在运行）
lg.sh

# 第4步：观察 HDFS 目标路径
hdfs dfs -ls /origin_data/gmall/db/
```

**预期**：出现各增量表 `_inc` 目录，数据通道已打通。

> **日期说明**：目标路径中的日期可能是当前系统日期而非模拟数据的业务日期。这是因为 Maxwell 输出的 `ts` 字段取自 MySQL binlog 的变动时间。下一节通过 `mock_date` 参数修正。

### 10. f3 启停脚本（f3.sh）

```bash
sudo vim /home/hadoop/bin/f3.sh
```

```bash
#!/bin/bash

case $1 in
"start")
    echo " --------启动 hadoop102 业务数据 Flume-------"
    ssh hadoop102 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/kafka_to_hdfs_db.conf >/dev/null 2>&1 &"
    ;;
"stop")
    echo " --------停止 hadoop102 业务数据 Flume-------"
    ssh hadoop102 "ps -ef | grep kafka_to_hdfs_db | grep -v grep | awk '{print \$2}' | xargs -n1 kill"
    ;;
*)
    echo "Usage: f3.sh start|stop"
    ;;
esac
```

```bash
sudo chmod 777 /home/hadoop/bin/f3.sh
```

### 11. Maxwell mock_date 时间戳修正

为了让 Maxwell 时间戳日期与模拟数据的业务日期一致，已对 Maxwell 源码增加 `mock_date` 参数。

编辑 `/opt/module/maxwell/config.properties`，完整配置如下：

```properties
log_level=info

# Maxwell 数据发送目的地：Kafka
producer=kafka

# 目标 Kafka 集群地址
kafka.bootstrap.servers=hadoop100:9092,hadoop101:9092,hadoop102:9092

# 目标 Kafka topic
kafka_topic=topic_db

# MySQL 相关配置
host=hadoop100
user=maxwell
password=maxwell
jdbc_options=useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true

# 过滤 gmall 中的 z_log 表数据（日志数据备份，无须采集）
filter=exclude:gmall.z_log

# 指定数据按照主键分组进入 Kafka 不同分区，避免数据倾斜
producer_partition_by=primary_key

# 修改数据时间戳的日期部分（教学环境专用）
mock_date=2026-06-18
```

重启 Maxwell 使配置生效：

```bash
mxw.sh restart
```

重新生成模拟数据：

```bash
lg.sh
```

观察 HDFS 目标路径，日期应与 `mock_date` 一致：

```bash
hdfs dfs -ls /origin_data/gmall/db/
```
### 12. Maxwell Bootstrap 配置（13张增量表）

#### 12.1 增量表清单（13张）

```
cart_info, comment_info, coupon_use, favor_info, order_detail, order_detail_activity,
order_detail_coupon, order_info, order_refund_info, order_status_log, payment_info,
refund_payment, user_info
```

### 13. Maxwell Bootstrap 脚本（mysql_to_kafka_inc_init.sh）

> 增量表需要在首日进行一次全量同步，后续每日再进行增量同步。首日全量使用 Maxwell 的 bootstrap 功能。

```bash
vim /home/hadoop/bin/mysql_to_kafka_inc_init.sh
```

```bash
#!/bin/bash

# 该脚本的作用是初始化所有的增量表，只需执行一次

MAXWELL_HOME=/opt/module/maxwell

import_data() {
    $MAXWELL_HOME/bin/maxwell-bootstrap --database gmall --table $1 --config $MAXWELL_HOME/config.properties
}

case $1 in
"cart_info")
  import_data cart_info
  ;;
"comment_info")
  import_data comment_info
  ;;
"coupon_use")
  import_data coupon_use
  ;;
"favor_info")
  import_data favor_info
  ;;
"order_detail")
  import_data order_detail
  ;;
"order_detail_activity")
  import_data order_detail_activity
  ;;
"order_detail_coupon")
  import_data order_detail_coupon
  ;;
"order_info")
  import_data order_info
  ;;
"order_refund_info")
  import_data order_refund_info
  ;;
"order_status_log")
  import_data order_status_log
  ;;
"payment_info")
  import_data payment_info
  ;;
"refund_payment")
  import_data refund_payment
  ;;
"user_info")
  import_data user_info
  ;;
"all")
  import_data cart_info
  import_data comment_info
  import_data coupon_use
  import_data favor_info
  import_data order_detail
  import_data order_detail_activity
  import_data order_detail_coupon
  import_data order_info
  import_data order_refund_info
  import_data order_status_log
  import_data payment_info
  import_data refund_payment
  import_data user_info
  ;;
esac
```

```bash
chmod 777 /home/hadoop/bin/mysql_to_kafka_inc_init.sh
```

#### 13.1 清理历史数据（可选）

为方便查看结果，先将 HDFS 上之前同步的增量表数据删除：

```bash
hadoop fs -ls /origin_data/gmall/db | grep _inc | awk '{print $8}' | xargs hadoop fs -rm -r -f
```

#### 13.2 执行首日全量

```bash
# 确保 Maxwell + Kafka + f3 都在运行
mysql_to_kafka_inc_init.sh all
```

#### 13.3 检查同步结果

观察 HDFS 上是否重新出现增量表数据：

```bash
hdfs dfs -ls /origin_data/gmall/db/ | grep _inc
```

**预期**：出现 13 个 `_inc` 目录。

---

## 第三部分：集群总启停脚本

```bash
vim /home/hadoop/bin/cluster.sh
```

```bash
#!/bin/bash

case $1 in
"start"){
    echo ================== 启动 集群 ==================

    #启动 Zookeeper集群
    zk.sh start

    #启动 Hadoop集群
    hdp.sh start

    #启动 Kafka采集集群
    kf.sh start

    #启动采集 Flume
    f1.sh start

    #启动日志消费 Flume
    f2.sh start

    #启动业务消费 Flume
    f3.sh start

    #启动 maxwell
    mxw.sh start
    };;
"stop"){
    echo ================== 停止 集群 ==================

    #停止 Maxwell
    mxw.sh stop

    #停止 业务消费Flume
    f3.sh stop

    #停止 日志消费Flume
    f2.sh stop

    #停止 日志采集Flume
    f1.sh stop

    #停止 Kafka采集集群
    kf.sh stop

    #循环直至 Kafka 集群进程全部停止
    kafka_count=$(xcall jps | grep Kafka | wc -l)
    while [ $kafka_count -gt 0 ]
    do
        sleep 1
        kafka_count=$(xcall jps | grep Kafka | wc -l)
        echo "当前未停止的 Kafka 进程数为 $kafka_count"
    done

    #停止 Hadoop集群
    hdp.sh stop

    #停止 Zookeeper集群
    zk.sh stop
    };;
*)
    echo "Usage: cluster.sh start|stop"
    ;;
esac
```

```bash
chmod 777 /home/hadoop/bin/cluster.sh
```

---

## 第四部分：Hive 3.1.3 + Hive on Spark 3.3.1

### 14. Hive 3.1.3 安装

在 hadoop100 上操作：

```bash
# 上传 hive-3.1.3.tar.gz 到 /opt/software/（或 wget 下载）
wget -P /opt/software/ https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz

# 解压
tar -zxvf /opt/software/hive-3.1.3.tar.gz -C /opt/module/
# 重命名
mv /opt/module/apache-hive-3.1.3-bin /opt/module/hive

# 解决日志 jar 冲突
mv /opt/module/hive/lib/log4j-slf4j-impl-2.17.1.jar /opt/module/hive/lib/log4j-slf4j-impl-2.17.1.jar.bak```

配置环境变量：

```bash
sudo vim /etc/profile.d/my_env.sh
```

添加：

```bash
# HIVE_HOME
export HIVE_HOME=/opt/module/hive
export PATH=$PATH:$HIVE_HOME/bin
```

使环境变量生效：

```bash
source /etc/profile.d/my_env.sh
```

### 15. Hive 元数据配置到 MySQL

#### 15.1 拷贝 JDBC 驱动

```bash
cp /opt/software/mysql-connector-j-8.0.33.jar /opt/module/hive/lib/
```

#### 15.2 配置 Metastore 到 MySQL

```bash
vim /opt/module/hive/conf/hive-site.xml
```

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- 配置 Hive 保存元数据信息所需的 MySQL URL 地址 -->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://hadoop100:3306/metastore?useSSL=false&amp;useUnicode=true&amp;characterEncoding=UTF-8&amp;allowPublicKeyRetrieval=true</value>
    </property>

    <!-- 配置 Hive 连接 MySQL 的驱动全类名 -->
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.cj.jdbc.Driver</value>
    </property>

    <!-- 配置 Hive 连接 MySQL 的用户名 -->
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>root</value>
    </property>

    <!-- 配置 Hive 连接 MySQL 的密码 -->
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>你的密码</value>
    </property>

    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>

    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>

    <property>
        <name>hive.server2.thrift.port</name>
        <value>10000</value>
    </property>

    <property>
        <name>hive.server2.thrift.bind.host</name>
        <value>hadoop100</value>
    </property>

    <property>
        <name>hive.metastore.event.db.notification.api.auth</name>
        <value>false</value>
    </property>

    <property>
        <name>hive.cli.print.header</name>
        <value>true</value>
    </property>

    <property>
        <name>hive.cli.print.current.db</name>
        <value>true</value>
    </property>

    <!-- ODS 层 JsonSerDe 兼容配置 -->
    <property>
        <name>metastore.storage.schema.reader.impl</name>
        <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
    </property>
</configuration>
```

> 密码写你的实际 MySQL root 密码。

### 14. 启动 Hive

#### 14.1 初始化元数据库

```bash
# 登录 MySQL
mysql -uroot -p

# 新建 Hive 元数据库
CREATE DATABASE metastore;
QUIT;
```

```bash
# 初始化 Hive 元数据库
schematool -initSchema -dbType mysql -verbose
```

> **报错**：`Schema initialization failed!` → 通常是 JDBC 驱动未拷贝或 MySQL 连接字符串写错

#### 14.2 修改元数据库字符集（避免中文注释乱码）

```sql
USE metastore;
ALTER TABLE COLUMNS_V2 MODIFY COLUMN COMMENT VARCHAR(256) CHARACTER SET utf8;
ALTER TABLE TABLE_PARAMS MODIFY COLUMN PARAM_VALUE MEDIUMTEXT CHARACTER SET utf8;
```

#### 14.3 启动 Hive 客户端测试

```bash
hive
```

```sql
show databases;
```

**预期输出**：

```
OK
database_name
default
Time taken: 0.955 seconds, Fetched: 1 row(s)
```

### 16. Hive on Spark 3.3.1

> **兼容性说明**：官网下载的 Hive 3.1.3 和 Spark 3.3.1 默认不兼容——Hive 3.1.3 原生支持 Spark 2.3.0。
> 本阶段使用的是**已重新编译好的 Hive 3.1.3**，使其适配 Spark 3.3.1。如果使用官网原版 Hive，Spark 任务会直接失败。

#### 16.1 安装 Spark（纯净版，不含 Hadoop）

在 hadoop100 上操作：

```bash
# 上传 spark-3.3.1-bin-without-hadoop.tgz 到 /opt/software/（或 wget 下载）
wget -P /opt/software/ https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-without-hadoop.tgz

tar -zxvf /opt/software/spark-3.3.1-bin-without-hadoop.tgz -C /opt/module/
mv /opt/module/spark-3.3.1-bin-without-hadoop /opt/module/spark
```

环境变量：

```bash
sudo vim /etc/profile.d/my_env.sh

# SPARK_HOME
export SPARK_HOME=/opt/module/spark
export PATH=$PATH:$SPARK_HOME/bin

source /etc/profile.d/my_env.sh
```

#### 16.2 配置 spark-env.sh

```bash
# 重命名模板文件
mv /opt/module/spark/conf/spark-env.sh.template /opt/module/spark/conf/spark-env.sh

# 编辑
vim /opt/module/spark/conf/spark-env.sh
```

在末尾添加：

```bash
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
```

#### 16.3 配置 spark-defaults.conf

```bash
vim /opt/module/hive/conf/spark-defaults.conf
```

```properties
spark.master                    yarn
spark.eventLog.enabled          true
spark.eventLog.dir              hdfs://hadoop100:8020/spark-history
spark.executor.memory           1g
spark.driver.memory             1g
```

创建 HDFS 历史日志目录：

```bash
hadoop fs -mkdir /spark-history
```

> `spark-defaults.conf` 中的参数与 `hive-site.xml` 对应——Hive 启动时会读取此文件配置 SparkSession。

#### 16.4 上传 Spark 纯净版 jar 到 HDFS

```bash
hadoop fs -mkdir /spark-jars
hadoop fs -put /opt/module/spark/jars/* /spark-jars
```

> **为什么要上传到 HDFS**：Hive 任务由 Spark 执行，Spark 任务资源由 YARN 调度，可能分配到集群任何一个节点。将 Spark 依赖上传到 HDFS 确保所有节点都能访问。
>
> **为什么用纯净版**：`spark-3.3.1-bin-without-hadoop` 不含 Hadoop/Hive 依赖，避免与集群已有组件 jar 冲突。

#### 16.5 切换 Hive 执行引擎为 Spark

```bash
vim /opt/module/hive/conf/hive-site.xml
```

添加：

```xml
<!-- Spark 依赖位置（端口号 8020 必须和 NameNode 的端口号一致） -->
<property>
    <name>spark.yarn.jars</name>
    <value>hdfs://hadoop100:8020/spark-jars/*</value>
</property>

<!-- Hive 执行引擎 -->
<property>
    <name>hive.execution.engine</name>
    <value>spark</value>
</property>
```

#### 16.6 YARN 容量调度器确认

阶段二已配置。确认 `/opt/module/hadoop/etc/hadoop/capacity-scheduler.xml`：

```xml
<property>
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
    <value>0.8</value>
</property>
```

> 默认值 0.1（10%）在学习环境中过于保守，导致 ApplicationMaster 无法申请到足够资源。设为 0.8（80%）确保 Spark Driver 能正常提交到 YARN。

分发并重启 YARN：

```bash
xsync /opt/module/hadoop/etc/hadoop/capacity-scheduler.xml
# 在 hadoop101 上
stop-yarn.sh
start-yarn.sh
```

#### 16.7 Hive on Spark 测试

```bash
hive
```

```sql
CREATE TABLE student(id INT, name STRING);
INSERT INTO student VALUES(1, 'abc');
```

访问 YARN 界面 http://192.168.100.131:8088，应能看到 Spark 任务。

测试完成后删除测试表：

```sql
DROP TABLE student;
```

> **常见报错**：
> - `Failed to execute spark task` + `NoClassDefFoundError` → 使用的不是重新编译的 Hive 版本
> - `Guava version mismatch` → 删除 `/opt/module/hive/lib/guava-19.0.jar`，拷贝 `/opt/module/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar` 到 hive/lib

#### 16.8 分发

Hive 和 Spark 只需部署在 hadoop100（hiveserver2 运行节点）。Spark 运行所需的 jar 已在 16.4 上传到 HDFS，YARN 调度任务时会自动分发到各节点，无需本地安装。

```bash
# 仅分发环境变量
xsync /etc/profile.d/my_env.sh
xcall "source /etc/profile"
```


---

## 17. 阶段验证清单

| # | 验证项 | 命令 | 预期 |
|---|--------|------|------|
| 1 | f2 日志消费 Flume | `xcall jps \| grep Application` | hadoop102 有 Application |
| 2 | 日志数据落地 HDFS | `hdfs dfs -ls /origin_data/gmall/log/topic_log/` | 有日期分区 |
| 3 | f3 业务消费 Flume | `xcall jps \| grep Application` | hadoop102 有两个 Application |
| 4 | 增量表数据落地 | `hdfs dfs -ls /origin_data/gmall/db/ \| grep _inc` | 13个目录 |
| 5 | DataX 全量同步 | `hdfs dfs -ls /origin_data/gmall/db/ \| grep _full` | 17个目录 |
| 6 | Hive 客户端 | `hive -e "show databases"` | 输出 default |
| 7 | Hive on Spark | `INSERT INTO test_spark...` | YARN 出现 Spark 任务 |

> **阶段四完成** | 下一阶段：数仓分层建模（ODS/DIM/DWD/DWS/ADS）
