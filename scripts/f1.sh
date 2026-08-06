#!/bin/bash

case $1 in
"start")
    echo "========== 启动 hadoop100 日志采集 Flume =========="
    ssh hadoop100 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/file_to_kafka.conf >/dev/null 2>&1 &"

    echo "========== 启动 hadoop101 日志采集 Flume =========="
    ssh hadoop101 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/file_to_kafka.conf >/dev/null 2>&1 &"
    ;;
"stop")
    echo "========== 停止 hadoop100 日志采集 Flume =========="
    ssh hadoop100 "ps -ef | grep file_to_kafka | grep -v grep | awk '{print \$2}' | xargs -r kill -9"

    echo "========== 停止 hadoop101 日志采集 Flume =========="
    ssh hadoop101 "ps -ef | grep file_to_kafka | grep -v grep | awk '{print \$2}' | xargs -r kill -9"
    ;;
*)
    echo "Usage: f1.sh start|stop"
    ;;
esac
