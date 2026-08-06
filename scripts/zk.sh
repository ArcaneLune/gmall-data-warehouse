#!/bin/bash

case $1 in
"start")
    for i in hadoop100 hadoop101 hadoop102
    do
        echo ---------- zookeeper $i 启动 ------------
        ssh $i "/opt/module/zookeeper-3.7.1/bin/zkServer.sh start"
    done
    ;;
"stop")
    for i in hadoop100 hadoop101 hadoop102
    do
        echo ---------- zookeeper $i 停止 ------------
        ssh $i "/opt/module/zookeeper-3.7.1/bin/zkServer.sh stop"
    done
    ;;
"status")
    for i in hadoop100 hadoop101 hadoop102
    do
        echo ---------- zookeeper $i 状态 ------------
        ssh $i "/opt/module/zookeeper-3.7.1/bin/zkServer.sh status"
    done
    ;;
*)
    echo "Usage: zk.sh start|stop|status"
    ;;
esac
