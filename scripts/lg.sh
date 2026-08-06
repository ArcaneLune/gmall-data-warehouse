#!/bin/bash

echo "========== hadoop100 生成数据 =========="
ssh hadoop100 "cd /opt/module/applog/ && java -jar /opt/module/applog/gmall-remake-mock-2023-05-15-3.jar  \$1 \$2 \$3 >/dev/null 2>&1 &"
