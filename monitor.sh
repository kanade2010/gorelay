#!/bin/bash

# nohup ./monitor_relay.sh >/dev/null 2>&1 &

# ========== 配置区 ==========
APP="relay"
ARGS=""
INTERVAL=10  # 检查间隔（秒）
# ========== 配置结束 ==========

monitor_app() {
    if ! pgrep -f "$APP" >/dev/null 2>&1; then
        echo "$(date '+%F %T') - [$APP] 崩溃或未运行，正在重启..." >/dev/null 2>&1
        nohup $APP $ARGS >/dev/null 2>&1 &
        echo "$(date '+%F %T') - [$APP] 已重新拉起" >/dev/null 2>&1
    fi
}

while true; do
    monitor_app
    sleep $INTERVAL
done
