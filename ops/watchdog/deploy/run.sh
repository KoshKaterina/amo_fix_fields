#!/bin/bash
# Обёртка для крона. На сервере: /opt/watchdog/run.sh
LOG=/var/log/watchdog.log
exec >> "$LOG" 2>&1
echo "=== $(date '+%F %T %Z') $1 ==="
/usr/bin/python3 /opt/watchdog/watchdog.py "$1"
echo "=== $(date '+%F %T %Z') $1: код выхода $? ==="
