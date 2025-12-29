#!/bin/bash
# ETL调度脚本 - 用于crontab定时执行

# 设置日志文件
LOG_DIR="/var/log/enterprise_dw"
mkdir -p $LOG_DIR

DATE=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/etl_${DATE}.log"

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "$(date): 开始执行定时ETL任务" >> $LOG_FILE

# 执行ETL流程
$SCRIPT_DIR/run_etl.sh >> $LOG_FILE 2>&1

# 检查执行结果
if [ $? -eq 0 ]; then
    echo "$(date): ETL任务执行成功" >> $LOG_FILE
    
    # 发送成功通知（可选）
    # echo "ETL任务执行成功 - $DATE" | mail -s "ETL Success" admin@company.com
    
else
    echo "$(date): ETL任务执行失败" >> $LOG_FILE
    
    # 发送失败告警（可选）
    # echo "ETL任务执行失败 - $DATE，请检查日志: $LOG_FILE" | mail -s "ETL FAILED" admin@company.com
    
    exit 1
fi

# 清理旧日志（保留30天）
find $LOG_DIR -name "etl_*.log" -mtime +30 -delete

echo "$(date): 定时ETL任务完成" >> $LOG_FILE