#!/bin/bash
# ETL流程执行脚本

set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# 设置环境变量
source $SCRIPT_DIR/setup_environment.sh

# 默认处理昨天的数据
TARGET_DATE=${1:-$(date -d "yesterday" +%Y-%m-%d)}

echo "开始执行ETL流程，目标日期: $TARGET_DATE"

# 1. 创建数据库和表结构（如果不存在）
echo "步骤1: 初始化数据库结构..."
hive -f $PROJECT_DIR/sql/create_database.sql
hive -f $PROJECT_DIR/sql/create_tables.sql

# 2. 运行ETL处理
echo "步骤2: 执行ETL数据处理..."
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-cores 2 \
    --executor-memory 4g \
    --driver-memory 2g \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --jars /opt/mysql-connector-java.jar \
    $PROJECT_DIR/src/etl_processor.py $TARGET_DATE

# 检查ETL执行结果
if [ $? -eq 0 ]; then
    echo "ETL处理成功完成"
else
    echo "ETL处理失败"
    exit 1
fi

# 3. 运行数据质量检查
echo "步骤3: 执行数据质量检查..."
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --executor-cores 1 \
    --executor-memory 2g \
    --driver-memory 1g \
    --conf spark.sql.catalogImplementation=hive \
    $PROJECT_DIR/src/data_quality_monitor.py $TARGET_DATE

# 检查数据质量结果
if [ $? -eq 0 ]; then
    echo "数据质量检查通过"
else
    echo "数据质量检查失败，请检查数据"
    exit 1
fi

# 4. 生成处理报告
echo "步骤4: 生成处理报告..."
REPORT_FILE="/tmp/etl_report_${TARGET_DATE}.txt"

cat > $REPORT_FILE << EOF
ETL处理报告
===================
处理日期: $TARGET_DATE
处理时间: $(date)
状态: 成功

数据统计:
EOF

# 添加数据统计信息
hive -e "
USE dwd_enterprise;
SELECT 'DWD用户表记录数:', COUNT(*) FROM dwd_user_info WHERE dt='$TARGET_DATE';
SELECT 'DWD订单表记录数:', COUNT(*) FROM dwd_order_info WHERE dt='$TARGET_DATE';

USE dws_enterprise;
SELECT 'DWS用户统计表记录数:', COUNT(*) FROM dws_user_daily_stats WHERE dt='$TARGET_DATE';
SELECT 'DWS产品统计表记录数:', COUNT(*) FROM dws_product_daily_stats WHERE dt='$TARGET_DATE';
" >> $REPORT_FILE

echo "处理报告已生成: $REPORT_FILE"
cat $REPORT_FILE

echo "ETL流程全部完成！"