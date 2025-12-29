#!/bin/bash
# 企业数据仓库环境设置脚本

set -e

echo "开始设置企业数据仓库环境..."

# 设置环境变量
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# 添加到PATH
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH

echo "环境变量设置完成"

# 创建必要的目录
echo "创建HDFS目录结构..."
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -mkdir -p /tmp/hive
hdfs dfs -mkdir -p /user/hadoop/data

# 设置目录权限
hdfs dfs -chmod 777 /user/hive/warehouse
hdfs dfs -chmod 777 /spark-logs
hdfs dfs -chmod 777 /tmp/hive

echo "HDFS目录创建完成"

# 初始化Hive元数据库
echo "初始化Hive元数据库..."
cd $HIVE_HOME/bin
./schematool -dbType mysql -initSchema

echo "Hive元数据库初始化完成"

# 启动Hadoop服务
echo "启动Hadoop服务..."
start-dfs.sh
start-yarn.sh

# 启动Hive Metastore服务
echo "启动Hive Metastore服务..."
nohup hive --service metastore > /var/log/hive/metastore.log 2>&1 &

# 启动Spark History Server
echo "启动Spark History Server..."
$SPARK_HOME/sbin/start-history-server.sh

echo "所有服务启动完成"

# 验证服务状态
echo "验证服务状态..."
jps

# 测试HDFS连接
echo "测试HDFS连接..."
hdfs dfs -ls /

# 测试Hive连接
echo "测试Hive连接..."
hive -e "SHOW DATABASES;"

echo "环境设置完成！"
echo "Web界面访问地址:"
echo "  - Hadoop NameNode: http://localhost:9870"
echo "  - YARN ResourceManager: http://localhost:8088"
echo "  - Spark History Server: http://localhost:18080"