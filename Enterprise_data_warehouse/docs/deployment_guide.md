# 企业数据仓库部署指南

## 环境要求

### 硬件要求
- **最小配置**: 4核CPU, 16GB内存, 500GB存储
- **推荐配置**: 8核CPU, 32GB内存, 2TB SSD存储
- **生产环境**: 集群部署，至少3个节点

### 软件要求
- **操作系统**: Ubuntu 18.04+ / CentOS 7+
- **Java**: OpenJDK 8 或 Oracle JDK 8
- **Python**: 3.7+
- **MySQL**: 5.7+ 或 8.0+

## 安装步骤

### 1. 基础环境准备

```bash
# 更新系统
sudo apt update && sudo apt upgrade -y

# 安装Java 8
sudo apt install openjdk-8-jdk -y

# 设置JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc

# 安装Python依赖
sudo apt install python3 python3-pip -y
pip3 install pyspark pandas numpy
```

### 2. Hadoop安装配置

```bash
# 下载Hadoop
cd /opt
sudo wget https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
sudo tar -xzf hadoop-3.3.4.tar.gz
sudo mv hadoop-3.3.4 hadoop
sudo chown -R $USER:$USER /opt/hadoop

# 设置环境变量
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# 配置Hadoop
cp /path/to/project/config/hadoop_config.xml $HADOOP_HOME/etc/hadoop/core-site.xml
```

### 3. Hive安装配置

```bash
# 下载Hive
cd /opt
sudo wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
sudo tar -xzf apache-hive-3.1.3-bin.tar.gz
sudo mv apache-hive-3.1.3-bin hive
sudo chown -R $USER:$USER /opt/hive

# 设置环境变量
echo 'export HIVE_HOME=/opt/hive' >> ~/.bashrc
echo 'export PATH=$PATH:$HIVE_HOME/bin' >> ~/.bashrc
source ~/.bashrc

# 下载MySQL连接器
cd $HIVE_HOME/lib
sudo wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.33.jar

# 配置Hive
cp /path/to/project/config/hive_config.xml $HIVE_HOME/conf/hive-site.xml
```

### 4. Spark安装配置

```bash
# 下载Spark
cd /opt
sudo wget https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
sudo tar -xzf spark-3.4.1-bin-hadoop3.tgz
sudo mv spark-3.4.1-bin-hadoop3 spark
sudo chown -R $USER:$USER /opt/spark

# 设置环境变量
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc

# 配置Spark
cp /path/to/project/config/spark_config.conf $SPARK_HOME/conf/spark-defaults.conf
```

### 5. MySQL安装配置

```bash
# 安装MySQL
sudo apt install mysql-server -y

# 安全配置
sudo mysql_secure_installation

# 创建Hive元数据库和用户
sudo mysql -u root -p << EOF
CREATE DATABASE hive_metastore;
CREATE USER 'hive'@'localhost' IDENTIFIED BY 'hive123';
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'localhost';
FLUSH PRIVILEGES;
EOF
```

## 部署流程

### 1. 初始化环境

```bash
# 克隆项目
git clone <project_repository>
cd enterprise_data_warehouse

# 设置执行权限
chmod +x scripts/*.sh

# 运行环境设置脚本
./scripts/setup_environment.sh
```

### 2. 创建数据库结构

```bash
# 创建数据库
hive -f sql/create_database.sql

# 创建表结构
hive -f sql/create_tables.sql
```

### 3. 插入测试数据

```bash
# 插入示例数据
hive -f data/sample_data.sql
```

### 4. 运行ETL测试

```bash
# 执行ETL流程
./scripts/run_etl.sh 2024-12-26
```

### 5. 设置定时任务

```bash
# 编辑crontab
crontab -e

# 添加定时任务（每天凌晨2点执行）
0 2 * * * /path/to/enterprise_data_warehouse/scripts/schedule_etl.sh
```

## 监控和维护

### 1. 服务监控

```bash
# 检查Hadoop服务
jps

# 检查HDFS状态
hdfs dfsadmin -report

# 检查YARN状态
yarn node -list
```

### 2. 日志查看

```bash
# Hadoop日志
tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log

# Hive日志
tail -f /var/log/hive/metastore.log

# Spark日志
tail -f $SPARK_HOME/logs/spark-*-history-server-*.out
```

### 3. 性能优化

- 调整Spark执行器数量和内存配置
- 优化Hive表分区策略
- 定期清理HDFS临时文件
- 监控集群资源使用情况

## 故障排除

### 常见问题

1. **Hive连接MySQL失败**
   - 检查MySQL服务状态
   - 验证连接配置和权限

2. **Spark任务执行缓慢**
   - 增加执行器数量
   - 调整内存配置
   - 检查数据倾斜问题

3. **HDFS空间不足**
   - 清理临时文件
   - 增加存储节点
   - 调整副本数量

### 联系支持

如遇到部署问题，请提供以下信息：
- 错误日志
- 系统配置
- 部署步骤记录