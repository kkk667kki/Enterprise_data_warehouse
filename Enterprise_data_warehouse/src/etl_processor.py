#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
企业数据仓库ETL处理器
负责数据的提取、转换和加载
"""

import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class ETLProcessor:
    def __init__(self, app_name="EnterpriseDataWarehouse"):
        """初始化ETL处理器"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
        # 设置日志级别
        self.spark.sparkContext.setLogLevel("WARN")
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def extract_data_from_mysql(self, table_name, mysql_config):
        """从MySQL提取数据"""
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", mysql_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", mysql_config["user"]) \
                .option("password", mysql_config["password"]) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            self.logger.info(f"成功从MySQL提取表 {table_name}，记录数: {df.count()}")
            return df
        except Exception as e:
            self.logger.error(f"从MySQL提取数据失败: {str(e)}")
            raise
    
    def load_to_ods(self, df, table_name, partition_date):
        """加载数据到ODS层"""
        try:
            # 添加分区字段
            df_with_partition = df.withColumn("dt", lit(partition_date))
            
            # 写入Hive表
            df_with_partition.write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable(f"ods_enterprise.{table_name}")
            
            self.logger.info(f"成功加载数据到ODS层表 {table_name}")
        except Exception as e:
            self.logger.error(f"加载数据到ODS层失败: {str(e)}")
            raise
    
    def transform_user_data(self, partition_date):
        """转换用户数据从ODS到DWD"""
        try:
            # 读取ODS层用户数据
            ods_df = self.spark.sql(f"""
                SELECT 
                    user_id,
                    username,
                    email,
                    phone,
                    register_time,
                    last_login_time,
                    status,
                    create_time,
                    update_time
                FROM ods_enterprise.ods_user_info
                WHERE dt = '{partition_date}'
            """)
            
            # 数据清洗和转换
            dwd_df = ods_df.select(
                col("user_id"),
                col("username"),
                col("email"),
                col("phone"),
                to_date(col("register_time")).alias("register_date"),
                to_date(col("last_login_time")).alias("last_login_date"),
                col("status"),
                when(col("status") == "active", True).otherwise(False).alias("is_active"),
                when(col("status") == "vip", "VIP")
                .when(col("status") == "premium", "Premium")
                .otherwise("Regular").alias("user_level"),
                to_timestamp(col("create_time")).alias("create_time"),
                to_timestamp(col("update_time")).alias("update_time")
            ).filter(col("user_id").isNotNull())
            
            # 写入DWD层
            dwd_df.withColumn("dt", lit(partition_date)) \
                .write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("dwd_enterprise.dwd_user_info")
            
            self.logger.info(f"成功转换用户数据到DWD层，处理记录数: {dwd_df.count()}")
            
        except Exception as e:
            self.logger.error(f"转换用户数据失败: {str(e)}")
            raise
    def transform_order_data(self, partition_date):
        """转换订单数据从ODS到DWD"""
        try:
            # 读取ODS层订单数据
            ods_df = self.spark.sql(f"""
                SELECT 
                    order_id,
                    user_id,
                    product_id,
                    product_name,
                    quantity,
                    unit_price,
                    total_amount,
                    order_status,
                    payment_method,
                    order_time,
                    payment_time,
                    create_time
                FROM ods_enterprise.ods_order_info
                WHERE dt = '{partition_date}'
            """)
            
            # 数据清洗和转换
            dwd_df = ods_df.select(
                col("order_id"),
                col("user_id"),
                col("product_id"),
                col("product_name"),
                # 假设从产品名称提取分类ID（实际应该从产品维度表获取）
                when(col("product_name").contains("手机"), "PHONE")
                .when(col("product_name").contains("电脑"), "COMPUTER")
                .when(col("product_name").contains("服装"), "CLOTHING")
                .otherwise("OTHER").alias("category_id"),
                col("quantity"),
                col("unit_price"),
                col("total_amount"),
                lit(0.0).alias("discount_amount"),  # 假设无折扣
                col("total_amount").alias("final_amount"),
                col("order_status"),
                col("payment_method"),
                to_date(col("order_time")).alias("order_date"),
                to_date(col("payment_time")).alias("payment_date"),
                to_timestamp(col("create_time")).alias("create_time")
            ).filter(
                col("order_id").isNotNull() & 
                col("user_id").isNotNull() & 
                col("total_amount") > 0
            )
            
            # 写入DWD层
            dwd_df.withColumn("dt", lit(partition_date)) \
                .write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("dwd_enterprise.dwd_order_info")
            
            self.logger.info(f"成功转换订单数据到DWD层，处理记录数: {dwd_df.count()}")
            
        except Exception as e:
            self.logger.error(f"转换订单数据失败: {str(e)}")
            raise
    
    def aggregate_user_daily_stats(self, partition_date):
        """聚合用户日统计数据到DWS层"""
        try:
            # 计算用户日统计
            user_stats_df = self.spark.sql(f"""
                SELECT 
                    user_id,
                    0 as login_count,  -- 需要从日志数据计算
                    COUNT(order_id) as order_count,
                    SUM(final_amount) as total_amount,
                    AVG(final_amount) as avg_order_amount,
                    MIN(create_time) as first_order_time,
                    MAX(create_time) as last_order_time,
                    to_date('{partition_date}') as stat_date
                FROM dwd_enterprise.dwd_order_info
                WHERE dt = '{partition_date}'
                GROUP BY user_id
            """)
            
            # 写入DWS层
            user_stats_df.withColumn("dt", lit(partition_date)) \
                .write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("dws_enterprise.dws_user_daily_stats")
            
            self.logger.info(f"成功聚合用户日统计数据，处理用户数: {user_stats_df.count()}")
            
        except Exception as e:
            self.logger.error(f"聚合用户日统计数据失败: {str(e)}")
            raise
    
    def aggregate_product_daily_stats(self, partition_date):
        """聚合产品日统计数据到DWS层"""
        try:
            # 计算产品日统计
            product_stats_df = self.spark.sql(f"""
                SELECT 
                    product_id,
                    product_name,
                    category_id,
                    COUNT(order_id) as order_count,
                    SUM(quantity) as total_quantity,
                    SUM(final_amount) as total_amount,
                    AVG(unit_price) as avg_price,
                    COUNT(DISTINCT user_id) as unique_users,
                    to_date('{partition_date}') as stat_date
                FROM dwd_enterprise.dwd_order_info
                WHERE dt = '{partition_date}'
                GROUP BY product_id, product_name, category_id
            """)
            
            # 写入DWS层
            product_stats_df.withColumn("dt", lit(partition_date)) \
                .write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("dws_enterprise.dws_product_daily_stats")
            
            self.logger.info(f"成功聚合产品日统计数据，处理产品数: {product_stats_df.count()}")
            
        except Exception as e:
            self.logger.error(f"聚合产品日统计数据失败: {str(e)}")
            raise
    
    def run_daily_etl(self, target_date=None):
        """运行日常ETL流程"""
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        self.logger.info(f"开始运行日期 {target_date} 的ETL流程")
        
        try:
            # MySQL配置（实际使用时需要配置真实的连接信息）
            mysql_config = {
                "url": "jdbc:mysql://localhost:3306/source_db",
                "user": "root",
                "password": "password"
            }
            
            # 1. 提取数据到ODS层
            self.logger.info("步骤1: 提取数据到ODS层")
            # user_df = self.extract_data_from_mysql("user_info", mysql_config)
            # self.load_to_ods(user_df, "ods_user_info", target_date)
            
            # order_df = self.extract_data_from_mysql("order_info", mysql_config)
            # self.load_to_ods(order_df, "ods_order_info", target_date)
            
            # 2. 转换数据到DWD层
            self.logger.info("步骤2: 转换数据到DWD层")
            self.transform_user_data(target_date)
            self.transform_order_data(target_date)
            
            # 3. 聚合数据到DWS层
            self.logger.info("步骤3: 聚合数据到DWS层")
            self.aggregate_user_daily_stats(target_date)
            self.aggregate_product_daily_stats(target_date)
            
            self.logger.info(f"ETL流程完成，处理日期: {target_date}")
            
        except Exception as e:
            self.logger.error(f"ETL流程执行失败: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    etl = ETLProcessor()
    
    # 获取命令行参数中的日期，如果没有则使用昨天
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    
    etl.run_daily_etl(target_date)