#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量监控模块
监控数据仓库中的数据质量问题
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import logging

class DataQualityMonitor:
    def __init__(self):
        """初始化数据质量监控器"""
        self.spark = SparkSession.builder \
            .appName("DataQualityMonitor") \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def check_data_completeness(self, table_name, partition_date):
        """检查数据完整性"""
        try:
            # 检查空值比例
            df = self.spark.sql(f"SELECT * FROM {table_name} WHERE dt = '{partition_date}'")
            total_count = df.count()
            
            if total_count == 0:
                self.logger.warning(f"表 {table_name} 在分区 {partition_date} 中没有数据")
                return False
            
            # 检查关键字段的空值情况
            null_checks = {}
            for column in df.columns:
                if column != 'dt':  # 排除分区字段
                    null_count = df.filter(col(column).isNull()).count()
                    null_ratio = null_count / total_count
                    null_checks[column] = {
                        'null_count': null_count,
                        'null_ratio': null_ratio
                    }
                    
                    if null_ratio > 0.1:  # 空值比例超过10%
                        self.logger.warning(f"字段 {column} 空值比例过高: {null_ratio:.2%}")
            
            self.logger.info(f"表 {table_name} 数据完整性检查完成，总记录数: {total_count}")
            return True
            
        except Exception as e:
            self.logger.error(f"数据完整性检查失败: {str(e)}")
            return False
    
    def check_data_consistency(self, partition_date):
        """检查数据一致性"""
        try:
            # 检查用户数据一致性
            user_ods_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT user_id) as count 
                FROM ods_enterprise.ods_user_info 
                WHERE dt = '{partition_date}'
            """).collect()[0]['count']
            
            user_dwd_count = self.spark.sql(f"""
                SELECT COUNT(DISTINCT user_id) as count 
                FROM dwd_enterprise.dwd_user_info 
                WHERE dt = '{partition_date}'
            """).collect()[0]['count']
            
            if user_ods_count != user_dwd_count:
                self.logger.warning(f"用户数据不一致: ODS({user_ods_count}) vs DWD({user_dwd_count})")
            
            # 检查订单数据一致性
            order_ods_count = self.spark.sql(f"""
                SELECT COUNT(*) as count 
                FROM ods_enterprise.ods_order_info 
                WHERE dt = '{partition_date}'
            """).collect()[0]['count']
            
            order_dwd_count = self.spark.sql(f"""
                SELECT COUNT(*) as count 
                FROM dwd_enterprise.dwd_order_info 
                WHERE dt = '{partition_date}'
            """).collect()[0]['count']
            
            consistency_ratio = order_dwd_count / order_ods_count if order_ods_count > 0 else 0
            
            if consistency_ratio < 0.95:  # 数据保留率低于95%
                self.logger.warning(f"订单数据一致性问题: 保留率 {consistency_ratio:.2%}")
            
            self.logger.info(f"数据一致性检查完成，订单保留率: {consistency_ratio:.2%}")
            return True
            
        except Exception as e:
            self.logger.error(f"数据一致性检查失败: {str(e)}")
            return False
    
    def check_data_accuracy(self, partition_date):
        """检查数据准确性"""
        try:
            # 检查金额字段的合理性
            invalid_amounts = self.spark.sql(f"""
                SELECT COUNT(*) as count
                FROM dwd_enterprise.dwd_order_info
                WHERE dt = '{partition_date}'
                AND (total_amount <= 0 OR unit_price <= 0 OR quantity <= 0)
            """).collect()[0]['count']
            
            if invalid_amounts > 0:
                self.logger.warning(f"发现 {invalid_amounts} 条金额异常的订单记录")
            
            # 检查日期字段的合理性
            invalid_dates = self.spark.sql(f"""
                SELECT COUNT(*) as count
                FROM dwd_enterprise.dwd_order_info
                WHERE dt = '{partition_date}'
                AND (order_date > current_date() OR order_date < '2020-01-01')
            """).collect()[0]['count']
            
            if invalid_dates > 0:
                self.logger.warning(f"发现 {invalid_dates} 条日期异常的订单记录")
            
            self.logger.info("数据准确性检查完成")
            return True
            
        except Exception as e:
            self.logger.error(f"数据准确性检查失败: {str(e)}")
            return False
    
    def generate_quality_report(self, partition_date):
        """生成数据质量报告"""
        try:
            report = {
                'check_date': partition_date,
                'timestamp': datetime.now().isoformat(),
                'tables_checked': [],
                'issues_found': [],
                'overall_status': 'PASS'
            }
            
            # 检查各层数据表
            tables_to_check = [
                'ods_enterprise.ods_user_info',
                'ods_enterprise.ods_order_info',
                'dwd_enterprise.dwd_user_info',
                'dwd_enterprise.dwd_order_info',
                'dws_enterprise.dws_user_daily_stats',
                'dws_enterprise.dws_product_daily_stats'
            ]
            
            for table in tables_to_check:
                if self.check_data_completeness(table, partition_date):
                    report['tables_checked'].append(table)
                else:
                    report['issues_found'].append(f"数据完整性问题: {table}")
                    report['overall_status'] = 'FAIL'
            
            # 检查数据一致性
            if not self.check_data_consistency(partition_date):
                report['issues_found'].append("数据一致性问题")
                report['overall_status'] = 'FAIL'
            
            # 检查数据准确性
            if not self.check_data_accuracy(partition_date):
                report['issues_found'].append("数据准确性问题")
                report['overall_status'] = 'FAIL'
            
            # 输出报告
            self.logger.info("=" * 50)
            self.logger.info("数据质量检查报告")
            self.logger.info("=" * 50)
            self.logger.info(f"检查日期: {report['check_date']}")
            self.logger.info(f"检查时间: {report['timestamp']}")
            self.logger.info(f"总体状态: {report['overall_status']}")
            self.logger.info(f"检查表数: {len(report['tables_checked'])}")
            self.logger.info(f"发现问题: {len(report['issues_found'])}")
            
            if report['issues_found']:
                self.logger.info("问题详情:")
                for issue in report['issues_found']:
                    self.logger.info(f"  - {issue}")
            
            return report
            
        except Exception as e:
            self.logger.error(f"生成质量报告失败: {str(e)}")
            return None
        finally:
            self.spark.stop()

if __name__ == "__main__":
    import sys
    
    monitor = DataQualityMonitor()
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    report = monitor.generate_quality_report(target_date)
    
    if report and report['overall_status'] == 'FAIL':
        sys.exit(1)  # 如果质量检查失败，返回错误码