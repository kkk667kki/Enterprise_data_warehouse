-- 创建数据仓库表结构

-- ========== ODS层表 ==========
USE ods_enterprise;

-- 用户信息原始表
CREATE TABLE IF NOT EXISTS ods_user_info (
    user_id STRING,
    username STRING,
    email STRING,
    phone STRING,
    register_time STRING,
    last_login_time STRING,
    status STRING,
    create_time STRING,
    update_time STRING
) 
COMMENT '用户信息原始表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ods_enterprise.db/ods_user_info';

-- 订单信息原始表
CREATE TABLE IF NOT EXISTS ods_order_info (
    order_id STRING,
    user_id STRING,
    product_id STRING,
    product_name STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_status STRING,
    payment_method STRING,
    order_time STRING,
    payment_time STRING,
    create_time STRING
)
COMMENT '订单信息原始表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ods_enterprise.db/ods_order_info';

-- ========== DWD层表 ==========
USE dwd_enterprise;

-- 用户信息明细表
CREATE TABLE IF NOT EXISTS dwd_user_info (
    user_id STRING,
    username STRING,
    email STRING,
    phone STRING,
    register_date DATE,
    last_login_date DATE,
    status STRING,
    is_active BOOLEAN,
    user_level STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP
)
COMMENT '用户信息明细表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dwd_enterprise.db/dwd_user_info';

-- 订单信息明细表
CREATE TABLE IF NOT EXISTS dwd_order_info (
    order_id STRING,
    user_id STRING,
    product_id STRING,
    product_name STRING,
    category_id STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    order_status STRING,
    payment_method STRING,
    order_date DATE,
    payment_date DATE,
    create_time TIMESTAMP
)
COMMENT '订单信息明细表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dwd_enterprise.db/dwd_order_info';

-- ========== DWS层表 ==========
USE dws_enterprise;

-- 用户日统计表
CREATE TABLE IF NOT EXISTS dws_user_daily_stats (
    user_id STRING,
    login_count INT,
    order_count INT,
    total_amount DECIMAL(10,2),
    avg_order_amount DECIMAL(10,2),
    first_order_time TIMESTAMP,
    last_order_time TIMESTAMP,
    stat_date DATE
)
COMMENT '用户日统计表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dws_enterprise.db/dws_user_daily_stats';

-- 产品日统计表
CREATE TABLE IF NOT EXISTS dws_product_daily_stats (
    product_id STRING,
    product_name STRING,
    category_id STRING,
    order_count INT,
    total_quantity INT,
    total_amount DECIMAL(10,2),
    avg_price DECIMAL(10,2),
    unique_users INT,
    stat_date DATE
)
COMMENT '产品日统计表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dws_enterprise.db/dws_product_daily_stats';

-- ========== ADS层表 ==========
USE ads_enterprise;

-- 用户行为分析表
CREATE TABLE IF NOT EXISTS ads_user_behavior_analysis (
    user_id STRING,
    total_orders INT,
    total_amount DECIMAL(10,2),
    avg_order_amount DECIMAL(10,2),
    first_order_date DATE,
    last_order_date DATE,
    order_frequency DECIMAL(5,2),
    user_lifecycle STRING,
    preferred_category STRING,
    risk_level STRING,
    create_time TIMESTAMP
)
COMMENT '用户行为分析表'
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ads_enterprise.db/ads_user_behavior_analysis';

-- 销售趋势分析表
CREATE TABLE IF NOT EXISTS ads_sales_trend_analysis (
    stat_date DATE,
    total_orders INT,
    total_amount DECIMAL(10,2),
    avg_order_amount DECIMAL(10,2),
    new_users INT,
    active_users INT,
    retention_rate DECIMAL(5,4),
    growth_rate DECIMAL(5,4),
    create_time TIMESTAMP
)
COMMENT '销售趋势分析表'
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/ads_enterprise.db/ads_sales_trend_analysis';

-- ========== DIM层表 ==========
USE dim_enterprise;

-- 日期维度表
CREATE TABLE IF NOT EXISTS dim_date (
    date_key STRING,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    weekday INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season STRING,
    month_name STRING,
    weekday_name STRING
)
COMMENT '日期维度表'
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dim_enterprise.db/dim_date';