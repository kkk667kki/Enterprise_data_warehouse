-- 创建数据仓库分层数据库

-- 1. 创建ODS层（原始数据存储层）
CREATE DATABASE IF NOT EXISTS ods_enterprise
COMMENT '原始数据存储层 - Operational Data Store'
LOCATION '/user/hive/warehouse/ods_enterprise.db';

-- 2. 创建DWD层（数据仓库明细层）
CREATE DATABASE IF NOT EXISTS dwd_enterprise
COMMENT '数据仓库明细层 - Data Warehouse Detail'
LOCATION '/user/hive/warehouse/dwd_enterprise.db';

-- 3. 创建DWS层（数据仓库汇总层）
CREATE DATABASE IF NOT EXISTS dws_enterprise
COMMENT '数据仓库汇总层 - Data Warehouse Summary'
LOCATION '/user/hive/warehouse/dws_enterprise.db';

-- 4. 创建ADS层（应用数据服务层）
CREATE DATABASE IF NOT EXISTS ads_enterprise
COMMENT '应用数据服务层 - Application Data Service'
LOCATION '/user/hive/warehouse/ads_enterprise.db';

-- 5. 创建DIM层（维度数据层）
CREATE DATABASE IF NOT EXISTS dim_enterprise
COMMENT '维度数据层 - Dimension Data'
LOCATION '/user/hive/warehouse/dim_enterprise.db';

-- 显示所有数据库
SHOW DATABASES;