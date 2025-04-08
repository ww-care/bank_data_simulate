#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库操作模块

负责数据库连接、表结构创建和数据导入操作。
"""

import os
import time
import pandas as pd
import mysql.connector
from typing import Dict, List, Any, Optional, Union, Tuple
from mysql.connector import MySQLConnection, Error

# 导入项目模块
from src.config_manager import get_config_manager
from src.logger import get_logger


class DatabaseManager:
    """数据库管理类，负责数据库连接和操作"""
    
    def __init__(self, db_type: str = 'mysql'):
        """
        初始化数据库管理器
        
        Args:
            db_type: 数据库类型，默认为mysql
        """
        self.db_type = db_type
        self.config_manager = get_config_manager()
        self.logger = get_logger('database_manager')
        
        # 数据库连接
        self.connection = None
        
        # SQL脚本目录
        current_file = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_file))
        self.sql_dir = os.path.join(project_root, 'sql')
        
        # 表名称和主键映射
        self.tables_info = {
            'customer': {'pk': 'customer_id', 'description': '客户表'},
            'fund_account': {'pk': 'account_id', 'description': '资金账户表'},
            'account_transaction': {'pk': 'transaction_id', 'description': '资金账户流水表'},
            'loan_record': {'pk': 'loan_id', 'description': '借款记录表'},
            'investment_record': {'pk': 'investment_id', 'description': '理财记录表'},
            'product': {'pk': 'product_id', 'description': '产品表'},
            'customer_event': {'pk': 'event_id', 'description': '客户事件表'},
            'app_user': {'pk': 'app_user_id', 'description': 'APP用户表'},
            'wechat_follower': {'pk': 'follower_id', 'description': '公众号粉丝表'},
            'work_wechat_contact': {'pk': 'contact_id', 'description': '企业微信联系人表'},
            'bank_manager': {'pk': 'manager_id', 'description': '银行经理表'},
            'channel_profile': {'pk': 'profile_id', 'description': '全渠道档案表'},
            'deposit_type': {'pk': 'deposit_type_id', 'description': '存款类型表'},
            'data_generation_log': {'pk': 'log_id', 'description': '数据生成日志表'}
        }
    
    def connect(self) -> bool:
        """
        连接到数据库
        
        Returns:
            连接是否成功
        """
        try:
            if self.connection is not None and self.connection.is_connected():
                return True
            
            db_config = self.config_manager.get_db_config(self.db_type)
            self.logger.info(f"正在连接到 {self.db_type} 数据库: {db_config.get('host')}:{db_config.get('port', '3306')}...")
            
            self.connection = mysql.connector.connect(
                host=db_config.get('host'),
                user=db_config.get('user'),
                password=db_config.get('password'),
                database=db_config.get('database'),
                port=int(db_config.get('port', 3306)),
                charset=db_config.get('charset', 'utf8mb4'),
                use_pure=True,
                connection_timeout=int(db_config.get('timeout', 10)),
                autocommit=True
            )
            
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                self.logger.info(f"成功连接到MySQL数据库，版本: {db_info}")
                return True
            else:
                self.logger.error("数据库连接失败")
                return False
        
        except Error as e:
            self.logger.error(f"数据库连接错误: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """关闭数据库连接"""
        if self.connection is not None and self.connection.is_connected():
            self.connection.close()
            self.logger.info("数据库连接已关闭")
    
    def execute_query(self, query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> List[Dict]:
        """
        执行查询SQL
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果列表
        """
        cursor = None
        try:
            if not self.connect():
                raise Exception("无法连接到数据库")
            
            cursor = self.connection.cursor(dictionary=True)
            start_time = time.time()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            result = cursor.fetchall()
            elapsed_time = time.time() - start_time
            
            self.logger.debug(f"查询执行完成，耗时: {elapsed_time:.3f}秒，返回 {len(result)} 条记录")
            return result
        
        except Error as e:
            self.logger.error(f"查询执行错误: {str(e)}")
            raise
        
        finally:
            if cursor:
                cursor.close()
    
    def execute_update(self, query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> int:
        """
        执行更新SQL（INSERT, UPDATE, DELETE等）
        
        Args:
            query: SQL更新语句
            params: 更新参数
            
        Returns:
            受影响的行数
        """
        cursor = None
        try:
            if not self.connect():
                raise Exception("无法连接到数据库")
            
            cursor = self.connection.cursor()
            start_time = time.time()
            
            # 记录SQL语句（用于调试）
            truncated_query = query[:500] + "..." if len(query) > 500 else query
            self.logger.debug(f"执行更新SQL: {truncated_query}")
            
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                affected_rows = cursor.rowcount
                self.connection.commit()
                
                elapsed_time = time.time() - start_time
                self.logger.debug(f"更新执行完成，耗时: {elapsed_time:.3f}秒，影响 {affected_rows} 行")
                
                return affected_rows
                
            except mysql.connector.IntegrityError as ie:
                # 处理数据完整性错误（如主键冲突）
                if 'Duplicate entry' in str(ie):
                    # 检查是否包含 ON DUPLICATE KEY UPDATE
                    if 'ON DUPLICATE KEY UPDATE' in query:
                        # 如果是 INSERT ... ON DUPLICATE KEY UPDATE，这个错误不应该发生
                        self.logger.error(f"带ON DUPLICATE KEY UPDATE的插入出错: {str(ie)}")
                        self.logger.error(f"SQL: {truncated_query}")
                        self.logger.error(f"Params: {params}")
                        raise
                    else:
                        # 尝试执行更新而不是插入
                        self.logger.warning(f"主键冲突，尝试执行更新操作: {str(ie)}")
                        
                        # 从错误信息中提取主键值
                        # 预期格式: Duplicate entry 'STATUS_xxx' for key 'primary'
                        error_parts = str(ie).split("'")
                        if len(error_parts) > 1:
                            primary_key_value = error_parts[1]
                            self.logger.info(f"主键值: {primary_key_value}")
                            
                            # 构造更新语句（这里假设是INSERT INTO generation_status）
                            if 'generation_status' in query:
                                # 为了安全，只在特定表上执行这个操作
                                try:
                                    # 假设params是一个元组，格式为 (id, run_id, start_time, ...)
                                    # 我们需要将其转换为更新语句。如果params结构不符合预期，这里会抛出异常
                                    if isinstance(params, tuple) and len(params) >= 8:
                                        update_query = """
                                        UPDATE generation_status 
                                        SET run_id = %s, last_update_time = %s, current_stage = %s, 
                                            completed_stages = %s, stage_progress = %s, status = %s, details = %s 
                                        WHERE id = %s
                                        """
                                        # 重新排列参数以匹配更新语句
                                        update_params = (params[1], params[3], params[4], 
                                                         params[5], params[6], params[7], 
                                                         params[8] if len(params) > 8 else '', primary_key_value)
                                        
                                        # 执行更新
                                        cursor = self.connection.cursor()
                                        cursor.execute(update_query, update_params)
                                        affected_rows = cursor.rowcount
                                        self.connection.commit()
                                        
                                        self.logger.info(f"成功将插入转换为更新，影响 {affected_rows} 行")
                                        return affected_rows
                                except Exception as convert_error:
                                    self.logger.error(f"尝试将插入转换为更新时出错: {str(convert_error)}")
                
                # 如果无法转换或其他完整性错误，回滚并抛出异常
                self.connection.rollback()
                raise
        
        except Error as e:
            self.logger.error(f"更新执行错误: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise
        
        finally:
            if cursor:
                cursor.close()
    
    def execute_many(self, query: str, params_list: List[Union[Dict, List, Tuple]]) -> int:
        """
        批量执行SQL（适用于大量数据插入）
        
        Args:
            query: SQL语句
            params_list: 参数列表
            
        Returns:
            受影响的行数
        """
        cursor = None
        try:
            if not self.connect():
                raise Exception("无法连接到数据库")
            
            cursor = self.connection.cursor()
            start_time = time.time()
            
            cursor.executemany(query, params_list)
            affected_rows = cursor.rowcount
            self.connection.commit()
            
            elapsed_time = time.time() - start_time
            self.logger.debug(f"批量执行完成，耗时: {elapsed_time:.3f}秒，影响 {affected_rows} 行")
            
            return affected_rows
        
        except Error as e:
            self.logger.error(f"批量执行错误: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise
        
        finally:
            if cursor:
                cursor.close()
    
    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        
        Args:
            table_name: 表名
            
        Returns:
            表是否存在
        """
        try:
            db_config = self.config_manager.get_db_config(self.db_type)
            database = db_config.get('database')
            
            query = """
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
            """
            params = (database, table_name)
            
            result = self.execute_query(query, params)
            return result[0]['count'] > 0
        
        except Exception as e:
            self.logger.error(f"检查表是否存在时出错: {str(e)}")
            return False
    
    def create_tables(self) -> bool:
        """
        创建所有表
        
        Returns:
            是否全部创建成功
        """
        # 检查SQL目录是否存在
        if not os.path.exists(self.sql_dir):
            self.logger.warning(f"SQL目录不存在: {self.sql_dir}，将使用内置SQL创建表")
            return self._create_tables_with_builtin_sql()
        
        success = True
        for table_name in self.tables_info.keys():
            sql_file = os.path.join(self.sql_dir, f"create_{table_name}.sql")
            
            # 如果SQL文件存在，则使用文件中的SQL
            if os.path.exists(sql_file):
                try:
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql = f.read()
                    
                    self.logger.info(f"正在创建表 {table_name}...")
                    self.execute_update(sql)
                    self.logger.info(f"表 {table_name} 创建成功")
                
                except Exception as e:
                    self.logger.error(f"创建表 {table_name} 时出错: {str(e)}")
                    success = False
            else:
                self.logger.warning(f"表 {table_name} 的SQL文件不存在: {sql_file}")
                success = self._create_table_with_builtin_sql(table_name) and success
        
        return success
    
    def _create_tables_with_builtin_sql(self) -> bool:
        """
        使用内置的SQL创建所有表
        
        Returns:
            是否全部创建成功
        """
        success = True
        for table_name in self.tables_info.keys():
            success = self._create_table_with_builtin_sql(table_name) and success
        return success
    
    def _create_table_with_builtin_sql(self, table_name: str) -> bool:
        """
        使用内置的SQL创建指定表
        
        Args:
            table_name: 表名
            
        Returns:
            是否创建成功
        """
        # 表结构定义
        table_sql = {
            'customer': """
                CREATE TABLE IF NOT EXISTS customer (
                    customer_id VARCHAR(20) PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    id_type VARCHAR(20) NOT NULL,
                    id_number VARCHAR(50) NOT NULL,
                    phone VARCHAR(20),
                    address VARCHAR(255),
                    email VARCHAR(100),
                    gender VARCHAR(10),
                    birth_date DATE,
                    registration_date DATE NOT NULL,
                    customer_type VARCHAR(20) NOT NULL COMMENT '个人/企业',
                    credit_score INT,
                    is_vip BOOLEAN,
                    branch_id VARCHAR(20),
                    occupation VARCHAR(100),
                    annual_income DECIMAL(18, 2),
                    business_type VARCHAR(50),
                    annual_revenue DECIMAL(18, 2),
                    establishment_date DATE,
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_type (customer_type),
                    INDEX idx_registration_date (registration_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户表';
            """,
            'fund_account': """
                CREATE TABLE IF NOT EXISTS fund_account (
                    account_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    account_type VARCHAR(20) NOT NULL COMMENT '活期账户/定期账户/贷款账户',
                    opening_date DATE NOT NULL,
                    balance DECIMAL(18, 2) NOT NULL,
                    currency VARCHAR(10) NOT NULL,
                    status VARCHAR(20) NOT NULL COMMENT '活跃/冻结/休眠/关闭',
                    branch_id VARCHAR(20),
                    deposit_type_id VARCHAR(20),
                    interest_rate DECIMAL(10, 6),
                    term INT COMMENT '月数',
                    maturity_date DATE,
                    loan_amount DECIMAL(18, 2),
                    issue_date DATE,
                    due_date DATE,
                    remaining_payments INT,
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_id (customer_id),
                    INDEX idx_account_type (account_type),
                    INDEX idx_status (status),
                    CONSTRAINT fk_fund_account_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资金账户表';
            """,
            'account_transaction': """
                CREATE TABLE IF NOT EXISTS account_transaction (
                    transaction_id VARCHAR(20) PRIMARY KEY,
                    account_id VARCHAR(20) NOT NULL,
                    transaction_type VARCHAR(20) NOT NULL COMMENT '存款/取款/转账/消费/其他',
                    amount DECIMAL(18, 2) NOT NULL,
                    transaction_datetime DATETIME NOT NULL,
                    status VARCHAR(20) NOT NULL COMMENT '成功/失败/处理中/取消',
                    description VARCHAR(255),
                    channel VARCHAR(20) COMMENT '网银/APP/ATM/柜台/微信/支付宝',
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_account_id (account_id),
                    INDEX idx_transaction_type (transaction_type),
                    INDEX idx_transaction_datetime (transaction_datetime),
                    INDEX idx_status (status),
                    CONSTRAINT fk_account_transaction_account FOREIGN KEY (account_id) REFERENCES fund_account (account_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资金账户流水表';
            """,
            'loan_record': """
                CREATE TABLE IF NOT EXISTS loan_record (
                    loan_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    account_id VARCHAR(20) NOT NULL,
                    loan_type VARCHAR(50) NOT NULL COMMENT '个人消费贷/住房贷款/汽车贷款/教育贷款/小微企业贷',
                    loan_amount DECIMAL(18, 2) NOT NULL,
                    interest_rate DECIMAL(10, 6) NOT NULL,
                    term INT NOT NULL COMMENT '月数',
                    application_date DATE NOT NULL,
                    approval_date DATE,
                    status VARCHAR(20) NOT NULL COMMENT '申请中/已批准/已放款/还款中/已结清/逾期/拒绝',
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_id (customer_id),
                    INDEX idx_account_id (account_id),
                    INDEX idx_loan_type (loan_type),
                    INDEX idx_status (status),
                    CONSTRAINT fk_loan_record_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
                    CONSTRAINT fk_loan_record_account FOREIGN KEY (account_id) REFERENCES fund_account (account_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='借款记录表';
            """,
            'investment_record': """
                CREATE TABLE IF NOT EXISTS investment_record (
                    investment_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    account_id VARCHAR(20) NOT NULL,
                    product_id VARCHAR(20) NOT NULL,
                    amount DECIMAL(18, 2) NOT NULL,
                    purchase_date DATE NOT NULL,
                    term INT NOT NULL COMMENT '天数',
                    maturity_date DATE NOT NULL,
                    status VARCHAR(20) NOT NULL COMMENT '持有中/已到期/已赎回/已违约',
                    channel VARCHAR(20),
                    expected_return DECIMAL(10, 6),
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_id (customer_id),
                    INDEX idx_account_id (account_id),
                    INDEX idx_product_id (product_id),
                    INDEX idx_status (status),
                    CONSTRAINT fk_investment_record_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
                    CONSTRAINT fk_investment_record_account FOREIGN KEY (account_id) REFERENCES fund_account (account_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='理财记录表';
            """,
            'product': """
                CREATE TABLE IF NOT EXISTS product (
                    product_id VARCHAR(20) PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    type VARCHAR(50) NOT NULL COMMENT '存款产品/贷款产品/理财产品',
                    interest_rate DECIMAL(10, 6),
                    term INT,
                    expected_return DECIMAL(10, 6),
                    risk_level VARCHAR(10),
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_type (type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产品表';
            """,
            'customer_event': """
                CREATE TABLE IF NOT EXISTS customer_event (
                    event_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    event_type VARCHAR(50) NOT NULL COMMENT '登录/查询/交易/咨询/购买/投诉/反馈',
                    event_channel VARCHAR(20) NOT NULL COMMENT 'app/online_banking/branch/call_center/atm/wechat/work_wechat',
                    event_datetime DATETIME NOT NULL,
                    event_result VARCHAR(20) NOT NULL COMMENT '成功/处理中/失败/取消',
                    product_id VARCHAR(20),
                    details VARCHAR(255),
                    is_vip_event BOOLEAN,
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_id (customer_id),
                    INDEX idx_event_type (event_type),
                    INDEX idx_event_datetime (event_datetime),
                    CONSTRAINT fk_customer_event_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户事件表';
            """,
            'app_user': """
                CREATE TABLE IF NOT EXISTS app_user (
                    app_user_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    registration_date DATE NOT NULL,
                    last_login_date DATE,
                    device_os VARCHAR(20),
                    device_type VARCHAR(20),
                    device_model VARCHAR(50),
                    activity_level VARCHAR(20),
                    used_features TEXT,
                    login_frequency INT,
                    push_notification BOOLEAN,
                    app_version VARCHAR(20),
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_id (customer_id),
                    CONSTRAINT fk_app_user_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='APP用户表';
            """,
            'wechat_follower': """
                CREATE TABLE IF NOT EXISTS wechat_follower (
                    follower_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    follow_date DATE NOT NULL,
                    last_read_date DATE,
                    interaction_level VARCHAR(20),
                    reading_frequency INT,
                    reading_unit VARCHAR(20),
                    has_participated_campaign BOOLEAN,
                    has_converted BOOLEAN,
                    is_subscribed BOOLEAN NOT NULL,
                    source VARCHAR(20),
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_id (customer_id),
                    CONSTRAINT fk_wechat_follower_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公众号粉丝表';
            """,
            'work_wechat_contact': """
                CREATE TABLE IF NOT EXISTS work_wechat_contact (
                    contact_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    manager_id VARCHAR(20) NOT NULL,
                    add_date DATE NOT NULL,
                    last_contact_date DATE,
                    contact_frequency VARCHAR(20),
                    days_between_contacts INT,
                    tags TEXT,
                    remark VARCHAR(100),
                    is_group_chat_member BOOLEAN,
                    priority_level VARCHAR(20),
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_id (customer_id),
                    INDEX idx_manager_id (manager_id),
                    CONSTRAINT fk_work_wechat_contact_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='企业微信联系人表';
            """,
            'bank_manager': """
                CREATE TABLE IF NOT EXISTS bank_manager (
                    manager_id VARCHAR(20) PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    branch_id VARCHAR(20) NOT NULL,
                    phone VARCHAR(20),
                    email VARCHAR(100),
                    customer_count INT,
                    position VARCHAR(50),
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_branch_id (branch_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='银行经理表';
            """,
            'channel_profile': """
                CREATE TABLE IF NOT EXISTS channel_profile (
                    profile_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(20) NOT NULL,
                    channels_count INT NOT NULL,
                    channels_used TEXT NOT NULL,
                    primary_channel VARCHAR(20) NOT NULL,
                    secondary_channel VARCHAR(20),
                    channel_frequency TEXT,
                    channel_scores TEXT,
                    last_active_days TEXT,
                    conversion_rate DECIMAL(5,4),
                    last_updated DATE,
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_customer_id (customer_id),
                    CONSTRAINT fk_channel_profile_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全渠道档案表';
            """,
            'deposit_type': """
                CREATE TABLE IF NOT EXISTS deposit_type (
                    deposit_type_id VARCHAR(20) PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    description VARCHAR(255),
                    base_interest_rate DECIMAL(10, 6) NOT NULL,
                    min_term INT,
                    max_term INT,
                    min_amount DECIMAL(18, 2),
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='存款类型表';
            """,
            'data_generation_log': """
                CREATE TABLE IF NOT EXISTS data_generation_log (
                    log_id VARCHAR(20) PRIMARY KEY,
                    generation_mode VARCHAR(20) NOT NULL COMMENT '历史/实时',
                    start_time DATETIME NOT NULL,
                    end_time DATETIME,
                    start_date DATE,
                    end_date DATE,
                    status VARCHAR(20) NOT NULL COMMENT '运行中/成功/失败/中断',
                    records_generated INT,
                    details TEXT,
                    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_generation_mode (generation_mode),
                    INDEX idx_status (status),
                    INDEX idx_start_date (start_date),
                    INDEX idx_end_date (end_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据生成日志表';
            """
        }
        
        if table_name not in table_sql:
            self.logger.error(f"未找到表 {table_name} 的SQL定义")
            return False
        
        try:
            if self.table_exists(table_name):
                self.logger.info(f"表 {table_name} 已存在，跳过创建")
                return True
            
            self.logger.info(f"正在创建表 {table_name}...")
            self.execute_update(table_sql[table_name])
            self.logger.info(f"表 {table_name} 创建成功")
            return True
        
        except Exception as e:
            self.logger.error(f"创建表 {table_name} 时出错: {str(e)}")
            return False
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> bool:
        """
        删除指定表
        
        Args:
            table_name: 表名
            if_exists: 是否仅在表存在时删除
            
        Returns:
            是否删除成功
        """
        try:
            if if_exists and not self.table_exists(table_name):
                self.logger.info(f"表 {table_name} 不存在，无需删除")
                return True
            
            sql = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{table_name}"
            self.execute_update(sql)
            self.logger.info(f"表 {table_name} 删除成功")
            return True
        
        except Exception as e:
            self.logger.error(f"删除表 {table_name} 时出错: {str(e)}")
            return False
    
    def truncate_table(self, table_name: str) -> bool:
        """
        清空指定表数据
        
        Args:
            table_name: 表名
            
        Returns:
            是否清空成功
        """
        try:
            if not self.table_exists(table_name):
                self.logger.warning(f"表 {table_name} 不存在，无法清空")
                return False
            
            sql = f"TRUNCATE TABLE {table_name}"
            self.execute_update(sql)
            self.logger.info(f"表 {table_name} 已清空")
            return True
        
        except Exception as e:
            self.logger.error(f"清空表 {table_name} 时出错: {str(e)}")
            return False
    
    def import_data(self, table_name: str, data: List[Dict[str, Any]], 
                    batch_size: int = 1000, update_on_duplicate: bool = False) -> int:
        """
        导入数据到指定表
        
        Args:
            table_name: 表名
            data: 数据列表，每个元素为一条记录的字典
            batch_size: 批处理大小
            update_on_duplicate: 遇到主键冲突时是否更新
            
        Returns:
            导入的记录数
        """
        if not data:
            self.logger.warning(f"没有数据需要导入到表 {table_name}")
            return 0
        
        try:
            total_imported = 0
            total_batches = (len(data) + batch_size - 1) // batch_size
            
            for i in range(0, len(data), batch_size):
                batch_data = data[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                if not batch_data:
                    continue
                
                # 构建INSERT语句
                columns = batch_data[0].keys()
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                # 处理主键冲突
                if update_on_duplicate:
                    updates = ', '.join([f"{col} = VALUES({col})" for col in columns 
                                        if col != self.tables_info.get(table_name, {}).get('pk')])
                    if updates:
                        sql += f" ON DUPLICATE KEY UPDATE {updates}"
                
                # 准备参数
                params = [[record.get(col) for col in columns] for record in batch_data]
                
                # 执行批量插入
                self.logger.info(f"正在导入第 {batch_num}/{total_batches} 批数据到表 {table_name}...")
                affected_rows = self.execute_many(sql, params)
                total_imported += affected_rows
                
                self.logger.info(f"第 {batch_num} 批导入完成，影响 {affected_rows} 行")
            
                self.logger.info(f"表 {table_name} 数据导入完成，共导入 {total_imported} 条记录")
            return total_imported
        
        except Exception as e:
            self.logger.error(f"导入数据到表 {table_name} 时出错: {str(e)}")
            raise
    
    def import_dataframe(self, table_name: str, df: pd.DataFrame, 
                         batch_size: int = 1000, update_on_duplicate: bool = False) -> int:
        """
        导入DataFrame数据到指定表
        
        Args:
            table_name: 表名
            df: DataFrame对象
            batch_size: 批处理大小
            update_on_duplicate: 遇到主键冲突时是否更新
            
        Returns:
            导入的记录数
        """
        if df.empty:
            self.logger.warning(f"DataFrame为空，没有数据需要导入到表 {table_name}")
            return 0
        
        # 将DataFrame转换为字典列表
        data = df.to_dict('records')
        return self.import_data(table_name, data, batch_size, update_on_duplicate)
    
    def get_last_timestamp(self, table_name: str, timestamp_column: str, 
                          condition: Optional[str] = None) -> Optional[str]:
        """
        获取表中最后一条记录的时间戳
        
        Args:
            table_name: 表名
            timestamp_column: 时间戳列名
            condition: 附加条件
            
        Returns:
            最后一条记录的时间戳，如果没有记录则返回None
        """
        try:
            if not self.table_exists(table_name):
                self.logger.warning(f"表 {table_name} 不存在")
                return None
            
            query = f"SELECT MAX({timestamp_column}) as last_timestamp FROM {table_name}"
            if condition:
                query += f" WHERE {condition}"
            
            result = self.execute_query(query)
            
            if result and result[0]['last_timestamp']:
                return result[0]['last_timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            return None
        
        except Exception as e:
            self.logger.error(f"获取表 {table_name} 的最后时间戳时出错: {str(e)}")
            return None
    
    def validate_data(self, table_name: str) -> Dict[str, Any]:
        """
        验证表数据的完整性和一致性
        
        Args:
            table_name: 表名
            
        Returns:
            验证结果统计信息
        """
        if not self.table_exists(table_name):
            return {'exists': False, 'error': f"表 {table_name} 不存在"}
        
        result = {
            'exists': True,
            'table_name': table_name,
            'record_count': 0,
            'null_count': {},
            'pk_unique': True,
            'fk_valid': True,
            'fk_invalid_count': 0
        }
        
        try:
            # 获取记录数
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = self.execute_query(count_query)
            result['record_count'] = count_result[0]['count']
            
            # 获取表结构信息
            structure_query = f"DESCRIBE {table_name}"
            structure = self.execute_query(structure_query)
            
            # 检查NULL值
            for column in structure:
                column_name = column['Field']
                if column['Null'] == 'NO':  # 不允许为NULL
                    null_query = f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {column_name} IS NULL"
                    null_result = self.execute_query(null_query)
                    null_count = null_result[0]['null_count']
                    if null_count > 0:
                        result['null_count'][column_name] = null_count
            
            # 检查主键唯一性
            pk = self.tables_info.get(table_name, {}).get('pk')
            if pk:
                pk_query = f"SELECT COUNT(*) as count, COUNT(DISTINCT {pk}) as distinct_count FROM {table_name}"
                pk_result = self.execute_query(pk_query)
                if pk_result[0]['count'] != pk_result[0]['distinct_count']:
                    result['pk_unique'] = False
            
            # TODO: 根据表之间的关系检查外键有效性
            
            return result
        
        except Exception as e:
            self.logger.error(f"验证表 {table_name} 数据时出错: {str(e)}")
            result['error'] = str(e)
            return result
    
    def generate_statistics(self, table_name: str) -> Dict[str, Any]:
        """
        生成表数据的统计信息
        
        Args:
            table_name: 表名
            
        Returns:
            统计信息
        """
        if not self.table_exists(table_name):
            return {'exists': False, 'error': f"表 {table_name} 不存在"}
        
        result = {
            'exists': True,
            'table_name': table_name,
            'record_count': 0,
            'columns': {}
        }
        
        try:
            # 获取记录数
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = self.execute_query(count_query)
            result['record_count'] = count_result[0]['count']
            
            # 获取表结构信息
            structure_query = f"DESCRIBE {table_name}"
            structure = self.execute_query(structure_query)
            
            # 对每个列生成统计信息
            for column in structure:
                column_name = column['Field']
                column_type = column['Type']
                
                # 跳过大文本字段
                if 'text' in column_type.lower():
                    continue
                
                column_stats = {'type': column_type}
                
                # 数值型和日期型列的统计
                if ('int' in column_type.lower() or 'decimal' in column_type.lower() or 
                    'float' in column_type.lower() or 'double' in column_type.lower() or
                    'date' in column_type.lower() or 'time' in column_type.lower()):
                    
                    stats_query = f"""
                    SELECT 
                        MIN({column_name}) as min_value,
                        MAX({column_name}) as max_value,
                        AVG({column_name}) as avg_value,
                        COUNT(DISTINCT {column_name}) as distinct_count
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    """
                    stats_result = self.execute_query(stats_query)
                    
                    if stats_result:
                        column_stats.update(stats_result[0])
                
                # 字符型列的统计
                elif 'char' in column_type.lower() or 'varchar' in column_type.lower():
                    stats_query = f"""
                    SELECT 
                        COUNT(DISTINCT {column_name}) as distinct_count,
                        AVG(LENGTH({column_name})) as avg_length,
                        MAX(LENGTH({column_name})) as max_length
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                    """
                    stats_result = self.execute_query(stats_query)
                    
                    if stats_result:
                        column_stats.update(stats_result[0])
                    
                    # 如果不同值较少，获取前10个值的分布
                    if stats_result and stats_result[0]['distinct_count'] < 100:
                        dist_query = f"""
                        SELECT {column_name} as value, COUNT(*) as count
                        FROM {table_name}
                        WHERE {column_name} IS NOT NULL
                        GROUP BY {column_name}
                        ORDER BY count DESC
                        LIMIT 10
                        """
                        dist_result = self.execute_query(dist_query)
                        column_stats['value_distribution'] = dist_result
                
                # 布尔型列的统计
                elif 'bool' in column_type.lower() or 'tinyint(1)' in column_type.lower():
                    stats_query = f"""
                    SELECT 
                        SUM(CASE WHEN {column_name} = TRUE THEN 1 ELSE 0 END) as true_count,
                        SUM(CASE WHEN {column_name} = FALSE THEN 1 ELSE 0 END) as false_count,
                        SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) as null_count
                    FROM {table_name}
                    """
                    stats_result = self.execute_query(stats_query)
                    
                    if stats_result:
                        column_stats.update(stats_result[0])
                
                result['columns'][column_name] = column_stats
            
            return result
        
        except Exception as e:
            self.logger.error(f"生成表 {table_name} 统计信息时出错: {str(e)}")
            result['error'] = str(e)
            return result
    
    def check_last_timestamp(self, table_name: str = 'data_generation_log', 
                            mode: str = 'realtime') -> Optional[str]:
        """
        检查最后一次数据生成的时间戳
        
        Args:
            table_name: 表名，默认为数据生成日志表
            mode: 数据生成模式，历史或实时
            
        Returns:
            最后一次生成数据的结束时间，如果没有记录则返回None
        """
        try:
            if not self.table_exists(table_name):
                self.logger.warning(f"表 {table_name} 不存在")
                return None
            
            query = """
            SELECT end_time, end_date 
            FROM data_generation_log 
            WHERE generation_mode = %s AND status = 'success'
            ORDER BY end_time DESC
            LIMIT 1
            """
            
            result = self.execute_query(query, (mode,))
            
            if result and (result[0]['end_time'] or result[0]['end_date']):
                if result[0]['end_time']:
                    return result[0]['end_time'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return result[0]['end_date'].strftime('%Y-%m-%d')
            
            return None
        
        except Exception as e:
            self.logger.error(f"检查最后时间戳时出错: {str(e)}")
            return None
    
    def log_data_generation(self, log_id: str, mode: str, start_time: str, 
                           status: str, start_date: Optional[str] = None, 
                           end_date: Optional[str] = None, end_time: Optional[str] = None,
                           records_generated: int = 0, details: Optional[str] = None) -> bool:
        """
        记录数据生成日志
        
        Args:
            log_id: 日志ID
            mode: 数据生成模式（历史/实时）
            start_time: 开始时间
            status: 状态（运行中/成功/失败/中断）
            start_date: 开始日期
            end_date: 结束日期
            end_time: 结束时间
            records_generated: 生成的记录数
            details: 详细信息
            
        Returns:
            是否记录成功
        """
        try:
            # 准备数据
            data = {
                'log_id': log_id,
                'generation_mode': mode,
                'start_time': start_time,
                'status': status,
                'records_generated': records_generated
            }
            
            if start_date:
                data['start_date'] = start_date
            
            if end_date:
                data['end_date'] = end_date
            
            if end_time:
                data['end_time'] = end_time
            
            if details:
                data['details'] = details
            
            # 检查日志是否已存在
            query = "SELECT COUNT(*) as count FROM data_generation_log WHERE log_id = %s"
            result = self.execute_query(query, (log_id,))
            
            if result[0]['count'] > 0:
                # 更新已有日志
                set_clauses = []
                params = []
                
                for key, value in data.items():
                    if key != 'log_id':  # 跳过主键
                        set_clauses.append(f"{key} = %s")
                        params.append(value)
                
                params.append(log_id)  # WHERE条件参数
                
                update_query = f"""
                UPDATE data_generation_log 
                SET {', '.join(set_clauses)}
                WHERE log_id = %s
                """
                
                self.execute_update(update_query, params)
                self.logger.debug(f"数据生成日志已更新: {log_id}")
            else:
                # 插入新日志
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
                
                insert_query = f"""
                INSERT INTO data_generation_log ({columns})
                VALUES ({placeholders})
                """
                
                self.execute_update(insert_query, list(data.values()))
                self.logger.debug(f"数据生成日志已创建: {log_id}")
            
            return True
        
        except Exception as e:
            self.logger.error(f"记录数据生成日志时出错: {str(e)}")
            return False


# 单例模式
_instance = None

def get_database_manager(db_type: str = 'mysql') -> DatabaseManager:
    """
    获取DatabaseManager的单例实例
    
    Args:
        db_type: 数据库类型
        
    Returns:
        DatabaseManager实例
    """
    global _instance
    if _instance is None:
        _instance = DatabaseManager(db_type)
    return _instance


if __name__ == "__main__":
    # 简单测试
    try:
        db_manager = get_database_manager()
        if db_manager.connect():
            print("数据库连接成功")
            
            # 创建表测试
            db_manager.create_tables()
            
            # 查询测试
            result = db_manager.execute_query("SHOW TABLES")
            print("数据库表:")
            for table in result:
                print(f"  - {list(table.values())[0]}")
        else:
            print("数据库连接失败")
    except Exception as e:
        print(f"测试过程中出错: {str(e)}")
    finally:
        if db_manager:
            db_manager.disconnect()