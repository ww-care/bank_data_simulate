#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据生成模块

负责生成各类银行业务实体的模拟数据，包括客户、账户、交易记录等。
"""

import os
import uuid
import random
import pandas as pd
import numpy as np
import datetime
import faker
from typing import Dict, List, Tuple, Optional, Any, Union

# 导入项目模块
from src.config_manager import get_config_manager
from src.database_manager import get_database_manager
from src.time_manager.time_manager import get_time_manager
from src.logger import get_logger
from src.data_generator.entity_generators import (
    CustomerGenerator, 
    BankManagerGenerator, 
    ProductGenerator, 
    FundAccountGenerator,
    TransactionGenerator,
    LoanRecordGenerator,
    InvestmentRecordGenerator,
    AppUserGenerator,
    WechatFollowerGenerator,
    WorkWechatContactGenerator,
    ChannelProfileGenerator,
    CustomerEventGenerator,
    DepositTypeGenerator
)


class DataGenerator:
    """数据生成器总控类，协调各实体生成器的工作"""
    
    def __init__(self):
        """初始化数据生成器"""
        self.logger = get_logger('data_generator')
        self.config_manager = get_config_manager()
        self.time_manager = get_time_manager()
        self.db_manager = get_database_manager()
        
        # 获取系统配置
        self.system_config = self.config_manager.get_system_config()
        
        # 设置随机种子，确保可重复性
        random_seed = self.system_config.get('random_seed', 42)
        random.seed(random_seed)
        np.random.seed(random_seed)
        
        # 设置区域，用于生成本地化数据
        locale = self.system_config.get('locale', 'zh_CN')
        self.faker = faker.Faker(locale)
        
        # 获取批处理大小
        self.batch_size = self.system_config.get('batch_size', 1000)
        
        # 初始化实体生成器
        self._init_entity_generators()
        
        # 生成的数据缓存
        self.data_cache = {}
        
    def _init_entity_generators(self):
        """初始化各实体生成器"""
        self.customer_generator = CustomerGenerator(self.faker, self.config_manager)
        self.bank_manager_generator = BankManagerGenerator(self.faker, self.config_manager)
        self.product_generator = ProductGenerator(self.faker, self.config_manager)
        self.deposit_type_generator = DepositTypeGenerator(self.faker, self.config_manager)
        self.fund_account_generator = FundAccountGenerator(self.faker, self.config_manager)
        self.transaction_generator = TransactionGenerator(self.faker, self.config_manager, self.time_manager)
        self.loan_record_generator = LoanRecordGenerator(self.faker, self.config_manager)
        self.investment_record_generator = InvestmentRecordGenerator(self.faker, self.config_manager)
        self.app_user_generator = AppUserGenerator(self.faker, self.config_manager)
        self.wechat_follower_generator = WechatFollowerGenerator(self.faker, self.config_manager)
        self.work_wechat_contact_generator = WorkWechatContactGenerator(self.faker, self.config_manager)
        self.channel_profile_generator = ChannelProfileGenerator(self.faker, self.config_manager)
        self.customer_event_generator = CustomerEventGenerator(self.faker, self.config_manager, self.time_manager)
                
    def generate_data(self, start_date: datetime.date, end_date: datetime.date, mode: str = 'historical') -> Dict[str, int]:
        """
        生成指定时间范围内的模拟数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            mode: 数据生成模式，'historical'或'realtime'
            
        Returns:
            各实体生成的记录数统计
        """
        self.logger.info(f"开始生成 {mode} 模式数据，时间范围: {start_date} 至 {end_date}")
        
        stats = {}
        
        try:
            if mode == 'historical':
                stats = self.generate_historical_data(start_date, end_date)
            elif mode == 'realtime':
                stats = self.generate_realtime_data(start_date, end_date)
            else:
                raise ValueError(f"未知的数据生成模式: {mode}")
        
        except Exception as e:
            self.logger.error(f"生成数据过程中出错: {str(e)}", exc_info=True)
            raise
        
        return stats
    
    def generate_historical_data(self, start_date: datetime.date, end_date: datetime.date) -> Dict[str, int]:
        """
        生成历史数据模式下的各类实体数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            各实体生成的记录数统计
        """
        stats = {}
        
        # 清空数据缓存
        self.data_cache = {}
        
        # 1. 生成基础实体
        self.logger.info("生成银行经理数据...")
        bank_managers = self.bank_manager_generator.generate()
        stats['bank_manager'] = self.import_data('bank_manager', bank_managers)
        self.data_cache['bank_manager'] = bank_managers
        
        self.logger.info("生成存款类型数据...")
        deposit_types = self.deposit_type_generator.generate()
        stats['deposit_type'] = self.import_data('deposit_type', deposit_types)
        self.data_cache['deposit_type'] = deposit_types
        
        self.logger.info("生成产品数据...")
        products = self.product_generator.generate()
        stats['product'] = self.import_data('product', products)
        self.data_cache['product'] = products
        
        self.logger.info("生成客户数据...")
        customers = self.customer_generator.generate(bank_managers)
        stats['customer'] = self.import_data('customer', customers)
        self.data_cache['customer'] = customers
        
        # 2. 生成关联实体
        self.logger.info("生成资金账户数据...")
        fund_accounts = self.fund_account_generator.generate(customers, deposit_types)
        stats['fund_account'] = self.import_data('fund_account', fund_accounts)
        self.data_cache['fund_account'] = fund_accounts
        
        self.logger.info("生成APP用户数据...")
        app_users = self.app_user_generator.generate(customers)
        stats['app_user'] = self.import_data('app_user', app_users)
        self.data_cache['app_user'] = app_users
        
        self.logger.info("生成公众号粉丝数据...")
        wechat_followers = self.wechat_follower_generator.generate(customers, app_users)
        stats['wechat_follower'] = self.import_data('wechat_follower', wechat_followers)
        self.data_cache['wechat_follower'] = wechat_followers
        
        self.logger.info("生成企业微信联系人数据...")
        work_wechat_contacts = self.work_wechat_contact_generator.generate(customers)
        stats['work_wechat_contact'] = self.import_data('work_wechat_contact', work_wechat_contacts)
        self.data_cache['work_wechat_contact'] = work_wechat_contacts
        
        self.logger.info("生成全渠道档案数据...")
        channel_profiles = self.channel_profile_generator.generate(
            customers, app_users, wechat_followers, work_wechat_contacts)
        stats['channel_profile'] = self.import_data('channel_profile', channel_profiles)
        self.data_cache['channel_profile'] = channel_profiles
        
        # 3. 生成交易和事件数据
        self.logger.info("生成贷款记录数据...")
        loan_records = self.loan_record_generator.generate(customers, fund_accounts)
        stats['loan_record'] = self.import_data('loan_record', loan_records)
        self.data_cache['loan_record'] = loan_records
        
        self.logger.info("生成投资记录数据...")
        investment_records = self.investment_record_generator.generate(
            customers, fund_accounts, products)
        stats['investment_record'] = self.import_data('investment_record', investment_records)
        self.data_cache['investment_record'] = investment_records
        
        self.logger.info("生成客户事件数据...")
        customer_events = self.customer_event_generator.generate(
            customers, products, start_date, end_date)
        stats['customer_event'] = self.import_data('customer_event', customer_events)
        self.data_cache['customer_event'] = customer_events
        
        # 4. 生成历史交易数据（按日期范围分批处理）
        total_transactions = 0
        date_ranges = self._split_date_range(start_date, end_date, days_per_batch=30)
        
        for batch_start, batch_end in date_ranges:
            self.logger.info(f"生成交易数据，时间范围: {batch_start} 至 {batch_end}...")
            transactions = self.transaction_generator.generate(
                fund_accounts, batch_start, batch_end, mode='historical')
            
            batch_count = self.import_data('account_transaction', transactions)
            total_transactions += batch_count
            
            self.logger.info(f"导入 {batch_count} 条交易记录，累计: {total_transactions}")
        
        stats['account_transaction'] = total_transactions
        
        self.logger.info(f"历史数据生成完成，总记录数: {sum(stats.values())}")
        return stats
    
    def generate_realtime_data(self, start_date: datetime.date, end_date: datetime.date) -> Dict[str, int]:
        """
        生成实时数据模式下的增量数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            各实体生成的记录数统计
        """
        stats = {}
        
        # 实时模式主要生成交易数据和客户事件
        self.logger.info(f"生成实时交易数据，时间范围: {start_date} 至 {end_date}...")
        
        # 从数据库获取现有账户数据
        fund_accounts = self._load_fund_accounts()
        
        # 生成交易数据
        transactions = self.transaction_generator.generate(
            fund_accounts, start_date, end_date, mode='realtime')
        
        stats['account_transaction'] = self.import_data('account_transaction', transactions)
        
        # 生成客户事件
        self.logger.info("生成实时客户事件数据...")
        customers = self._load_customers()
        products = self._load_products()
        
        customer_events = self.customer_event_generator.generate(
            customers, products, start_date, end_date, mode='realtime')
        
        stats['customer_event'] = self.import_data('customer_event', customer_events)
        
        self.logger.info(f"实时数据生成完成，总记录数: {sum(stats.values())}")
        return stats
    
    def import_data(self, table_name: str, data: List[Dict]) -> int:
        """
        将生成的数据导入数据库
        
        Args:
            table_name: 表名
            data: 数据列表
            
        Returns:
            导入的记录数
        """
        if not data:
            self.logger.warning(f"没有数据需要导入到 {table_name}")
            return 0
        
        try:
            # 将数据转换为DataFrame
            df = pd.DataFrame(data)
            
            # 分批导入数据库
            records_count = self.db_manager.import_dataframe(
                table_name, df, batch_size=self.batch_size)
            
            self.logger.info(f"已导入 {records_count} 条记录到表 {table_name}")
            return records_count
            
        except Exception as e:
            self.logger.error(f"导入数据到表 {table_name} 时出错: {str(e)}")
            raise
    
    def _split_date_range(self, start_date: datetime.date, end_date: datetime.date, 
                         days_per_batch: int = 30) -> List[Tuple[datetime.date, datetime.date]]:
        """
        将日期范围分割为多个小范围
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            days_per_batch: 每个批次的天数
            
        Returns:
            日期范围列表，每个元素为(开始日期, 结束日期)
        """
        date_ranges = []
        current_date = start_date
        
        while current_date <= end_date:
            batch_end = min(current_date + datetime.timedelta(days=days_per_batch-1), end_date)
            date_ranges.append((current_date, batch_end))
            current_date = batch_end + datetime.timedelta(days=1)
        
        return date_ranges
    
    def _load_fund_accounts(self) -> List[Dict]:
        """
        从数据库加载账户数据
        
        Returns:
            账户数据列表
        """
        try:
            query = """
            SELECT * FROM fund_account 
            WHERE status = 'active' 
            ORDER BY RAND() 
            LIMIT 1000
            """
            
            fund_accounts = self.db_manager.execute_query(query)
            self.logger.info(f"从数据库加载了 {len(fund_accounts)} 个活跃账户")
            return fund_accounts
            
        except Exception as e:
            self.logger.error(f"从数据库加载账户数据时出错: {str(e)}")
            return []
    
    def _load_customers(self) -> List[Dict]:
        """
        从数据库加载客户数据
        
        Returns:
            客户数据列表
        """
        try:
            query = """
            SELECT * FROM customer 
            ORDER BY RAND() 
            LIMIT 500
            """
            
            customers = self.db_manager.execute_query(query)
            self.logger.info(f"从数据库加载了 {len(customers)} 个客户")
            return customers
            
        except Exception as e:
            self.logger.error(f"从数据库加载客户数据时出错: {str(e)}")
            return []
    
    def _load_products(self) -> List[Dict]:
        """
        从数据库加载产品数据
        
        Returns:
            产品数据列表
        """
        try:
            query = "SELECT * FROM product"
            
            products = self.db_manager.execute_query(query)
            self.logger.info(f"从数据库加载了 {len(products)} 个产品")
            return products
            
        except Exception as e:
            self.logger.error(f"从数据库加载产品数据时出错: {str(e)}")
            return []
    
    def generate_data_for_timeperiod(self, start_time: datetime.datetime, 
                                   end_time: datetime.datetime, mode: str = 'realtime') -> Dict[str, int]:
        """
        为指定的时间段生成数据（包含时间信息）
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            mode: 数据生成模式
            
        Returns:
            各实体生成的记录数统计
        """
        return self.generate_data(start_time.date(), end_time.date(), mode)


# 单例模式
_instance = None

def get_data_generator() -> DataGenerator:
    """
    获取DataGenerator的单例实例
    
    Returns:
        DataGenerator实例
    """
    global _instance
    if _instance is None:
        _instance = DataGenerator()
    return _instance


if __name__ == "__main__":
    # 简单测试
    generator = get_data_generator()
    
    # 计算测试时间范围
    time_manager = get_time_manager()
    start_date, end_date = time_manager.calculate_historical_period()
    
    print(f"测试时间范围: {start_date} 至 {end_date}")
    # 仅生成少量测试数据
    test_start_date = end_date - datetime.timedelta(days=7)
    
    # 生成测试数据
    generator.generate_data(test_start_date, end_date, mode='historical')
