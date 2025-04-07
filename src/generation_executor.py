#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据生成执行器模块

负责执行数据生成流程，并提供断点续传功能。
"""

import os
import datetime
from typing import Dict, Optional, Callable

from src.config_manager import get_config_manager
from src.database_manager import get_database_manager
from src.logger import get_logger
from src.time_manager.time_manager import get_time_manager
from src.data_generator.data_generator import get_data_generator
from src.checkpoint_manager import get_checkpoint_manager
from src.data_validator import get_validator

class GenerationExecutor:
    """数据生成执行器，支持断点续传"""
    
    def __init__(self, batch_size: int = 1000, logger=None):
        """
        初始化数据生成执行器
        
        Args:
            batch_size: 批处理大小
            logger: 日志记录器
        """
        self.logger = logger or get_logger('generation_executor')
        self.config_manager = get_config_manager()
        self.time_manager = get_time_manager()
        self.db_manager = get_database_manager()
        self.data_generator = get_data_generator()
        self.checkpoint_manager = get_checkpoint_manager(self.db_manager, self.logger)
        self.validator = get_validator()
        
        self.batch_size = batch_size
        self.total_stats = {}
    
    def initialize_run(self, skip_stages: list = None) -> None:
        """
        初始化一次新的运行
        
        Args:
            skip_stages: 要跳过的阶段列表
        """
        self.checkpoint_manager.initialize_run(skip_stages)
        self.logger.info(f"初始化数据生成运行，跳过阶段: {skip_stages if skip_stages else '无'}")
    
    def resume_from_last(self) -> bool:
        """
        从上次运行中恢复
        
        Returns:
            是否成功恢复
        """
        last_status = self.checkpoint_manager.resume_from_last()
        if last_status:
            self.logger.info(f"从上次运行中恢复: {last_status['details']}")
            return True
        else:
            self.logger.warning("找不到可恢复的状态")
            return False
    
    def execute(self, start_date: datetime.date, end_date: datetime.date) -> Dict:
        """
        执行数据生成流程
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            生成统计信息
        """
        self.logger.info(f"开始执行数据生成流程，时间范围: {start_date} 至 {end_date}")
        
        # 生成基础实体
        self._generate_bank_managers()
        self._generate_deposit_types()
        self._generate_products()
        self._generate_customers()
        
        # 生成关联实体
        self._generate_fund_accounts()
        self._generate_app_users()
        self._generate_wechat_followers()
        self._generate_work_wechat_contacts()
        self._generate_channel_profiles()
        
        # 生成业务数据
        self._generate_loan_records()
        self._generate_investment_records()
        self._generate_customer_events()
        
        # 生成交易数据
        self._generate_transactions(start_date, end_date)
        
        # 执行完成
        self.checkpoint_manager.complete_run()
        self.logger.info("数据生成流程执行完成")
        
        return self.total_stats
    
    def _generate_bank_managers(self) -> None:
        """生成银行经理数据"""
        stage = 'bank_manager'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成银行经理数据...")
        
        # 生成数据
        bank_managers = self.data_generator.bank_manager_generator.generate()
        
        # 更新进度
        self.checkpoint_manager.update_progress(50, "银行经理数据生成完成，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, bank_managers)
        self.total_stats[stage] = count
        
        # 缓存数据以供后续使用
        self.data_generator.data_cache[stage] = bank_managers
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"银行经理数据生成完成，共生成 {count} 条记录")
    
    def _generate_deposit_types(self) -> None:
        """生成存款类型数据"""
        stage = 'deposit_type'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成存款类型数据...")
        
        # 生成数据
        deposit_types = self.data_generator.deposit_type_generator.generate()
        
        # 更新进度
        self.checkpoint_manager.update_progress(50, "存款类型数据生成完成，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, deposit_types)
        self.total_stats[stage] = count
        
        # 缓存数据以供后续使用
        self.data_generator.data_cache[stage] = deposit_types
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"存款类型数据生成完成，共生成 {count} 条记录")
    
    def _generate_products(self) -> None:
        """生成产品数据"""
        stage = 'product'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成产品数据...")
        
        # 生成数据
        products = self.data_generator.product_generator.generate()
        
        # 更新进度
        self.checkpoint_manager.update_progress(50, "产品数据生成完成，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, products)
        self.total_stats[stage] = count
        
        # 缓存数据以供后续使用
        self.data_generator.data_cache[stage] = products
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"产品数据生成完成，共生成 {count} 条记录")
    
    def _generate_customers(self) -> None:
        """生成客户数据"""
        stage = 'customer'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成客户数据...")
        
        # 获取银行经理数据
        bank_managers = self.data_generator.data_cache.get('bank_manager')
        if not bank_managers:
            self.logger.info("从数据库加载银行经理数据...")
            bank_managers = self.db_manager.execute_query("SELECT * FROM bank_manager")
        
        # 生成数据
        customers = self.data_generator.customer_generator.generate(bank_managers)
        
        # 更新进度
        self.checkpoint_manager.update_progress(50, "客户数据生成完成，准备导入数据库")
        
        # 分批导入数据库
        total_count = 0
        total_batches = (len(customers) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(customers), self.batch_size):
            batch = customers[i:i+self.batch_size]
            count = self.data_generator.import_data(stage, batch)
            total_count += count
            
            progress = min(50 + 50 * (i + self.batch_size) / len(customers), 99)
            self.checkpoint_manager.update_progress(
                progress, 
                f"客户数据导入中: {i//self.batch_size + 1}/{total_batches} 批次，共 {total_count} 条"
            )
        
        self.total_stats[stage] = total_count
        
        # 缓存数据以供后续使用
        self.data_generator.data_cache[stage] = customers
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"客户数据生成完成，共生成 {total_count} 条记录")
    
    def _generate_fund_accounts(self) -> None:
        """生成资金账户数据"""
        stage = 'fund_account'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成资金账户数据...")
        
        # 获取客户和存款类型数据
        customers = self.data_generator.data_cache.get('customer')
        deposit_types = self.data_generator.data_cache.get('deposit_type')
        
        if not customers:
            self.logger.info("从数据库加载客户数据...")
            customers = self.db_manager.execute_query("SELECT * FROM customer")
        
        if not deposit_types:
            self.logger.info("从数据库加载存款类型数据...")
            deposit_types = self.db_manager.execute_query("SELECT * FROM deposit_type")
        
        # 更新进度
        self.checkpoint_manager.update_progress(10, "开始生成资金账户数据")
        
        # 生成数据
        fund_accounts = self.data_generator.fund_account_generator.generate(customers, deposit_types)
        
        # 更新进度
        self.checkpoint_manager.update_progress(50, "资金账户数据生成完成，准备导入数据库")
        
        # 分批导入数据库
        total_count = 0
        total_batches = (len(fund_accounts) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(fund_accounts), self.batch_size):
            batch = fund_accounts[i:i+self.batch_size]
            count = self.data_generator.import_data(stage, batch)
            total_count += count
            
            progress = min(50 + 50 * (i + self.batch_size) / len(fund_accounts), 99)
            self.checkpoint_manager.update_progress(
                progress, 
                f"资金账户数据导入中: {i//self.batch_size + 1}/{total_batches} 批次，共 {total_count} 条"
            )
        
        self.total_stats[stage] = total_count
        
        # 缓存数据以供后续使用
        self.data_generator.data_cache[stage] = fund_accounts
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"资金账户数据生成完成，共生成 {total_count} 条记录")
    
    def _generate_loan_records(self) -> None:
        """生成贷款记录数据"""
        stage = 'loan_record'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成贷款记录数据...")
        
        # 获取客户和账户数据
        customers = self.data_generator.data_cache.get('customer')
        fund_accounts = self.data_generator.data_cache.get('fund_account')
        
        if not customers:
            self.logger.info("从数据库加载客户数据...")
            customers = self.db_manager.execute_query("SELECT * FROM customer")
        
        if not fund_accounts:
            self.logger.info("从数据库加载资金账户数据...")
            fund_accounts = self.db_manager.execute_query("SELECT * FROM fund_account")
        
        # 更新进度
        self.checkpoint_manager.update_progress(10, "准备生成贷款记录数据")
        
        # 分批处理客户
        total_loan_records = []
        total_customers = len(customers)
        processed_customers = 0
        
        # 定义进度回调函数
        def progress_callback(progress, processed, total):
            # 将生成阶段的进度（0-100）映射到整体的10%-90%
            overall_progress = 10 + (progress * 0.8)
            self.checkpoint_manager.update_progress(
                overall_progress,
                f"贷款记录生成中: {processed}/{total} 客户处理完成，{progress:.1f}%"
            )
        
        # 分批生成贷款记录
        for i in range(0, total_customers, self.batch_size):
            batch_customers = customers[i:min(i+self.batch_size, total_customers)]
            
            # 更新进度
            processed_customers += len(batch_customers)
            batch_progress = (processed_customers / total_customers) * 100
            self.checkpoint_manager.update_progress(
                10 + (batch_progress * 0.4), 
                f"贷款记录生成第 {i//self.batch_size + 1} 批客户: {processed_customers}/{total_customers}"
            )
            
            # 为当前批次生成贷款记录
            batch_loan_records = self.data_generator.loan_record_generator.generate(
                batch_customers, fund_accounts
            )
            
            total_loan_records.extend(batch_loan_records)
            
            # 频繁保存状态，以防中断
            self.checkpoint_manager.update_progress(
                10 + (batch_progress * 0.8),
                f"贷款记录已生成 {len(total_loan_records)} 条，处理了 {processed_customers}/{total_customers} 客户"
            )
        
        # 更新进度
        self.checkpoint_manager.update_progress(90, f"贷款记录生成完成，共 {len(total_loan_records)} 条，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, total_loan_records)
        self.total_stats[stage] = count
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"贷款记录数据生成完成，共生成 {count} 条记录")
    
    def _generate_investment_records(self) -> None:
        """生成投资记录数据"""
        stage = 'investment_record'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成投资记录数据...")
        
        # 获取客户、账户和产品数据
        customers = self.data_generator.data_cache.get('customer')
        fund_accounts = self.data_generator.data_cache.get('fund_account')
        products = self.data_generator.data_cache.get('product')
        
        if not customers:
            self.logger.info("从数据库加载客户数据...")
            customers = self.db_manager.execute_query("SELECT * FROM customer")
        
        if not fund_accounts:
            self.logger.info("从数据库加载资金账户数据...")
            fund_accounts = self.db_manager.execute_query("SELECT * FROM fund_account")
        
        if not products:
            self.logger.info("从数据库加载产品数据...")
            products = self.db_manager.execute_query("SELECT * FROM product")
        
        # 更新进度
        self.checkpoint_manager.update_progress(10, "准备生成投资记录数据")
        
        # 分批处理客户
        total_investment_records = []
        total_customers = len(customers)
        processed_customers = 0
        
        # 分批生成投资记录
        for i in range(0, total_customers, self.batch_size):
            batch_customers = customers[i:min(i+self.batch_size, total_customers)]
            
            # 更新进度
            processed_customers += len(batch_customers)
            batch_progress = (processed_customers / total_customers) * 100
            self.checkpoint_manager.update_progress(
                10 + (batch_progress * 0.4), 
                f"投资记录生成第 {i//self.batch_size + 1} 批客户: {processed_customers}/{total_customers}"
            )
            
            # 为当前批次生成投资记录
            batch_investment_records = self.data_generator.investment_record_generator.generate(
                batch_customers, fund_accounts, products
            )
            
            total_investment_records.extend(batch_investment_records)
            
            # 频繁保存状态，以防中断
            self.checkpoint_manager.update_progress(
                10 + (batch_progress * 0.8),
                f"投资记录已生成 {len(total_investment_records)} 条，处理了 {processed_customers}/{total_customers} 客户"
            )
        
        # 更新进度
        self.checkpoint_manager.update_progress(90, f"投资记录生成完成，共 {len(total_investment_records)} 条，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, total_investment_records)
        self.total_stats[stage] = count
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"投资记录数据生成完成，共生成 {count} 条记录")
    
    def _generate_app_users(self) -> None:
        """生成APP用户数据"""
        stage = 'app_user'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成APP用户数据...")
        
        # 获取客户数据
        customers = self.data_generator.data_cache.get('customer')
        if not customers:
            self.logger.info("从数据库加载客户数据...")
            customers = self.db_manager.execute_query("SELECT * FROM customer")
        
        # 更新进度
        self.checkpoint_manager.update_progress(30, "生成APP用户数据中...")
        
        # 生成数据
        app_users = self.data_generator.app_user_generator.generate(customers)
        
        # 更新进度
        self.checkpoint_manager.update_progress(60, "APP用户数据生成完成，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, app_users)
        self.total_stats[stage] = count
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"APP用户数据生成完成，共生成 {count} 条记录")
    
    def _generate_wechat_followers(self) -> None:
        """生成公众号粉丝数据"""
        stage = 'wechat_follower'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成公众号粉丝数据...")
        
        # 获取客户和APP用户数据
        customers = self.data_generator.data_cache.get('customer')
        app_users = self.data_generator.data_cache.get('app_user')
        
        if not customers:
            self.logger.info("从数据库加载客户数据...")
            customers = self.db_manager.execute_query("SELECT * FROM customer")
        
        if not app_users:
            self.logger.info("从数据库加载APP用户数据...")
            app_users = self.db_manager.execute_query("SELECT * FROM app_user")
        
        # 更新进度
        self.checkpoint_manager.update_progress(30, "生成公众号粉丝数据中...")
        
        # 生成数据
        wechat_followers = self.data_generator.wechat_follower_generator.generate(customers, app_users)
        
        # 更新进度
        self.checkpoint_manager.update_progress(60, "公众号粉丝数据生成完成，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, wechat_followers)
        self.total_stats[stage] = count
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"公众号粉丝数据生成完成，共生成 {count} 条记录")
    
    def _generate_work_wechat_contacts(self) -> None:
        """生成企业微信联系人数据"""
        stage = 'work_wechat_contact'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成企业微信联系人数据...")
        
        # 获取客户数据
        customers = self.data_generator.data_cache.get('customer')
        if not customers:
            self.logger.info("从数据库加载客户数据...")
            customers = self.db_manager.execute_query("SELECT * FROM customer")
        
        # 更新进度
        self.checkpoint_manager.update_progress(30, "生成企业微信联系人数据中...")
        
        # 生成数据
        work_wechat_contacts = self.data_generator.work_wechat_contact_generator.generate(customers)
        
        # 更新进度
        self.checkpoint_manager.update_progress(60, "企业微信联系人数据生成完成，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, work_wechat_contacts)
        self.total_stats[stage] = count
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"企业微信联系人数据生成完成，共生成 {count} 条记录")
    
    def _generate_channel_profiles(self) -> None:
        """生成全渠道档案数据"""
        stage = 'channel_profile'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成全渠道档案数据...")
        
        # 获取各类数据
        customers = self.data_generator.data_cache.get('customer')
        app_users = self.data_generator.data_cache.get('app_user')
        wechat_followers = self.data_generator.data_cache.get('wechat_follower')
        work_wechat_contacts = self.data_generator.data_cache.get('work_wechat_contact')
        
        # 如果缓存中没有，从数据库加载
        if not customers:
            self.logger.info("从数据库加载客户数据...")
            customers = self.db_manager.execute_query("SELECT * FROM customer")
        
        if not app_users:
            self.logger.info("从数据库加载APP用户数据...")
            app_users = self.db_manager.execute_query("SELECT * FROM app_user")
        
        if not wechat_followers:
            self.logger.info("从数据库加载公众号粉丝数据...")
            wechat_followers = self.db_manager.execute_query("SELECT * FROM wechat_follower")
        
        if not work_wechat_contacts:
            self.logger.info("从数据库加载企业微信联系人数据...")
            work_wechat_contacts = self.db_manager.execute_query("SELECT * FROM work_wechat_contact")
        
        # 更新进度
        self.checkpoint_manager.update_progress(30, "生成全渠道档案数据中...")
        
        # 生成数据
        channel_profiles = self.data_generator.channel_profile_generator.generate(
            customers, app_users, wechat_followers, work_wechat_contacts
        )
        
        # 更新进度
        self.checkpoint_manager.update_progress(60, "全渠道档案数据生成完成，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, channel_profiles)
        self.total_stats[stage] = count
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"全渠道档案数据生成完成，共生成 {count} 条记录")
    
    def _generate_customer_events(self) -> None:
        """生成客户事件数据"""
        stage = 'customer_event'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info("生成客户事件数据...")
        
        # 获取客户和产品数据
        customers = self.data_generator.data_cache.get('customer')
        products = self.data_generator.data_cache.get('product')
        
        if not customers:
            self.logger.info("从数据库加载客户数据...")
            customers = self.db_manager.execute_query("SELECT * FROM customer")
        
        if not products:
            self.logger.info("从数据库加载产品数据...")
            products = self.db_manager.execute_query("SELECT * FROM product")
        
        # 计算时间范围
        start_date, end_date = self.time_manager.calculate_historical_period()
        
        # 更新进度
        self.checkpoint_manager.update_progress(10, "准备生成客户事件数据")
        
        # 确定要生成的客户子集（避免为所有客户生成事件）
        max_event_customers = min(len(customers), 500)  # 最多为500个客户生成事件
        selected_customers = customers[:max_event_customers]
        
        # 分批生成事件
        total_events = []
        total_days = (end_date - start_date).days + 1
        days_per_batch = 30  # 每批30天
        
        for i in range(0, total_days, days_per_batch):
            batch_start = start_date + datetime.timedelta(days=i)
            batch_end = min(start_date + datetime.timedelta(days=i+days_per_batch-1), end_date)
            
            # 更新进度
            progress = 10 + (i / total_days) * 80
            self.checkpoint_manager.update_progress(
                progress,
                f"生成客户事件数据: {batch_start} 至 {batch_end}"
            )
            
            # 生成当前时间段的事件
            batch_events = self.data_generator.customer_event_generator.generate(
                selected_customers, products, batch_start, batch_end
            )
            
            total_events.extend(batch_events)
        
        # 更新进度
        self.checkpoint_manager.update_progress(90, f"客户事件数据生成完成，共 {len(total_events)} 条，准备导入数据库")
        
        # 导入数据库
        count = self.data_generator.import_data(stage, total_events)
        self.total_stats[stage] = count
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"客户事件数据生成完成，共生成 {count} 条记录")
    
    def _generate_transactions(self, start_date: datetime.date, end_date: datetime.date) -> None:
        """
        生成交易数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
        """
        stage = 'transaction'
        if self.checkpoint_manager.should_skip_stage(stage):
            self.logger.info(f"跳过 {stage} 阶段")
            return
        
        self.checkpoint_manager.start_stage(stage)
        self.logger.info(f"生成交易数据，时间范围: {start_date} 至 {end_date}...")
        
        # 获取活跃账户数据
        fund_accounts = self.data_generator.data_cache.get('fund_account')
        if not fund_accounts:
            self.logger.info("从数据库加载资金账户数据...")
            fund_accounts = self.db_manager.execute_query("SELECT * FROM fund_account WHERE status = 'active'")
        
        # 将活跃账户限制在合理范围内，避免生成过多交易
        active_accounts = [acc for acc in fund_accounts if acc.get('status') == 'active']
        if len(active_accounts) > 1000:
            self.logger.info(f"限制活跃账户数量从 {len(active_accounts)} 到 1000")
            active_accounts = active_accounts[:1000]
        
        # 分割时间范围为多个小批次
        date_ranges = self.time_manager._split_date_range(start_date, end_date, days_per_batch=15)
        total_date_ranges = len(date_ranges)
        
        # 生成交易数据
        total_transactions = 0
        
        for i, (batch_start, batch_end) in enumerate(date_ranges):
            # 更新进度
            progress = (i / total_date_ranges) * 100
            self.checkpoint_manager.update_progress(
                progress, 
                f"生成交易数据: 第 {i+1}/{total_date_ranges} 个时间段，{batch_start} 至 {batch_end}"
            )
            
            # 生成当前时间段的交易数据
            transactions = self.data_generator.transaction_generator.generate(
                active_accounts, batch_start, batch_end, mode='historical'
            )
            
            # 导入数据库
            batch_count = self.data_generator.import_data('account_transaction', transactions)
            total_transactions += batch_count
            
            # 更新进度
            self.checkpoint_manager.update_progress(
                progress + (1 / total_date_ranges) * 50,
                f"交易数据导入中: 第 {i+1}/{total_date_ranges} 个时间段完成，累计 {total_transactions} 条交易"
            )
            
            self.logger.info(f"时间段 {batch_start} 至 {batch_end} 的交易数据已生成并导入，共 {batch_count} 条")
        
        self.total_stats[stage] = total_transactions
        
        # 完成阶段
        self.checkpoint_manager.complete_stage(stage)
        self.logger.info(f"交易数据生成完成，共生成 {total_transactions} 条记录")