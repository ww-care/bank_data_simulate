#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据生成器单元测试

测试数据生成器各组件的功能和集成情况
"""

import os
import sys
import unittest
import datetime
import random

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 导入待测试模块
from src.data_generator.data_generator import get_data_generator
from src.data_generator.entity_generators import (
    CustomerGenerator, BankManagerGenerator, ProductGenerator,
    FundAccountGenerator, DepositTypeGenerator
)
from src.logger import get_logger
from src.config_manager import get_config_manager


class TestEntityGenerators(unittest.TestCase):
    """测试各实体生成器"""
    
    def setUp(self):
        """初始化测试环境"""
        self.config_manager = get_config_manager()
        self.logger = get_logger('test_generators', level='debug')
        
        # 使用Faker创建随机数据生成器
        import faker
        self.faker = faker.Faker('zh_CN')
        
        # 设置随机种子保证测试可重复性
        random.seed(42)
    
    def test_bank_manager_generator(self):
        """测试银行经理生成器"""
        generator = BankManagerGenerator(self.faker, self.config_manager)
        managers = generator.generate(count=5)
        
        self.assertEqual(len(managers), 5, "应该生成5个银行经理")
        for manager in managers:
            self.assertIn('manager_id', manager, "银行经理应该有ID")
            self.assertIn('name', manager, "银行经理应该有姓名")
            self.assertIn('branch_id', manager, "银行经理应该有支行ID")
    
    def test_product_generator(self):
        """测试产品生成器"""
        generator = ProductGenerator(self.faker, self.config_manager)
        products = generator.generate(count=10)
        
        self.assertEqual(len(products), 10, "应该生成10个产品")
        for product in products:
            self.assertIn('product_id', product, "产品应该有ID")
            self.assertIn('name', product, "产品应该有名称")
            self.assertIn('type', product, "产品应该有类型")
    
    def test_deposit_type_generator(self):
        """测试存款类型生成器"""
        generator = DepositTypeGenerator(self.faker, self.config_manager)
        deposit_types = generator.generate(count=5)
        
        self.assertEqual(len(deposit_types), 5, "应该生成5个存款类型")
        for deposit_type in deposit_types:
            self.assertIn('deposit_type_id', deposit_type, "存款类型应该有ID")
            self.assertIn('name', deposit_type, "存款类型应该有名称")
            self.assertIn('base_interest_rate', deposit_type, "存款类型应该有基准利率")
    
    def test_customer_generator(self):
        """测试客户生成器"""
        # 先生成银行经理
        bank_manager_generator = BankManagerGenerator(self.faker, self.config_manager)
        managers = bank_manager_generator.generate(count=5)
        
        # 生成客户
        generator = CustomerGenerator(self.faker, self.config_manager)
        customers = generator.generate(managers, count=20)
        
        self.assertEqual(len(customers), 20, "应该生成20个客户")
        
        # 检查客户属性
        for customer in customers:
            self.assertIn('customer_id', customer, "客户应该有ID")
            self.assertIn('name', customer, "客户应该有姓名")
            self.assertIn('customer_type', customer, "客户应该有类型")
            self.assertIn('id_type', customer, "客户应该有证件类型")
            self.assertIn('id_number', customer, "客户应该有证件号码")
            self.assertIn('credit_score', customer, "客户应该有信用分")
            self.assertIn('registration_date', customer, "客户应该有注册日期")
            
            # 检查客户类型特有的属性
            if customer['customer_type'] == 'personal':
                self.assertIn('gender', customer, "个人客户应该有性别")
                self.assertIn('birth_date', customer, "个人客户应该有出生日期")
                self.assertIn('occupation', customer, "个人客户应该有职业")
            else:
                self.assertIn('business_type', customer, "企业客户应该有行业类型")
                self.assertIn('establishment_date', customer, "企业客户应该有成立日期")
    
    def test_fund_account_generator(self):
        """测试资金账户生成器"""
        # 先生成银行经理
        bank_manager_generator = BankManagerGenerator(self.faker, self.config_manager)
        managers = bank_manager_generator.generate(count=3)
        
        # 生成客户
        customer_generator = CustomerGenerator(self.faker, self.config_manager)
        customers = customer_generator.generate(managers, count=10)
        
        # 生成存款类型
        deposit_type_generator = DepositTypeGenerator(self.faker, self.config_manager)
        deposit_types = deposit_type_generator.generate(count=3)
        
        # 生成资金账户
        generator = FundAccountGenerator(self.faker, self.config_manager)
        accounts = generator.generate(customers, deposit_types)
        
        self.assertGreater(len(accounts), 0, "应该生成至少一个账户")
        
        # 检查资金账户属性
        for account in accounts:
            self.assertIn('account_id', account, "账户应该有ID")
            self.assertIn('customer_id', account, "账户应该有客户ID")
            self.assertIn('account_type', account, "账户应该有类型")
            self.assertIn('status', account, "账户应该有状态")
            self.assertIn('balance', account, "账户应该有余额")
            self.assertIn('opening_date', account, "账户应该有开户日期")
            
            # 检查客户ID是否在客户列表中
            customer_found = False
            for customer in customers:
                if customer['customer_id'] == account['customer_id']:
                    customer_found = True
                    break
            self.assertTrue(customer_found, f"账户的客户ID {account['customer_id']} 应该存在于客户列表中")


class TestDataGenerator(unittest.TestCase):
    """测试数据生成器总控类"""
    
    def setUp(self):
        """初始化测试环境"""
        self.data_generator = get_data_generator()
        self.logger = get_logger('test_data_generator', level='debug')
        
        # 测试用的小时间范围（仅测试最近2天的数据，避免生成过多）
        self.today = datetime.date.today()
        self.test_start_date = self.today - datetime.timedelta(days=2)
        self.test_end_date = self.today - datetime.timedelta(days=1)
    
    def test_minimal_data_generation(self):
        """测试最小数据生成
        
        生成极小数量的数据，并检查各实体是否被创建
        """
        # 使用小配置覆盖默认配置
        small_config = {
            'customer': {
                'total_count': 5  # 仅生成5个客户用于测试
            }
        }
        
        try:
            # 备份原始配置
            original_config = self.data_generator.config_manager.get_entity_config('customer')
            
            # 更新为小配置
            self.data_generator.config_manager.update_entity_config('customer', small_config['customer'])
            
            # 生成测试数据
            stats = self.data_generator.generate_data(self.test_start_date, self.test_end_date, mode='historical')
            
            # 检查是否生成了各种实体
            self.assertIn('customer', stats, "应该生成客户数据")
            self.assertIn('bank_manager', stats, "应该生成银行经理数据")
            self.assertIn('product', stats, "应该生成产品数据")
            self.assertIn('fund_account', stats, "应该生成资金账户数据")
            
            # 检查数据缓存
            self.assertIn('customer', self.data_generator.data_cache, "客户数据应该在缓存中")
            self.assertEqual(stats['customer'], len(self.data_generator.data_cache['customer']), "客户统计数应该与缓存匹配")
            
            # 检查生成的交易数据
            self.assertIn('account_transaction', stats, "应该生成交易数据")
            
            # 打印统计数据
            self.logger.info(f"测试生成的数据统计: {stats}")
            
        finally:
            # 恢复原始配置
            self.data_generator.config_manager.update_entity_config('customer', original_config)


if __name__ == '__main__':
    unittest.main()