#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
实体数据生成器模块

包含各类银行业务实体的数据生成器，如客户、账户、交易记录等。
"""

import uuid
import random
import datetime
import faker
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Union


class BaseEntityGenerator:
    """实体生成器基类，提供通用功能"""
    
    def __init__(self, fake_generator: faker.Faker, config_manager):
        """
        初始化实体生成器
        
        Args:
            fake_generator: Faker实例，用于生成随机数据
            config_manager: 配置管理器实例
        """
        self.faker = fake_generator
        self.config_manager = config_manager
    
    def generate_id(self, prefix: str = '') -> str:
        """
        生成实体ID
        
        Args:
            prefix: ID前缀
            
        Returns:
            生成的ID
        """
        # 生成16位唯一标识符
        id_value = uuid.uuid4().hex[:16].upper()
        if prefix:
            return f"{prefix}{id_value}"
        return id_value
    
    def random_choice(self, choices: List, weights: Optional[List[float]] = None) -> Any:
        """
        从列表中随机选择一项
        
        Args:
            choices: 候选项列表
            weights: 权重列表
            
        Returns:
            选中的项
        """
        if not choices:
            return None
        return random.choices(choices, weights=weights, k=1)[0]
    
    def random_date(self, start_date: datetime.date, end_date: datetime.date) -> datetime.date:
        """
        生成指定范围内的随机日期
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            随机日期
        """
        days_diff = (end_date - start_date).days
        if days_diff <= 0:
            return start_date
        
        random_days = random.randint(0, days_diff)
        return start_date + datetime.timedelta(days=random_days)
    
    def random_datetime(self, start_date: datetime.date, end_date: datetime.date) -> datetime.datetime:
        """
        生成指定范围内的随机日期时间
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            随机日期时间
        """
        random_date = self.random_date(start_date, end_date)
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        
        return datetime.datetime.combine(
            random_date, 
            datetime.time(random_hour, random_minute, random_second)
        )
    
    def get_distribution_value(self, distribution: Dict, type_key: str = None) -> Any:
        """
        根据配置的分布获取随机值
        
        Args:
            distribution: 分布配置字典
            type_key: 类型键名
            
        Returns:
            随机值
        """
        if not distribution:
            return None
        
        if type_key and type_key in distribution:
            distribution = distribution[type_key]
        
        if 'values' in distribution and 'weights' in distribution:
            return self.random_choice(distribution['values'], distribution['weights'])
        
        if all(isinstance(key, str) and isinstance(value, (int, float)) for key, value in distribution.items()):
            # 如果是字符串键和数值权重的字典
            keys = list(distribution.keys())
            weights = list(distribution.values())
            return self.random_choice(keys, weights)
        
        # 处理带有范围的分布
        if all(key in distribution for key in ['range', 'ratio']) and all(isinstance(dist.get('range'), list) for _, dist in distribution.items() if isinstance(dist, dict)):
            # 随机决定类别
            categories = list(distribution.keys())
            category_weights = [dist.get('ratio', 1) for dist in distribution.values()]
            selected_category = self.random_choice(categories, category_weights)
            
            # 从选中类别的范围内生成随机值
            selected_range = distribution[selected_category]['range']
            return random.uniform(selected_range[0], selected_range[1])
        
        return None

class CustomerGenerator(BaseEntityGenerator):
    """客户数据生成器"""
    
    def generate(self, bank_managers: List[Dict], count: Optional[int] = None) -> List[Dict]:
        """
        生成客户数据
        
        Args:
            bank_managers: 银行经理数据列表
            count: 生成的客户数量，默认从配置中获取
            
        Returns:
            客户数据列表
        """
        # 获取客户配置
        customer_config = self.config_manager.get_entity_config('customer')
        
        # 确定客户数量
        if count is None:
            count = customer_config.get('total_count', 1000)
        
        # 确定客户类型分布
        type_dist = customer_config.get('type_distribution', {})
        personal_ratio = type_dist.get('personal', 0.8)
        corporate_ratio = type_dist.get('corporate', 0.2)
        
        # 计算各类型客户数量
        personal_count = int(count * personal_ratio)
        corporate_count = count - personal_count
        
        # 生成客户数据
        customers = []
        
        # 生成个人客户
        customers.extend(self._generate_personal_customers(customer_config, personal_count, bank_managers))
        
        # 生成企业客户
        customers.extend(self._generate_corporate_customers(customer_config, corporate_count, bank_managers))
        
        return customers
    
    def _generate_personal_customers(self, config: Dict, count: int, bank_managers: List[Dict]) -> List[Dict]:
        """
        生成个人客户数据
        
        Args:
            config: 客户配置
            count: 生成数量
            bank_managers: 银行经理数据列表
            
        Returns:
            个人客户数据列表
        """
        personal_config = config.get('personal', {})
        vip_ratio = config.get('vip_ratio', {}).get('personal', 0.15)
        
        # 年龄分布
        age_dist = personal_config.get('age_distribution', {})
        age_ranges = list(age_dist.keys())
        age_weights = list(age_dist.values())
        
        # 性别分布
        gender_dist = personal_config.get('gender_distribution', {})
        gender_male_ratio = gender_dist.get('male', 0.5)
        
        # 职业分布
        occupation_dist = personal_config.get('occupation_distribution', {})
        occupations = list(occupation_dist.keys())
        occupation_weights = list(occupation_dist.values())
        
        # 年收入分布参数
        income_config = personal_config.get('annual_income', {})
        income_mean = income_config.get('mean', 60000)
        income_std = income_config.get('std_dev', 30000)
        income_min = income_config.get('min', 20000)
        income_max = income_config.get('max', 300000)
        
        # 信用评分规则
        credit_config = config.get('credit_score', {})
        credit_range = credit_config.get('range', {'min': 350, 'max': 850})
        credit_dist = credit_config.get('distribution', {})
        vip_bonus = credit_config.get('vip_bonus', 50)
        
        customers = []
        for _ in range(count):
            # 生成基本信息
            customer_id = self.generate_id('C')
            gender = 'male' if random.random() < gender_male_ratio else 'female'
            
            if gender == 'male':
                name = self.faker.name_male()
            else:
                name = self.faker.name_female()
            
            # 生成年龄
            age_range = self.random_choice(age_ranges, age_weights)
            if age_range == '18-25':
                age = random.randint(18, 25)
            elif age_range == '26-40':
                age = random.randint(26, 40)
            elif age_range == '41-60':
                age = random.randint(41, 60)
            else:  # 60+
                age = random.randint(61, 85)
            
            # 计算出生日期
            today = datetime.date.today()
            birth_date = today.replace(year=today.year - age)
            
            # 生成注册日期
            max_years_ago = min(age - 18, 10)  # 最长10年，且注册时要满18岁
            registration_years_ago = random.randint(0, max_years_ago)
            registration_date = today.replace(year=today.year - registration_years_ago)
            registration_date -= datetime.timedelta(days=random.randint(0, 365))
            
            # 是否VIP客户
            is_vip = random.random() < vip_ratio
            
            # 生成信用评分
            credit_category_items = list(credit_dist.items())
            credit_categories = [item[0] for item in credit_category_items]
            credit_weights = [item[1].get('ratio', 0.25) for item in credit_category_items]
            
            credit_category = self.random_choice(credit_categories, credit_weights)
            credit_range_for_category = credit_dist[credit_category]['range']
            credit_score = random.randint(credit_range_for_category[0], credit_range_for_category[1])
            
            if is_vip:
                credit_score = min(credit_score + vip_bonus, credit_range['max'])
            
            # 生成年收入
            annual_income = max(income_min, min(income_max, round(np.random.normal(income_mean, income_std))))
            
            # 分配银行经理
            bank_manager = self.random_choice(bank_managers)
            
            # 创建客户记录
            customer = {
                'customer_id': customer_id,
                'name': name,
                'id_type': 'ID_CARD', 
                'id_number': self.faker.ssn(),
                'phone': self.faker.phone_number(),
                'address': self.faker.address(),
                'email': self.faker.email(),
                'gender': gender,
                'birth_date': birth_date.strftime('%Y-%m-%d'),
                'registration_date': registration_date.strftime('%Y-%m-%d'),
                'customer_type': 'personal',
                'credit_score': credit_score,
                'is_vip': is_vip,
                'branch_id': bank_manager['branch_id'],
                'occupation': self.random_choice(occupations, occupation_weights),
                'annual_income': annual_income,
                'business_type': None,
                'annual_revenue': None,
                'establishment_date': None
            }
            
            customers.append(customer)
        
        return customers
    
    def _generate_corporate_customers(self, config: Dict, count: int, bank_managers: List[Dict]) -> List[Dict]:
        """
        生成企业客户数据
        
        Args:
            config: 客户配置
            count: 生成数量
            bank_managers: 银行经理数据列表
            
        Returns:
            企业客户数据列表
        """
        corporate_config = config.get('corporate', {})
        vip_ratio = config.get('vip_ratio', {}).get('corporate', 0.35)
        
        # 企业规模分布
        size_dist = corporate_config.get('size_distribution', {})
        sizes = list(size_dist.keys())
        size_weights = list(size_dist.values())
        
        # 行业分布
        industry_dist = corporate_config.get('industry_distribution', {})
        industries = list(industry_dist.keys())
        industry_weights = list(industry_dist.values())
        
        # 注册资本分布
        capital_config = corporate_config.get('registered_capital', {})
        
        # 信用评分规则
        credit_config = config.get('credit_score', {})
        credit_range = credit_config.get('range', {'min': 350, 'max': 850})
        credit_dist = credit_config.get('distribution', {})
        vip_bonus = credit_config.get('vip_bonus', 50)
        
        # 当前日期
        today = datetime.date.today()
        
        customers = []
        for _ in range(count):
            # 生成基本信息
            customer_id = self.generate_id('B')  # B表示企业(Business)
            company_name = self.faker.company()
            
            # 确定企业规模
            size = self.random_choice(sizes, size_weights)
            
            # 确定行业
            industry = self.random_choice(industries, industry_weights)
            
            # 生成注册资本
            capital_range = capital_config.get(size, {'min': 100000, 'max': 1000000})
            registered_capital = random.randint(capital_range['min'], capital_range['max'])
            
            # 生成成立日期
            years_ago = random.randint(1, 30)  # 企业成立最长30年
            establishment_date = today.replace(year=today.year - years_ago)
            establishment_date -= datetime.timedelta(days=random.randint(0, 365))
            
            # 生成注册日期（成为银行客户的日期）
            max_reg_years = min(years_ago, 10)  # 最长10年，且不能早于成立日期
            registration_years_ago = random.randint(0, max_reg_years)
            registration_date = today.replace(year=today.year - registration_years_ago)
            registration_date -= datetime.timedelta(days=random.randint(0, 365))
            
            # 是否VIP客户
            is_vip = random.random() < vip_ratio
            
            # 生成信用评分
            credit_category_items = list(credit_dist.items())
            credit_categories = [item[0] for item in credit_category_items]
            credit_weights = [item[1].get('ratio', 0.25) for item in credit_category_items]
            
            credit_category = self.random_choice(credit_categories, credit_weights)
            credit_range_for_category = credit_dist[credit_category]['range']
            credit_score = random.randint(credit_range_for_category[0], credit_range_for_category[1])
            
            if is_vip:
                credit_score = min(credit_score + vip_bonus, credit_range['max'])
            
            # 生成年营收
            if size == 'small':
                annual_revenue = random.randint(500000, 5000000)
            elif size == 'medium':
                annual_revenue = random.randint(5000000, 50000000)
            else:  # large
                annual_revenue = random.randint(50000000, 500000000)
            
            # 分配银行经理
            bank_manager = self.random_choice(bank_managers)
            
            # 创建客户记录
            customer = {
                'customer_id': customer_id,
                'name': company_name,
                'id_type': 'BUSINESS_LICENSE', 
                'id_number': self.generate_id('BL'),  # 营业执照号
                'phone': self.faker.phone_number(),
                'address': self.faker.address(),
                'email': self.faker.company_email(),
                'gender': None,  # 企业客户没有性别
                'birth_date': None,  # 企业客户没有出生日期
                'registration_date': registration_date.strftime('%Y-%m-%d'),
                'customer_type': 'corporate',
                'credit_score': credit_score,
                'is_vip': is_vip,
                'branch_id': bank_manager['branch_id'],
                'occupation': None,  # 企业客户没有职业
                'annual_income': None,  # 企业客户没有个人收入
                'business_type': industry,
                'annual_revenue': annual_revenue,
                'establishment_date': establishment_date.strftime('%Y-%m-%d')
            }
            
            customers.append(customer)
        
        return customers

class BankManagerGenerator(BaseEntityGenerator):
    """银行经理数据生成器"""
    
    def generate(self, count: int = 50) -> List[Dict]:
        """
        生成银行经理数据
        
        Args:
            count: 生成的银行经理数量
            
        Returns:
            银行经理数据列表
        """
        managers = []
        branches = ['B001', 'B002', 'B003', 'B004', 'B005']  # 简化的支行编号
        positions = ['初级客户经理', '中级客户经理', '高级客户经理', '资深客户经理', '团队主管']
        
        for _ in range(count):
            manager_id = self.generate_id('M')
            branch_id = self.random_choice(branches)
            name = self.faker.name()
            position = self.random_choice(positions)
            
            manager = {
                'manager_id': manager_id,
                'name': name,
                'branch_id': branch_id,
                'phone': self.faker.phone_number(),
                'email': self.faker.company_email(),
                'customer_count': random.randint(50, 200),
                'position': position
            }
            
            managers.append(manager)
            
        return managers

class ProductGenerator(BaseEntityGenerator):
    """产品数据生成器"""
    
    def generate(self, count: int = 30) -> List[Dict]:
        """
        生成银行产品数据
        
        Args:
            count: 生成的产品数量
            
        Returns:
            产品数据列表
        """
        # 产品类型分布
        type_distribution = {
            'deposit': 0.35,   # 存款产品
            'loan': 0.30,      # 贷款产品
            'investment': 0.35  # 理财产品
        }
        
        # 存款产品类型
        deposit_types = ['活期存款', '定期存款', '大额存单', '协定存款']
        # 贷款产品类型
        loan_types = ['个人消费贷款', '住房贷款', '汽车贷款', '教育贷款', '小微企业贷款', '经营贷款', '信用贷款']
        # 理财产品风险等级
        risk_levels = {'低': 0.45, '中': 0.35, '高': 0.20}
        
        products = []
        
        for _ in range(count):
            product_id = self.generate_id('P')
            
            # 确定产品类型
            product_type = self.random_choice(list(type_distribution.keys()), 
                                           list(type_distribution.values()))
            
            if product_type == 'deposit':
                # 存款产品
                name = f"{self.random_choice(deposit_types)}{self.faker.word()}版"
                term = random.choice([0, 3, 6, 12, 24, 36, 60])  # 0表示活期，其他为月数
                interest_rate = random.uniform(0.01, 0.04)  # 1%-4%之间的利率
                expected_return = None
                risk_level = '低'
                
            elif product_type == 'loan':
                # 贷款产品
                name = f"{self.random_choice(loan_types)}{self.faker.word()}版"
                term = random.choice([6, 12, 24, 36, 60, 120, 240, 360])  # 月数
                interest_rate = random.uniform(0.03, 0.10)  # 3%-10%之间的利率
                expected_return = None
                risk_level = None
                
            else:  # investment
                # 理财产品
                risk_level = self.random_choice(list(risk_levels.keys()), 
                                            list(risk_levels.values()))
                name = f"{'稳健' if risk_level == '低' else '增值' if risk_level == '中' else '进取'}{self.faker.word()}理财"
                term = random.choice([30, 60, 90, 180, 270, 365, 730])  # 天数
                interest_rate = None
                
                # 根据风险等级确定预期收益率
                if risk_level == '低':
                    expected_return = random.uniform(0.025, 0.045)  # 2.5%-4.5%
                elif risk_level == '中':
                    expected_return = random.uniform(0.045, 0.070)  # 4.5%-7.0%
                else:  # 高风险
                    expected_return = random.uniform(0.070, 0.120)  # 7.0%-12.0%
            
            # 创建产品记录
            product = {
                'product_id': product_id,
                'name': name,
                'type': product_type,
                'interest_rate': interest_rate,
                'term': term,
                'expected_return': expected_return,
                'risk_level': risk_level
            }
            
            products.append(product)
        
        return products

class DepositTypeGenerator(BaseEntityGenerator):
    """存款类型数据生成器"""
    
    def generate(self, count: int = 10) -> List[Dict]:
        """
        生成存款类型数据
        
        Args:
            count: 生成的存款类型数量
            
        Returns:
            存款类型数据列表
        """
        deposit_types = [
            {'name': '活期存款', 'min_term': 0, 'max_term': 0, 'min_amount': 0, 'base_rate': 0.01},
            {'name': '三个月定期', 'min_term': 3, 'max_term': 3, 'min_amount': 50, 'base_rate': 0.015},
            {'name': '六个月定期', 'min_term': 6, 'max_term': 6, 'min_amount': 50, 'base_rate': 0.02},
            {'name': '一年定期', 'min_term': 12, 'max_term': 12, 'min_amount': 50, 'base_rate': 0.0275},
            {'name': '两年定期', 'min_term': 24, 'max_term': 24, 'min_amount': 50, 'base_rate': 0.0325},
            {'name': '三年定期', 'min_term': 36, 'max_term': 36, 'min_amount': 50, 'base_rate': 0.035},
            {'name': '五年定期', 'min_term': 60, 'max_term': 60, 'min_amount': 50, 'base_rate': 0.035},
            {'name': '大额存单', 'min_term': 12, 'max_term': 60, 'min_amount': 200000, 'base_rate': 0.04},
            {'name': '协定存款', 'min_term': 0, 'max_term': 0, 'min_amount': 50000, 'base_rate': 0.015},
            {'name': '通知存款', 'min_term': 0, 'max_term': 0, 'min_amount': 5000, 'base_rate': 0.0125},
        ]
        
        result = []
        for i, deposit_type in enumerate(deposit_types[:count]):
            deposit_type_id = self.generate_id('DT')
            
            # 创建存款类型记录
            record = {
                'deposit_type_id': deposit_type_id,
                'name': deposit_type['name'],
                'description': f"('不定期' if deposit_type['max_term'] == 0 else f\"{deposit_type['min_term']}个月\")",
                'base_interest_rate': deposit_type['base_rate'],
                'min_term': deposit_type['min_term'],
                'max_term': deposit_type['max_term'],
                'min_amount': deposit_type['min_amount']
            }
            
            result.append(record)
        
        return result

class FundAccountGenerator(BaseEntityGenerator):
    """资金账户数据生成器"""
    
    def generate(self, customers: List[Dict], deposit_types: List[Dict]) -> List[Dict]:
        """
        生成资金账户数据
        
        Args:
            customers: 客户数据列表
            deposit_types: 存款类型数据列表
            
        Returns:
            资金账户数据列表
        """
        # 获取账户配置
        account_config = self.config_manager.get_entity_config('account')
        
        # 账户类型分布
        type_dist = account_config.get('type_distribution', {})
        type_keys = list(type_dist.keys())
        type_weights = list(type_dist.values())
        
        # 账户状态分布
        status_dist = account_config.get('status_distribution', {})
        status_keys = list(status_dist.keys())
        status_weights = list(status_dist.values())
        
        # 币种分布
        currency_dist = account_config.get('currency_distribution', {})
        currency_keys = list(currency_dist.keys())
        currency_weights = list(currency_dist.values())
        
        # 账户数量分布
        count_config = account_config.get('count_per_customer', {})
        personal_mean = count_config.get('personal', {}).get('mean', 2.0)
        personal_std = count_config.get('personal', {}).get('std_dev', 0.5)
        corporate_mean = count_config.get('corporate', {}).get('mean', 3.2)
        corporate_std = count_config.get('corporate', {}).get('std_dev', 0.8)
        vip_multiplier = count_config.get('vip_multiplier', 1.4)
        
        # 账户余额配置
        balance_config = account_config.get('balance', {})
        
        # 当前日期
        today = datetime.date.today()
        
        # 生成账户数据
        accounts = []
        
        for customer in customers:
            # 确定账户数量
            is_vip = customer.get('is_vip', False)
            is_personal = customer.get('customer_type') == 'personal'
            
            if is_personal:
                # 个人客户
                account_count_mean = personal_mean
                account_count_std = personal_std
            else:
                # 企业客户
                account_count_mean = corporate_mean
                account_count_std = corporate_std
            
            # VIP客户拥有更多账户
            if is_vip:
                account_count_mean *= vip_multiplier
            
            # 生成账户数量（至少1个账户）
            account_count = max(1, int(np.random.normal(account_count_mean, account_count_std)))
            
            # 客户注册日期
            registration_date = datetime.datetime.strptime(customer['registration_date'], '%Y-%m-%d').date()
            
            # 为客户生成账户
            for _ in range(account_count):
                account_id = self.generate_id('A')
                
                # 账户类型
                account_type = self.random_choice(type_keys, type_weights)
                
                # 账户状态
                status = self.random_choice(status_keys, status_weights)
                
                # 币种
                currency = self.random_choice(currency_keys, currency_weights)
                
                # 账户开户日期（不早于客户注册日期）
                days_since_registration = (today - registration_date).days
                opening_date = registration_date + datetime.timedelta(days=random.randint(0, days_since_registration))
                
                # 存款类型（只对current和fixed账户有效）
                deposit_type_id = None
                interest_rate = None
                term = None
                maturity_date = None
                
                if account_type in ['current', 'fixed']:
                    # 选择合适的存款类型
                    suitable_deposit_types = [dt for dt in deposit_types 
                                             if (account_type == 'current' and dt['max_term'] == 0) or
                                                (account_type == 'fixed' and dt['max_term'] > 0)]
                    
                    if suitable_deposit_types:
                        deposit_type = self.random_choice(suitable_deposit_types)
                        deposit_type_id = deposit_type['deposit_type_id']
                        interest_rate = deposit_type['base_interest_rate']
                        
                        # 只有定期存款才有期限和到期日
                        if account_type == 'fixed':
                            term = deposit_type['max_term']
                            maturity_date = opening_date + datetime.timedelta(days=term * 30)
                
                # 获取余额范围
                balance_range = balance_config.get(account_type, {})
                if is_personal:
                    balance_range = balance_range.get('personal', {})
                else:
                    balance_range = balance_range.get('corporate', {})
                
                min_balance = balance_range.get('min', 1000)
                max_balance = balance_range.get('max', 100000)
                mean_balance = balance_range.get('mean', 50000)
                std_dev = balance_range.get('std_dev', 30000)
                
                # 生成余额（正态分布，范围限制）
                balance = max(min_balance, min(max_balance, 
                                              round(np.random.normal(mean_balance, std_dev), 2)))
                
                # 创建账户记录
                account = {
                    'account_id': account_id,
                    'customer_id': customer['customer_id'],
                    'account_type': account_type,
                    'status': status,
                    'currency': currency,
                    'opening_date': opening_date.strftime('%Y-%m-%d'),
                    'balance': balance,
                    'branch_id': customer['branch_id'],
                    'deposit_type_id': deposit_type_id,
                    'interest_rate': interest_rate,
                    'term': term,
                    'maturity_date': maturity_date.strftime('%Y-%m-%d') if maturity_date else None
                }
                
                accounts.append(account)
        
        return accounts
    
class TransactionGenerator(BaseEntityGenerator):
    """交易记录生成器"""
    
    def __init__(self, fake_generator: faker.Faker, config_manager, time_manager):
        """
        初始化交易记录生成器
        
        Args:
            fake_generator: Faker实例，用于生成随机数据
            config_manager: 配置管理器实例
            time_manager: 时间管理器实例，用于处理交易时间逻辑
        """
        super().__init__(fake_generator, config_manager)
        self.time_manager = time_manager
    
    def generate(self, fund_accounts: List[Dict], start_date: datetime.date, 
                 end_date: datetime.date, mode: str = 'historical') -> List[Dict]:
        """
        生成账户交易记录
        
        Args:
            fund_accounts: 资金账户数据列表
            start_date: 开始日期
            end_date: 结束日期
            mode: 数据生成模式，'historical'或'realtime'
            
        Returns:
            交易记录列表
        """
        # 获取交易配置
        transaction_config = self.config_manager.get_entity_config('transaction')
        
        # 交易类型分布
        type_dist = transaction_config.get('type_distribution', {})
        type_keys = list(type_dist.keys())
        type_weights = list(type_dist.values())
        
        # 交易渠道分布
        channel_dist = transaction_config.get('channel_distribution', {})
        channel_keys = list(channel_dist.keys())
        channel_weights = list(channel_dist.values())
        
        # 交易金额规则
        amount_config = transaction_config.get('amount', {})
        
        # 交易时间分布
        time_dist = transaction_config.get('time_distribution', {})
        
        # 准备变量
        transactions = []
        current_date = start_date
        
        # 按日期顺序生成交易
        while current_date <= end_date:
            # 确定当天是否为工作日
            is_workday = self.time_manager.is_workday(current_date)
            
            # 根据是否工作日选择交易时间分布
            if is_workday:
                day_type = 'workday'
            else:
                day_type = 'weekend'
            
            # 获取当天的交易量因子(考虑工作日/周末、月初月末等因素)
            day_factor = self.time_manager.get_date_weight(current_date)
            
            # 为每个账户生成交易
            for account in fund_accounts:
                # 跳过非活动状态账户
                if account['status'] != 'active':
                    continue
                
                # 确定账户类型与客户类型
                account_type = account['account_type']
                is_personal = account['customer_id'].startswith('C')  # C开头为个人客户
                is_vip = account.get('is_vip', False)
                
                # 获取交易频率配置
                frequency_config = transaction_config.get('frequency', {}).get(account_type, {})
                
                # 根据账户类型和客户类型确定交易频率
                if is_personal:
                    # 个人客户
                    transactions_per_day = frequency_config.get('transactions_per_month', {}).get('personal', {})
                else:
                    # 企业客户
                    transactions_per_day = frequency_config.get('transactions_per_month', {}).get('corporate', {})
                
                # 默认值
                daily_mean = transactions_per_day.get('mean', 20) / 30  # 月平均交易次数除以30天
                daily_min = transactions_per_day.get('min', 10) / 30
                daily_max = transactions_per_day.get('max', 30) / 30
                
                # VIP客户交易频率增加
                if is_vip:
                    vip_multiplier = transaction_config.get('frequency', {}).get('vip_multiplier', 1.25)
                    daily_mean *= vip_multiplier
                    daily_min *= vip_multiplier
                    daily_max *= vip_multiplier
                
                # 考虑日因子对交易量的影响
                daily_mean *= day_factor
                daily_min *= day_factor
                daily_max *= day_factor
                
                # 生成当天交易数量
                transaction_count = max(0, min(int(np.random.normal(daily_mean, daily_mean/3)), int(daily_max)))
                
                # 为该账户生成当天交易
                for _ in range(transaction_count):
                    transaction_id = self.generate_id('T')
                    
                    # 确定交易类型
                    transaction_type = self.random_choice(type_keys, type_weights)
                    
                    # 确定交易渠道
                    channel = self.random_choice(channel_keys, channel_weights)
                    
                    # 确定交易时间分布类型并生成具体时间
                    time_periods = time_dist.get(day_type, {})
                    period_keys = list(time_periods.keys())
                    period_weights = [time_periods[k].get('ratio', 0.2) for k in period_keys]
                    
                    time_period = self.random_choice(period_keys, period_weights)
                    
                    # 为不同时间段设置时间范围
                    if time_period == 'morning':       # 9:00-12:00
                        start_hour, end_hour = 9, 12
                    elif time_period == 'lunch':       # 12:00-14:00
                        start_hour, end_hour = 12, 14
                    elif time_period == 'afternoon':   # 14:00-17:00
                        start_hour, end_hour = 14, 17
                    elif time_period == 'evening':     # 17:00-22:00
                        start_hour, end_hour = 17, 22
                    else:  # night                     # 22:00-次日9:00
                        start_hour, end_hour = 22, 9
                    
                    # 生成时间
                    if start_hour < end_hour:
                        hour = random.randint(start_hour, end_hour - 1)
                        minute = random.randint(0, 59)
                        second = random.randint(0, 59)
                        transaction_datetime = datetime.datetime.combine(
                            current_date, datetime.time(hour, minute, second))
                    else:  # 跨日
                        if random.random() < 0.7:  # 70%在当天晚上
                            hour = random.randint(start_hour, 23)
                            transaction_datetime = datetime.datetime.combine(
                                current_date, datetime.time(hour, random.randint(0, 59), random.randint(0, 59)))
                        else:  # 30%在次日凌晨
                            hour = random.randint(0, end_hour - 1)
                            next_date = current_date + datetime.timedelta(days=1)
                            transaction_datetime = datetime.datetime.combine(
                                next_date, datetime.time(hour, random.randint(0, 59), random.randint(0, 59)))
                    
                    # 确定交易金额范围
                    if is_personal:
                        amount_ranges = amount_config.get('personal', {})
                    else:
                        amount_ranges = amount_config.get('corporate', {})
                    
                    # 选择金额等级
                    amount_level_items = [(k, v.get('ratio', 0.33)) for k, v in amount_ranges.items()]
                    amount_level_keys = [item[0] for item in amount_level_items]
                    amount_level_weights = [item[1] for item in amount_level_items]
                    
                    amount_level = self.random_choice(amount_level_keys, amount_level_weights)
                    amount_range = amount_ranges[amount_level]['range']
                    
                    # 生成交易金额
                    amount = round(random.uniform(amount_range[0], amount_range[1]), 2)
                    
                    # 创建交易记录
                    transaction = {
                        'transaction_id': transaction_id,
                        'account_id': account['account_id'],
                        'transaction_type': transaction_type,
                        'amount': amount,
                        'transaction_datetime': transaction_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        'status': 'success',
                        'description': self._generate_description(transaction_type, amount),
                        'channel': channel
                    }
                    
                    transactions.append(transaction)
            
            # 进入下一天
            current_date += datetime.timedelta(days=1)
        
        return transactions
    
    def _generate_description(self, transaction_type: str, amount: float) -> str:
        """
        根据交易类型和金额生成交易描述
        
        Args:
            transaction_type: 交易类型
            amount: 交易金额
            
        Returns:
            交易描述
        """
        if transaction_type == 'deposit':
            return f"存款 {amount:.2f}元"
        elif transaction_type == 'withdrawal':
            return f"取款 {amount:.2f}元"
        elif transaction_type == 'transfer':
            if random.random() < 0.5:  # 50%概率为转入
                return f"转入 {amount:.2f}元，{self.faker.name()}"
            else:  # 50%概率为转出
                return f"转出 {amount:.2f}元，{self.faker.name()}"
        elif transaction_type == 'consumption':
            merchants = ['超市购物', '餐饮消费', '交通出行', '电子产品', '服装鞋帽', 
                        '生活用品', '娱乐消费', '医疗健康', '教育培训', '酒店住宿']
            merchant = self.random_choice(merchants)
            return f"{merchant}，{amount:.2f}元"
        else:  # other
            return f"其他交易，{amount:.2f}元"
        
class LoanRecordGenerator(BaseEntityGenerator):
    """贷款记录生成器"""
    
    def generate(self, customers: List[Dict], fund_accounts: List[Dict]) -> List[Dict]:
        """
        生成贷款记录数据
        
        Args:
            customers: 客户数据列表
            fund_accounts: 资金账户数据列表
            
        Returns:
            贷款记录列表
        """
        # 获取贷款配置
        loan_config = self.config_manager.get_entity_config('loan')
        
        # 贷款类型分布
        type_dist = loan_config.get('type_distribution', {})
        type_keys = list(type_dist.keys())
        type_weights = list(type_dist.values())
        
        # 贷款期限分布
        term_config = loan_config.get('term_distribution', {})
        
        # 贷款利率规则
        interest_config = loan_config.get('interest_rate', {})
        base_rate = interest_config.get('base_rate', 0.0325)
        
        # 贷款状态分布
        status_dist = loan_config.get('status_distribution', {})
        status_keys = list(status_dist.keys())
        status_weights = list(status_dist.values())
        
        # 审批时间规则
        approval_config = loan_config.get('approval_time', {})
        
        # 当前日期
        today = datetime.date.today()
        
        # 按客户筛选出有贷款资质的客户
        eligible_customers = []
        for customer in customers:
            # 获取客户的账户
            customer_accounts = [acc for acc in fund_accounts if acc['customer_id'] == customer['customer_id']]
            if not customer_accounts:
                continue
                
            # 信用评分要求
            credit_score = customer.get('credit_score', 0)
            if credit_score < 550:  # 信用分低于550不发放贷款
                continue
                
            is_vip = customer.get('is_vip', False)
            is_personal = customer.get('customer_type') == 'personal'
            
            eligible_customers.append({
                'customer': customer,
                'accounts': customer_accounts,
                'is_vip': is_vip,
                'is_personal': is_personal
            })
        
        # 确定发放贷款的比例（根据信用分和VIP状态调整）
        loan_ratio = 0.30  # 默认30%的合格客户有贷款
        
        # 随机选择一部分客户发放贷款
        selected_count = int(len(eligible_customers) * loan_ratio)
        selected_customers = random.sample(eligible_customers, min(selected_count, len(eligible_customers)))
        
        loan_records = []
        
        for selected in selected_customers:
            customer = selected['customer']
            accounts = selected['accounts']
            is_vip = selected['is_vip']
            is_personal = selected['is_personal']
            
            # 个人客户和企业客户适用的贷款类型不同
            if is_personal:
                # 个人客户：个人消费贷、住房贷款、汽车贷款、教育贷款
                suitable_types = ['personal_consumption', 'mortgage', 'car', 'education']
                suitable_weights = [0.50, 0.30, 0.12, 0.08]  # 按比例调整
            else:
                # 企业客户：小微企业贷
                suitable_types = ['small_business']
                suitable_weights = [1.0]
            
            # 生成贷款记录
            loan_id = self.generate_id('L')
            
            # 确定贷款类型
            loan_type = self.random_choice(suitable_types, suitable_weights)
            
            # 确定贷款期限
            term_dist = term_config.copy()
            if loan_type == 'mortgage':
                # 住房贷款以长期为主
                term_dist['short_term']['ratio'] = 0.05
                term_dist['medium_term']['ratio'] = 0.15
                term_dist['long_term']['ratio'] = 0.80
            elif loan_type == 'small_business':
                # 小微企业贷以中短期为主
                term_dist['short_term']['ratio'] = 0.30
                term_dist['medium_term']['ratio'] = 0.60
                term_dist['long_term']['ratio'] = 0.10
            
            term_categories = list(term_dist.keys())
            term_weights = [term_dist[cat].get('ratio', 0.33) for cat in term_categories]
            
            term_category = self.random_choice(term_categories, term_weights)
            term_months = self.random_choice(term_dist[term_category]['months'])
            
            # 贷款金额范围
            if loan_type == 'personal_consumption':
                min_amount, max_amount = 10000, 200000
            elif loan_type == 'mortgage':
                min_amount, max_amount = 300000, 3000000
            elif loan_type == 'car':
                min_amount, max_amount = 50000, 500000
            elif loan_type == 'education':
                min_amount, max_amount = 20000, 150000
            else:  # 小微企业贷
                min_amount, max_amount = 200000, 5000000
            
            # 根据信用分和VIP状态调整金额上限
            credit_score = customer.get('credit_score', 650)
            
            if credit_score > 700:
                max_amount *= 1.2
            if is_vip:
                max_amount *= 1.3
            
            # 生成贷款金额
            loan_amount = round(random.uniform(min_amount, max_amount), 2)
            
            # 计算利率
            # 基准利率 + 贷款类型调整 + 信用评分影响
            type_adjustment_min = interest_config.get(loan_type, {}).get('min_adjustment', 0.02)
            type_adjustment_max = interest_config.get(loan_type, {}).get('max_adjustment', 0.04)
            type_adjustment = random.uniform(type_adjustment_min, type_adjustment_max)
            
            credit_impact_full = interest_config.get('credit_score_impact', 0.20)
            credit_score_normalized = (credit_score - 550) / (850 - 550)  # 归一化到0-1
            credit_impact = credit_impact_full * (1 - credit_score_normalized)
            
            interest_rate = base_rate + type_adjustment - credit_impact
            
            if is_vip:
                interest_rate = max(0.01, interest_rate * 0.9)  # VIP客户利率优惠10%
            
            interest_rate = round(interest_rate, 4)
            
            # 生成申请日期（过去1-12个月内）
            days_ago = random.randint(30, 365)
            application_date = today - datetime.timedelta(days=days_ago)
            
            # 确定贷款规模以设置审批时间
            if loan_amount < 100000:
                size_category = 'small'
            elif loan_amount < 1000000:
                size_category = 'medium'
            else:
                size_category = 'large'
            
            # 审批时间
            min_approval_days = approval_config.get(size_category, {}).get('min_days', 3)
            max_approval_days = approval_config.get(size_category, {}).get('max_days', 7)
            
            if is_vip:
                vip_reduction = approval_config.get('vip_time_reduction', 0.3)
                min_approval_days = max(1, int(min_approval_days * (1 - vip_reduction)))
                max_approval_days = max(2, int(max_approval_days * (1 - vip_reduction)))
            
            approval_days = random.randint(min_approval_days, max_approval_days)
            approval_date = application_date + datetime.timedelta(days=approval_days)
            
            # 贷款状态
            loan_status = self.random_choice(status_keys, status_weights)
            
            # 为贷款选择一个账户
            account = self.random_choice(accounts)
            
            # 创建贷款记录
            loan_record = {
                'loan_id': loan_id,
                'customer_id': customer['customer_id'],
                'account_id': account['account_id'],
                'loan_type': loan_type,
                'loan_amount': loan_amount,
                'interest_rate': interest_rate,
                'term': term_months,
                'application_date': application_date.strftime('%Y-%m-%d'),
                'approval_date': approval_date.strftime('%Y-%m-%d'),
                'status': loan_status
            }
            
            loan_records.append(loan_record)
        
        return loan_records
    
class InvestmentRecordGenerator(BaseEntityGenerator):
    """理财投资记录生成器"""
    
    def generate(self, customers: List[Dict], fund_accounts: List[Dict], products: List[Dict]) -> List[Dict]:
        """
        生成理财投资记录数据
        
        Args:
            customers: 客户数据列表
            fund_accounts: 资金账户数据列表
            products: 产品数据列表
            
        Returns:
            理财投资记录列表
        """
        # 获取理财配置
        investment_config = self.config_manager.get_entity_config('investment')
        
        # 筛选出理财类产品
        investment_products = [p for p in products if p.get('type') == 'investment']
        if not investment_products:
            return []  # 没有理财产品则无法生成投资记录
        
        # 风险等级分布
        risk_dist = investment_config.get('risk_level_distribution', {})
        risk_levels = list(risk_dist.keys())
        risk_weights = list(risk_dist.values())
        
        # 投资金额规则
        amount_config = investment_config.get('amount', {})
        
        # 投资期限分布
        term_config = investment_config.get('term_distribution', {})
        
        # 投资渠道分布
        channel_dist = investment_config.get('channel_distribution', {})
        channel_keys = list(channel_dist.keys())
        channel_weights = list(channel_dist.values())
        
        # 当前日期
        today = datetime.date.today()
        
        # 筛选有投资能力的客户
        investment_eligible_customers = []
        
        for customer in customers:
            # 获取客户的账户，筛选出非贷款且状态为active的账户
            customer_accounts = [acc for acc in fund_accounts 
                               if acc['customer_id'] == customer['customer_id'] 
                               and acc['account_type'] != 'loan'
                               and acc['status'] == 'active']
            
            if not customer_accounts:
                continue
            
            # 确定客户总资产（账户余额总和）
            total_assets = sum(float(acc.get('balance', 0)) for acc in customer_accounts)
            
            is_vip = customer.get('is_vip', False)
            is_personal = customer.get('customer_type') == 'personal'
            
            # 资产达到一定水平才考虑投资理财
            min_investment_assets = 50000  # 最低5万元资产才考虑理财
            if total_assets < min_investment_assets:
                continue
            
            # 年龄分析（仅针对个人客户）
            if is_personal and customer.get('birth_date'):
                birth_date = datetime.datetime.strptime(customer['birth_date'], '%Y-%m-%d').date()
                age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
                
                # 根据年龄确定风险偏好
                if age < 30:
                    risk_preference = 'high'  # 年轻客户风险偏好较高
                elif age < 50:
                    risk_preference = 'medium'  # 中年客户风险偏好中等
                else:
                    risk_preference = 'low'  # 年长客户风险偏好较低
            else:
                # 企业客户根据规模分配风险偏好
                annual_revenue = customer.get('annual_revenue', 0)
                if annual_revenue > 100000000:  # 亿元以上营收
                    risk_preference = 'medium'
                elif annual_revenue > 10000000:  # 千万以上营收
                    risk_preference = 'medium'
                else:
                    risk_preference = 'low'
            
            investment_eligible_customers.append({
                'customer': customer,
                'accounts': customer_accounts,
                'is_vip': is_vip,
                'is_personal': is_personal,
                'total_assets': total_assets,
                'risk_preference': risk_preference
            })
        
        # 确定投资理财的比例
        base_investment_ratio = 0.65  # 基础投资比例
        
        investment_records = []
        
        for eligible in investment_eligible_customers:
            customer = eligible['customer']
            accounts = eligible['accounts']
            is_vip = eligible['is_vip']
            is_personal = eligible['is_personal']
            total_assets = eligible['total_assets']
            risk_preference = eligible['risk_preference']
            
            # 根据客户特征调整投资比例
            investment_probability = base_investment_ratio
            if is_vip:
                investment_probability += 0.15  # VIP客户更可能投资
            if total_assets > 200000:
                investment_probability += 0.10  # 资产较高的客户更可能投资
            
            # 决定客户是否进行投资
            if random.random() > investment_probability:
                continue
            
            # 决定投资产品的数量（1-3个）
            num_investments = min(3, max(1, int(np.random.normal(1.5, 0.7))))
            
            # 根据风险偏好筛选适合的产品
            if risk_preference == 'low':
                suitable_products = [p for p in investment_products if p.get('risk_level') == '低']
                # 如果没有匹配的低风险产品，也可以接受一部分中风险产品
                if len(suitable_products) < 3:
                    suitable_products.extend([p for p in investment_products if p.get('risk_level') == '中'][:2])
            elif risk_preference == 'medium':
                suitable_products = [p for p in investment_products if p.get('risk_level') in ['低', '中']]
                # 可以接受少量高风险产品
                if len(suitable_products) < 5:
                    suitable_products.extend([p for p in investment_products if p.get('risk_level') == '高'][:1])
            else:  # high
                suitable_products = investment_products  # 接受所有风险级别
            
            # 如果没有合适的产品，则跳过
            if not suitable_products:
                continue
            
            # 决定投资金额上限（总资产的一定比例）
            if is_personal:
                investment_ratio = 0.40  # 个人最多投资40%资产
            else:
                investment_ratio = 0.30  # 企业最多投资30%资产
            
            if is_vip:
                investment_ratio += 0.10  # VIP客户投资比例更高
            
            investment_limit = total_assets * investment_ratio
            
            # 生成投资记录
            remaining_limit = investment_limit
            
            for i in range(num_investments):
                if remaining_limit < 10000 or not suitable_products:  # 至少1万元才能投资
                    break
                
                # 随机选择一个产品
                product = self.random_choice(suitable_products)
                
                # 从可选产品中移除已选产品（避免重复投资）
                suitable_products.remove(product)
                
                # 生成投资ID
                investment_id = self.generate_id('I')
                
                # 选择一个账户
                account = self.random_choice(accounts)
                
                # 决定投资金额
                if is_personal:
                    amount_range = amount_config.get('personal', {})
                else:
                    amount_range = amount_config.get('corporate', {})
                
                min_amount = max(10000, amount_range.get('min', 50000))
                max_amount = min(remaining_limit, amount_range.get('max', 200000))
                mean_amount = min(max_amount, amount_range.get('mean', 100000))
                std_dev = amount_range.get('std_dev', 50000)
                
                # VIP客户投资金额更大
                if is_vip:
                    vip_multiplier = amount_config.get('vip_multiplier', 1.75)
                    mean_amount = min(max_amount, mean_amount * vip_multiplier)
                
                # 生成投资金额
                investment_amount = max(min_amount, min(max_amount, 
                                        round(np.random.normal(mean_amount, std_dev), 2)))
                
                # 更新剩余额度
                remaining_limit -= investment_amount
                
                # 投资日期（1年内随机日期）
                days_ago = random.randint(1, 365)
                purchase_date = today - datetime.timedelta(days=days_ago)
                
                # 投资期限
                term = product.get('term', 90)  # 默认90天
                
                # 到期日
                maturity_date = purchase_date + datetime.timedelta(days=term)
                
                # 投资状态
                if maturity_date > today:
                    status = 'active'  # 未到期
                else:
                    status = 'expired'  # 已到期
                
                # 投资渠道
                channel = self.random_choice(channel_keys, channel_weights)
                
                # 创建投资记录
                investment_record = {
                    'investment_id': investment_id,
                    'customer_id': customer['customer_id'],
                    'account_id': account['account_id'],
                    'product_id': product['product_id'],
                    'amount': investment_amount,
                    'purchase_date': purchase_date.strftime('%Y-%m-%d'),
                    'term': term,
                    'maturity_date': maturity_date.strftime('%Y-%m-%d'),
                    'status': status,
                    'channel': channel,
                    'expected_return': product.get('expected_return')
                }
                
                investment_records.append(investment_record)
        
        return investment_records
    
class AppUserGenerator(BaseEntityGenerator):
    """APP用户数据生成器"""
    
    def generate(self, customers: List[Dict]) -> List[Dict]:
        """
        生成APP用户数据
        
        Args:
            customers: 客户数据列表
            
        Returns:
            APP用户数据列表
        """
        # 获取APP用户配置
        app_config = self.config_manager.get_entity_config('app_user')
        
        # APP使用率
        penetration_rate = app_config.get('penetration_rate', {})
        personal_rate = penetration_rate.get('personal', 0.65)
        corporate_rate = penetration_rate.get('corporate', 0.55)
        
        # 年龄因素影响
        age_factor = penetration_rate.get('age_factor', {})
        
        # 活跃度分布
        activity_level = app_config.get('activity_level', {})
        
        # 设备类型分布
        device_dist = app_config.get('device_distribution', {})
        ios_ratio = device_dist.get('ios', 0.50)
        tablet_ratio = device_dist.get('tablet_ratio', 0.08)
        
        # 功能使用分布
        feature_usage = app_config.get('feature_usage', {})
        
        # 当前日期
        today = datetime.date.today()
        
        app_users = []
        
        for customer in customers:
            is_personal = customer.get('customer_type') == 'personal'
            is_vip = customer.get('is_vip', False)
            
            # 确定该客户是否使用APP
            base_probability = personal_rate if is_personal else corporate_rate
            
            # VIP客户APP使用率提高
            if is_vip:
                base_probability = min(1.0, base_probability * 1.2)
            
            # 年龄因素（仅针对个人客户）
            if is_personal and customer.get('birth_date'):
                birth_date = datetime.datetime.strptime(customer['birth_date'], '%Y-%m-%d').date()
                age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
                
                if age <= 40:
                    age_probability = age_factor.get('18-40', 0.80)
                elif age <= 60:
                    age_probability = age_factor.get('41-60', 0.50)
                else:
                    age_probability = age_factor.get('60+', 0.20)
                
                # 综合概率
                app_probability = (base_probability + age_probability) / 2
            else:
                app_probability = base_probability
            
            # 决定是否创建APP用户
            if random.random() > app_probability:
                continue
                
            # 生成APP用户ID
            app_user_id = self.generate_id('APP')
            
            # 注册日期（比客户注册日期晚，但不晚于今天）
            registration_date = datetime.datetime.strptime(customer['registration_date'], '%Y-%m-%d').date()
            
            # APP上线日期（假设APP在2020年1月1日上线）
            app_launch_date = datetime.date(2020, 1, 1)
            
            # 注册日期不早于APP上线日期和客户注册日期
            earliest_date = max(app_launch_date, registration_date)
            
            if earliest_date > today:
                continue  # 如果客户注册日期晚于今天，则跳过
            
            days_since_earliest = (today - earliest_date).days
            
            # 生成APP注册日期
            if days_since_earliest <= 0:
                continue  # 如果可选时间范围为0，则跳过
            
            app_registration_days_ago = random.randint(0, days_since_earliest)
            app_registration_date = today - datetime.timedelta(days=app_registration_days_ago)
            
            # 确定活跃度
            activity_keys = list(activity_level.keys())
            activity_weights = [activity_level[k].get('ratio', 0.25) for k in activity_keys]
            
            activity = self.random_choice(activity_keys, activity_weights)
            
            # 根据活跃度确定上次登录日期
            if activity == 'high':  # 高度活跃
                weekly_usage = activity_level['high'].get('weekly_usage', [3, 7])
                max_days_ago = random.randint(1, 3)  # 最近3天内
            elif activity == 'medium':  # 中度活跃
                weekly_usage = activity_level['medium'].get('weekly_usage', [1, 2])
                max_days_ago = random.randint(3, 10)  # 最近3-10天内
            elif activity == 'low':  # 低度活跃
                monthly_usage = activity_level['low'].get('monthly_usage', [1, 3])
                max_days_ago = random.randint(10, 30)  # 最近10-30天内
            else:  # inactive
                max_days_ago = random.randint(90, 180)  # 最近3-6个月内
            
            # 确保上次登录日期不早于APP注册日期
            days_since_app_registration = (today - app_registration_date).days
            last_login_days_ago = min(max_days_ago, days_since_app_registration)
            
            last_login_date = today - datetime.timedelta(days=last_login_days_ago)
            
            # 设备类型
            device_os = 'ios' if random.random() < ios_ratio else 'android'
            device_type = 'tablet' if random.random() < tablet_ratio else 'mobile'
            
            # 功能使用情况
            used_features = []
            for feature, usage_rate in feature_usage.items():
                if random.random() < usage_rate:
                    used_features.append(feature)
            
            # 设备信息
            if device_os == 'ios':
                device_models = ['iPhone 12', 'iPhone 13', 'iPhone 14', 'iPhone 15', 
                                'iPad Air', 'iPad Pro', 'iPad Mini']
                if device_type == 'tablet':
                    device_model = self.random_choice([m for m in device_models if 'iPad' in m])
                else:
                    device_model = self.random_choice([m for m in device_models if 'iPhone' in m])
            else:  # android
                if device_type == 'tablet':
                    device_models = ['Samsung Galaxy Tab S7', 'Xiaomi Pad 5', 
                                    'Huawei MatePad Pro', 'OPPO Pad']
                else:
                    device_models = ['Samsung Galaxy S21', 'Samsung Galaxy S22', 
                                    'Xiaomi 12', 'Xiaomi 13', 'Huawei P40', 
                                    'Huawei P50', 'OPPO Find X5', 'OPPO Find X6', 
                                    'Vivo X80', 'Vivo X90']
                device_model = self.random_choice(device_models)
            
            # 创建APP用户记录
            app_user = {
                'app_user_id': app_user_id,
                'customer_id': customer['customer_id'],
                'registration_date': app_registration_date.strftime('%Y-%m-%d'),
                'last_login_date': last_login_date.strftime('%Y-%m-%d'),
                'device_os': device_os,
                'device_type': device_type,
                'device_model': device_model,
                'activity_level': activity,
                'used_features': ','.join(used_features),
                'login_frequency': random.choice(weekly_usage) if activity in ['high', 'medium'] else 
                                  random.choice(monthly_usage) if activity == 'low' else 0,
                'push_notification': random.choice([True, False]),
                'app_version': f"{random.randint(5, 8)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
            }
            
            app_users.append(app_user)
        
        return app_users
    
class WechatFollowerGenerator(BaseEntityGenerator):
    """微信公众号粉丝数据生成器"""
    
    def generate(self, customers: List[Dict], app_users: List[Dict]) -> List[Dict]:
        """
        生成微信公众号粉丝数据
        
        Args:
            customers: 客户数据列表
            app_users: APP用户数据列表
            
        Returns:
            微信公众号粉丝数据列表
        """
        # 获取公众号粉丝配置
        wechat_config = self.config_manager.get_entity_config('wechat_follower')
        
        # 关注率
        follow_ratio = wechat_config.get('follow_ratio', {})
        with_app_ratio = follow_ratio.get('with_app', 0.55)  # 拥有APP的客户关注公众号的比例
        without_app_ratio = follow_ratio.get('without_app', 0.15)  # 无APP的客户关注公众号的比例
        
        # 互动水平分布
        interaction_level = wechat_config.get('interaction_level', {})
        
        # 活动参与规则
        campaign_participation = wechat_config.get('campaign_participation', {})
        
        # 当前日期
        today = datetime.date.today()
        
        # 已有APP用户的客户ID集合
        app_user_customer_ids = set(app_user['customer_id'] for app_user in app_users)
        
        # 生成公众号粉丝数据
        wechat_followers = []
        
        for customer in customers:
            customer_id = customer['customer_id']
            is_personal = customer.get('customer_type') == 'personal'
            is_vip = customer.get('is_vip', False)
            
            # 确定该客户是否关注公众号
            has_app = customer_id in app_user_customer_ids
            base_probability = with_app_ratio if has_app else without_app_ratio
            
            # VIP客户公众号关注率提高
            if is_vip:
                base_probability = min(1.0, base_probability * 1.2)
            
            # 企业客户关注率稍低
            if not is_personal:
                base_probability *= 0.8
            
            # 决定是否创建公众号粉丝
            if random.random() > base_probability:
                continue
            
            # 生成公众号粉丝ID
            follower_id = self.generate_id('WF')
            
            # 关注日期
            # 微信公众号平台上线日期（假设在2017年1月1日）
            wechat_platform_launch = datetime.date(2017, 1, 1)
            
            # 银行公众号开通日期（假设在2017年6月1日）
            bank_wechat_launch = datetime.date(2017, 6, 1)
            
            # 客户注册日期
            registration_date = datetime.datetime.strptime(customer['registration_date'], '%Y-%m-%d').date()
            
            # 关注日期不早于公众号开通日期和客户注册日期
            earliest_date = max(bank_wechat_launch, registration_date)
            
            if earliest_date > today:
                continue  # 如果客户注册日期晚于今天，则跳过
            
            days_since_earliest = (today - earliest_date).days
            
            # 生成关注日期
            if days_since_earliest <= 0:
                continue  # 如果可选时间范围为0，则跳过
                
            follow_days_ago = random.randint(0, days_since_earliest)
            follow_date = today - datetime.timedelta(days=follow_days_ago)
            
            # 确定互动水平
            interaction_keys = list(interaction_level.keys())
            interaction_weights = [interaction_level[k].get('ratio', 0.33) for k in interaction_keys]
            
            interaction = self.random_choice(interaction_keys, interaction_weights)
            
            # 根据互动水平确定阅读频率
            if interaction == 'high':  # 高互动
                weekly_reading = interaction_level['high'].get('weekly_reading', [1, 5])
                reading_frequency = random.randint(weekly_reading[0], weekly_reading[1])
                reading_unit = 'weekly'
            elif interaction == 'medium':  # 中互动
                monthly_reading = interaction_level['medium'].get('monthly_reading', [1, 3])
                reading_frequency = random.randint(monthly_reading[0], monthly_reading[1])
                reading_unit = 'monthly'
            else:  # 低互动
                yearly_reading = interaction_level['low'].get('yearly_reading', [1, 12])
                reading_frequency = random.randint(yearly_reading[0], yearly_reading[1])
                reading_unit = 'yearly'
            
            # 上次阅读日期
            if reading_unit == 'weekly':
                last_read_days_ago = random.randint(0, 7)
            elif reading_unit == 'monthly':
                last_read_days_ago = random.randint(0, 30)
            else:  # yearly
                last_read_days_ago = random.randint(0, 90)
            
            # 确保上次阅读日期不早于关注日期
            days_since_follow = (today - follow_date).days
            last_read_days_ago = min(last_read_days_ago, days_since_follow)
            
            last_read_date = today - datetime.timedelta(days=last_read_days_ago)
            
            # 决定是否参与活动
            participation_rate = campaign_participation.get('participation_rate', 0.15)
            
            # VIP和高互动粉丝更可能参与活动
            if is_vip:
                participation_rate *= 1.5
            if interaction == 'high':
                participation_rate *= 1.3
            elif interaction == 'medium':
                participation_rate *= 1.1
            
            has_participated = random.random() < participation_rate
            
            # 如果参与活动，决定是否转化
            conversion_rate = campaign_participation.get('conversion_rate', 0.10)
            has_converted = has_participated and (random.random() < conversion_rate)
            
            # 创建公众号粉丝记录
            wechat_follower = {
                'follower_id': follower_id,
                'customer_id': customer_id,
                'follow_date': follow_date.strftime('%Y-%m-%d'),
                'last_read_date': last_read_date.strftime('%Y-%m-%d'),
                'interaction_level': interaction,
                'reading_frequency': reading_frequency,
                'reading_unit': reading_unit,
                'has_participated_campaign': has_participated,
                'has_converted': has_converted,
                'is_subscribed': True,  # 默认已订阅
                'source': 'QR_code' if random.random() < 0.7 else 'search'  # 70%通过二维码关注
            }
            
            wechat_followers.append(wechat_follower)
        
        return wechat_followers
    
class WorkWechatContactGenerator(BaseEntityGenerator):
    """企业微信联系人数据生成器"""
    
    def generate(self, customers: List[Dict]) -> List[Dict]:
        """
        生成企业微信联系人数据
        
        Args:
            customers: 客户数据列表
            
        Returns:
            企业微信联系人数据列表
        """
        # 企业微信联系人渗透率（较低，因为企业微信主要用于高价值客户）
        personal_penetration = 0.25  # 个人客户企业微信使用率
        corporate_penetration = 0.45  # 企业客户企业微信使用率
        vip_multiplier = 2.0  # VIP客户更可能使用企业微信
        
        # 可选的标签
        possible_tags = ['重点客户', '贵宾客户', '理财客户', '贷款客户', '高净值', '稳健型', '积极型', 
                       '已婚', '有子女', '自雇人士', '企业主', '专业人士', '投资客户', '存款大户']
        
        # 可选的备注前缀
        remark_prefixes = ['高潜力', '定期回访', '跟进理财', '长期关系', '家族客户', '企业主', '已购买保险',
                         '大客户', '老客户', '战略客户', '核心业务', '集团关系']
        
        # 当前日期
        today = datetime.date.today()
        
        # 企业微信上线日期（假设在2019年1月1日）
        work_wechat_launch = datetime.date(2019, 1, 1)
        
        work_wechat_contacts = []
        
        for customer in customers:
            is_personal = customer.get('customer_type') == 'personal'
            is_vip = customer.get('is_vip', False)
            
            # 确定该客户是否使用企业微信
            base_probability = personal_penetration if is_personal else corporate_penetration
            
            # VIP客户使用率提高
            if is_vip:
                base_probability = min(1.0, base_probability * vip_multiplier)
            
            # 高信用分客户使用率提高
            credit_score = customer.get('credit_score', 600)
            if credit_score > 700:
                base_probability = min(1.0, base_probability * 1.5)
            
            # 决定是否创建企业微信联系人
            if random.random() > base_probability:
                continue
            
            # 生成企业微信联系人ID
            contact_id = self.generate_id('WW')
            
            # 关联的银行经理
            bank_manager_id = self._get_manager_id(customer)
            
            # 添加日期
            registration_date = datetime.datetime.strptime(customer['registration_date'], '%Y-%m-%d').date()
            earliest_date = max(work_wechat_launch, registration_date)
            
            if earliest_date > today:
                continue  # 如果客户注册日期晚于今天，则跳过
            
            days_since_earliest = (today - earliest_date).days
            
            # 生成添加日期
            if days_since_earliest <= 0:
                continue  # 如果可选时间范围为0，则跳过
                
            add_days_ago = random.randint(0, days_since_earliest)
            add_date = today - datetime.timedelta(days=add_days_ago)
            
            # 生成标签（0-3个）
            tag_count = random.randint(0, 3)
            tags = random.sample(possible_tags, tag_count)
            
            # 是否有备注
            has_remark = random.random() < 0.6  # 60%几率有备注
            
            if has_remark:
                prefix = random.choice(remark_prefixes)
                remark = f"{prefix}_{customer.get('name', '')}"
            else:
                remark = None
            
            # 最近联系日期
            # 高频客户（近期有联系）
            if random.random() < 0.7:  # 70%是高频客户
                last_contact_days_ago = random.randint(0, 30)
            else:  # 低频客户（较久没联系）
                last_contact_days_ago = random.randint(31, 180)
            
            # 确保最近联系日期不早于添加日期
            days_since_add = (today - add_date).days
            last_contact_days_ago = min(last_contact_days_ago, days_since_add)
            
            last_contact_date = today - datetime.timedelta(days=last_contact_days_ago)
            
            # 联系频率
            if is_vip or random.random() < 0.3:  # VIP或30%的客户是高频联系
                contact_frequency = 'high'  # 高频联系
                days_between_contacts = random.randint(7, 30)
            elif random.random() < 0.5:  # 50%的剩余客户是中频联系
                contact_frequency = 'medium'  # 中频联系
                days_between_contacts = random.randint(31, 90)
            else:  # 剩余客户是低频联系
                contact_frequency = 'low'  # 低频联系
                days_between_contacts = random.randint(91, 180)
            
            # 创建企业微信联系人记录
            work_wechat_contact = {
                'contact_id': contact_id,
                'customer_id': customer['customer_id'],
                'manager_id': bank_manager_id,
                'add_date': add_date.strftime('%Y-%m-%d'),
                'last_contact_date': last_contact_date.strftime('%Y-%m-%d'),
                'contact_frequency': contact_frequency,
                'days_between_contacts': days_between_contacts,
                'tags': ','.join(tags) if tags else None,
                'remark': remark,
                'is_group_chat_member': random.random() < 0.4,  # 40%几率在群聊中
                'priority_level': 'high' if is_vip else 'medium' if credit_score > 650 else 'normal'
            }
            
            work_wechat_contacts.append(work_wechat_contact)
        
        return work_wechat_contacts
    
    def _get_manager_id(self, customer: Dict) -> str:
        """
        获取客户关联的银行经理ID
        
        Args:
            customer: 客户数据
            
        Returns:
            银行经理ID
        """
        # 假设数据库中有客户管理关系表
        # 这里简单返回一个模拟的ID
        return f"M{customer['customer_id'][1:9]}"
    
class ChannelProfileGenerator(BaseEntityGenerator):
    """全渠道档案数据生成器"""
    
    def generate(self, customers: List[Dict], app_users: List[Dict], 
                wechat_followers: List[Dict], work_wechat_contacts: List[Dict]) -> List[Dict]:
        """
        生成全渠道档案数据
        
        Args:
            customers: 客户数据列表
            app_users: APP用户数据列表
            wechat_followers: 微信公众号粉丝数据列表
            work_wechat_contacts: 企业微信联系人数据列表
            
        Returns:
            全渠道档案数据列表
        """
        # 获取全渠道配置
        channel_config = self.config_manager.get_entity_config('channel_profile')
        
        # 渠道数量分布
        channel_count_dist = channel_config.get('channel_count_distribution', {})
        
        # 渠道偏好规则
        channel_preference = channel_config.get('channel_preference', {})
        
        # 渠道转换规则
        channel_conversion = channel_config.get('channel_conversion', {})
        
        # 创建客户ID到各渠道数据的映射
        customers_dict = {c['customer_id']: c for c in customers}
        app_users_dict = {a['customer_id']: a for a in app_users}
        wechat_followers_dict = {w['customer_id']: w for w in wechat_followers}
        work_wechat_contacts_dict = {w['customer_id']: w for w in work_wechat_contacts}
        
        # 当前日期
        today = datetime.date.today()
        
        # 生成全渠道档案
        channel_profiles = []
        
        for customer in customers:
            customer_id = customer['customer_id']
            is_personal = customer.get('customer_type') == 'personal'
            is_vip = customer.get('is_vip', False)
            
            # 确定该客户使用的渠道
            channels_used = []
            
            # 所有客户默认使用线下柜台渠道
            channels_used.append('offline')
            
            # 检查是否使用APP渠道
            if customer_id in app_users_dict:
                channels_used.append('app')
            
            # 检查是否使用公众号渠道
            if customer_id in wechat_followers_dict:
                channels_used.append('wechat')
            
            # 检查是否使用企业微信渠道
            if customer_id in work_wechat_contacts_dict:
                channels_used.append('work_wechat')
            
            # 检查是否使用网银渠道（根据一定概率决定）
            has_online_banking = False
            if is_personal:
                # 个人客户网银使用率为50%
                if random.random() < 0.5:
                    channels_used.append('online_banking')
                    has_online_banking = True
            else:
                # 企业客户网银使用率为80%
                if random.random() < 0.8:
                    channels_used.append('online_banking')
                    has_online_banking = True
            
            # 获取渠道数量
            channel_count = len(channels_used)
            
            # 生成渠道档案ID
            profile_id = self.generate_id('CP')
            
            # 确定主要渠道和次要渠道
            # 根据客户年龄/企业规模确定渠道偏好
            if is_personal and customer.get('birth_date'):
                birth_date = datetime.datetime.strptime(customer['birth_date'], '%Y-%m-%d').date()
                age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
                
                if age <= 35:
                    electronic_ratio = channel_preference.get('age_18_35', {}).get('electronic', 0.70)
                elif age <= 55:
                    electronic_ratio = channel_preference.get('age_36_55', {}).get('electronic', 0.50)
                else:
                    electronic_ratio = channel_preference.get('age_56_plus', {}).get('electronic', 0.30)
            else:
                # 企业客户
                online_banking_ratio = channel_preference.get('corporate', {}).get('online_banking', 0.60)
                electronic_ratio = online_banking_ratio
            
            # VIP客户更倾向于特殊渠道（企业微信）
            if is_vip and 'work_wechat' in channels_used:
                primary_channel = 'work_wechat'
            else:
                # 根据电子渠道偏好度决定主要渠道
                electronic_channels = [c for c in channels_used if c != 'offline']
                if electronic_channels and random.random() < electronic_ratio:
                    # 偏好电子渠道
                    if 'app' in electronic_channels:
                        primary_channel = 'app'  # APP优先级最高
                    elif 'online_banking' in electronic_channels:
                        primary_channel = 'online_banking'
                    else:
                        primary_channel = random.choice(electronic_channels)
                else:
                    # 偏好线下渠道
                    primary_channel = 'offline'
            
            # 次要渠道（除主要渠道外使用最频繁的渠道）
            if len(channels_used) > 1:
                secondary_channels = [c for c in channels_used if c != primary_channel]
                secondary_channel = random.choice(secondary_channels)
            else:
                secondary_channel = None
            
            # 渠道使用频率
            channel_frequency = {}
            for channel in channels_used:
                if channel == primary_channel:
                    frequency = random.randint(8, 15)  # 主要渠道使用频率高
                elif channel == secondary_channel:
                    frequency = random.randint(4, 7)  # 次要渠道使用频率中等
                else:
                    frequency = random.randint(1, 3)  # 其他渠道使用频率低
                
                channel_frequency[channel] = frequency
            
            # 渠道偏好得分
            channel_scores = {}
            for channel in channels_used:
                if channel == primary_channel:
                    score = random.randint(85, 100)  # 主要渠道得分高
                elif channel == secondary_channel:
                    score = random.randint(70, 84)  # 次要渠道得分中等
                else:
                    score = random.randint(50, 69)  # 其他渠道得分低
                
                channel_scores[channel] = score
            
            # 全渠道转化率
            if channel_count >= 3:
                conversion_rate = random.uniform(0.40, 0.60)  # 多渠道客户转化率高
            elif channel_count == 2:
                conversion_rate = random.uniform(0.25, 0.39)  # 双渠道客户转化率中等
            else:
                conversion_rate = random.uniform(0.10, 0.24)  # 单渠道客户转化率低
            
            # VIP客户转化率提高
            if is_vip:
                conversion_rate = min(0.85, conversion_rate * 1.3)
            
            # 首选渠道最近活动日期
            last_active_days = {}
            for channel in channels_used:
                if channel == 'app' and customer_id in app_users_dict:
                    # 从APP用户数据获取最近登录日期
                    last_login_str = app_users_dict[customer_id].get('last_login_date')
                    if last_login_str:
                        last_login_date = datetime.datetime.strptime(last_login_str, '%Y-%m-%d').date()
                        last_active_days[channel] = (today - last_login_date).days
                elif channel == 'wechat' and customer_id in wechat_followers_dict:
                    # 从公众号粉丝数据获取最近阅读日期
                    last_read_str = wechat_followers_dict[customer_id].get('last_read_date')
                    if last_read_str:
                        last_read_date = datetime.datetime.strptime(last_read_str, '%Y-%m-%d').date()
                        last_active_days[channel] = (today - last_read_date).days
                elif channel == 'work_wechat' and customer_id in work_wechat_contacts_dict:
                    # 从企业微信联系人数据获取最近联系日期
                    last_contact_str = work_wechat_contacts_dict[customer_id].get('last_contact_date')
                    if last_contact_str:
                        last_contact_date = datetime.datetime.strptime(last_contact_str, '%Y-%m-%d').date()
                        last_active_days[channel] = (today - last_contact_date).days
                else:
                    # 其他渠道随机生成
                    if channel == primary_channel:
                        last_active_days[channel] = random.randint(0, 7)  # 主要渠道最近活跃
                    elif channel == secondary_channel:
                        last_active_days[channel] = random.randint(3, 15)  # 次要渠道较近活跃
                    else:
                        last_active_days[channel] = random.randint(10, 45)  # 其他渠道活跃度较低
            
            # 创建全渠道档案记录
            channel_profile = {
                'profile_id': profile_id,
                'customer_id': customer_id,
                'channels_count': channel_count,
                'channels_used': ','.join(channels_used),
                'primary_channel': primary_channel,
                'secondary_channel': secondary_channel,
                'channel_frequency': str(channel_frequency),  # 将字典转为字符串存储
                'channel_scores': str(channel_scores),  # 将字典转为字符串存储
                'last_active_days': str(last_active_days),  # 将字典转为字符串存储
                'conversion_rate': conversion_rate,
                'last_updated': today.strftime('%Y-%m-%d')
            }
            
            channel_profiles.append(channel_profile)
        
        return channel_profiles
    
class CustomerEventGenerator(BaseEntityGenerator):
    """客户事件数据生成器"""
    
    def __init__(self, fake_generator: faker.Faker, config_manager, time_manager):
        """
        初始化客户事件生成器
        
        Args:
            fake_generator: Faker实例，用于生成随机数据
            config_manager: 配置管理器实例
            time_manager: 时间管理器实例，用于处理事件时间逻辑
        """
        super().__init__(fake_generator, config_manager)
        self.time_manager = time_manager
    
    def generate(self, customers: List[Dict], products: List[Dict], 
                start_date: datetime.date, end_date: datetime.date, 
                mode: str = 'historical') -> List[Dict]:
        """
        生成客户事件数据
        
        Args:
            customers: 客户数据列表
            products: 产品数据列表
            start_date: 开始日期
            end_date: 结束日期
            mode: 数据生成模式，'historical'或'realtime'
            
        Returns:
            客户事件数据列表
        """
        # 事件类型及其权重
        event_types = {
            'login': 40,          # 登录事件
            'inquiry': 25,        # 查询事件
            'transaction': 15,    # 交易事件
            'consultation': 10,   # 咨询事件
            'purchase': 5,        # 购买事件
            'complaint': 2,       # 投诉事件
            'feedback': 3         # 反馈事件
        }
        
        # 事件渠道及其权重
        event_channels = {
            'app': 35,            # 手机APP
            'online_banking': 25, # 网上银行
            'branch': 15,         # 支行柜台
            'call_center': 10,    # 呼叫中心
            'atm': 8,             # ATM机
            'wechat': 5,          # 微信公众号
            'work_wechat': 2      # 企业微信
        }
        
        # 事件结果及其权重
        event_results = {
            'success': 90,        # 成功
            'pending': 5,         # 处理中
            'failed': 3,          # 失败
            'canceled': 2         # 取消
        }
        
        # 当前日期
        today = datetime.date.today()
        
        events = []
        
        # 选择部分客户生成事件数据
        if mode == 'historical':
            # 历史模式，选择较多客户
            selected_customers = random.sample(customers, min(len(customers), 500))
        else:
            # 实时模式，选择较少客户
            selected_customers = random.sample(customers, min(len(customers), 100))
        
        # 计算日期范围内的天数
        days_in_range = (end_date - start_date).days + 1
        
        for customer in selected_customers:
            customer_id = customer['customer_id']
            is_personal = customer.get('customer_type') == 'personal'
            is_vip = customer.get('is_vip', False)
            
            # 确定该客户在日期范围内的事件数量
            if is_vip:
                # VIP客户事件较多
                daily_event_mean = 2.5
            else:
                # 普通客户事件较少
                daily_event_mean = 1.0
            
            # 考虑个人/企业客户的差异
            if not is_personal:
                daily_event_mean *= 1.5  # 企业客户事件较多
            
            # 预计总事件数
            total_events = int(daily_event_mean * days_in_range)
            
            # 为该客户生成事件
            current_date = start_date
            
            while current_date <= end_date:
                # 根据时间权重确定当天的事件数量
                day_weight = self.time_manager.get_date_weight(current_date)
                is_workday = self.time_manager.is_workday(current_date)
                
                # 工作日事件较多
                if is_workday:
                    day_weight *= 1.2
                
                # 调整当天事件数
                day_events_count = int(daily_event_mean * day_weight)
                
                # 随机波动
                day_events_count = max(0, day_events_count + random.randint(-1, 1))
                
                # 生成当天事件
                for _ in range(day_events_count):
                    # 生成事件ID
                    event_id = self.generate_id('E')
                    
                    # 确定事件类型
                    event_type = self.random_choice(list(event_types.keys()), list(event_types.values()))
                    
                    # 确定事件渠道
                    event_channel = self.random_choice(list(event_channels.keys()), list(event_channels.values()))
                    
                    # 确定事件结果
                    event_result = self.random_choice(list(event_results.keys()), list(event_results.values()))
                    
                    # 生成事件时间
                    if event_channel in ['branch']:
                        # 线下渠道的时间在营业时间内
                        hour = random.randint(9, 17)
                    else:
                        # 其他渠道时间分布更广
                        hour = random.randint(7, 23)
                    
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    
                    event_datetime = datetime.datetime.combine(
                        current_date, datetime.time(hour, minute, second))
                    
                    # 关联产品（只有部分事件类型有产品关联）
                    product_id = None
                    if event_type in ['purchase', 'consultation'] and random.random() < 0.8:
                        # 80%的购买和咨询事件关联产品
                        if products:
                            product = random.choice(products)
                            product_id = product.get('product_id')
                    
                    # 事件内容
                    details = self._generate_event_details(event_type, event_channel, product_id)
                    
                    # 创建事件记录
                    event = {
                        'event_id': event_id,
                        'customer_id': customer_id,
                        'event_type': event_type,
                        'event_channel': event_channel,
                        'event_datetime': event_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        'event_result': event_result,
                        'product_id': product_id,
                        'details': details,
                        'is_vip_event': is_vip
                    }
                    
                    events.append(event)
                
                # 进入下一天
                current_date += datetime.timedelta(days=1)
        
        return events
    
    def _generate_event_details(self, event_type: str, event_channel: str, product_id: Optional[str]) -> str:
        """
        生成事件详情
        
        Args:
            event_type: 事件类型
            event_channel: 事件渠道
            product_id: 关联的产品ID
            
        Returns:
            事件详情描述
        """
        if event_type == 'login':
            return f"通过{self._get_channel_name(event_channel)}登录系统"
            
        elif event_type == 'inquiry':
            inquiry_types = ['账户余额', '交易明细', '贷款信息', '理财产品', '存款利率', '汇率信息', '网点信息']
            inquiry = random.choice(inquiry_types)
            return f"在{self._get_channel_name(event_channel)}查询{inquiry}"
            
        elif event_type == 'transaction':
            transaction_types = ['转账', '缴费', '充值', '提现', '汇款', '还款']
            transaction = random.choice(transaction_types)
            return f"在{self._get_channel_name(event_channel)}进行{transaction}操作"
            
        elif event_type == 'consultation':
            if product_id:
                return f"在{self._get_channel_name(event_channel)}咨询产品 {product_id} 的相关信息"
            else:
                consultation_types = ['理财规划', '贷款方案', '账户管理', '手续费用', '增值服务']
                consultation = random.choice(consultation_types)
                return f"在{self._get_channel_name(event_channel)}咨询{consultation}"
                
        elif event_type == 'purchase':
            if product_id:
                return f"在{self._get_channel_name(event_channel)}购买产品 {product_id}"
            else:
                return f"在{self._get_channel_name(event_channel)}进行产品购买"
                
        elif event_type == 'complaint':
            complaint_types = ['系统问题', '服务质量', '手续费用', '产品不满', '流程复杂', '信息不清']
            complaint = random.choice(complaint_types)
            return f"在{self._get_channel_name(event_channel)}投诉{complaint}"
            
        elif event_type == 'feedback':
            feedback_types = ['使用体验', '功能建议', '服务评价', '产品意见']
            feedback = random.choice(feedback_types)
            return f"在{self._get_channel_name(event_channel)}提交{feedback}"
            
        else:
            return f"在{self._get_channel_name(event_channel)}进行{event_type}操作"
    
    def _get_channel_name(self, channel_code: str) -> str:
        """
        获取渠道的中文名称
        
        Args:
            channel_code: 渠道代码
            
        Returns:
            渠道中文名称
        """
        channel_names = {
            'app': '手机银行APP',
            'online_banking': '网上银行',
            'branch': '银行网点',
            'call_center': '客服中心',
            'atm': 'ATM自助设备',
            'wechat': '微信公众号',
            'work_wechat': '企业微信'
        }
        
        return channel_names.get(channel_code, channel_code)

class SearchTermGenerator(BaseEntityGenerator):
    """客户搜索词数据生成器"""
    
    def generate(self, customers: List[Dict], products: List[Dict], 
                start_date: datetime.date, end_date: datetime.date) -> List[Dict]:
        """
        生成客户搜索词数据
        
        Args:
            customers: 客户数据列表
            products: 产品数据列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            搜索词记录列表
        """
        # 搜索词类别及其权重
        search_categories = {
            'account': 25,         # 账户相关搜索
            'transaction': 20,     # 交易相关搜索
            'product': 30,         # 产品相关搜索
            'service': 15,         # 服务相关搜索
            'branch': 10,          # 网点相关搜索
        }
        
        # 各类别下的常见搜索词
        search_terms = {
            'account': [
                '如何查询余额', '账户明细怎么看', '账单查询', '如何修改密码', 
                '开通短信通知', '账户安全设置', '转账限额', '冻结账户', 
                '挂失银行卡', '解除限制'
            ],
            'transaction': [
                '如何转账', '跨行转账手续费', '转账限额是多少', '手机银行转账',
                '二维码支付', '大额转账', '汇款到境外', '批量转账',
                '转账记录查询', '提现手续费'
            ],
            'product': [
                '理财产品推荐', '高收益理财', '稳健理财', '基金产品',
                '定期存款利率', '大额存单', '结构性存款', '外币存款',
                '贷款产品', '房贷利率', '消费贷款', '信用贷款'
            ],
            'service': [
                '在线客服', '预约办理', '投诉电话', '网银激活',
                '手机银行注册', '贵宾服务', '积分兑换', '代缴费',
                '电子回单', '账单分期'
            ],
            'branch': [
                '附近网点', '营业时间', '自助设备', '智能柜员机',
                '周末营业网点', '外币兑换网点', '贵宾理财中心', '24小时网点',
                '预约取号', '停车信息'
            ]
        }
        
        # 搜索结果类型及其权重
        result_types = {
            'successful': 75,      # 搜索成功，找到匹配结果
            'redirected': 15,      # 被重定向到相关页面
            'no_result': 8,        # 无搜索结果
            'error': 2             # 搜索过程出错
        }
        
        # 搜索来源渠道及其权重
        search_sources = {
            'app': 55,             # 手机银行APP
            'online_banking': 30,  # 网上银行
            'website': 10,         # 银行官网
            'wechat': 5            # 微信公众号
        }
        
        # 当前日期
        today = datetime.date.today()
        
        # 只选择部分客户生成搜索记录
        search_probability = 0.35  # 35%的客户有搜索行为
        search_customers = []
        
        for customer in customers:
            is_personal = customer.get('customer_type') == 'personal'
            is_vip = customer.get('is_vip', False)
            
            # VIP客户和个人客户更可能进行搜索
            if is_vip:
                search_probability += 0.15
            if is_personal:
                search_probability += 0.10
            
            if random.random() < search_probability:
                search_customers.append(customer)
            
            # 重置概率为基准值
            search_probability = 0.35
        
        # 生成搜索记录
        search_records = []
        
        for customer in search_customers:
            customer_id = customer['customer_id']
            is_personal = customer.get('customer_type') == 'personal'
            is_vip = customer.get('is_vip', False)
            
            # 确定该客户在日期范围内的搜索次数
            days_in_range = (end_date - start_date).days + 1
            
            if is_vip:
                # VIP客户搜索较多
                search_frequency = random.uniform(0.3, 0.5)  # 每天0.3-0.5次
            else:
                # 普通客户搜索较少
                search_frequency = random.uniform(0.1, 0.3)  # 每天0.1-0.3次
            
            # 总搜索次数
            total_searches = int(search_frequency * days_in_range)
            
            # 随机分布搜索在日期范围内
            search_dates = []
            for _ in range(total_searches):
                day_offset = random.randint(0, days_in_range - 1)
                search_date = start_date + datetime.timedelta(days=day_offset)
                search_dates.append(search_date)
            
            # 生成每次搜索的记录
            for search_date in search_dates:
                # 生成搜索记录ID
                search_id = self.generate_id('S')
                
                # 确定搜索词类别
                search_category = self.random_choice(
                    list(search_categories.keys()), 
                    list(search_categories.values())
                )
                
                # 如果客户是企业客户，调整搜索词偏好
                if not is_personal:
                    # 企业客户更关注交易和服务
                    if search_category == 'account':
                        search_category = 'transaction' if random.random() < 0.7 else search_category
                    elif search_category == 'product':
                        search_category = 'service' if random.random() < 0.5 else search_category
                
                # 确定具体搜索词
                if search_category == 'product' and random.random() < 0.4 and products:
                    # 40%的产品搜索直接搜索产品名称
                    product = random.choice(products)
                    search_term = product.get('name', '理财产品')
                else:
                    # 其他搜索使用预定义搜索词
                    search_term = random.choice(search_terms[search_category])
                
                # 搜索时间（在一天中随机分布）
                hour = random.randint(7, 23)  # 假设大部分搜索在7点到23点
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                
                search_datetime = datetime.datetime.combine(
                    search_date, datetime.time(hour, minute, second))
                
                # 搜索结果
                result_type = self.random_choice(
                    list(result_types.keys()), 
                    list(result_types.values())
                )
                
                # 搜索来源
                search_source = self.random_choice(
                    list(search_sources.keys()), 
                    list(search_sources.values())
                )
                
                # 创建搜索记录
                search_record = {
                    'search_id': search_id,
                    'customer_id': customer_id,
                    'search_term': search_term,
                    'search_category': search_category,
                    'search_datetime': search_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                    'result_type': result_type,
                    'search_source': search_source,
                    'result_count': random.randint(0, 20) if result_type != 'no_result' else 0,
                    'session_id': self.generate_id('SES'),  # 会话ID
                    'is_advanced_search': random.random() < 0.15  # 15%是高级搜索
                }
                
                search_records.append(search_record)
        
        return search_records
    
class TransactionAnalyticsGenerator(BaseEntityGenerator):
    """交易分析数据生成器"""
    
    def generate(self, transactions: List[Dict], customers: List[Dict], fund_accounts: List[Dict]) -> Dict[str, Any]:
        """
        基于交易记录生成交易分析结果
        
        Args:
            transactions: 交易记录列表
            customers: 客户数据列表
            fund_accounts: 资金账户数据列表
            
        Returns:
            交易分析结果
        """
        if not transactions:
            return {"status": "no_data", "message": "无交易数据可供分析"}
            
        # 创建客户ID和账户ID的映射
        customer_map = {c['customer_id']: c for c in customers}
        account_map = {a['account_id']: a for a in fund_accounts}
        
        # 将交易按渠道分组
        channel_transactions = {}
        for tx in transactions:
            channel = tx.get('channel', 'unknown')
            if channel not in channel_transactions:
                channel_transactions[channel] = []
            channel_transactions[channel].append(tx)
        
        # 按时间段统计交易量和交易金额
        time_stats = self._analyze_time_distribution(transactions)
        
        # 按渠道统计交易量和交易金额
        channel_stats = self._analyze_channel_distribution(channel_transactions)
        
        # 按交易类型统计
        type_stats = self._analyze_transaction_types(transactions)
        
        # 按客户分组统计
        customer_stats = self._analyze_customer_segments(
            transactions, customers, customer_map, account_map)
        
        # 异常交易分析
        anomaly_stats = self._detect_anomalies(transactions, account_map)
        
        # 整合分析结果
        analytics_result = {
            "status": "success",
            "transaction_count": len(transactions),
            "unique_customers": len(set(tx.get('account_id', '') for tx in transactions)),
            "time_distribution": time_stats,
            "channel_distribution": channel_stats,
            "transaction_types": type_stats,
            "customer_segments": customer_stats,
            "anomalies": anomaly_stats,
            "generated_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return analytics_result
    
    def _analyze_time_distribution(self, transactions: List[Dict]) -> Dict[str, Any]:
        """分析交易的时间分布"""
        # 按小时统计
        hourly_stats = {}
        for hour in range(24):
            hourly_stats[hour] = {"count": 0, "amount": 0}
            
        # 按星期几统计
        weekday_stats = {}
        for day in range(7):
            weekday_stats[day] = {"count": 0, "amount": 0}
            
        # 按日期统计
        daily_stats = {}
        
        for tx in transactions:
            tx_datetime_str = tx.get('transaction_datetime')
            if not tx_datetime_str:
                continue
                
            try:
                tx_datetime = datetime.datetime.strptime(tx_datetime_str, '%Y-%m-%d %H:%M:%S')
                tx_hour = tx_datetime.hour
                tx_weekday = tx_datetime.weekday()
                tx_date = tx_datetime.strftime('%Y-%m-%d')
                tx_amount = float(tx.get('amount', 0))
                
                # 更新小时统计
                if tx_hour in hourly_stats:
                    hourly_stats[tx_hour]["count"] += 1
                    hourly_stats[tx_hour]["amount"] += tx_amount
                
                # 更新星期几统计
                if tx_weekday in weekday_stats:
                    weekday_stats[tx_weekday]["count"] += 1
                    weekday_stats[tx_weekday]["amount"] += tx_amount
                
                # 更新日期统计
                if tx_date not in daily_stats:
                    daily_stats[tx_date] = {"count": 0, "amount": 0}
                daily_stats[tx_date]["count"] += 1
                daily_stats[tx_date]["amount"] += tx_amount
                
            except (ValueError, TypeError):
                continue
        
        # 计算高峰期
        peak_hour = max(hourly_stats.items(), key=lambda x: x[1]["count"])[0]
        peak_weekday = max(weekday_stats.items(), key=lambda x: x[1]["count"])[0]
        peak_date = max(daily_stats.items(), key=lambda x: x[1]["count"])[0] if daily_stats else None
        
        return {
            "hourly_distribution": hourly_stats,
            "weekday_distribution": weekday_stats,
            "daily_distribution": daily_stats,
            "peak_hour": peak_hour,
            "peak_weekday": peak_weekday,
            "peak_date": peak_date,
            "weekday_names": ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
        }
    
    def _analyze_channel_distribution(self, channel_transactions: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """分析交易的渠道分布"""
        channel_stats = {}
        total_count = 0
        total_amount = 0
        
        for channel, txs in channel_transactions.items():
            channel_count = len(txs)
            channel_amount = sum(float(tx.get('amount', 0)) for tx in txs)
            
            channel_stats[channel] = {
                "count": channel_count,
                "amount": channel_amount
            }
            
            total_count += channel_count
            total_amount += channel_amount
        
        # 计算百分比
        if total_count > 0:
            for channel in channel_stats:
                channel_stats[channel]["count_percentage"] = round(
                    channel_stats[channel]["count"] / total_count * 100, 2)
        
        if total_amount > 0:
            for channel in channel_stats:
                channel_stats[channel]["amount_percentage"] = round(
                    channel_stats[channel]["amount"] / total_amount * 100, 2)
        
        # 提取主要渠道
        main_channel_by_count = max(channel_stats.items(), 
                                   key=lambda x: x[1]["count"])[0] if channel_stats else None
        main_channel_by_amount = max(channel_stats.items(), 
                                    key=lambda x: x[1]["amount"])[0] if channel_stats else None
        
        return {
            "channel_stats": channel_stats,
            "total_count": total_count,
            "total_amount": total_amount,
            "main_channel_by_count": main_channel_by_count,
            "main_channel_by_amount": main_channel_by_amount,
            "channel_names": {
                "online_banking": "网上银行",
                "mobile_app": "手机银行",
                "atm": "ATM自助",
                "counter": "柜台",
                "third_party": "第三方支付",
                "unknown": "未知渠道"
            }
        }
    
    def _analyze_transaction_types(self, transactions: List[Dict]) -> Dict[str, Any]:
        """分析交易类型分布"""
        type_stats = {}
        
        for tx in transactions:
            tx_type = tx.get('transaction_type', 'unknown')
            tx_amount = float(tx.get('amount', 0))
            
            if tx_type not in type_stats:
                type_stats[tx_type] = {"count": 0, "amount": 0}
            
            type_stats[tx_type]["count"] += 1
            type_stats[tx_type]["amount"] += tx_amount
        
        # 计算总量
        total_count = sum(stats["count"] for stats in type_stats.values())
        total_amount = sum(stats["amount"] for stats in type_stats.values())
        
        # 计算百分比
        if total_count > 0:
            for tx_type in type_stats:
                type_stats[tx_type]["count_percentage"] = round(
                    type_stats[tx_type]["count"] / total_count * 100, 2)
        
        if total_amount > 0:
            for tx_type in type_stats:
                type_stats[tx_type]["amount_percentage"] = round(
                    type_stats[tx_type]["amount"] / total_amount * 100, 2)
        
        return {
            "type_stats": type_stats,
            "total_count": total_count,
            "total_amount": total_amount,
            "type_names": {
                "deposit": "存款",
                "withdrawal": "取款",
                "transfer": "转账",
                "consumption": "消费",
                "other": "其他",
                "unknown": "未知类型"
            }
        }
    
    def _analyze_customer_segments(self, transactions: List[Dict], 
                                 customers: List[Dict], 
                                 customer_map: Dict[str, Dict],
                                 account_map: Dict[str, Dict]) -> Dict[str, Any]:
        """分析不同客户群体的交易行为"""
        # 按客户ID分组交易
        customer_transactions = {}
        
        for tx in transactions:
            account_id = tx.get('account_id')
            if not account_id or account_id not in account_map:
                continue
                
            customer_id = account_map[account_id].get('customer_id')
            if not customer_id:
                continue
                
            if customer_id not in customer_transactions:
                customer_transactions[customer_id] = []
                
            customer_transactions[customer_id].append(tx)
        
        # 统计个人客户和企业客户
        personal_customers = {}
        corporate_customers = {}
        
        for customer_id, txs in customer_transactions.items():
            if customer_id not in customer_map:
                continue
                
            customer = customer_map[customer_id]
            customer_type = customer.get('customer_type')
            is_vip = customer.get('is_vip', False)
            
            tx_count = len(txs)
            tx_amount = sum(float(tx.get('amount', 0)) for tx in txs)
            
            customer_summary = {
                "transaction_count": tx_count,
                "transaction_amount": tx_amount,
                "average_amount": tx_amount / tx_count if tx_count > 0 else 0,
                "is_vip": is_vip
            }
            
            if customer_type == 'personal':
                personal_customers[customer_id] = customer_summary
            elif customer_type == 'corporate':
                corporate_customers[customer_id] = customer_summary
        
        # 统计VIP客户和普通客户
        vip_stats = {
            "count": 0,
            "transaction_count": 0,
            "transaction_amount": 0,
            "average_per_customer": 0
        }
        
        regular_stats = {
            "count": 0,
            "transaction_count": 0,
            "transaction_amount": 0,
            "average_per_customer": 0
        }
        
        # 合并个人和企业客户统计
        all_customer_summaries = {**personal_customers, **corporate_customers}
        
        for customer_id, summary in all_customer_summaries.items():
            if summary["is_vip"]:
                vip_stats["count"] += 1
                vip_stats["transaction_count"] += summary["transaction_count"]
                vip_stats["transaction_amount"] += summary["transaction_amount"]
            else:
                regular_stats["count"] += 1
                regular_stats["transaction_count"] += summary["transaction_count"]
                regular_stats["transaction_amount"] += summary["transaction_amount"]
        
        # 计算人均值
        if vip_stats["count"] > 0:
            vip_stats["average_per_customer"] = vip_stats["transaction_amount"] / vip_stats["count"]
            
        if regular_stats["count"] > 0:
            regular_stats["average_per_customer"] = regular_stats["transaction_amount"] / regular_stats["count"]
        
        return {
            "personal": {
                "count": len(personal_customers),
                "transaction_count": sum(c["transaction_count"] for c in personal_customers.values()),
                "transaction_amount": sum(c["transaction_amount"] for c in personal_customers.values()),
                "average_per_customer": sum(c["transaction_amount"] for c in personal_customers.values()) / len(personal_customers) if personal_customers else 0
            },
            "corporate": {
                "count": len(corporate_customers),
                "transaction_count": sum(c["transaction_count"] for c in corporate_customers.values()),
                "transaction_amount": sum(c["transaction_amount"] for c in corporate_customers.values()),
                "average_per_customer": sum(c["transaction_amount"] for c in corporate_customers.values()) / len(corporate_customers) if corporate_customers else 0
            },
            "vip": vip_stats,
            "regular": regular_stats
        }
    
    def _detect_anomalies(self, transactions: List[Dict], account_map: Dict[str, Dict]) -> Dict[str, Any]:
        """检测异常交易"""
        # 大额交易阈值（个人和企业不同）
        personal_large_amount_threshold = 50000  # 5万以上视为大额
        corporate_large_amount_threshold = 200000  # 20万以上视为大额
        
        # 频繁交易阈值
        frequency_threshold = 10  # 短时间内10次以上交易视为频繁
        
        # 异常时间交易
        odd_hour_ranges = [(0, 5)]  # 凌晨0点到5点视为异常时间
        
        # 统计结果
        large_transactions = []
        frequent_accounts = {}
        odd_hour_transactions = []
        
        # 账户最近一次交易时间记录
        last_transaction_time = {}
        
        for tx in transactions:
            tx_datetime_str = tx.get('transaction_datetime')
            if not tx_datetime_str:
                continue
                
            try:
                tx_datetime = datetime.datetime.strptime(tx_datetime_str, '%Y-%m-%d %H:%M:%S')
                tx_amount = float(tx.get('amount', 0))
                account_id = tx.get('account_id')
                
                if not account_id or account_id not in account_map:
                    continue
                
                # 检查是否大额交易
                account = account_map[account_id]
                is_personal = account.get('customer_id', '').startswith('C')
                threshold = personal_large_amount_threshold if is_personal else corporate_large_amount_threshold
                
                if tx_amount >= threshold:
                    large_transactions.append({
                        "transaction_id": tx.get('transaction_id'),
                        "account_id": account_id,
                        "amount": tx_amount,
                        "datetime": tx_datetime_str,
                        "is_personal": is_personal
                    })
                
                # 检查是否频繁交易
                current_date = tx_datetime.date()
                if account_id not in frequent_accounts:
                    frequent_accounts[account_id] = {"date": current_date, "count": 1}
                elif frequent_accounts[account_id]["date"] == current_date:
                    frequent_accounts[account_id]["count"] += 1
                else:
                    frequent_accounts[account_id] = {"date": current_date, "count": 1}
                    
                # 检查是否异常时间交易
                tx_hour = tx_datetime.hour
                for start, end in odd_hour_ranges:
                    if start <= tx_hour < end:
                        odd_hour_transactions.append({
                            "transaction_id": tx.get('transaction_id'),
                            "account_id": account_id,
                            "amount": tx_amount,
                            "datetime": tx_datetime_str,
                            "hour": tx_hour
                        })
                        break
            
            except (ValueError, TypeError):
                continue
        
        # 筛选真正频繁交易的账户
        frequent_account_list = [
            {"account_id": account_id, "count": data["count"], "date": data["date"].strftime('%Y-%m-%d')}
            for account_id, data in frequent_accounts.items()
            if data["count"] >= frequency_threshold
        ]
        
        return {
            "large_transactions": {
                "count": len(large_transactions),
                "personal_threshold": personal_large_amount_threshold,
                "corporate_threshold": corporate_large_amount_threshold,
                "transactions": large_transactions[:10]  # 只返回前10条作为示例
            },
            "frequent_transactions": {
                "count": len(frequent_account_list),
                "threshold": frequency_threshold,
                "accounts": frequent_account_list[:10]  # 只返回前10条作为示例
            },
            "odd_hour_transactions": {
                "count": len(odd_hour_transactions),
                "hour_ranges": [[start, end] for start, end in odd_hour_ranges],
                "transactions": odd_hour_transactions[:10]  # 只返回前10条作为示例
            }
        }