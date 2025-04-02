#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
集成测试脚本

用于测试数据生成器与运行脚本的集成，生成少量测试数据。
"""

import os
import sys
import datetime
import unittest

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 导入项目模块
from src.config_manager import get_config_manager
from src.database_manager import get_database_manager
from src.logger import get_logger
from src.time_manager.time_manager import get_time_manager
from src.data_generator.data_generator import get_data_generator
from src.data_validator import get_validator


class IntegrationTest(unittest.TestCase):
    """集成测试类"""
    
    def setUp(self):
        """初始化测试环境"""
        self.logger = get_logger('test_integration', level='debug')
        self.logger.info("初始化测试环境...")
        
        self.config_manager = get_config_manager()
        self.time_manager = get_time_manager()
        self.db_manager = get_database_manager()
        self.data_generator = get_data_generator()
        self.validator = get_validator()
        
        # 确保数据库表已创建
        self.db_manager.create_tables()
        
        # 测试用的时间范围（仅测试最近3天的数据）
        today = datetime.date.today()
        self.test_end_date = today - datetime.timedelta(days=1)
        self.test_start_date = today - datetime.timedelta(days=3)
        
        self.logger.info(f"测试数据时间范围: {self.test_start_date} 至 {self.test_end_date}")
    
    def tearDown(self):
        """清理测试环境"""
        self.logger.info("清理测试环境...")
        self.db_manager.disconnect()
    
    def test_historical_data_generation(self):
        """测试历史数据生成"""
        self.logger.info("开始测试历史数据生成...")
        
        # 生成少量历史数据
        generation_stats = self.data_generator.generate_data(
            self.test_start_date, self.test_end_date, mode='historical')
        
        total_records = sum(generation_stats.values())
        self.logger.info(f"生成了 {total_records} 条历史数据记录")
        
        # 验证生成的数据
        validation_results = self.validator.validate(self.data_generator.data_cache)
        
        # 断言
        self.assertGreater(total_records, 0, "应该生成至少一条记录")
        self.assertEqual(validation_results['status'], 'success', "数据验证应该成功")
    
    def test_realtime_data_generation(self):
        """测试实时数据生成"""
        self.logger.info("开始测试实时数据生成...")
        
        # 先确保有历史数据
        self.test_historical_data_generation()
        
        # 生成实时数据（只生成最后一天的最后一小时数据作为测试）
        last_day = self.test_end_date
        start_datetime = datetime.datetime.combine(last_day, datetime.time(22, 0, 0))
        end_datetime = datetime.datetime.combine(last_day, datetime.time(23, 0, 0))
        
        # 添加时区信息
        start_datetime = self.time_manager.timezone.localize(start_datetime)
        end_datetime = self.time_manager.timezone.localize(end_datetime)
        
        self.logger.info(f"测试实时数据时间范围: {start_datetime} 至 {end_datetime}")
        
        # 生成实时数据
        realtime_stats = self.data_generator.generate_data_for_timeperiod(
            start_datetime, end_datetime, mode='realtime')
        
        total_realtime_records = sum(realtime_stats.values())
        self.logger.info(f"生成了 {total_realtime_records} 条实时数据记录")
        
        # 验证实时数据
        validation_results = self.validator.validate(self.data_generator.data_cache)
        
        # 断言
        self.assertGreaterEqual(total_realtime_records, 0, "应该生成零条或多条记录")
        self.assertEqual(validation_results['status'], 'success', "数据验证应该成功")


if __name__ == "__main__":
    unittest.main()