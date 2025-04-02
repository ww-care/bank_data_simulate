#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
小型数据生成测试脚本

生成少量测试数据，用于验证数据生成功能。
"""

import os
import sys
import datetime

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

def main():
    logger = get_logger('mini_test', level='debug')
    logger.info("开始小型数据生成测试...")
    
    try:
        # 初始化组件
        config_manager = get_config_manager()
        time_manager = get_time_manager()
        db_manager = get_database_manager()
        
        # 确保数据库表已创建
        logger.info("确保数据库表已创建...")
        if not db_manager.create_tables():
            logger.error("无法创建数据库表，测试终止")
            return False
        
        # 暂时覆盖配置，减少生成的数据量
        logger.info("暂时调整配置，减少数据生成量...")
        original_customer_config = config_manager.get_entity_config('customer')
        small_customer_config = {'total_count': 5}  # 只生成5个客户
        config_manager.update_entity_config('customer', small_customer_config)
        
        # 设置小的时间范围
        today = datetime.date.today()
        test_end_date = today - datetime.timedelta(days=1)
        test_start_date = today - datetime.timedelta(days=2)
        logger.info(f"测试数据时间范围: {test_start_date} 至 {test_end_date}")
        
        # 获取数据生成器
        data_generator = get_data_generator()
        
        # 生成历史数据
        logger.info("开始生成少量历史数据...")
        generation_stats = data_generator.generate_data(
            test_start_date, test_end_date, mode='historical')
        
        # 打印生成统计
        total_records = sum(generation_stats.values())
        logger.info(f"生成了 {total_records} 条历史数据记录")
        for entity, count in generation_stats.items():
            logger.info(f"  - {entity}: {count} 条记录")
        
        # 验证生成的数据
        logger.info("开始验证生成的数据...")
        validator = get_validator()
        validation_results = validator.validate(data_generator.data_cache)
        
        if validation_results['status'] == 'success':
            logger.info("数据验证成功!")
        else:
            logger.warning(f"数据验证发现 {validation_results['total_errors']} 个错误")
            for error_type, error_count in validation_results['error_counts'].items():
                logger.warning(f"  - {error_type}: {error_count} 个错误")
        
        # 检查数据库中的记录数
        logger.info("检查数据库中的记录数...")
        for entity in generation_stats.keys():
            count_result = db_manager.execute_query(f"SELECT COUNT(*) as count FROM {entity}")
            if count_result:
                db_count = count_result[0]['count']
                logger.info(f"  - {entity}: 数据库中有 {db_count} 条记录")
                
                # 检查是否与生成统计一致
                if db_count != generation_stats[entity]:
                    logger.warning(f"  - {entity}: 数据库记录数 ({db_count}) 与生成统计 ({generation_stats[entity]}) 不一致!")
        
        # 恢复原始配置
        config_manager.update_entity_config('customer', original_customer_config)
        logger.info("已恢复原始配置")
        
        logger.info("小型数据生成测试完成")
        return True
        
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}", exc_info=True)
        return False
    finally:
        # 断开数据库连接
        if 'db_manager' in locals():
            db_manager.disconnect()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
