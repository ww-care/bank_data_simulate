#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
历史数据生成脚本

用于生成银行历史数据，覆盖从过去一年到昨天的时间范围。
"""

import os
import sys
import time
import uuid
import datetime
import argparse

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(os.path.join(project_root, 'src'))

# 导入项目模块
from src.config_manager import get_config_manager
from src.database_manager import get_database_manager
from src.logger import get_logger
from src.time_manager.time_manager import get_time_manager


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='生成银行历史数据')
    parser.add_argument('--config-dir', type=str, help='配置文件目录路径')
    parser.add_argument('--log-level', type=str, default='info', 
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='日志级别')
    args = parser.parse_args()
    
    # 初始化日志
    logger = get_logger('historical_data', level=args.log_level)
    logger.info("开始生成银行历史数据...")
    
    try:
        # 初始化配置管理器
        config_manager = get_config_manager(args.config_dir)
        
        # 初始化时间管理器
        time_manager = get_time_manager()
        
        # 初始化数据库管理器
        db_manager = get_database_manager()
        
        # 创建数据库表
        if not db_manager.create_tables():
            logger.error("创建数据库表失败，终止数据生成")
            return 1
        
        # 计算历史数据时间范围
        start_date, end_date = time_manager.calculate_historical_period()
        logger.info(f"历史数据时间范围: {start_date} 至 {end_date}")
        
        # 生成日志ID
        log_id = f"HIST_{uuid.uuid4().hex[:8]}_{time_manager.format_date(datetime.date.today(), '%Y%m%d')}"
        
        # 记录开始状态
        start_time = time_manager.get_current_time()
        db_manager.log_data_generation(
            log_id=log_id,
            mode='historical',
            start_time=time_manager.format_datetime(start_time),
            status='running',
            start_date=time_manager.format_date(start_date),
            end_date=time_manager.format_date(end_date),
            details='开始生成历史数据'
        )
        
        # 调用数据生成模块生成历史数据
        from src.data_generator.data_generator import get_data_generator
        from src.data_validator import get_validator
        
        logger.info("开始调用数据生成器生成历史数据...")
        data_generator = get_data_generator()
        
        # 生成历史数据
        generation_stats = data_generator.generate_data(start_date, end_date, mode='historical')
        
        # 记录生成的数据统计信息
        total_records = sum(generation_stats.values())
        details = f"历史数据生成完成，共生成 {total_records} 条记录"
        logger.info(details)
        
        # 验证生成的数据
        logger.info("开始验证生成的数据...")
        validator = get_validator()
        validation_results = validator.validate(data_generator.data_cache)
        
        logger.info(f"数据验证完成，状态: {validation_results['status']}")
        if validation_results['status'] == 'failed':
            logger.warning(f"数据验证发现 {validation_results['total_errors']} 个错误")
        
        # 记录完成状态
        end_time = time_manager.get_current_time()
        db_manager.log_data_generation(
            log_id=log_id,
            mode='historical',
            start_time=time_manager.format_datetime(start_time),
            end_time=time_manager.format_datetime(end_time),
            status='success',
            start_date=time_manager.format_date(start_date),
            end_date=time_manager.format_date(end_date),
            records_generated=total_records,  # 实际生成的记录数
            details='历史数据生成完成'
        )
        
        execution_time = (end_time - start_time).total_seconds()
        logger.info(f"历史数据生成完成，执行时间: {execution_time:.2f} 秒")
        
        return 0
        
    except Exception as e:
        logger.error(f"历史数据生成过程中出错: {str(e)}", exc_info=True)
        return 1
    
    finally:
        # 关闭数据库连接
        if 'db_manager' in locals():
            db_manager.disconnect()


if __name__ == "__main__":
    sys.exit(main())