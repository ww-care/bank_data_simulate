#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
实时数据生成脚本

用于生成银行实时数据，根据执行时间生成对应时间段的数据。
- 13点执行：生成当天0-12点数据
- 次日1点执行：生成前一天13-23点数据
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
sys.path.append(project_root)

# 导入项目模块
from src.config_manager import get_config_manager
from src.database_manager import get_database_manager
from src.logger import get_logger
from src.time_manager.time_manager import get_time_manager


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='生成银行实时数据')
    parser.add_argument('--config-dir', type=str, help='配置文件目录路径')
    parser.add_argument('--log-level', type=str, default='info', 
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='日志级别')
    parser.add_argument('--force', action='store_true', help='强制执行，忽略时间检查')
    args = parser.parse_args()
    
    # 初始化日志
    logger = get_logger('realtime_data', level=args.log_level)
    logger.info("开始生成银行实时数据...")
    
    try:
        # 初始化配置管理器
        config_manager = get_config_manager(args.config_dir)
        
        # 初始化时间管理器
        time_manager = get_time_manager()
        
        # 初始化数据库管理器
        db_manager = get_database_manager()
        
        # 检查数据库表是否存在
        if not db_manager.table_exists('customer'):
            logger.error("数据库表不存在，请先运行历史数据生成脚本创建表结构")
            return 1
        
        # 获取当前时间
        current_time = time_manager.get_current_time()
        hour = current_time.hour
        
        # 检查执行时间
        if not args.force and hour != 13 and hour != 1:
            logger.warning(f"当前时间 {hour}点 不是预定的执行时间(13点或1点)")
            logger.warning("如果需要在非预定时间执行，请使用 --force 参数")
            return 1
        
        # 计算实时数据时间范围
        start_time, end_time = time_manager.get_time_range_for_generation('realtime')
        
        # 获取上次生成的时间点
        last_time = time_manager.get_last_generation_time(db_manager, 'realtime')
        
        # 检查时间连续性
        if last_time is not None:
            # 将时区调整为相同
            if last_time.tzinfo is None:
                last_time = time_manager.timezone.localize(last_time)
            
            # 确保新的开始时间不早于上次的结束时间
            if start_time < last_time:
                logger.warning(f"计算的开始时间 {start_time} 早于上次生成的结束时间 {last_time}")
                start_time = last_time
                
                # 如果开始时间已经接近或超过结束时间，直接退出
                if (end_time - start_time).total_seconds() < 60:  # 少于1分钟
                    logger.info("没有新的时间段需要生成数据，退出程序")
                    return 0
        
        logger.info(f"实时数据时间范围: {start_time} 至 {end_time}")
        
        # 生成日志ID
        time_str = time_manager.format_datetime(current_time, '%Y%m%d%H%M')
        log_id = f"REAL_{time_str}_{uuid.uuid4().hex[:8]}"
        
        # 记录开始状态
        exec_start_time = time_manager.get_current_time()
        db_manager.log_data_generation(
            log_id=log_id,
            mode='realtime',
            start_time=time_manager.format_datetime(exec_start_time),
            status='running',
            start_date=time_manager.format_date(start_time.date()),
            end_date=time_manager.format_date(end_time.date()),
            details=f'开始生成实时数据: {start_time} 至 {end_time}'
        )
        
        # TODO: 调用数据生成模块生成实时数据
        # 这里应该调用DataGenerator的生成方法
        logger.info("数据生成器尚未实现，仅执行框架测试")
        
        # 如果时间范围跨越多个小时，可以按小时分割生成
        time_slots = time_manager.split_time_range(start_time, end_time)
        total_records = 0
        
        for slot_start, slot_end in time_slots:
            logger.info(f"生成时间段 {slot_start} - {slot_end} 的数据")
            # TODO: 为每个时间段生成数据
            time.sleep(1)  # 模拟数据生成耗时
            slot_records = 0  # 该时间段生成的记录数
            total_records += slot_records
        
        # 记录完成状态
        exec_end_time = time_manager.get_current_time()
        db_manager.log_data_generation(
            log_id=log_id,
            mode='realtime',
            start_time=time_manager.format_datetime(exec_start_time),
            end_time=time_manager.format_datetime(exec_end_time),
            status='success',
            start_date=time_manager.format_date(start_time.date()),
            end_date=time_manager.format_date(end_time.date()),
            records_generated=total_records,
            details=f'实时数据生成完成: {start_time} 至 {end_time}, 共生成 {total_records} 条记录'
        )
        
        execution_time = (exec_end_time - exec_start_time).total_seconds()
        logger.info(f"实时数据生成完成，执行时间: {execution_time:.2f} 秒，生成记录数: {total_records}")
        
        return 0
        
    except Exception as e:
        logger.error(f"实时数据生成过程中出错: {str(e)}", exc_info=True)
        return 1
    
    finally:
        # 关闭数据库连接
        if 'db_manager' in locals():
            db_manager.disconnect()


if __name__ == "__main__":
    sys.exit(main())