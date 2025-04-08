#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
带数据库断点续传功能的数据生成脚本

可以从中断的地方恢复数据生成，特别适用于处理耗时较长的生成阶段。
"""

import os
import sys
import time
import signal
import argparse
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
from src.generation_executor import GenerationExecutor
from src.checkpoint_manager import get_checkpoint_manager

# 全局变量，用于处理信号
g_checkpoint_manager = None
g_logger = None

def signal_handler(sig, frame):
    """处理终止信号"""
    if g_logger:
        g_logger.warning("收到终止信号，正在暂停运行...")
    
    if g_checkpoint_manager:
        g_checkpoint_manager.pause_run("用户中断")
        
    if g_logger:
        g_logger.info("状态已保存，可以安全退出")
    
    sys.exit(0)

def main():
    """主函数"""
    global g_checkpoint_manager, g_logger
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='带数据库断点续传功能的银行数据生成')
    parser.add_argument('--config-dir', type=str, help='配置文件目录路径')
    parser.add_argument('--log-level', type=str, default='info', 
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='日志级别')
    parser.add_argument('--resume', action='store_true', help='从上次中断点恢复')
    parser.add_argument('--skip-to', type=str, help='跳过前面的生成阶段，直接从指定阶段开始')
    parser.add_argument('--batch-size', type=int, default=1000, help='批处理大小')
    parser.add_argument('--clear-history', action='store_true', help='清除所有历史状态记录')
    args = parser.parse_args()
    
    # 初始化日志
    g_logger = get_logger('checkpoint_generator', level=args.log_level)
    g_logger.info("开始生成银行历史数据（带断点续传）...")
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)  # 处理Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 处理终止信号
    
    try:
        # 初始化各模块
        config_manager = get_config_manager(args.config_dir)
        time_manager = get_time_manager()
        db_manager = get_database_manager()
        
        # 初始化断点管理器
        g_checkpoint_manager = get_checkpoint_manager(db_manager, g_logger)
        
        # 如果需要清除历史状态
        if args.clear_history:
            g_logger.info("清除所有历史状态记录...")
            db_manager.execute_update("DELETE FROM generation_status")
            g_logger.info("历史状态记录已清除")
            if not args.resume and not args.skip_to:
                return 0
        
        # 确保数据库表存在
        if not db_manager.table_exists('customer'):
            g_logger.info("创建数据库表结构...")
            db_manager.create_tables()
        
        # 计算历史数据时间范围
        start_date, end_date = time_manager.calculate_historical_period()
        g_logger.info(f"历史数据时间范围: {start_date} 至 {end_date}")
        
        # 创建数据生成执行器
        executor = GenerationExecutor(batch_size=args.batch_size, logger=g_logger)
        
        # 确定要跳过的阶段
        skip_stages = []
        if args.skip_to:
            all_stages = g_checkpoint_manager.all_stages
            try:
                skip_to_index = all_stages.index(args.skip_to)
                skip_stages = all_stages[:skip_to_index]
                g_logger.info(f"将跳过以下阶段: {', '.join(skip_stages)}")
            except ValueError:
                g_logger.error(f"未知的阶段名称: {args.skip_to}，可用的阶段: {', '.join(all_stages)}")
                return 1
        
        # 是否从上次状态恢复
        if args.resume:
            if executor.resume_from_last():
                g_logger.info("成功从上次运行状态恢复")
            else:
                g_logger.warning("找不到可恢复的状态，将从头开始")
                executor.initialize_run(skip_stages)
        else:
            # 初始化新的运行
            executor.initialize_run(skip_stages)
        
        # 执行数据生成
        start_time = time.time()
        # 如果是恢复模式，应该保留原有的运行ID，不需要重新设置
        # 只在非恢复模式下设置新的运行ID
        if not args.resume:
            run_id = f"RUN_{int(start_time)}"
            g_logger.info(f"使用新的运行ID: {run_id}")
            executor.checkpoint_manager.run_id = run_id
        else:
            g_logger.info(f"继续使用原有运行ID: {executor.checkpoint_manager.run_id}")
        
        stats = executor.execute(start_date, end_date)
        end_time = time.time()
        
        # 统计信息
        execution_time = end_time - start_time
        total_records = sum(stats.values())
        g_logger.info(f"数据生成完成，总执行时间: {execution_time:.2f} 秒，总记录数: {total_records}")
        
        # 详细统计
        g_logger.info("各表记录数统计:")
        for stage, count in stats.items():
            g_logger.info(f"  - {stage}: {count} 条记录")
        
        return 0
        
    except Exception as e:
        g_logger.error(f"数据生成过程中出错: {str(e)}", exc_info=True)
        
        if g_checkpoint_manager:
            g_checkpoint_manager.fail_run(str(e))
            g_logger.info("已保存错误状态，可以使用 --resume 参数从此处恢复")
        
        return 1
    
    finally:
        # 关闭数据库连接
        if 'db_manager' in locals():
            db_manager.disconnect()

if __name__ == "__main__":
    sys.exit(main())
