#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调度管理脚本

负责按照设定的时间规则执行实时数据生成任务。
- 每天13点：生成当天0-12点数据
- 每天1点：生成前一天13-23点数据
"""

import os
import sys
import time
import signal
import subprocess
import schedule
import datetime
import argparse

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 导入项目模块
from src.logger import get_logger


# 全局变量
running = True
logger = None
realtime_script = os.path.join(current_dir, 'run_realtime_data.py')
config_dir = None
log_level = None


def signal_handler(sig, frame):
    """处理信号（用于优雅退出）"""
    global running
    if logger:
        logger.info("收到退出信号，调度器将在下一次循环结束后退出...")
    running = False


def run_realtime_data():
    """执行实时数据生成脚本"""
    try:
        cmd = [sys.executable, realtime_script]
        
        if config_dir:
            cmd.extend(['--config-dir', config_dir])
        
        if log_level:
            cmd.extend(['--log-level', log_level])
        
        if logger:
            logger.info(f"执行命令: {' '.join(cmd)}")
        
        # 使用subprocess执行脚本
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            if logger:
                logger.info("实时数据生成任务执行成功")
            if result.stdout:
                if logger:
                    logger.debug(f"输出: {result.stdout}")
        else:
            if logger:
                logger.error(f"实时数据生成任务执行失败，退出码: {result.returncode}")
                logger.error(f"错误信息: {result.stderr}")
    
    except Exception as e:
        if logger:
            logger.error(f"执行实时数据生成任务时出错: {str(e)}")


def main():
    """主函数"""
    global logger, config_dir, log_level
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='银行数据模拟系统调度管理器')
    parser.add_argument('--config-dir', type=str, help='配置文件目录路径')
    parser.add_argument('--log-level', type=str, default='info', 
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='日志级别')
    parser.add_argument('--test-run', action='store_true', 
                        help='测试模式，立即执行一次任务并退出')
    args = parser.parse_args()
    
    # 保存配置参数
    config_dir = args.config_dir
    log_level = args.log_level
    
    # 初始化日志
    logger = get_logger('scheduler', level=log_level)
    logger.info("调度管理器启动...")
    
    # 注册信号处理函数
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if args.test_run:
        logger.info("测试模式：立即执行一次任务并退出")
        run_realtime_data()
        return 0
    
    # 设置调度计划
    schedule.every().day.at("13:00").do(run_realtime_data)
    schedule.every().day.at("01:00").do(run_realtime_data)
    
    logger.info("调度计划已设置：每天13:00和01:00执行实时数据生成任务")
    
    # 计算下次执行时间
    next_run = schedule.next_run()
    if next_run:
        next_run_time = next_run.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"下次执行时间: {next_run_time}")
    
    # 主循环
    while running:
        schedule.run_pending()
        time.sleep(30)  # 每30秒检查一次
        
        # 重新计算并显示下次执行时间（每小时显示一次）
        current_minute = datetime.datetime.now().minute
        if current_minute == 0:  # 整点时显示
            next_run = schedule.next_run()
            if next_run:
                next_run_time = next_run.strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"下次执行时间: {next_run_time}")
    
    logger.info("调度管理器正在退出...")
    return 0


if __name__ == "__main__":
    sys.exit(main())