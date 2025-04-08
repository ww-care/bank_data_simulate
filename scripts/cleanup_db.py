#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库清理工具

用于清空或删除数据库中的表，方便从头开始生成数据。
"""

import os
import sys
import argparse

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 导入项目模块
from src.database_manager import get_database_manager
from src.logger import get_logger
from src.checkpoint_manager import get_checkpoint_manager

def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='数据库清理工具')
    parser.add_argument('--clear-all', action='store_true', help='清空所有表的数据')
    parser.add_argument('--drop-all', action='store_true', help='删除所有表')
    parser.add_argument('--table', type=str, help='指定要操作的表名')
    parser.add_argument('--clear-checkpoint', action='store_true', help='清除断点状态记录')
    parser.add_argument('--log-level', type=str, default='info', 
                       choices=['debug', 'info', 'warning', 'error', 'critical'],
                       help='日志级别')
    args = parser.parse_args()
    
    # 初始化日志
    logger = get_logger('db_cleanup', level=args.log_level)
    
    # 初始化数据库管理器
    db_manager = get_database_manager()
    
    # 获取表列表
    tables = []
    if args.table:
        tables = [args.table]
    else:
        # 获取所有表
        try:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name NOT LIKE 'generation_status'  -- 排除状态表
            """
            results = db_manager.execute_query(query)
            tables = [row['table_name'] for row in results]
        except Exception as e:
            logger.error(f"获取表列表失败: {str(e)}")
            return 1
    
    # 操作表
    if args.clear_all or args.drop_all:
        if not tables:
            logger.warning("没有找到可操作的表")
            return 0
        
        for table in tables:
            if args.drop_all:
                logger.info(f"正在删除表 {table}...")
                try:
                    db_manager.execute_update(f"DROP TABLE IF EXISTS {table}")
                    logger.info(f"表 {table} 已删除")
                except Exception as e:
                    logger.error(f"删除表 {table} 失败: {str(e)}")
            elif args.clear_all:
                logger.info(f"正在清空表 {table}...")
                try:
                    db_manager.execute_update(f"TRUNCATE TABLE {table}")
                    logger.info(f"表 {table} 已清空")
                except Exception as e:
                    logger.error(f"清空表 {table} 失败: {str(e)}")
    
    # 清除断点状态
    if args.clear_checkpoint:
        logger.info("正在清除断点状态记录...")
        try:
            db_manager.execute_update("DELETE FROM generation_status")
            logger.info("断点状态记录已清除")
        except Exception as e:
            logger.error(f"清除断点状态记录失败: {str(e)}")
    
    # 如果没有指定任何操作，显示帮助信息
    if not any([args.clear_all, args.drop_all, args.clear_checkpoint]):
        parser.print_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
