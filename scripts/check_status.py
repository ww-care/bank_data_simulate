#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
状态查看工具

用于检查数据生成的状态，查看或清除断点状态。
"""

import os
import sys
import time
import argparse
import json

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 导入项目模块
from src.database_manager import get_database_manager
from src.logger import get_logger

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='查看或清除数据生成状态')
    parser.add_argument('--clear-all', action='store_true', help='清除所有状态记录')
    parser.add_argument('--clear-last', action='store_true', help='只清除最近一条状态记录')
    parser.add_argument('--show-detail', action='store_true', help='显示详细状态信息')
    args = parser.parse_args()
    
    logger = get_logger('status_checker')
    db_manager = get_database_manager()
    
    # 检查表是否存在
    if not db_manager.table_exists('generation_status'):
        logger.error("状态表 generation_status 不存在")
        return 1
    
    # 清除所有状态
    if args.clear_all:
        db_manager.execute_update("DELETE FROM generation_status")
        logger.info("所有状态记录已清除")
        return 0
    
    # 清除最近一条状态
    if args.clear_last:
        query = """
        SELECT id FROM generation_status 
        ORDER BY last_update_time DESC LIMIT 1
        """
        result = db_manager.execute_query(query)
        if result:
            status_id = result[0]['id']
            db_manager.execute_update("DELETE FROM generation_status WHERE id = %s", (status_id,))
            logger.info(f"最近一条状态记录 {status_id} 已清除")
        else:
            logger.info("没有找到状态记录")
        return 0
    
    # 查询状态记录
    query = """
    SELECT * FROM generation_status 
    ORDER BY last_update_time DESC
    """
    results = db_manager.execute_query(query)
    
    if not results:
        logger.info("没有找到状态记录")
        return 0
    
    logger.info(f"共找到 {len(results)} 条状态记录")
    
    # 显示状态记录
    for i, row in enumerate(results):
        print(f"\n[记录 {i+1}]")
        print(f"ID: {row['id']}")
        print(f"运行ID: {row['run_id']}")
        print(f"开始时间: {row['start_time']}")
        print(f"最后更新时间: {row['last_update_time']}")
        print(f"当前阶段: {row['current_stage']}")
        print(f"状态: {row['status']}")
        
        # 解析已完成阶段
        if row['completed_stages']:
            try:
                completed_stages = json.loads(row['completed_stages'])
                print(f"已完成阶段: {', '.join(completed_stages)}")
                print(f"阶段完成数: {len(completed_stages)}")
            except:
                print(f"已完成阶段(原始数据): {row['completed_stages']}")
        else:
            print("已完成阶段: 无")
        
        print(f"阶段进度: {row['stage_progress']:.1f}%")
        
        # 显示详细信息
        if args.show_detail:
            print(f"详细信息: {row['details']}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
