#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库清理脚本

清理所有表中的数据，以便重新导入
"""

import os
import sys

# 添加项目根目录到系统路径
current_file = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
sys.path.append(project_root)

# 导入数据库管理器
from src.database_manager import get_database_manager

def clean_database():
    """清理数据库中的所有表数据"""
    db_manager = get_database_manager()
    
    # 连接数据库
    if not db_manager.connect():
        print("数据库连接失败，无法清理数据")
        return False
    
    try:
        # 需要按照外键依赖关系的反序清理表
        # 先禁用外键检查
        db_manager.execute_update("SET FOREIGN_KEY_CHECKS = 0")
        
        # 定义表清理顺序（子表在前，父表在后）
        tables_to_clean = [
            'account_transaction',      # 账户交易记录
            'loan_record',              # 借款记录
            'investment_record',        # 理财记录
            'customer_event',           # 客户事件
            'app_user',                 # APP用户
            'wechat_follower',          # 公众号粉丝
            'work_wechat_contact',      # 企业微信联系人
            'channel_profile',          # 全渠道档案
            'fund_account',             # 资金账户
            'customer',                 # 客户
            'bank_manager',             # 银行经理
            'product',                  # 产品
            'deposit_type',             # 存款类型
            'data_generation_log'       # 数据生成日志
        ]
        
        # 清空表数据
        for table in tables_to_clean:
            print(f"正在清理表: {table}")
            success = db_manager.truncate_table(table)
            if success:
                print(f"  表 {table} 清理成功")
            else:
                print(f"  表 {table} 清理失败或不存在")
        
        # 重新启用外键检查
        db_manager.execute_update("SET FOREIGN_KEY_CHECKS = 1")
        
        print("所有表数据清理完成")
        return True
        
    except Exception as e:
        print(f"清理数据库时出错: {str(e)}")
        # 确保重新启用外键检查
        db_manager.execute_update("SET FOREIGN_KEY_CHECKS = 1")
        return False
    
    finally:
        # 关闭数据库连接
        db_manager.disconnect()


def clean_and_rebuild_database():
    """清理并重建数据库表结构"""
    db_manager = get_database_manager()
    
    # 连接数据库
    if not db_manager.connect():
        print("数据库连接失败，无法重建表结构")
        return False
    
    try:
        # 需要按照外键依赖关系的反序删除表
        # 先禁用外键检查
        db_manager.execute_update("SET FOREIGN_KEY_CHECKS = 0")
        
        # 定义表删除顺序（子表在前，父表在后）
        tables_to_drop = [
            'account_transaction',
            'loan_record',
            'investment_record',
            'customer_event',
            'app_user',
            'wechat_follower',
            'work_wechat_contact',
            'channel_profile',
            'fund_account',
            'customer',
            'bank_manager',
            'product',
            'deposit_type',
            'data_generation_log'
        ]
        
        # 删除表
        for table in tables_to_drop:
            print(f"正在删除表: {table}")
            success = db_manager.drop_table(table)
            if success:
                print(f"  表 {table} 删除成功")
            else:
                print(f"  表 {table} 删除失败或不存在")
        
        # 重新启用外键检查
        db_manager.execute_update("SET FOREIGN_KEY_CHECKS = 1")
        
        # 重新创建所有表
        print("\n开始重新创建表结构...")
        if db_manager.create_tables():
            print("所有表结构重建成功")
        else:
            print("部分表结构重建失败，请检查日志")
        
        return True
        
    except Exception as e:
        print(f"重建数据库时出错: {str(e)}")
        # 确保重新启用外键检查
        db_manager.execute_update("SET FOREIGN_KEY_CHECKS = 1")
        return False
    
    finally:
        # 关闭数据库连接
        db_manager.disconnect()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='数据库清理工具')
    parser.add_argument('--rebuild', action='store_true', help='是否重建表结构(drop并重新create)')
    args = parser.parse_args()
    
    if args.rebuild:
        print("=== 开始清理并重建数据库表结构 ===")
        clean_and_rebuild_database()
    else:
        print("=== 开始清理数据库数据 ===")
        clean_database()
    
    print("操作完成")
