#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库连接测试脚本
"""

import os
import sys

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 导入项目模块
from src.config_manager import get_config_manager
from src.database_manager import get_database_manager
from src.logger import get_logger

def main():
    logger = get_logger('db_check', level='debug')
    logger.info("正在检查数据库连接...")
    
    try:
        # 读取配置
        config_manager = get_config_manager()
        db_config = config_manager.get_db_config()
        logger.info(f"数据库配置: {db_config}")
        
        # 测试数据库连接
        db_manager = get_database_manager()
        connected = db_manager.connect()
        
        if connected:
            logger.info("数据库连接成功!")
            
            # 检查数据库表
            tables_result = db_manager.execute_query("SHOW TABLES")
            if tables_result:
                logger.info("数据库表列表:")
                for table in tables_result:
                    table_name = list(table.values())[0]
                    logger.info(f"  - {table_name}")
            else:
                logger.info("数据库中没有表")
                
            # 创建表
            logger.info("尝试创建数据库表...")
            tables_created = db_manager.create_tables()
            logger.info(f"表创建结果: {'成功' if tables_created else '失败'}")
            
            # 再次获取表列表
            tables_result = db_manager.execute_query("SHOW TABLES")
            logger.info("创建后的数据库表列表:")
            for table in tables_result:
                table_name = list(table.values())[0]
                logger.info(f"  - {table_name}")
                
                # 获取表中的记录数
                count_result = db_manager.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
                record_count = count_result[0]['count']
                logger.info(f"    记录数: {record_count}")
            
            db_manager.disconnect()
            return True
        else:
            logger.error("数据库连接失败!")
            return False
    
    except Exception as e:
        logger.error(f"测试数据库连接时出错: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
