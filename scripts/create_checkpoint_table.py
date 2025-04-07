#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
创建数据生成断点状态表

用于创建追踪数据生成进度的状态表。
"""

import os
import sys

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 导入项目模块
from src.database_manager import get_database_manager
from src.logger import get_logger

def create_checkpoint_table():
    """创建断点状态表"""
    logger = get_logger('setup')
    db_manager = get_database_manager()
    
    try:
        # 创建断点状态表
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS generation_status (
            id VARCHAR(50) PRIMARY KEY,
            run_id VARCHAR(50) NOT NULL,
            start_time DATETIME NOT NULL,
            last_update_time DATETIME NOT NULL,
            current_stage VARCHAR(50),
            completed_stages TEXT,
            stage_progress FLOAT DEFAULT 0,
            status VARCHAR(20) NOT NULL,
            details TEXT
        );
        """
        
        db_manager.execute_query(create_table_sql, fetch=False)
        logger.info("断点状态表创建成功")
        
        return True
    except Exception as e:
        logger.error(f"创建断点状态表失败: {str(e)}")
        return False
    finally:
        db_manager.disconnect()

if __name__ == "__main__":
    success = create_checkpoint_table()
    sys.exit(0 if success else 1)
