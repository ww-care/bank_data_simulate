#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
配置文件检查脚本
"""

import os
import sys
import json

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 导入项目模块
from src.config_manager import get_config_manager
from src.logger import get_logger

def main():
    logger = get_logger('config_check', level='debug')
    logger.info("正在检查配置文件...")
    
    try:
        # 初始化配置管理器
        config_manager = get_config_manager()
        
        # 读取数据库配置
        db_config = config_manager.read_db_config()
        logger.info(f"数据库配置已加载: {len(db_config)} 个配置节")
        
        # 读取数据生成规则配置
        data_generation_config = config_manager.read_data_generation_config()
        logger.info(f"数据生成规则配置已加载: {len(data_generation_config)} 个配置节")
        
        # 打印主要配置项
        system_config = config_manager.get_system_config()
        logger.info(f"系统配置: {json.dumps(system_config, ensure_ascii=False, indent=2)}")
        
        customer_config = config_manager.get_entity_config('customer')
        logger.info(f"客户配置: 计划生成 {customer_config.get('total_count', 'unknown')} 个客户")
        
        # 检查所有需要的配置节点是否存在
        required_entities = [
            'customer', 'account', 'transaction', 'loan', 'investment',
            'app_user', 'wechat_follower', 'channel_profile', 'seasonal_cycle'
        ]
        
        missing_entities = []
        for entity in required_entities:
            try:
                config_manager.get_entity_config(entity)
            except ValueError:
                missing_entities.append(entity)
        
        if missing_entities:
            logger.warning(f"配置文件中缺少以下实体的配置: {', '.join(missing_entities)}")
        else:
            logger.info("所有必需的配置节点均已存在")
        
        logger.info("配置文件检查完成")
        return True
        
    except Exception as e:
        logger.error(f"检查配置文件时出错: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
