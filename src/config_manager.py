#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
配置管理模块

负责读取和管理系统配置文件，包括数据库连接配置和数据生成规则配置。
"""

import os
import yaml
import configparser
from typing import Dict, Any, Optional


class ConfigManager:
    """配置管理类，负责读取和解析配置文件"""

    def __init__(self, config_dir: str = None):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件目录，默认为项目根目录下的config文件夹
        """
        # 如果未指定配置目录，则使用默认目录
        if config_dir is None:
            # 获取当前文件的绝对路径
            current_file = os.path.abspath(__file__)
            # 获取src目录的父目录（项目根目录）
            project_root = os.path.dirname(os.path.dirname(current_file))
            # 配置文件目录
            self.config_dir = os.path.join(project_root, 'config')
        else:
            self.config_dir = config_dir
        
        # 数据库配置文件路径
        self.db_config_path = os.path.join(self.config_dir, 'database.ini')
        # 数据生成规则配置文件路径
        self.data_config_path = os.path.join(self.config_dir, 'bank_data_simulation_config.yaml')
        
        # 存储已加载的配置
        self._db_config = None
        self._data_generation_config = None
    
    def read_db_config(self) -> Dict[str, Dict[str, str]]:
        """
        读取数据库配置文件
        
        Returns:
            包含数据库连接信息的字典
        """
        if self._db_config is not None:
            return self._db_config
        
        if not os.path.exists(self.db_config_path):
            raise FileNotFoundError(f"数据库配置文件不存在: {self.db_config_path}")
        
        config = configparser.ConfigParser()
        config.read(self.db_config_path, encoding='utf-8')
        
        # 转换为字典格式
        self._db_config = {section: dict(config[section]) for section in config.sections()}
        return self._db_config
    
    def get_db_config(self, db_type: str = 'mysql') -> Dict[str, str]:
        """
        获取指定类型的数据库配置
        
        Args:
            db_type: 数据库类型，默认为mysql
            
        Returns:
            包含指定数据库连接信息的字典
        """
        db_config = self.read_db_config()
        
        if db_type not in db_config:
            raise ValueError(f"未找到数据库类型 '{db_type}' 的配置信息")
        
        return db_config[db_type]
    
    def read_data_generation_config(self) -> Dict[str, Any]:
        """
        读取数据生成规则配置文件
        
        Returns:
            包含数据生成规则的字典
        """
        if self._data_generation_config is not None:
            return self._data_generation_config
        
        if not os.path.exists(self.data_config_path):
            raise FileNotFoundError(f"数据生成规则配置文件不存在: {self.data_config_path}")
        
        with open(self.data_config_path, 'r', encoding='utf-8') as f:
            self._data_generation_config = yaml.safe_load(f)
        
        return self._data_generation_config
    
    def get_system_config(self) -> Dict[str, Any]:
        """
        获取系统基础配置
        
        Returns:
            系统配置字典
        """
        config = self.read_data_generation_config()
        return config.get('system', {})
    
    def get_entity_config(self, entity_name: str) -> Dict[str, Any]:
        """
        获取指定实体的配置
        
        Args:
            entity_name: 实体名称，如'customer', 'account'等
            
        Returns:
            指定实体的配置字典
        """
        config = self.read_data_generation_config()
        
        if entity_name not in config:
            raise ValueError(f"未找到实体 '{entity_name}' 的配置信息")
        
        return config[entity_name]
    
    def save_config(self, config_data: Dict[str, Any], file_name: str) -> bool:
        """
        保存配置到文件
        
        Args:
            config_data: 要保存的配置数据
            file_name: 配置文件名
            
        Returns:
            保存是否成功
        """
        file_path = os.path.join(self.config_dir, file_name)
        
        # 确保目录存在
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # 根据文件扩展名选择保存方式
        if file_name.endswith('.yaml') or file_name.endswith('.yml'):
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
        elif file_name.endswith('.ini'):
            config = configparser.ConfigParser()
            for section, section_data in config_data.items():
                config[section] = section_data
            with open(file_path, 'w', encoding='utf-8') as f:
                config.write(f)
        else:
            raise ValueError(f"不支持的配置文件格式: {file_name}")
        
        return True
    
    def update_entity_config(self, entity_name: str, new_config: Dict[str, Any]) -> bool:
        """
        更新指定实体的配置
        
        Args:
            entity_name: 实体名称
            new_config: 新的配置数据
            
        Returns:
            更新是否成功
        """
        config = self.read_data_generation_config()
        
        if entity_name not in config:
            config[entity_name] = {}
        
        # 递归更新配置
        def update_dict(d, u):
            for k, v in u.items():
                if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                    update_dict(d[k], v)
                else:
                    d[k] = v
        
        update_dict(config[entity_name], new_config)
        
        # 保存更新后的配置
        self._data_generation_config = config
        return self.save_config(config, os.path.basename(self.data_config_path))


# 单例模式
_instance = None

def get_config_manager(config_dir: Optional[str] = None) -> ConfigManager:
    """
    获取ConfigManager的单例实例
    
    Args:
        config_dir: 配置文件目录
        
    Returns:
        ConfigManager实例
    """
    global _instance
    if _instance is None:
        _instance = ConfigManager(config_dir)
    return _instance


if __name__ == "__main__":
    # 简单测试
    config_manager = get_config_manager()
    try:
        db_config = config_manager.get_db_config()
        print("数据库配置:", db_config)
    except FileNotFoundError:
        print("数据库配置文件不存在，需要先创建配置文件")
    
    try:
        system_config = config_manager.get_system_config()
        print("系统配置:", system_config)
    except FileNotFoundError:
        print("数据生成规则配置文件不存在，需要先创建配置文件")