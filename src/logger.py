#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志管理模块

提供统一的日志记录功能，支持日志分级、文件输出和控制台输出。
"""

import os
import logging
import datetime
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler


class Logger:
    """日志管理类，提供日志记录功能"""
    
    # 日志级别映射
    LEVELS = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }
    
    def __init__(self, name: str = 'bank_data_simulation', log_dir: str = None, 
                 level: str = 'info', console_output: bool = True, file_output: bool = True,
                 max_bytes: int = 10485760, backup_count: int = 10):
        """
        初始化日志管理器
        
        Args:
            name: 日志名称
            log_dir: 日志文件目录，默认为项目根目录下的logs文件夹
            level: 日志级别，可选值为debug, info, warning, error, critical
            console_output: 是否输出到控制台
            file_output: 是否输出到文件
            max_bytes: 单个日志文件最大字节数，默认为10MB
            backup_count: 备份文件数量，默认为10个
        """
        self.name = name
        self.level = self.LEVELS.get(level.lower(), logging.INFO)
        self.console_output = console_output
        self.file_output = file_output
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        
        # 如果未指定日志目录，则使用默认目录
        if log_dir is None:
            # 获取当前文件的绝对路径
            current_file = os.path.abspath(__file__)
            # 获取src目录的父目录（项目根目录）
            project_root = os.path.dirname(os.path.dirname(current_file))
            # 日志目录
            self.log_dir = os.path.join(project_root, 'logs')
        else:
            self.log_dir = log_dir
        
        # 确保日志目录存在
        os.makedirs(self.log_dir, exist_ok=True)
        
        # 创建Logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.level)
        self.logger.propagate = False
        
        # 清理已存在的处理器
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # 添加控制台输出
        if self.console_output:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.level)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
        
        # 添加文件输出
        if self.file_output:
            # 日志文件名：name_date.log
            today = datetime.datetime.now().strftime('%Y%m%d')
            log_file = os.path.join(self.log_dir, f"{name}_{today}.log")
            
            # 使用RotatingFileHandler进行日志轮转
            file_handler = RotatingFileHandler(
                log_file, maxBytes=self.max_bytes, backupCount=self.backup_count, encoding='utf-8')
            file_handler.setLevel(self.level)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def get_logger(self) -> logging.Logger:
        """
        获取logger实例
        
        Returns:
            logging.Logger实例
        """
        return self.logger
    
    def debug(self, message: str, *args, **kwargs) -> None:
        """
        记录debug级别日志
        
        Args:
            message: 日志消息
            *args, **kwargs: 传递给logger.debug的其他参数
        """
        self.logger.debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs) -> None:
        """
        记录info级别日志
        
        Args:
            message: 日志消息
            *args, **kwargs: 传递给logger.info的其他参数
        """
        self.logger.info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs) -> None:
        """
        记录warning级别日志
        
        Args:
            message: 日志消息
            *args, **kwargs: 传递给logger.warning的其他参数
        """
        self.logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs) -> None:
        """
        记录error级别日志
        
        Args:
            message: 日志消息
            *args, **kwargs: 传递给logger.error的其他参数
        """
        self.logger.error(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs) -> None:
        """
        记录critical级别日志
        
        Args:
            message: 日志消息
            *args, **kwargs: 传递给logger.critical的其他参数
        """
        self.logger.critical(message, *args, **kwargs)
    
    def set_level(self, level: str) -> None:
        """
        设置日志级别
        
        Args:
            level: 日志级别，可选值为debug, info, warning, error, critical
        """
        if level.lower() in self.LEVELS:
            self.level = self.LEVELS[level.lower()]
            self.logger.setLevel(self.level)
            for handler in self.logger.handlers:
                handler.setLevel(self.level)
    
    def archive_logs(self, days: int = 30) -> int:
        """
        归档指定天数前的日志文件
        
        Args:
            days: 归档多少天前的日志，默认为30天
            
        Returns:
            归档的文件数量
        """
        archive_dir = os.path.join(self.log_dir, 'archive')
        os.makedirs(archive_dir, exist_ok=True)
        
        # 计算截止日期
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
        cutoff_date_str = cutoff_date.strftime('%Y%m%d')
        
        # 遍历日志目录
        count = 0
        for filename in os.listdir(self.log_dir):
            if not os.path.isfile(os.path.join(self.log_dir, filename)):
                continue
            
            # 检查是否是日志文件且日期早于截止日期
            parts = filename.split('_')
            if len(parts) >= 2 and parts[0] == self.name and parts[1].endswith('.log'):
                file_date = parts[1].replace('.log', '')
                if file_date < cutoff_date_str:
                    # 移动到归档目录
                    src_path = os.path.join(self.log_dir, filename)
                    dst_path = os.path.join(archive_dir, filename)
                    os.rename(src_path, dst_path)
                    count += 1
        
        self.info(f"已归档 {count} 个日志文件到 {archive_dir}")
        return count


# 单例模式
_instances: Dict[str, Logger] = {}

def get_logger(name: str = 'bank_data_simulation', log_dir: Optional[str] = None, 
               level: str = 'info', console_output: bool = True, 
               file_output: bool = True) -> Logger:
    """
    获取Logger的单例实例
    
    Args:
        name: 日志名称
        log_dir: 日志文件目录
        level: 日志级别
        console_output: 是否输出到控制台
        file_output: 是否输出到文件
        
    Returns:
        Logger实例
    """
    global _instances
    if name not in _instances:
        _instances[name] = Logger(name, log_dir, level, console_output, file_output)
    return _instances[name]


if __name__ == "__main__":
    # 简单测试
    logger = get_logger(level='debug')
    logger.debug("这是一条调试日志")
    logger.info("这是一条信息日志")
    logger.warning("这是一条警告日志")
    logger.error("这是一条错误日志")
    logger.critical("这是一条严重错误日志")