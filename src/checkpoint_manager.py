#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
断点续传状态管理模块

负责跟踪数据生成过程的状态，支持中断和恢复。
"""

import os
import uuid
import json
import datetime
from typing import List, Dict, Any, Optional

class CheckpointManager:
    """断点续传状态管理器"""
    
    def __init__(self, db_manager, logger=None):
        """
        初始化断点管理器
        
        Args:
            db_manager: 数据库管理器实例
            logger: 日志管理器实例
        """
        self.db_manager = db_manager
        self.logger = logger
        
        # 确保状态表存在
        self._ensure_status_table()
        
        # 当前运行ID
        self.run_id = f"RUN_{uuid.uuid4().hex[:8]}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # 状态ID
        self.status_id = f"STATUS_{uuid.uuid4().hex}"
        
        # 所有可能的生成阶段
        self.all_stages = [
            'bank_manager', 'deposit_type', 'product', 'customer', 
            'fund_account', 'app_user', 'wechat_follower', 'work_wechat_contact',
            'channel_profile', 'loan_record', 'investment_record', 
            'customer_event', 'transaction'
        ]
        
        # 已完成的阶段
        self.completed_stages = []
        
        # 当前阶段
        self.current_stage = None
        
        # 当前阶段进度 (0-100)
        self.stage_progress = 0
        
        # 当前状态
        self.status = "initialized"  # initialized, running, paused, completed, failed
        
        # 开始时间
        self.start_time = datetime.datetime.now()
        
        # 最后更新时间
        self.last_update_time = self.start_time
        
        # 详情信息
        self.details = "初始化状态"
    
    def _ensure_status_table(self):
        """确保状态表存在"""
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
        try:
            self.db_manager.execute_update(create_table_sql)
            if self.logger:
                self.logger.info("状态表检查或创建完成")
        except Exception as e:
            if self.logger:
                self.logger.error(f"创建状态表失败: {str(e)}")
            raise
    
    def initialize_run(self, skip_stages: List[str] = None) -> str:
        """
        初始化一次新的运行
        
        Args:
            skip_stages: 要跳过的阶段列表
            
        Returns:
            运行ID
        """
        self.completed_stages = skip_stages or []
        self.status = "initialized"
        self.details = f"初始化完成，跳过阶段: {', '.join(self.completed_stages) if self.completed_stages else '无'}"
        
        # 保存初始状态
        self._save_status()
        
        if self.logger:
            self.logger.info(f"初始化运行: {self.run_id}, 状态ID: {self.status_id}")
        
        return self.run_id
    
    def resume_from_last(self) -> Optional[Dict[str, Any]]:
        """
        从最后一次运行状态恢复
        
        Returns:
            上次运行的状态信息，如果没有则返回None
        """
        # 查询最后一次运行状态
        query = """
        SELECT * FROM generation_status 
        WHERE status IN ('paused', 'running', 'failed') 
        ORDER BY last_update_time DESC LIMIT 1
        """
        
        try:
            results = self.db_manager.execute_query(query)
            if not results:
                if self.logger:
                    self.logger.info("没有找到可恢复的运行状态")
                return None
            
            last_status = results[0]
            
            # 更新当前状态
            self.status_id = last_status['id']
            self.run_id = last_status['run_id']
            self.start_time = last_status['start_time']
            self.current_stage = last_status['current_stage']
            self.stage_progress = last_status['stage_progress']
            
            # 解析已完成阶段
            if last_status['completed_stages']:
                self.completed_stages = json.loads(last_status['completed_stages'])
            else:
                self.completed_stages = []
            
            self.status = "running"  # 恢复为运行状态
            self.details = f"从上次状态恢复: {last_status['details']}"
            
            # 更新状态
            self._save_status()
            
            if self.logger:
                self.logger.info(f"已恢复运行: {self.run_id}, 当前阶段: {self.current_stage}, 进度: {self.stage_progress:.1f}%")
            
            return {
                'run_id': self.run_id,
                'current_stage': self.current_stage,
                'completed_stages': self.completed_stages,
                'stage_progress': self.stage_progress,
                'details': last_status['details']
            }
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"恢复上次运行状态失败: {str(e)}")
            return None
    
    def start_stage(self, stage: str) -> bool:
        """
        开始一个新阶段
        
        Args:
            stage: 阶段名称
            
        Returns:
            是否成功开始
        """
        if stage in self.completed_stages:
            if self.logger:
                self.logger.info(f"阶段 {stage} 已完成，跳过")
            return False
        
        self.current_stage = stage
        self.stage_progress = 0
        self.status = "running"
        self.details = f"开始阶段: {stage}"
        self.last_update_time = datetime.datetime.now()
        
        # 保存状态
        self._save_status()
        
        if self.logger:
            self.logger.info(f"开始阶段: {stage}")
        
        return True
    
    def update_progress(self, progress: float, details: str = None) -> None:
        """
        更新当前阶段的进度
        
        Args:
            progress: 进度百分比 (0-100)
            details: 详情信息
        """
        if not self.current_stage:
            return
        
        self.stage_progress = progress
        if details:
            self.details = details
        self.last_update_time = datetime.datetime.now()
        
        # 每10%更新一次数据库，避免频繁写入
        if int(progress) % 10 == 0 or progress >= 99.9:
            self._save_status()
            
        if self.logger and int(progress) % 10 == 0:
            self.logger.info(f"阶段 {self.current_stage} 进度: {progress:.1f}%")
    
    def complete_stage(self, stage: str) -> None:
        """
        完成一个阶段
        
        Args:
            stage: 阶段名称
        """
        if stage != self.current_stage:
            if self.logger:
                self.logger.warning(f"当前阶段 {self.current_stage} 与完成阶段 {stage} 不一致")
        
        if stage not in self.completed_stages:
            self.completed_stages.append(stage)
        
        self.stage_progress = 100
        self.details = f"阶段 {stage} 已完成"
        self.last_update_time = datetime.datetime.now()
        
        # 保存状态
        self._save_status()
        
        if self.logger:
            self.logger.info(f"阶段 {stage} 已完成")
    
    def pause_run(self, reason: str = None) -> None:
        """
        暂停当前运行
        
        Args:
            reason: 暂停原因
        """
        self.status = "paused"
        self.details = f"暂停运行: {reason}" if reason else "暂停运行"
        self.last_update_time = datetime.datetime.now()
        
        # 保存状态
        self._save_status()
        
        if self.logger:
            self.logger.info(f"暂停运行: {reason}" if reason else "暂停运行")
    
    def complete_run(self) -> None:
        """完成整个运行"""
        self.status = "completed"
        self.details = "全部阶段已完成"
        self.last_update_time = datetime.datetime.now()
        
        # 保存状态
        self._save_status()
        
        if self.logger:
            self.logger.info("全部阶段已完成")
    
    def fail_run(self, error: str) -> None:
        """
        标记运行失败
        
        Args:
            error: 错误信息
        """
        self.status = "failed"
        self.details = f"运行失败: {error}"
        self.last_update_time = datetime.datetime.now()
        
        # 保存状态
        self._save_status()
        
        if self.logger:
            self.logger.error(f"运行失败: {error}")
    
    def should_skip_stage(self, stage: str) -> bool:
        """
        检查是否应该跳过某个阶段
        
        Args:
            stage: 阶段名称
            
        Returns:
            是否应该跳过
        """
        return stage in self.completed_stages
    
    def get_next_stage(self) -> Optional[str]:
        """
        获取下一个应该执行的阶段
        
        Returns:
            下一个阶段名称，如果全部完成则返回None
        """
        for stage in self.all_stages:
            if stage not in self.completed_stages:
                return stage
        return None
    
    def _save_status(self) -> None:
        """保存当前状态到数据库"""
        try:
            # 准备数据
            completed_stages_json = json.dumps(self.completed_stages)
            
            # 检查记录是否存在
            check_sql = "SELECT id FROM generation_status WHERE id = %s"
            result = self.db_manager.execute_query(check_sql, (self.status_id,))
            
            if result:
                # 更新现有记录
                update_sql = """
                UPDATE generation_status 
                SET run_id = %s, last_update_time = %s, current_stage = %s, 
                    completed_stages = %s, stage_progress = %s, status = %s, details = %s 
                WHERE id = %s
                """
                self.db_manager.execute_update(
                    update_sql, 
                    (self.run_id, self.last_update_time, self.current_stage, 
                     completed_stages_json, self.stage_progress, self.status, 
                     self.details, self.status_id)
                )
            else:
                # 插入新记录
                insert_sql = """
                INSERT INTO generation_status 
                (id, run_id, start_time, last_update_time, current_stage, 
                 completed_stages, stage_progress, status, details) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.db_manager.execute_update(
                    insert_sql, 
                    (self.status_id, self.run_id, self.start_time, self.last_update_time, 
                     self.current_stage, completed_stages_json, self.stage_progress, 
                     self.status, self.details)
                )
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"保存状态失败: {str(e)}")


# 单例模式
_checkpoint_manager_instance = None

def get_checkpoint_manager(db_manager=None, logger=None):
    """
    获取CheckpointManager的单例实例
    
    Args:
        db_manager: 数据库管理器实例
        logger: 日志管理器实例
        
    Returns:
        CheckpointManager实例
    """
    global _checkpoint_manager_instance
    if _checkpoint_manager_instance is None:
        if db_manager is None:
            from src.database_manager import get_database_manager
            db_manager = get_database_manager()
        
        if logger is None:
            from src.logger import get_logger
            logger = get_logger('checkpoint_manager')
            
        _checkpoint_manager_instance = CheckpointManager(db_manager, logger)
    
    return _checkpoint_manager_instance