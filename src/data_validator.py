#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据验证模块

负责验证生成的数据的完整性、唯一性和类型一致性。
"""

import os
import json
import datetime
import logging
from typing import Dict, List, Tuple, Set, Any, Optional, Union

class DataValidator:
    """数据验证类，专注于验证数据完整性、唯一性和类型一致性"""
    
    def __init__(self, logger=None, log_dir="logs"):
        """
        初始化数据验证器
        
        Args:
            logger: 日志记录器，如果为None则创建新的记录器
            log_dir: 日志文件目录
        """
        self.logger = logger or logging.getLogger("data_validator")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        self.log_dir = log_dir
        
        # 确保日志目录存在
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 存储验证结果
        self.validation_results = {}
        self.error_counts = {}
        self.warnings = {}
    
    def validate(self, data_cache: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        验证所有生成的数据的完整性、唯一性和类型一致性
        
        Args:
            data_cache: 包含所有生成数据的字典，键为实体类型，值为数据列表
            
        Returns:
            验证结果摘要
        """
        self.logger.info("开始验证数据...")
        self.validation_results = {}
        self.error_counts = {}
        self.warnings = {}
        
        # 验证数据完整性（必填字段）
        completeness_result = self._validate_data_completeness(data_cache)
        self.validation_results["data_completeness"] = completeness_result
        
        # 验证数据唯一性（ID不重复）
        uniqueness_result = self._validate_data_uniqueness(data_cache)
        self.validation_results["data_uniqueness"] = uniqueness_result
        
        # 验证数据类型一致性
        type_result = self._validate_data_types(data_cache)
        self.validation_results["data_types"] = type_result
        
        # 验证外键有效性
        foreign_key_result = self._validate_foreign_keys(data_cache)
        self.validation_results["foreign_keys"] = foreign_key_result
        
        # 验证时间顺序逻辑
        time_sequence_result = self._validate_time_sequence(data_cache)
        self.validation_results["time_sequence"] = time_sequence_result
        
        # 验证总体数据量
        data_volume_result = self._validate_data_volume(data_cache)
        self.validation_results["data_volume"] = data_volume_result
        
        # 计算总体验证结果
        total_errors = sum(self.error_counts.values())
        total_warnings = sum(len(warnings) for warnings in self.warnings.values())
        
        # 当前时间戳
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 生成验证摘要
        validation_summary = {
            "status": "success" if total_errors == 0 else "failed",
            "total_errors": total_errors,
            "total_warnings": total_warnings,
            "error_counts": self.error_counts,
            "warnings_counts": {k: len(v) for k, v in self.warnings.items()},
            "entity_stats": {entity: len(data) for entity, data in data_cache.items()},
            "validation_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "detail_results": self.validation_results
        }
        
        # 写入结果到JSON文件
        results_file = os.path.join(self.log_dir, f"validation_results_{timestamp}.json")
        self._write_results_to_file(validation_summary, results_file)
        
        self.logger.info(f"数据验证完成。错误: {total_errors}, 警告: {total_warnings}")
        self.logger.info(f"详细验证结果已保存到: {results_file}")
        
        return validation_summary
    
    def _write_results_to_file(self, results: Dict[str, Any], file_path: str):
        """
        将验证结果写入JSON文件
        
        Args:
            results: 验证结果
            file_path: 输出文件路径
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            self.logger.info(f"验证结果已保存到: {file_path}")
        except Exception as e:
            self.logger.error(f"保存验证结果到文件时出错: {str(e)}")
    
    def _parse_date(self, date_str: str) -> datetime.date:
        """解析日期字符串为日期对象"""
        if not date_str:
            raise ValueError("日期字符串为空")
            
        # 尝试不同格式的日期解析
        formats = [
            '%Y-%m-%d',        # 2021-01-01
            '%Y/%m/%d',        # 2021/01/01
            '%Y-%m-%d %H:%M:%S', # 2021-01-01 12:34:56
            '%Y/%m/%d %H:%M:%S'  # 2021/01/01 12:34:56
        ]
        
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(date_str, fmt)
                return dt.date() if '%H' not in fmt else dt
            except ValueError:
                continue
                
        raise ValueError(f"无法解析日期字符串: {date_str}")
    
    def _validate_data_completeness(self, data_cache: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """验证数据完整性（必填字段不为空）"""
        result = {"status": "success", "errors": [], "warnings": []}
        error_count = 0
        
        # 各实体的必填字段
        required_fields = {
            "customer": ["customer_id", "name", "id_type", "id_number", "customer_type", "registration_date"],
            "bank_manager": ["manager_id", "name", "branch_id"],
            "product": ["product_id", "name", "type"],
            "deposit_type": ["deposit_type_id", "name", "base_interest_rate"],
            "fund_account": ["account_id", "customer_id", "account_type", "status", "opening_date", "balance"],
            "account_transaction": ["transaction_id", "account_id", "amount", "transaction_datetime", "transaction_type"],
            "loan_record": ["loan_id", "customer_id", "account_id", "loan_type", "loan_amount", "application_date"],
            "investment_record": ["investment_id", "customer_id", "account_id", "product_id", "amount", "purchase_date"],
            "app_user": ["app_user_id", "customer_id", "registration_date", "device_os"],
            "wechat_follower": ["follower_id", "customer_id", "follow_date", "interaction_level"],
            "work_wechat_contact": ["contact_id", "customer_id", "manager_id", "add_date"],
            "channel_profile": ["profile_id", "customer_id", "channels_used", "primary_channel"],
            "customer_event": ["event_id", "customer_id", "event_type", "event_datetime"]
        }
        
        # 验证每个实体的必填字段
        for entity, fields in required_fields.items():
            if entity not in data_cache:
                continue
                
            data_list = data_cache[entity]
            missing_fields = {}
            
            for i, data in enumerate(data_list):
                for field in fields:
                    if field not in data or data[field] is None or data[field] == "":
                        if i not in missing_fields:
                            missing_fields[i] = []
                        missing_fields[i].append(field)
            
            if missing_fields:
                error_count += len(missing_fields)
                error_msg = f"实体类型 {entity} 中有 {len(missing_fields)} 条记录缺少必填字段"
                result["errors"].append(error_msg)
                
                # 显示前5个异常记录
                for i, fields in list(missing_fields.items())[:5]:
                    id_field = next((f for f in data_list[i].keys() if 'id' in f.lower()), None)
                    data_id = data_list[i].get(id_field, "unknown") if id_field else f"记录索引{i}"
                    result["errors"].append(f"  - 记录ID={data_id}, 缺少字段: {', '.join(fields)}")
                
                self.logger.error(error_msg)
        
        if error_count > 0:
            result["status"] = "failed"
            self.error_counts["data_completeness"] = error_count
        
        return result
    
    def _validate_data_uniqueness(self, data_cache: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """验证数据唯一性（ID不重复）"""
        result = {"status": "success", "errors": [], "warnings": []}
        error_count = 0
        
        # 各实体类型的ID字段名
        id_fields = {
            "customer": "customer_id",
            "bank_manager": "manager_id",
            "product": "product_id",
            "deposit_type": "deposit_type_id",
            "fund_account": "account_id",
            "account_transaction": "transaction_id",
            "loan_record": "loan_id",
            "investment_record": "investment_id",
            "app_user": "app_user_id",
            "wechat_follower": "follower_id",
            "work_wechat_contact": "contact_id",
            "channel_profile": "profile_id",
            "customer_event": "event_id"
        }
        
        # 验证每个实体类型的ID唯一性
        for entity, id_field in id_fields.items():
            if entity not in data_cache:
                continue
                
            data_list = data_cache[entity]
            id_set = set()
            duplicate_ids = set()
            
            for data in data_list:
                if id_field not in data:
                    error_count += 1
                    error_msg = f"实体类型 {entity} 中存在没有ID字段 {id_field} 的记录"
                    result["errors"].append(error_msg)
                    self.logger.error(error_msg)
                    continue
                    
                entity_id = data[id_field]
                if entity_id in id_set:
                    duplicate_ids.add(entity_id)
                else:
                    id_set.add(entity_id)
            
            if duplicate_ids:
                error_count += len(duplicate_ids)
                error_msg = f"实体类型 {entity} 中存在 {len(duplicate_ids)} 个重复ID"
                result["errors"].append(error_msg)
                for dup_id in list(duplicate_ids)[:5]:  # 只显示前5个重复ID
                    result["errors"].append(f"  - 重复ID示例: {dup_id}")
                self.logger.error(error_msg)
        
        if error_count > 0:
            result["status"] = "failed"
            self.error_counts["data_uniqueness"] = error_count
        
        return result

# 单例模式
_validator_instance = None

def get_validator(logger=None, log_dir="logs"):
    """获取数据验证器的单例实例"""
    global _validator_instance
    if _validator_instance is None:
        _validator_instance = DataValidator(logger, log_dir)
    return _validator_instance


if __name__ == "__main__":
    # 简单测试
    validator = DataValidator()
    test_data = {
        "customer": [
            {"customer_id": "C001", "name": "张三", "id_type": "ID_CARD", "id_number": "123456", "customer_type": "personal", "registration_date": "2021-01-01", "credit_score": 750, "is_vip": True},
            {"customer_id": "C002", "name": "李四", "id_type": "ID_CARD", "id_number": "123457", "customer_type": "personal", "registration_date": "2021-01-02", "credit_score": "800", "is_vip": 1}
        ],
        "fund_account": [
            {"account_id": "A001", "customer_id": "C001", "account_type": "current", "status": "active", "opening_date": "2021-01-01", "balance": 1000.50},
            {"account_id": "A001", "customer_id": "C002", "account_type": "fixed", "status": "active", "opening_date": "2021-01-02", "balance": "2000.75"}
        ]
    }
    
    results = validator.validate(test_data)
    print(f"验证结果: {results['status']}")
    print(f"总错误数: {results['total_errors']}")
    print(f"详细错误: {results['error_counts']}")