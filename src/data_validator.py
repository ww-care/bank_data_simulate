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
    
    def _validate_data_types(self, data_cache: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """验证数据类型一致性"""
        result = {"status": "success", "errors": [], "warnings": []}
        error_count = 0
        
        # 各字段的预期数据类型
        field_types = [
            # (实体, 字段, 期望类型, 说明)
            ("customer", "customer_id", str, "客户ID"),
            ("customer", "credit_score", (int, float), "信用分"),
            ("customer", "is_vip", bool, "是否VIP"),
            ("customer", "registration_date", str, "注册日期"),
            
            ("bank_manager", "manager_id", str, "经理ID"),
            ("bank_manager", "customer_count", (int, float), "客户数量"),
            
            ("product", "product_id", str, "产品ID"),
            ("product", "interest_rate", (int, float), "利率"),
            ("product", "expected_return", (int, float), "预期回报"),
            
            ("fund_account", "account_id", str, "账户ID"),
            ("fund_account", "balance", (int, float), "余额"),
            ("fund_account", "interest_rate", (int, float), "利率"),
            
            ("account_transaction", "transaction_id", str, "交易ID"),
            ("account_transaction", "account_id", str, "账户ID"),
            ("account_transaction", "amount", (int, float), "交易金额"),
            ("account_transaction", "transaction_datetime", str, "交易时间"),
            
            ("loan_record", "loan_id", str, "贷款ID"),
            ("loan_record", "loan_amount", (int, float), "贷款金额"),
            ("loan_record", "interest_rate", (int, float), "贷款利率"),
            
            ("investment_record", "investment_id", str, "投资ID"),
            ("investment_record", "amount", (int, float), "投资金额"),
            
            ("app_user", "app_user_id", str, "APP用户ID"),
            ("app_user", "login_frequency", (int, float), "登录频率"),
            
            ("customer_event", "event_id", str, "事件ID"),
            ("customer_event", "event_datetime", str, "事件时间")
        ]
        
        # 验证每个字段的数据类型
        for entity, field, expected_type, description in field_types:
            if entity not in data_cache:
                continue
                
            data_list = data_cache[entity]
            type_errors = []
            
            for data in data_list:
                if field not in data or data[field] is None:
                    continue  # 字段不存在或为空
                
                value = data[field]
                id_field = next((f for f in data.keys() if 'id' in f.lower()), None)
                data_id = data.get(id_field, "unknown") if id_field else "未知"
                
                # 如果期望类型是元组，表示多个可接受的类型
                if isinstance(expected_type, tuple):
                    if not any(isinstance(value, t) for t in expected_type):
                        # 尝试转换字符串到数值
                        if (int in expected_type or float in expected_type) and isinstance(value, str):
                            try:
                                float(value)  # 检查是否可以转换为数值
                                continue  # 可以转换，则视为类型正确
                            except (ValueError, TypeError):
                                pass  # 转换失败，记录错误
                                
                        type_errors.append((data_id, value, type(value).__name__))
                else:
                    if not isinstance(value, expected_type):
                        # 尝试转换字符串
                        if expected_type in (int, float) and isinstance(value, str):
                            try:
                                if expected_type == int:
                                    int(value)
                                else:
                                    float(value)
                                continue  # 可以转换，则视为类型正确
                            except (ValueError, TypeError):
                                pass  # 转换失败，记录错误
                                
                        # 尝试转换布尔值
                        if expected_type == bool and isinstance(value, (int, str)):
                            if isinstance(value, int) and value in (0, 1):
                                continue
                            if isinstance(value, str) and value.lower() in ('true', 'false', '0', '1'):
                                continue
                                
                        type_errors.append((data_id, value, type(value).__name__))
            
            if type_errors:
                error_count += len(type_errors)
                error_msg = f"实体类型 {entity} 中有 {len(type_errors)} 条记录的 {field} 字段类型不匹配"
                result["errors"].append(error_msg)
                
                expected_type_name = (
                    expected_type.__name__ if not isinstance(expected_type, tuple) 
                    else " 或 ".join(t.__name__ for t in expected_type)
                )
                result["errors"].append(f"  - 期望类型: {expected_type_name} ({description})")
                
                for rec_id, value, actual_type in type_errors[:5]:  # 只显示前5个错误
                    result["errors"].append(f"  - 类型错误: ID={rec_id}, 值={value}, 实际类型={actual_type}")
                
                self.logger.error(error_msg)
        
        if error_count > 0:
            result["status"] = "failed"
            self.error_counts["data_types"] = error_count
        
        return result

    def _validate_foreign_keys(self, data_cache: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """验证外键有效性（确保引用的外键确实存在）"""
        result = {"status": "success", "errors": [], "warnings": []}
        error_count = 0
        
        # 定义外键关系
        foreign_keys = [
            # (实体, 外键字段, 目标实体, 目标字段)
            ("fund_account", "customer_id", "customer", "customer_id"),
            ("account_transaction", "account_id", "fund_account", "account_id"),
            ("loan_record", "customer_id", "customer", "customer_id"),
            ("loan_record", "account_id", "fund_account", "account_id"),
            ("investment_record", "customer_id", "customer", "customer_id"),
            ("investment_record", "account_id", "fund_account", "account_id"),
            ("investment_record", "product_id", "product", "product_id"),
            ("customer_event", "customer_id", "customer", "customer_id"),
            ("app_user", "customer_id", "customer", "customer_id"),
            ("wechat_follower", "customer_id", "customer", "customer_id"),
            ("work_wechat_contact", "customer_id", "customer", "customer_id"),
            ("work_wechat_contact", "manager_id", "bank_manager", "manager_id"),
            ("channel_profile", "customer_id", "customer", "customer_id")
        ]
        
        # 验证每个外键关系
        for entity, fk_field, target_entity, target_field in foreign_keys:
            if entity not in data_cache or target_entity not in data_cache:
                continue
                
            source_data = data_cache[entity]
            target_data = data_cache[target_entity]
            
            # 构建目标字段值集合，用于快速查找
            target_values = set(item.get(target_field) for item in target_data if target_field in item)
            
            # 验证外键
            invalid_fks = []
            for i, data in enumerate(source_data):
                if fk_field not in data or data[fk_field] is None or data[fk_field] == "":
                    continue  # 跳过空值
                
                fk_value = data[fk_field]
                if fk_value not in target_values:
                    id_field = next((f for f in data.keys() if 'id' in f.lower()), None)
                    record_id = data.get(id_field, f"index_{i}") if id_field else f"index_{i}"
                    invalid_fks.append((record_id, fk_value))
            
            if invalid_fks:
                error_count += len(invalid_fks)
                error_msg = f"实体 {entity} 中有 {len(invalid_fks)} 条记录的外键 {fk_field} 在目标实体 {target_entity} 中不存在"
                result["errors"].append(error_msg)
                
                # 显示前5个无效外键
                for record_id, fk_value in invalid_fks[:5]:
                    result["errors"].append(f"  - 记录ID={record_id}, 外键值={fk_value} 在 {target_entity}.{target_field} 中不存在")
                
                self.logger.error(error_msg)
        
        if error_count > 0:
            result["status"] = "failed"
            self.error_counts["foreign_keys"] = error_count
        
        return result
    
    def _validate_time_sequence(self, data_cache: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """验证时间顺序逻辑（确保相关实体间的时间顺序合理）"""
        result = {"status": "success", "errors": [], "warnings": []}
        error_count = 0
        
        # 定义时间顺序检查规则
        time_rules = [
            # (实体, 时间字段, 参考实体, 参考时间字段, 外键关系, 比较类型)
            # 比较类型: 'after' - 应该在参考时间之后, 'before' - 应该在参考时间之前
            ("fund_account", "opening_date", "customer", "registration_date", 
            {"customer_id": "customer_id"}, "after"),
            ("account_transaction", "transaction_datetime", "fund_account", "opening_date", 
            {"account_id": "account_id"}, "after"),
            ("loan_record", "approval_date", "loan_record", "application_date", 
            None, "after"),  # 同一记录的字段比较
            ("investment_record", "purchase_date", "fund_account", "opening_date", 
            {"account_id": "account_id"}, "after"),
            ("app_user", "registration_date", "customer", "registration_date", 
            {"customer_id": "customer_id"}, "after"),
            ("wechat_follower", "follow_date", "customer", "registration_date", 
            {"customer_id": "customer_id"}, "after"),
            ("work_wechat_contact", "add_date", "customer", "registration_date", 
            {"customer_id": "customer_id"}, "after")
        ]
        
        for entity, time_field, ref_entity, ref_time_field, fk_relation, compare_type in time_rules:
            if entity not in data_cache:
                continue
                
            # 同一记录的字段比较
            if entity == ref_entity:
                invalid_records = []
                for i, data in enumerate(data_cache[entity]):
                    if (time_field not in data or data[time_field] is None or 
                        ref_time_field not in data or data[ref_time_field] is None):
                        continue
                    
                    # 转换为日期对象
                    try:
                        if isinstance(data[time_field], str):
                            time_value = self._parse_date(data[time_field])
                        else:
                            time_value = data[time_field]
                            
                        if isinstance(data[ref_time_field], str):
                            ref_time_value = self._parse_date(data[ref_time_field])
                        else:
                            ref_time_value = data[ref_time_field]
                        
                        # 比较日期
                        if compare_type == 'after' and time_value < ref_time_value:
                            id_field = next((f for f in data.keys() if 'id' in f.lower()), None)
                            record_id = data.get(id_field, f"index_{i}") if id_field else f"index_{i}"
                            invalid_records.append((record_id, data[time_field], data[ref_time_field]))
                        elif compare_type == 'before' and time_value > ref_time_value:
                            id_field = next((f for f in data.keys() if 'id' in f.lower()), None)
                            record_id = data.get(id_field, f"index_{i}") if id_field else f"index_{i}"
                            invalid_records.append((record_id, data[time_field], data[ref_time_field]))
                    except (ValueError, TypeError) as e:
                        # 日期解析错误，跳过
                        continue
                
                if invalid_records:
                    error_count += len(invalid_records)
                    error_msg = f"实体 {entity} 中有 {len(invalid_records)} 条记录的 {time_field} 不符合与 {ref_time_field} 的时间顺序关系"
                    result["errors"].append(error_msg)
                    
                    # 显示前5个无效记录
                    for record_id, time_val, ref_time_val in invalid_records[:5]:
                        relation = "应该晚于" if compare_type == "after" else "应该早于"
                        result["errors"].append(
                            f"  - 记录ID={record_id}, {time_field}={time_val} {relation} {ref_time_field}={ref_time_val}")
                    
                    self.logger.error(error_msg)
            
            # 跨实体的字段比较
            elif ref_entity in data_cache and fk_relation:
                # 构建参考实体的映射表
                ref_map = {}
                for ref_data in data_cache[ref_entity]:
                    for entity_fk, ref_fk in fk_relation.items():
                        if ref_fk in ref_data and ref_data[ref_fk] is not None:
                            if ref_time_field in ref_data and ref_data[ref_time_field] is not None:
                                ref_map[ref_data[ref_fk]] = ref_data[ref_time_field]
                
                # 验证时间顺序
                invalid_records = []
                for i, data in enumerate(data_cache[entity]):
                    if time_field not in data or data[time_field] is None:
                        continue
                    
                    # 查找关联的参考记录
                    ref_time_value = None
                    for entity_fk, ref_fk in fk_relation.items():
                        if entity_fk in data and data[entity_fk] in ref_map:
                            ref_time_value = ref_map[data[entity_fk]]
                            break
                    
                    if ref_time_value is None:
                        continue  # 找不到参考记录
                    
                    # 转换为日期对象进行比较
                    try:
                        if isinstance(data[time_field], str):
                            time_value = self._parse_date(data[time_field])
                        else:
                            time_value = data[time_field]
                            
                        if isinstance(ref_time_value, str):
                            ref_time_value = self._parse_date(ref_time_value)
                        
                        # 比较日期
                        if compare_type == 'after' and time_value < ref_time_value:
                            id_field = next((f for f in data.keys() if 'id' in f.lower()), None)
                            record_id = data.get(id_field, f"index_{i}") if id_field else f"index_{i}"
                            invalid_records.append((record_id, data[time_field], ref_time_value))
                        elif compare_type == 'before' and time_value > ref_time_value:
                            id_field = next((f for f in data.keys() if 'id' in f.lower()), None)
                            record_id = data.get(id_field, f"index_{i}") if id_field else f"index_{i}"
                            invalid_records.append((record_id, data[time_field], ref_time_value))
                    except (ValueError, TypeError) as e:
                        # 日期解析错误，跳过
                        continue
                
                if invalid_records:
                    error_count += len(invalid_records)
                    error_msg = f"实体 {entity} 中有 {len(invalid_records)} 条记录的 {time_field} 不符合与 {ref_entity}.{ref_time_field} 的时间顺序关系"
                    result["errors"].append(error_msg)
                    
                    # 显示前5个无效记录
                    for record_id, time_val, ref_time_val in invalid_records[:5]:
                        relation = "应该晚于" if compare_type == "after" else "应该早于"
                        result["errors"].append(
                            f"  - 记录ID={record_id}, {time_field}={time_val} {relation} 关联记录的 {ref_time_field}={ref_time_val}")
                    
                    self.logger.error(error_msg)
        
        if error_count > 0:
            result["status"] = "failed"
            self.error_counts["time_sequence"] = error_count
        
        return result
    
    def _validate_data_volume(self, data_cache: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """验证总体数据量（确保各实体的记录数在预期范围内）"""
        result = {"status": "success", "errors": [], "warnings": []}
        error_count = 0
        
        # 定义实体预期的最小记录数
        min_expected_counts = {
            "customer": 100,        # 至少应有100个客户
            "fund_account": 150,    # 至少应有150个账户
            "bank_manager": 10,     # 至少应有10个银行经理
            "product": 20,          # 至少应有20个产品
            "deposit_type": 5       # 至少应有5种存款类型
        }
        
        # 定义实体间预期的比例关系 (实体A : 实体B = 比例下限 : 比例上限)
        entity_ratios = [
            # (实体A, 实体B, 最小比例, 最大比例)
            ("fund_account", "customer", 1.0, 5.0),        # 每个客户平均1-5个账户
            ("account_transaction", "fund_account", 5.0, 100.0), # 每个账户平均5-100笔交易
            ("loan_record", "customer", 0.1, 1.0),        # 10-100%的客户有贷款
            ("investment_record", "customer", 0.2, 2.0),   # 20%-200%的客户有投资(一个客户可多笔)
            ("app_user", "customer", 0.3, 0.8),            # 30-80%的客户使用APP
            ("work_wechat_contact", "bank_manager", 5.0, 100.0) # 每个经理平均管理5-100个联系人
        ]
        
        # 检查最小记录数
        for entity, min_count in min_expected_counts.items():
            if entity not in data_cache:
                result["warnings"].append(f"实体 {entity} 在数据缓存中不存在")
                continue
                
            actual_count = len(data_cache[entity])
            if actual_count < min_count:
                error_count += 1
                error_msg = f"实体 {entity} 的记录数 ({actual_count}) 低于最小预期值 ({min_count})"
                result["errors"].append(error_msg)
                self.logger.error(error_msg)
                
                # 建议解决方案
                result["errors"].append(f"  - 建议: 增加 {entity} 的生成数量配置")
        
        # 检查实体间比例关系
        for entity_a, entity_b, min_ratio, max_ratio in entity_ratios:
            if entity_a not in data_cache or entity_b not in data_cache:
                continue
                
            count_a = len(data_cache[entity_a])
            count_b = len(data_cache[entity_b])
            
            if count_b == 0:  # 避免除以零
                continue
                
            actual_ratio = count_a / count_b
            
            if actual_ratio < min_ratio:
                error_count += 1
                error_msg = f"实体 {entity_a} 与 {entity_b} 的比例 ({actual_ratio:.2f}) 低于最小预期比例 ({min_ratio})"
                result["errors"].append(error_msg)
                result["errors"].append(f"  - {entity_a}: {count_a} 条记录, {entity_b}: {count_b} 条记录")
                result["errors"].append(f"  - 建议: 增加 {entity_a} 或减少 {entity_b} 的生成数量")
                self.logger.error(error_msg)
            elif actual_ratio > max_ratio:
                error_count += 1
                error_msg = f"实体 {entity_a} 与 {entity_b} 的比例 ({actual_ratio:.2f}) 高于最大预期比例 ({max_ratio})"
                result["errors"].append(error_msg)
                result["errors"].append(f"  - {entity_a}: {count_a} 条记录, {entity_b}: {count_b} 条记录")
                result["errors"].append(f"  - 建议: 减少 {entity_a} 或增加 {entity_b} 的生成数量")
                self.logger.error(error_msg)
        
        # 统计总体数据情况
        total_records = sum(len(data) for data in data_cache.values())
        result["summary"] = {
            "total_records": total_records,
            "entity_counts": {entity: len(data) for entity, data in data_cache.items()}
        }
        
        self.logger.info(f"总数据量: {total_records} 条记录, 分布在 {len(data_cache)} 个实体中")
        
        if error_count > 0:
            result["status"] = "failed"
            self.error_counts["data_volume"] = error_count
        
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