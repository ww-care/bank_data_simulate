#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
时间管理模块

负责处理系统中所有与时间相关的操作，包括时间范围计算、时间格式转换等。
"""

import os
import datetime
import pytz
from typing import Dict, Tuple, Optional, List, Union

# 导入项目模块
from src.config_manager import get_config_manager
from src.logger import get_logger


class TimeManager:
    """时间管理类，负责处理时间相关的操作"""
    
    def __init__(self, timezone: str = 'Asia/Shanghai'):
        """
        初始化时间管理器
        
        Args:
            timezone: 时区，默认为Asia/Shanghai（中国标准时间）
        """
        self.logger = get_logger('time_manager')
        self.config_manager = get_config_manager()
        
        # 设置时区
        try:
            self.timezone = pytz.timezone(timezone)
        except pytz.exceptions.UnknownTimeZoneError:
            self.logger.warning(f"未知时区: {timezone}，将使用默认时区: Asia/Shanghai")
            self.timezone = pytz.timezone('Asia/Shanghai')
        
        # 缓存当前时间，避免在短时间内多次调用时出现时间差异
        self.current_time = None
        self.current_time_updated_at = None
    
    def get_current_time(self, refresh: bool = False) -> datetime.datetime:
        """
        获取当前时间（带时区信息）
        
        Args:
            refresh: 是否强制刷新缓存
            
        Returns:
            当前时间
        """
        now = datetime.datetime.now(self.timezone)
        
        # 判断是否需要更新缓存
        if refresh or self.current_time is None or \
           self.current_time_updated_at is None or \
           (now - self.current_time_updated_at).total_seconds() > 5:  # 5秒缓存期
            self.current_time = now
            self.current_time_updated_at = now
        
        return self.current_time
    
    def format_datetime(self, dt: datetime.datetime, format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
        """
        格式化日期时间
        
        Args:
            dt: 日期时间对象
            format_str: 格式字符串
            
        Returns:
            格式化后的字符串
        """
        return dt.strftime(format_str)
    
    def format_date(self, date: datetime.date, format_str: str = '%Y-%m-%d') -> str:
        """
        格式化日期
        
        Args:
            date: 日期对象
            format_str: 格式字符串
            
        Returns:
            格式化后的字符串
        """
        return date.strftime(format_str)
    
    def parse_datetime(self, datetime_str: str, format_str: str = '%Y-%m-%d %H:%M:%S') -> datetime.datetime:
        """
        解析日期时间字符串
        
        Args:
            datetime_str: 日期时间字符串
            format_str: 格式字符串
            
        Returns:
            日期时间对象
        """
        try:
            dt = datetime.datetime.strptime(datetime_str, format_str)
            return self.timezone.localize(dt) if dt.tzinfo is None else dt
        except ValueError as e:
            self.logger.error(f"解析日期时间字符串失败: {str(e)}")
            raise
    
    def parse_date(self, date_str: str, format_str: str = '%Y-%m-%d') -> datetime.date:
        """
        解析日期字符串
        
        Args:
            date_str: 日期字符串
            format_str: 格式字符串
            
        Returns:
            日期对象
        """
        try:
            return datetime.datetime.strptime(date_str, format_str).date()
        except ValueError as e:
            self.logger.error(f"解析日期字符串失败: {str(e)}")
            raise
    
    def calculate_historical_period(self) -> Tuple[datetime.date, datetime.date]:
        """
        计算历史数据的时间范围
        
        Returns:
            开始日期和结束日期的元组
        """
        try:
            # 从配置中获取历史数据时间范围
            system_config = self.config_manager.get_system_config()
            start_date_str = system_config.get('historical_start_date', None)
            end_date_str = system_config.get('historical_end_date', None)
            
            # 如果配置中没有指定，则使用默认范围
            if not start_date_str:
                # 默认为一年前
                end_date = (self.get_current_time() - datetime.timedelta(days=1)).date()
                start_date = end_date.replace(year=end_date.year - 1)
                self.logger.info(f"未指定历史数据开始日期，将使用默认值: {start_date}")
            else:
                start_date = self.parse_date(start_date_str)
            
            if not end_date_str:
                # 默认为昨天
                end_date = (self.get_current_time() - datetime.timedelta(days=1)).date()
                self.logger.info(f"未指定历史数据结束日期，将使用默认值: {end_date}")
            elif end_date_str.lower() == 'yesterday':
                end_date = (self.get_current_time() - datetime.timedelta(days=1)).date()
            else:
                end_date = self.parse_date(end_date_str)
            
            # 确保开始日期早于结束日期
            if start_date > end_date:
                self.logger.warning(f"历史数据开始日期 {start_date} 晚于结束日期 {end_date}，将交换两个日期")
                start_date, end_date = end_date, start_date
            
            return start_date, end_date
        
        except Exception as e:
            self.logger.error(f"计算历史数据时间范围失败: {str(e)}")
            # 使用默认范围（过去一年）
            end_date = (self.get_current_time() - datetime.timedelta(days=1)).date()
            start_date = end_date.replace(year=end_date.year - 1)
            
            self.logger.warning(f"将使用默认的历史数据时间范围: {start_date} 至 {end_date}")
            return start_date, end_date
    
    def get_time_range_for_generation(self, mode: str = 'realtime') -> Tuple[datetime.datetime, datetime.datetime]:
        """
        获取数据生成的时间范围
        
        Args:
            mode: 数据生成模式（historical/realtime）
            
        Returns:
            开始时间和结束时间的元组
        """
        now = self.get_current_time()
        hour = now.hour
        
        if mode.lower() == 'historical':
            # 历史模式使用calculate_historical_period计算的日期范围
            start_date, end_date = self.calculate_historical_period()
            
            # 转换为datetime对象，并添加时区信息
            start_datetime = self.timezone.localize(
                datetime.datetime.combine(start_date, datetime.time(0, 0, 0)))
            end_datetime = self.timezone.localize(
                datetime.datetime.combine(end_date, datetime.time(23, 59, 59)))
            
            return start_datetime, end_datetime
        
        elif mode.lower() == 'realtime':
            # 实时模式根据当前小时决定时间范围
            today = now.date()
            yesterday = today - datetime.timedelta(days=1)
            
            if hour >= 13:
                # 13点后执行，生成当天0-12点的数据
                start_datetime = self.timezone.localize(
                    datetime.datetime.combine(today, datetime.time(0, 0, 0)))
                end_datetime = self.timezone.localize(
                    datetime.datetime.combine(today, datetime.time(12, 59, 59)))
            else:
                # 13点前执行（次日1点），生成前一天13-23点的数据
                start_datetime = self.timezone.localize(
                    datetime.datetime.combine(yesterday, datetime.time(13, 0, 0)))
                end_datetime = self.timezone.localize(
                    datetime.datetime.combine(yesterday, datetime.time(23, 59, 59)))
            
            return start_datetime, end_datetime
        
        else:
            self.logger.error(f"未知的数据生成模式: {mode}")
            raise ValueError(f"未知的数据生成模式: {mode}")
    
    def get_last_generation_time(self, db_manager, mode: str = 'realtime') -> Optional[datetime.datetime]:
        """
        获取上次数据生成的时间
        
        Args:
            db_manager: 数据库管理器对象
            mode: 数据生成模式（historical/realtime）
            
        Returns:
            上次生成数据的结束时间，如果没有记录则返回None
        """
        try:
            timestamp_str = db_manager.check_last_timestamp('data_generation_log', mode)
            
            if timestamp_str:
                try:
                    # 尝试解析日期时间
                    return self.parse_datetime(timestamp_str)
                except:
                    try:
                        # 尝试解析日期
                        return self.timezone.localize(datetime.datetime.combine(
                            self.parse_date(timestamp_str), datetime.time(23, 59, 59)))
                    except:
                        self.logger.error(f"无法解析上次生成时间: {timestamp_str}")
                        return None
            
            return None
        except Exception as e:
            self.logger.error(f"获取上次生成时间失败: {str(e)}")
            return None
    
    def is_workday(self, date: datetime.date) -> bool:
        """
        判断给定日期是否为工作日（简单判断，不考虑节假日）
        
        Args:
            date: 日期
            
        Returns:
            是否为工作日
        """
        # 周一到周五为工作日
        return date.weekday() < 5
    
    def is_business_hour(self, time: datetime.time) -> bool:
        """
        判断给定时间是否为营业时间（9:00-17:00）
        
        Args:
            time: 时间
            
        Returns:
            是否为营业时间
        """
        return datetime.time(9, 0) <= time <= datetime.time(17, 0)
    
    def get_time_weight(self, dt: datetime.datetime) -> float:
        """
        获取给定时间的业务权重，用于模拟不同时间点的交易频率
        
        Args:
            dt: 日期时间
            
        Returns:
            业务权重系数
        """
        date = dt.date()
        time = dt.time()
        hour = dt.hour
        
        # 基础权重
        weight = 1.0
        
        # 工作日和周末权重
        if self.is_workday(date):
            weight *= 1.5  # 工作日交易量更大
        else:
            weight *= 0.6  # 周末交易量较少
        
        # 不同时间段权重
        if 9 <= hour < 12:  # 上午高峰
            weight *= 1.8
        elif 12 <= hour < 14:  # 午休时间
            weight *= 0.7
        elif 14 <= hour < 17:  # 下午高峰
            weight *= 1.5
        elif 17 <= hour < 22:  # 晚间
            weight *= 0.9
        else:  # 深夜
            weight *= 0.3
        
        return weight
    
    def get_date_weight(self, date: datetime.date) -> float:
        """
        获取给定日期的业务权重，用于模拟月初、月末等特殊时段的交易频率
        
        Args:
            date: 日期
            
        Returns:
            业务权重系数
        """
        day = date.day
        month = date.month
        
        # 基础权重
        weight = 1.0
        
        # 月初(1-10日)交易量较大
        if 1 <= day <= 10:
            weight *= 1.3
        # 月末(21日-月底)交易量也较大
        elif day >= 21:
            weight *= 1.2
            
        # 特殊月份权重
        if month in [1, 2]:  # 春节前后
            weight *= 1.4
        elif month in [7, 8]:  # 暑期
            weight *= 1.1
        elif month == 12:  # 年末
            weight *= 1.3
            
        return weight
    
    def split_time_range(self, start_time: datetime.datetime, end_time: datetime.datetime, 
                        interval_hours: int = 1) -> List[Tuple[datetime.datetime, datetime.datetime]]:
        """
        将时间范围分割为多个时间段
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            interval_hours: 每个时间段的小时数
            
        Returns:
            时间段列表，每个元素为(开始时间, 结束时间)
        """
        if start_time > end_time:
            raise ValueError("开始时间必须早于结束时间")
        
        time_slots = []
        current_time = start_time
        
        while current_time < end_time:
            next_time = current_time + datetime.timedelta(hours=interval_hours)
            if next_time > end_time:
                next_time = end_time
            
            time_slots.append((current_time, next_time))
            current_time = next_time
        
        return time_slots
    
    def time_diff_hours(self, start_time: datetime.datetime, end_time: datetime.datetime) -> float:
        """
        计算两个时间之间的小时差
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            小时差
        """
        if isinstance(start_time, str):
            start_time = self.parse_datetime(start_time)
        if isinstance(end_time, str):
            end_time = self.parse_datetime(end_time)
            
        # 确保时区一致
        if start_time.tzinfo is None:
            start_time = self.timezone.localize(start_time)
        if end_time.tzinfo is None:
            end_time = self.timezone.localize(end_time)
            
        return (end_time - start_time).total_seconds() / 3600
    
    def time_diff_days(self, start_date: Union[datetime.date, str], 
                      end_date: Union[datetime.date, str]) -> int:
        """
        计算两个日期之间的天数差
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            天数差
        """
        if isinstance(start_date, str):
            start_date = self.parse_date(start_date)
        if isinstance(end_date, str):
            end_date = self.parse_date(end_date)
            
        return (end_date - start_date).days
    
    def add_days(self, date: Union[datetime.date, str], days: int) -> datetime.date:
        """
        给日期添加天数
        
        Args:
            date: 日期
            days: 天数
            
        Returns:
            新日期
        """
        if isinstance(date, str):
            date = self.parse_date(date)
            
        return date + datetime.timedelta(days=days)
    
    def split_date_range(self, start_date: datetime.date, end_date: datetime.date,
                        days_per_batch: int = 30) -> List[Tuple[datetime.date, datetime.date]]:
        """
        将日期范围分割为多个日期段
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            days_per_batch: 每个日期段的天数
            
        Returns:
            日期段列表，每个元素为(开始日期, 结束日期)
        """
        if start_date > end_date:
            raise ValueError("开始日期必须早于结束日期")
        
        date_ranges = []
        current_date = start_date
        
        while current_date <= end_date:
            batch_end = min(current_date + datetime.timedelta(days=days_per_batch-1), end_date)
            date_ranges.append((current_date, batch_end))
            current_date = batch_end + datetime.timedelta(days=1)
        
        return date_ranges
    
    def generate_date_range(self, start_date: Union[datetime.date, str], 
                           end_date: Union[datetime.date, str]) -> List[datetime.date]:
        """
        生成日期范围
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            日期列表
        """
        if isinstance(start_date, str):
            start_date = self.parse_date(start_date)
        if isinstance(end_date, str):
            end_date = self.parse_date(end_date)
            
        date_list = []
        current_date = start_date
        
        while current_date <= end_date:
            date_list.append(current_date)
            current_date = current_date + datetime.timedelta(days=1)
            
        return date_list


    def format_time_for_db(self, dt: Optional[datetime.datetime] = None) -> str:
        """
        格式化时间为数据库可用的格式
        
        Args:
            dt: 日期时间，默认为当前时间
            
        Returns:
            格式化后的时间字符串
        """
        if dt is None:
            dt = self.get_current_time()
            
        return self.format_datetime(dt, '%Y-%m-%d %H:%M:%S')


# 单例模式
_instance = None

def get_time_manager(timezone: str = 'Asia/Shanghai') -> TimeManager:
    """
    获取TimeManager的单例实例
    
    Args:
        timezone: 时区
        
    Returns:
        TimeManager实例
    """
    global _instance
    if _instance is None:
        _instance = TimeManager(timezone)
    return _instance


if __name__ == "__main__":
    # 简单测试
    time_manager = get_time_manager()
    
    print(f"当前时间: {time_manager.format_datetime(time_manager.get_current_time())}")
    
    start_date, end_date = time_manager.calculate_historical_period()
    print(f"历史数据时间范围: {start_date} 至 {end_date}")
    
    start_time, end_time = time_manager.get_time_range_for_generation('realtime')
    print(f"实时数据时间范围: {start_time} 至 {end_time}")
    
    workday = datetime.date(2025, 3, 31)  # 星期一
    weekend = datetime.date(2025, 3, 30)  # 星期日
    print(f"{workday} 是否为工作日: {time_manager.is_workday(workday)}")
    print(f"{weekend} 是否为工作日: {time_manager.is_workday(weekend)}")
    
    business_time = datetime.time(10, 30)
    non_business_time = datetime.time(20, 30)
    print(f"{business_time} 是否为营业时间: {time_manager.is_business_hour(business_time)}")
    print(f"{non_business_time} 是否为营业时间: {time_manager.is_business_hour(non_business_time)}")