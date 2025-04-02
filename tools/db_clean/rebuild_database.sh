#!/bin/bash

# 银行数据库重建脚本 - 删除并重新创建表

echo "======================================"
echo "  银行数据模拟系统 - 数据库重建工具"
echo "======================================"
echo ""
echo "警告: 本脚本将删除所有表并重新创建，此操作不可逆！"
echo "所有数据将被永久删除！"
echo ""
read -p "确定要继续重建数据库吗？(y/n): " confirm

if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "操作已取消"
    exit 0
fi

echo ""
read -p "再次确认: 此操作将删除所有数据，是否继续？(yes/no): " final_confirm

if [ "$final_confirm" != "yes" ]; then
    echo "操作已取消"
    exit 0
fi

# 切换到脚本所在目录
cd "$(dirname "$0")" || exit

# 执行重建
python clean_database.py --rebuild

echo ""
echo "数据库重建完成！"
