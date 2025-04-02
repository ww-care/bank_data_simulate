#!/bin/bash

# 银行数据库清理脚本 - 仅清理数据

echo "======================================"
echo "  银行数据模拟系统 - 数据库清理工具"
echo "======================================"
echo ""
echo "本脚本将清理所有表中的数据，但保留表结构"
echo ""
read -p "确定要继续清理数据吗？(y/n): " confirm

if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "操作已取消"
    exit 0
fi

# 切换到脚本所在目录
cd "$(dirname "$0")" || exit

# 执行清理
python clean_database.py

echo ""
echo "清理完成！"
