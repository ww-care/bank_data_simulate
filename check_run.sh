#!/bin/bash

# 检查Python环境
echo "检查Python环境..."
python3 --version

# 切换到项目目录
cd "$(dirname "$0")"

# 运行数据库检查脚本
echo -e "\n运行数据库检查脚本..."
python3 scripts/check_database.py

# 检查结果
if [ $? -eq 0 ]; then
    echo "数据库检查成功! 现在可以进行功能测试"
else
    echo "数据库检查失败! 请检查配置和环境"
    exit 1
fi
