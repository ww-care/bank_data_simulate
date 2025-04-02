#!/bin/bash

# 获取脚本路径
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 检查虚拟环境
if [ ! -d "venv" ]; then
    echo "创建Python虚拟环境..."
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

echo "========================================="
echo "运行单元测试"
echo "========================================="
python -m unittest discover -s tests

echo
echo "========================================="
echo "运行集成测试"
echo "========================================="
python scripts/test_integration.py

echo
echo "测试完成"
