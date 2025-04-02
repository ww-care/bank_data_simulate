#!/bin/bash

# 银行数据模拟系统综合测试脚本

# 获取脚本路径
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 彩色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 函数: 运行测试并检查结果
run_test() {
    local test_name="$1"
    local test_command="$2"
    
    echo -e "${BLUE}=======================================================${NC}"
    echo -e "${BLUE}开始测试: ${test_name}${NC}"
    echo -e "${BLUE}-------------------------------------------------------${NC}"
    
    eval $test_command
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}测试通过: ${test_name}${NC}"
        return 0
    else
        echo -e "${RED}测试失败: ${test_name}${NC}"
        return 1
    fi
}

# 打印系统信息
echo -e "${BLUE}=======================================================${NC}"
echo -e "${BLUE}系统信息${NC}"
echo -e "${BLUE}-------------------------------------------------------${NC}"
echo "操作系统: $(uname -s)"
echo "Python版本: $(python3 --version)"
echo "当前目录: $(pwd)"
echo "当前时间: $(date)"

# 测试1: 配置文件检查
run_test "配置文件检查" "python3 scripts/check_config.py"
CONFIG_TEST=$?

# 测试2: 数据库连接检查
run_test "数据库连接检查" "python3 scripts/check_database.py"
DB_TEST=$?

# 如果前两项测试通过，继续更高级的测试
if [ $CONFIG_TEST -eq 0 ] && [ $DB_TEST -eq 0 ]; then
    # 测试3: 小型数据生成测试
    run_test "小型数据生成测试" "python3 scripts/test_mini_generation.py"
    MINI_TEST=$?
    
    if [ $MINI_TEST -eq 0 ]; then
        # 测试4: 集成测试
        run_test "集成测试" "python3 scripts/test_integration.py"
        INTEGRATION_TEST=$?
    else
        INTEGRATION_TEST=1
        echo -e "${YELLOW}由于小型数据生成测试失败，跳过集成测试${NC}"
    fi
else
    MINI_TEST=1
    INTEGRATION_TEST=1
    echo -e "${YELLOW}由于基础测试失败，跳过高级测试${NC}"
fi

# 总结测试结果
echo -e "${BLUE}=======================================================${NC}"
echo -e "${BLUE}测试结果摘要${NC}"
echo -e "${BLUE}-------------------------------------------------------${NC}"

if [ $CONFIG_TEST -eq 0 ]; then
    echo -e "配置文件检查: ${GREEN}通过${NC}"
else
    echo -e "配置文件检查: ${RED}失败${NC}"
fi

if [ $DB_TEST -eq 0 ]; then
    echo -e "数据库连接检查: ${GREEN}通过${NC}"
else
    echo -e "数据库连接检查: ${RED}失败${NC}"
fi

if [ $MINI_TEST -eq 0 ]; then
    echo -e "小型数据生成测试: ${GREEN}通过${NC}"
else
    echo -e "小型数据生成测试: ${RED}失败${NC}"
fi

if [ $INTEGRATION_TEST -eq 0 ]; then
    echo -e "集成测试: ${GREEN}通过${NC}"
else
    echo -e "集成测试: ${RED}失败${NC}"
fi

# 确定最终测试结果
if [ $CONFIG_TEST -eq 0 ] && [ $DB_TEST -eq 0 ] && [ $MINI_TEST -eq 0 ] && [ $INTEGRATION_TEST -eq 0 ]; then
    echo -e "${GREEN}总体结果: 所有测试通过!${NC}"
    exit 0
else
    echo -e "${RED}总体结果: 部分测试失败${NC}"
    exit 1
fi
