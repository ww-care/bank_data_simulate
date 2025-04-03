#!/bin/bash

# 银行数据模拟系统运行脚本

# 获取脚本路径
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 设置 PYTHONPATH
export PYTHONPATH="$SCRIPT_DIR"

# 检查虚拟环境
if [ ! -d ".venv" ]; then
    echo "创建Python虚拟环境..."
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    source .venv/bin/activate
fi

# 函数：显示帮助信息
show_help() {
    echo "银行数据模拟系统运行脚本"
    echo "用法: $0 [选项]"
    echo
    echo "选项:"
    echo "  -h, --help              显示帮助信息"
    echo "  -i, --init              初始化系统（创建表结构）"
    echo "  --historical            生成历史数据"
    echo "  --realtime              生成实时数据"
    echo "  -f, --force             强制执行（跳过时间检查）"
    echo "  -s, --scheduler         启动调度服务"
    echo "  -d, --debug             启用调试模式"
    echo "  -c, --config DIR        指定配置目录"
    echo
    echo "示例:"
    echo "  $0 --historical         生成历史数据"
    echo "  $0 --realtime --force   强制生成实时数据"
    echo "  $0 --scheduler          启动调度服务"
}

# 默认参数
CONFIG_DIR="config"
LOG_LEVEL="info"
FORCE=""

# 解析参数
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -h|--help)
            show_help
            exit 0
            ;;
        -i|--init)
            ACTION="init"
            shift
            ;;
        --historical)
            ACTION="historical"
            shift
            ;;
        --realtime)
            ACTION="realtime"
            shift
            ;;
        -f|--force)
            FORCE="--force"
            shift
            ;;
        -s|--scheduler)
            ACTION="scheduler"
            shift
            ;;
        -d|--debug)
            LOG_LEVEL="debug"
            shift
            ;;
        -c|--config)
            CONFIG_DIR="$2"
            shift
            shift
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done
set -- "${POSITIONAL[@]}"

# 没有参数时显示帮助
if [ -z "$ACTION" ]; then
    show_help
    exit 1
fi

# 检查配置目录
if [ ! -d "$CONFIG_DIR" ]; then
    echo "错误: 配置目录 '$CONFIG_DIR' 不存在"
    exit 1
fi

# 执行对应操作
if [ "$ACTION" == "init" ]; then
    echo "初始化系统..."
    python scripts/run_historical_data.py --config-dir "$CONFIG_DIR" --log-level "$LOG_LEVEL"
    exit $?
elif [ "$ACTION" == "historical" ]; then
    echo "生成历史数据..."
    python scripts/run_historical_data.py --config-dir "$CONFIG_DIR" --log-level "$LOG_LEVEL"
    exit $?
elif [ "$ACTION" == "realtime" ]; then
    echo "生成实时数据..."
    python scripts/run_realtime_data.py --config-dir "$CONFIG_DIR" --log-level "$LOG_LEVEL" $FORCE
    exit $?
elif [ "$ACTION" == "scheduler" ]; then
    echo "启动调度服务..."
    python scripts/scheduler.py --config-dir "$CONFIG_DIR" --log-level "$LOG_LEVEL"
    exit $?
fi

echo "错误: 未知操作"
exit 1