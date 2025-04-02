# 银行数据模拟系统

银行数据模拟系统是一个专为银行业务开发测试和数据分析场景设计的数据生成工具。系统可以根据配置生成符合真实业务规则的客户、账户、交易等各类银行业务数据，支持历史数据生成和实时数据模拟。

## 功能特点

- **历史数据生成**: 一次性生成从过去一年到昨天的完整历史数据
- **实时数据生成**: 定时执行，生成增量交易数据，模拟真实业务流
- **业务规则符合**: 生成的数据符合银行业务规则和实体间关系约束
- **时间分布合理**: 考虑工作时间、非工作时间、周末等因素影响的交易频率
- **高度可配置**: 通过配置文件灵活调整数据生成参数和规则

## 系统架构

系统采用模块化设计，主要包括以下核心模块：

- **配置管理模块**: 读取和管理系统配置文件
- **数据库操作模块**: 处理与MySQL数据库的所有交互
- **时间管理模块**: 处理系统中所有与时间相关的操作
- **数据生成模块**: 生成符合业务规则的模拟数据
- **调度管理模块**: 管理实时数据生成的定时任务

## 数据模型

系统模拟了银行业务的核心数据实体，包括：

- 客户信息（个人客户、企业客户）
- 资金账户（活期账户、定期账户、贷款账户）
- 交易记录（存款、取款、转账、消费等）
- 借款记录（各类贷款）
- 理财记录（各类理财产品）
- 产品信息（存款产品、贷款产品、理财产品）
- 多渠道信息（APP用户、公众号粉丝、企业微信联系人）

## 安装与配置

### 环境要求

- Python 3.8+
- MySQL 8.0+

### 安装步骤

1. 克隆仓库
   ```
   git clone git@github.com:ww-care/bank_data_simulate.git
   cd bank-data-simulation
   ```

2. 创建Python虚拟环境
   ```
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

3. 安装依赖包
   ```
   pip install -r requirements.txt
   ```

4. 配置数据库
   - 创建MySQL数据库
   ```sql
   CREATE DATABASE bank_data_simulation CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'bank_user'@'localhost' IDENTIFIED BY 'bank_password';
   GRANT ALL PRIVILEGES ON bank_data_simulation.* TO 'bank_user'@'localhost';
   FLUSH PRIVILEGES;
   ```
   
   - 修改配置文件 `config/database.ini` 中的数据库连接信息

5. 修改数据生成规则
   - 根据需要调整 `config/bank_data_simulation_config.yaml` 中的各项规则参数

## 使用方法

### 生成历史数据

运行以下命令生成从过去一年到昨天的历史数据：

```
python scripts/run_historical_data.py
```

可选参数：
- `--config-dir`: 指定配置文件目录
- `--log-level`: 设置日志级别(debug, info, warning, error, critical)

### 生成实时数据

运行以下命令生成特定时间段的实时数据：

```
python scripts/run_realtime_data.py
```

可选参数：
- `--config-dir`: 指定配置文件目录
- `--log-level`: 设置日志级别
- `--force`: 强制执行，忽略时间检查（默认只在13点和1点执行）

### 启动调度器

运行以下命令启动调度管理器，它会在每天的13点和次日1点自动执行实时数据生成任务：

```
python scripts/scheduler.py
```

可选参数：
- `--config-dir`: 指定配置文件目录
- `--log-level`: 设置日志级别
- `--test-run`: 测试模式，立即执行一次任务并退出

## 数据生成规则

系统根据配置文件中定义的业务规则生成模拟数据，主要规则包括：

- **客户数据规则**: 客户类型分布、客户属性、信用评分等
- **账户数据规则**: 账户数量分布、账户类型分布、账户余额范围等
- **交易数据规则**: 交易频率、交易金额、交易时间分布、交易类型和渠道等
- **贷款数据规则**: 贷款类型、期限、利率、审批时间等
- **理财数据规则**: 产品类型、风险等级、期限、收益率等
- **渠道数据规则**: APP使用率、活跃度分布、功能使用率等
- **季节性规则**: 工作日/周末、月初/月末、季度末等时间因素影响

详细规则配置请参考配置文件 `config/bank_data_simulation_config.yaml`。

## 项目结构

```
bank_data_simulation/
├── config/                 # 配置文件目录
│   ├── database.ini        # 数据库连接配置
│   └── bank_data_simulation_config.yaml  # 数据生成规则配置
├── logs/                   # 日志文件目录
├── scripts/                # 脚本文件目录
│   ├── run_historical_data.py  # 历史数据生成脚本
│   ├── run_realtime_data.py    # 实时数据生成脚本
│   └── scheduler.py            # 调度管理脚本
├── src/                    # 源代码目录
│   ├── config_manager.py   # 配置管理模块
│   ├── database_manager.py # 数据库操作模块
│   ├── logger.py           # 日志管理模块
│   ├── utils.py            # 工具函数模块
│   ├── data_generator/     # 数据生成模块
│   ├── data_exporter/      # 数据导出模块
│   └── time_manager/       # 时间管理模块
├── tests/                  # 测试代码目录
├── .gitignore              # Git忽略文件
├── README.md               # 项目说明文档
└── requirements.txt        # 项目依赖包
```

## 开发计划

- [x] 基础框架搭建 (配置管理、数据库操作、日志管理)
- [x] 数据模型实现 (设计和创建数据库表结构、实现基础实体生成器)
- [x] 历史数据生成功能 (实现历史数据的完整生成流程)
- [x] 实时数据生成功能 (实现增量数据生成、时间连续性保证)
- [ ] 系统集成与测试 (集成各功能模块、性能测试和优化)

当前状态：数据生成器与运行脚本已完全集成，可以生成历史数据和实时数据。下一步是进行系统性能测试和优化。
