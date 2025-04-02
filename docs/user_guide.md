# 银行数据模拟系统使用指南

本文档提供银行数据模拟系统的使用说明，包括安装配置、基本操作和配置文件说明等内容。

## 1. 系统概述

银行数据模拟系统是一个专为银行业务开发测试和数据分析场景设计的数据生成工具。系统可以根据配置生成符合真实业务规则的客户、账户、交易等各类银行业务数据，支持历史数据生成和实时数据模拟。

### 1.1 主要功能

- **历史数据生成**: 一次性生成从过去一年到昨天的完整历史数据
- **实时数据生成**: 定时执行，生成增量交易数据，模拟真实业务流
- **业务规则符合**: 生成的数据符合银行业务规则和实体间关系约束
- **时间分布合理**: 考虑工作时间、非工作时间、周末等因素影响的交易频率
- **高度可配置**: 通过配置文件灵活调整数据生成参数和规则

### 1.2 适用场景

- **开发测试**: 为开发和测试团队提供贴近真实业务场景的测试数据
- **系统演示**: 提供完整的银行业务数据样本，便于系统演示和培训
- **性能测试**: 支持大规模数据生成，满足性能压测需求
- **数据分析**: 提供结构化的模拟数据，用于开发和验证数据分析模型

## 2. 安装与配置

### 2.1 环境要求

- Python 3.8+
- MySQL 8.0+
- 操作系统: Windows/Linux/macOS

### 2.2 安装步骤

1. 克隆或下载项目代码
   ```bash
   git clone https://github.com/yourusername/bank-data-simulation.git
   cd bank-data-simulation
   ```

2. 安装依赖包
   ```bash
   pip install -r requirements.txt
   ```

3. 配置数据库
   - 创建MySQL数据库
   ```sql
   CREATE DATABASE bank_data_simulation CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'bank_user'@'localhost' IDENTIFIED BY 'bank_password';
   GRANT ALL PRIVILEGES ON bank_data_simulation.* TO 'bank_user'@'localhost';
   FLUSH PRIVILEGES;
   ```
   
   - 修改配置文件 `config/database.ini` 中的数据库连接信息
   ```ini
   [mysql]
   host = localhost
   port = 3306
   user = your_username
   password = your_password
   database = bank_data_simulation
   charset = utf8mb4
   ```

### 2.3 配置数据生成规则

数据生成规则配置在 `config/bank_data_simulation_config.yaml` 文件中，可以根据需要进行调整。主要配置项包括：

- 系统基础配置（随机种子、历史数据日期范围等）
- 客户数据生成规则（客户数量、类型分布等）
- 账户数据生成规则（账户数量、余额范围等）
- 交易数据生成规则（交易频率、金额、时间分布等）
- 贷款和理财产品规则（利率、期限等）
- 季节性和周期性规则（日、周、月、季、年周期特征）

详细的配置说明请参见[配置文件说明](#5-配置文件说明)章节。

## 3. 基本操作

系统提供了便捷的运行脚本 `run.sh`（Windows下为`run.bat`），用于执行各种操作。

### 3.1 生成历史数据

生成从过去一年到昨天的历史数据：

```bash
./run.sh --historical
```

可选参数：
- `--debug`: 启用调试模式，显示更详细的日志
- `--config DIR`: 指定配置文件目录

### 3.2 生成实时数据

生成特定时间段的实时数据：

```bash
./run.sh --realtime
```

默认情况下，实时数据生成脚本只在13点和次日1点执行。如果需要在其他时间执行，可以使用 `--force` 参数强制执行：

```bash
./run.sh --realtime --force
```

### 3.3 启动调度服务

启动调度服务，它会在每天的13点和次日1点自动执行实时数据生成任务：

```bash
./run.sh --scheduler
```

调度服务将在后台运行，可以使用 `Ctrl+C` 终止。

### 3.4 初始化系统

初始化系统（创建数据库表结构，未生成数据）：

```bash
./run.sh --init
```

### 3.5 运行测试

运行系统测试，验证各模块功能：

```bash
./run_tests.sh
```

## 4. 使用案例

### 4.1 开发测试场景

为开发团队提供测试数据：

1. 调整配置文件，设置适量的数据生成量
   ```yaml
   customer:
     total_count: 1000  # 设置为所需的客户数量
   ```

2. 生成历史数据
   ```bash
   ./run.sh --historical
   ```

3. 使用生成的数据进行开发测试

### 4.2 性能测试场景

生成大量数据用于性能测试：

1. 调整配置文件，设置大数据量
   ```yaml
   customer:
     total_count: 50000  # 设置为大量客户
   system:
     batch_size: 5000    # 增加批处理大小，提高性能
   ```

2. 生成历史数据
   ```bash
   ./run.sh --historical
   ```

3. 使用生成的大量数据进行性能测试

### 4.3 持续数据模拟场景

为长期测试环境提供持续的数据流：

1. 首先生成历史数据
   ```bash
   ./run.sh --historical
   ```

2. 启动调度服务，持续生成实时数据
   ```bash
   ./run.sh --scheduler
   ```

3. 系统会在每天的13点和次日1点自动生成当天的增量数据

## 5. 配置文件说明

### 5.1 数据库配置 (database.ini)

```ini
[mysql]
host = localhost        # 数据库主机
port = 3306             # 数据库端口
user = bank_user        # 数据库用户名
password = bank_password # 数据库密码
database = bank_data_simulation # 数据库名
charset = utf8mb4       # 字符集
timeout = 10            # 连接超时时间(秒)
```

### 5.2 数据生成规则配置 (bank_data_simulation_config.yaml)

配置文件结构概览：

```yaml
# 系统基础配置
system:
  random_seed: 42                # 随机种子，保证可重复性
  locale: 'zh_CN'                # 地区设置，用于生成本地化数据
  historical_start_date: '2024-04-01'  # 历史数据开始日期
  historical_end_date: '2025-03-30'    # 历史数据结束日期
  batch_size: 1000               # 批处理大小，用于优化内存使用

# 客户数据生成规则
customer:
  total_count: 10000             # 总客户数量
  # 更多客户配置...

# 账户数据生成规则
account:
  # 账户配置...

# 交易数据生成规则
transaction:
  # 交易配置...

# 贷款数据生成规则
loan:
  # 贷款配置...

# 理财记录生成规则
investment:
  # 理财配置...

# 季节性和周期性规则
seasonal_cycle:
  # 周期配置...
```

#### 5.2.1 客户数据配置

```yaml
customer:
  total_count: 10000             # 总客户数量
  
  # 客户类型分布
  type_distribution:
    personal: 0.8                # 个人客户占比
    corporate: 0.2               # 企业客户占比
  
  # VIP客户比例
  vip_ratio:
    personal: 0.15               # 个人VIP客户比例
    corporate: 0.35              # 企业VIP客户比例
  
  # 个人客户属性
  personal:
    # 年龄分布
    age_distribution:
      18-25: 0.15
      26-40: 0.40
      41-60: 0.35
      60+: 0.10
    
    # 性别分布
    gender_distribution:
      male: 0.52
      female: 0.48
    
    # 职业分布
    occupation_distribution:
      professional: 0.25
      technical: 0.15
      service: 0.20
      sales: 0.10
      administrative: 0.15
      manual_labor: 0.10
      retired: 0.05
```

#### 5.2.2 交易数据配置

```yaml
transaction:
  # 交易频率规则
  frequency:
    current:
      transactions_per_month:
        personal:
          min: 10
          max: 30
          mean: 20
  
  # 交易时间分布
  time_distribution:
    workday_ratio: 0.80          # 工作日交易占比
    workday:
      morning:                   # 9:00-12:00
        ratio: 0.35
        peak_time: '10:30'
      lunch:                     # 12:00-14:00
        ratio: 0.15
        peak_time: '13:00'
```

#### 5.2.3 季节性和周期性规则

```yaml
seasonal_cycle:
  # 日周期
  daily_cycle:
    early_morning:               # 7:00-9:00
      ratio: 0.05
      main_channel: 'mobile_app'
    morning:                     # 9:00-12:00
      ratio: 0.30
      main_business: 'corporate'
  
  # 周周期
  weekly_cycle:
    monday:
      ratio: 0.18
      corporate_ratio_increase: 0.15
    tuesday_thursday:
      ratio: 0.17                # 每天占比
      balanced: true
```

配置文件中的比例和数值可以根据实际业务需求进行调整，系统会根据这些规则生成符合特定分布和规律的数据。

## 6. 日志和监控

### 6.1 日志文件

系统运行过程中的日志保存在 `logs/` 目录下，主要的日志文件包括：

- `bank_data_simulation_YYYYMMDD.log`: 系统总体日志
- `historical_data_YYYYMMDD.log`: 历史数据生成日志
- `realtime_data_YYYYMMDD.log`: 实时数据生成日志
- `scheduler_YYYYMMDD.log`: 调度服务日志
- `database_manager_YYYYMMDD.log`: 数据库操作日志
- `validation_results_YYYYMMDD_HHMMSS.json`: 数据验证结果

### 6.2 监控数据生成进度

可以通过日志文件或数据库查询方式监控数据生成进度：

1. 查看日志文件了解生成进度和结果
   ```bash
   tail -f logs/realtime_data_20250402.log
   ```

2. 查询数据生成日志表
   ```sql
   SELECT * FROM data_generation_log ORDER BY start_time DESC LIMIT 10;
   ```

3. 查询各表的记录数量
   ```sql
   SELECT 'customer' as table_name, COUNT(*) as record_count FROM customer
   UNION ALL
   SELECT 'fund_account', COUNT(*) FROM fund_account
   UNION ALL
   SELECT 'account_transaction', COUNT(*) FROM account_transaction;
   ```

## 7. 故障排除

### 7.1 数据库连接失败

**问题表现**: 运行脚本时出现数据库连接错误。

**解决方法**:
1. 检查 MySQL 服务是否正在运行
2. 验证 `database.ini` 中的连接信息是否正确
3. 确认数据库用户是否具有足够权限
4. 检查网络连接（如果数据库在远程服务器）

### 7.2 数据生成量过大导致性能问题

**问题表现**: 生成大量数据时系统运行缓慢或内存不足。

**解决方法**:
1. 减小配置文件中的数据量设置
2. 增大 `batch_size` 值，优化内存使用
3. 分阶段生成数据，先生成基础数据，再生成交易数据
4. 使用性能更好的硬件环境

### 7.3 调度服务不执行

**问题表现**: 调度服务启动后，没有在预定时间执行数据生成任务。

**解决方法**:
1. 检查系统时间是否正确
2. 查看调度服务日志是否有错误信息
3. 尝试使用 `--force` 参数手动触发实时数据生成
4. 重启调度服务

### 7.4 生成的数据不符合预期

**问题表现**: 生成的数据量或分布与配置不符。

**解决方法**:
1. 检查配置文件中的参数设置
2. 查看验证结果文件中的错误信息
3. 设置日志级别为 `debug` 获取更详细信息
4. 运行单元测试检查各模块功能

## 8. 常见问题

### 8.1 如何只生成特定类型的数据？

**问题**: 只想生成客户和账户数据，不需要交易数据。

**解决方法**:
可以修改配置文件，将交易相关的参数设置为最小值或0：
```yaml
transaction:
  frequency:
    current:
      transactions_per_month:
        personal:
          min: 0
          max: 0
          mean: 0
```

### 8.2 如何导出生成的数据？

**问题**: 需要将生成的数据导出为CSV文件进行其他分析。

**解决方法**:
目前系统未内置数据导出功能，可以通过MySQL客户端工具导出：
```sql
SELECT * FROM customer INTO OUTFILE '/tmp/customers.csv'
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n';
```

或者使用MySQL工具如mysqldump：
```bash
mysqldump -u username -p bank_data_simulation > bank_data_backup.sql
```

### 8.3 如何修改数据生成的时间范围？

**问题**: 需要生成特定时间范围的历史数据。

**解决方法**:
修改配置文件中的时间范围设置：
```yaml
system:
  historical_start_date: '2024-01-01'  # 自定义开始日期
  historical_end_date: '2024-03-31'    # 自定义结束日期
```

### 8.4 如何生成多语言环境的数据？

**问题**: 需要生成英文或其他语言环境的数据。

**解决方法**:
修改配置文件中的地区设置：
```yaml
system:
  locale: 'en_US'  # 英文(美国)
  # 或其他支持的地区: 'en_GB', 'fr_FR', 'de_DE', 'ja_JP', 等
```

## 9. 数据分析示例

### 9.1 客户分布分析

SQL示例：
```sql
-- 按客户类型统计
SELECT customer_type, COUNT(*) as count, 
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customer), 2) as percentage
FROM customer
GROUP BY customer_type;

-- 按年龄段统计个人客户
SELECT 
  CASE 
    WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) < 25 THEN '18-24岁'
    WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) < 40 THEN '25-39岁'
    WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) < 60 THEN '40-59岁'
    ELSE '60岁以上'
  END as age_group,
  COUNT(*) as count
FROM customer
WHERE customer_type = 'personal'
GROUP BY age_group
ORDER BY age_group;
```

### 9.2 交易数据分析

SQL示例：
```sql
-- 按交易类型统计
SELECT transaction_type, COUNT(*) as count, 
       ROUND(SUM(amount), 2) as total_amount,
       ROUND(AVG(amount), 2) as avg_amount
FROM account_transaction
GROUP BY transaction_type
ORDER BY count DESC;

-- 按星期几统计交易
SELECT 
  DAYNAME(transaction_date) as day_of_week,
  COUNT(*) as transaction_count,
  ROUND(SUM(amount), 2) as total_amount
FROM account_transaction
GROUP BY day_of_week
ORDER BY FIELD(day_of_week, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');
```

### 9.3 贷款分析

SQL示例：
```sql
-- 按贷款类型和状态统计
SELECT loan_type, status, COUNT(*) as count,
       ROUND(SUM(loan_amount), 2) as total_amount,
       ROUND(AVG(interest_rate) * 100, 2) as avg_interest_rate
FROM loan_record
GROUP BY loan_type, status
ORDER BY loan_type, status;
```

## 10. 后续发展计划

银行数据模拟系统计划在后续版本中添加以下功能：

1. **数据导出功能**: 直接导出为CSV、JSON等格式
2. **数据可视化界面**: 添加Web界面，便于配置和监控
3. **更多数据库支持**: 添加对PostgreSQL、MongoDB等数据库的支持
4. **异常数据模拟**: 模拟欺诈交易、系统异常等场景
5. **多语言环境支持**: 增强国际化支持
6. **性能优化**: 支持并行处理，提高大数据量生成效率

## 11. 附录

### 11.1 配置参数速查表

| 配置项 | 说明 | 默认值 | 可选值 |
|--------|------|-------|-------|
| system.random_seed | 随机种子 | 42 | 任意整数 |
| system.locale | 地区设置 | zh_CN | en_US, fr_FR, de_DE 等 |
| system.batch_size | 批处理大小 | 1000 | 500-10000 |
| customer.total_count | 客户总数 | 10000 | 1-1000000 |
| customer.type_distribution.personal | 个人客户比例 | 0.8 | 0-1 |
| transaction.frequency.*.transactions_per_month | 每月交易次数 | 视类型而定 | 1-100 |

### 11.2 数据库表关系图

```
customer [1]----<*>[fund_account]
          |         |
          |         |
          v         v
[customer_event]  [account_transaction]
          |
          |
          v
      [product]
```

### 11.3 相关资源

- [MySQL 官方文档](https://dev.mysql.com/doc/)
- [Python 官方文档](https://docs.python.org/3/)
- [Faker 文档](https://faker.readthedocs.io/) - 用于生成随机数据
- [pandas 文档](https://pandas.pydata.org/docs/) - 用于数据处理