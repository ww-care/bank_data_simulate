# 银行数据模拟系统开发指南

本文档提供银行数据模拟系统的开发指南，包括模块说明、扩展方法和调试技巧等内容。

## 1. 项目结构

```
bank_data_simulation/
├── config/                 # 配置文件目录
│   ├── database.ini        # 数据库连接配置
│   └── bank_data_simulation_config.yaml  # 数据生成规则配置
├── logs/                   # 日志文件目录
├── scripts/                # 脚本文件目录
│   ├── run_historical_data.py  # 历史数据生成脚本
│   ├── run_realtime_data.py    # 实时数据生成脚本
│   ├── scheduler.py            # 调度管理脚本
│   └── test_integration.py     # 集成测试脚本
├── src/                    # 源代码目录
│   ├── config_manager.py   # 配置管理模块
│   ├── database_manager.py # 数据库操作模块
│   ├── logger.py           # 日志管理模块
│   ├── data_validator.py   # 数据验证模块
│   ├── data_generator/     # 数据生成模块
│   │   ├── __init__.py
│   │   ├── data_generator.py   # 数据生成器总控类
│   │   └── entity_generators.py # 实体生成器
│   └── time_manager/       # 时间管理模块
│       └── time_manager.py # 时间管理类
├── tests/                  # 测试代码目录
│   ├── __init__.py
│   └── test_data_generator.py  # 数据生成器单元测试
├── docs/                   # 文档目录
├── run.sh                  # 运行脚本
├── run_tests.sh            # 测试运行脚本
└── requirements.txt        # 项目依赖包
```

## 2. 核心模块说明

### 2.1 配置管理模块 (config_manager.py)

配置管理模块负责读取和管理系统配置文件，包括数据库连接配置和数据生成规则配置。

**主要功能**：
- 读取YAML和INI格式配置文件
- 获取特定实体的配置
- 更新配置并保存

**扩展方式**：
- 添加新的配置文件格式支持
- 增强配置合并和覆盖逻辑

### 2.2 数据库操作模块 (database_manager.py)

数据库操作模块负责与MySQL数据库的所有交互，包括表结构创建、数据导入、查询等。

**主要功能**：
- 连接数据库
- 创建数据库表
- 批量导入数据
- 执行查询
- 生成数据库统计信息

**扩展方式**：
- 添加对更多数据库类型的支持
- 增强数据导入性能优化
- 添加数据导出功能

### 2.3 时间管理模块 (time_manager.py)

时间管理模块负责处理系统中所有与时间相关的操作，包括时间范围计算、时间格式转换等。

**主要功能**：
- 计算历史数据时间范围
- 计算实时数据的时间段
- 时间格式转换
- 计算时间权重（模拟业务时间分布特征）

**扩展方式**：
- 增强时间分布模型
- 添加特殊日期（如节假日）处理

### 2.4 数据生成模块 (data_generator/)

数据生成模块负责生成各类银行业务实体的模拟数据，是系统的核心功能。

#### 2.4.1 数据生成器总控类 (data_generator.py)

**主要功能**：
- 协调各实体生成器
- 生成历史数据和实时数据
- 数据导入数据库

**扩展方式**：
- 优化大数据量的处理流程
- 增强实体间关联逻辑

#### 2.4.2 实体生成器 (entity_generators.py)

**主要功能**：
- 生成客户、账户、交易记录等各类实体数据
- 确保实体间的关联性和业务规则合理性

**扩展方式**：
- 添加新的实体类型
- 增强业务规则合理性
- 优化数据生成的随机分布

### 2.5 数据验证模块 (data_validator.py)

数据验证模块负责验证生成的数据的完整性、唯一性和类型一致性。

**主要功能**：
- 验证数据完整性（必填字段）
- 验证数据唯一性（ID不重复）
- 验证数据类型一致性

**扩展方式**：
- 增强业务规则验证
- 添加数据关系验证

## 3. 开发流程

### 3.1 添加新的实体类型

1. 在`entity_generators.py`中创建新的实体生成器类
2. 在`data_generator.py`中集成新的实体生成器
3. 在`database_manager.py`中添加相应的表定义
4. 更新配置文件中的相关规则

例如，如果要添加一个"贷款申请记录"实体：

```python
# 1. 在entity_generators.py中添加生成器
class LoanApplicationGenerator(BaseEntityGenerator):
    def generate(self, customers, managers):
        # 实现逻辑
        pass

# 2. 在data_generator.py中集成
def _init_entity_generators(self):
    # 已有代码...
    self.loan_application_generator = LoanApplicationGenerator(self.faker, self.config_manager)

def generate_historical_data(self):
    # 已有代码...
    loan_applications = self.loan_application_generator.generate(customers, bank_managers)
    stats['loan_application'] = self.import_data('loan_application', loan_applications)
```

### 3.2 添加新的数据分布规则

1. 在配置文件`bank_data_simulation_config.yaml`中添加新的分布规则
2. 在相应的实体生成器中使用新规则

例如，添加客户年龄的新分布规则：

```yaml
# 配置文件中
customer:
  personal:
    custom_age_distribution:
      university: 
        range: [18, 24]
        ratio: 0.15
      working_young:
        range: [25, 35]
        ratio: 0.30
      working_mature:
        range: [36, 55]
        ratio: 0.40
      retirement:
        range: [56, 85]
        ratio: 0.15
```

```python
# 在实体生成器中使用
custom_age_dist = personal_config.get('custom_age_distribution', {})
if custom_age_dist:
    # 使用自定义年龄分布
    category_items = list(custom_age_dist.items())
    categories = [item[0] for item in category_items]
    weights = [item[1].get('ratio', 0.25) for item in category_items]
    
    selected_category = self.random_choice(categories, weights)
    age_range = custom_age_dist[selected_category]['range']
    age = random.randint(age_range[0], age_range[1])
```

### 3.3 调整交易分布规则

1. 修改配置文件中的交易时间分布规则
2. 调整`time_manager.py`中的时间权重计算逻辑

```yaml
# 配置文件中
transaction:
  time_distribution:
    workday:
      morning_peak:
        start: "09:30"
        end: "11:30"
        weight: 1.8
```

```python
# 在time_manager.py中
def get_time_weight(self, dt: datetime.datetime) -> float:
    # 获取配置中的时间分布
    time_dist = self.config_manager.get_entity_config('transaction').get('time_distribution', {})
    
    # 根据配置计算权重
    # ...
```

## 4. 调试技巧

### 4.1 日志调试

系统各模块均使用了统一的日志管理器，可以通过调整日志级别进行调试：

```python
logger = get_logger('my_module', level='debug')
logger.debug("调试信息")
logger.info("一般信息")
logger.warning("警告信息")
logger.error("错误信息")
```

在运行脚本时可以指定日志级别：
```bash
./run.sh --historical --debug
```

### 4.2 配置调试

可以通过修改配置文件进行调试，例如：

1. 减少生成的数据量：
```yaml
customer:
  total_count: 10  # 仅生成少量客户用于调试
```

2. 使用固定随机种子确保可重复性：
```yaml
system:
  random_seed: 42  # 固定随机种子
```

### 4.3 数据库调试

1. 检查数据库表结构：
```python
db_manager = get_database_manager()
tables = db_manager.execute_query("SHOW TABLES")
print("数据库表:", tables)

# 检查特定表的结构
structure = db_manager.execute_query(f"DESCRIBE {table_name}")
print(f"表 {table_name} 结构:", structure)
```

2. 查询生成的数据：
```python
customers = db_manager.execute_query("SELECT * FROM customer LIMIT 5")
print("客户示例:", customers)
```

## 5. 性能优化

### 5.1 数据生成优化

1. 使用批处理：
   - 调整`batch_size`配置项
   - 对于大数据量，分批次生成和导入

2. 数据生成与数据库导入分离：
   - 先生成所有数据
   - 批量导入到数据库

### 5.2 数据库优化

1. 使用批量插入：
```python
db_manager.execute_many(sql, batch_data)
```

2. 使用索引优化查询：
```python
# 为常用查询字段添加索引
db_manager.execute_update("CREATE INDEX idx_customer_type ON customer(customer_type)")
```

## 6. 测试指南

### 6.1 单元测试

1. 在`tests/`目录下添加测试模块
2. 使用`unittest`框架编写测试

运行单元测试：
```bash
./run_tests.sh
```

### 6.2 集成测试

集成测试用于验证各模块协同工作的正确性：

```python
# 集成测试示例
def test_end_to_end(self):
    # 1. 生成数据
    # 2. 导入数据库
    # 3. 验证数据
    pass
```

## 7. 常见问题

### 7.1 数据库连接问题

**问题**: 无法连接到数据库  
**解决方案**:
- 检查数据库配置文件中的连接信息
- 确保MySQL服务已启动
- 确保数据库用户有足够权限

### 7.2 数据生成问题

**问题**: 生成的数据量不符合预期  
**解决方案**:
- 检查配置文件中的数量设置
- 调整随机分布参数
- 检查数据生成的循环逻辑

### 7.3 调度执行问题

**问题**: 定时调度未执行  
**解决方案**:
- 检查调度服务是否正常运行
- 检查时间点配置
- 查看调度日志中的错误信息

## 8. 贡献指南

1. Fork项目并克隆到本地
2. 创建新的特性分支
3. 开发新功能或修复Bug
4. 编写和运行测试
5. 提交Pull Request

## 9. 未来扩展方向

### 9.1 数据导出功能

实现将生成的数据导出为CSV、JSON等格式，便于数据分析和迁移。

### 9.2 更多数据库支持

添加对PostgreSQL、Oracle、MongoDB等数据库的支持，增强系统适用性。

### 9.3 数据可视化界面

开发Web界面，提供数据生成配置、执行状态监控和生成结果查看功能。

### 9.4 更复杂的业务场景

添加更多银行业务场景的模拟，如信用卡交易、外汇交易、跨行结算等。

### 9.5 并行处理能力

引入多进程或多线程处理，提高大数据量生成效率。

## 10. 联系方式

如有问题，请联系项目维护者或在项目中提交Issue。