# 数据库清理工具

此工具用于清理和重建银行数据模拟系统的数据库内容。

## 功能

该工具提供两种模式：

1. **数据清理模式**：仅清空表数据，保留表结构（使用 `TRUNCATE TABLE` 语句）
2. **重建模式**：完全删除表并重新创建（使用 `DROP TABLE` 然后 `CREATE TABLE`）

## 使用方法

### 前提条件

确保已经在项目根目录下安装了所有依赖，并且数据库配置正确。

### 仅清理数据

```bash
python clean_database.py
```

这将按照外键依赖关系的正确顺序清空所有表中的数据，但保留表结构。

### 重建整个数据库

```bash
python clean_database.py --rebuild
```

这将先删除所有表，然后重新按照 `database_manager.py` 中定义的最新表结构创建表。

## 表清理顺序

工具按照以下顺序清理表，以避免外键约束错误：

1. 账户交易记录 (account_transaction)
2. 借款记录 (loan_record)
3. 理财记录 (investment_record)
4. 客户事件 (customer_event)
5. APP用户 (app_user)
6. 公众号粉丝 (wechat_follower)
7. 企业微信联系人 (work_wechat_contact)
8. 全渠道档案 (channel_profile)
9. 资金账户 (fund_account)
10. 客户 (customer)
11. 银行经理 (bank_manager)
12. 产品 (product)
13. 存款类型 (deposit_type)
14. 数据生成日志 (data_generation_log)

## 注意事项

- 执行前请确保已备份重要数据
- 该脚本会自动处理外键关系（临时禁用外键检查，完成后重新启用）
- 如果表不存在，脚本会给出提示，但会继续执行其他表的清理
- 您可以根据需要修改脚本，仅清理或重建特定的表

## 异常处理

如果清理过程中发生错误，脚本会:

1. 输出错误信息
2. 确保重新启用外键检查
3. 关闭数据库连接

## 开发者信息

如果您需要修改表的清理顺序或添加新表，请编辑 `clean_database.py` 文件中的 `tables_to_clean` 和 `tables_to_drop` 列表。