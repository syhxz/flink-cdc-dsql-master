# 修改日志 (CHANGELOG)

## [Enhanced-1.0.0] - 2025-08-04

### 新增功能 (Added)
- ✅ **Amazon DSQL 连接器**: 完整的 DSQL 源和汇连接器实现
- ✅ **PostgreSQL 到 PostgreSQL CDC**: 使用 DSQL 连接器实现 PostgreSQL 间的 CDC
- ✅ **MySQL 连接器支持**: 添加了 MySQL CDC 连接器和 JDBC 驱动
- ✅ **批处理优化**: 改进了批处理性能和错误处理
- ✅ **连接池管理**: 优化了数据库连接池配置

### 修复问题 (Fixed)
- 🔧 **调试日志清理**: 移除了多余的调试输出
- 🔧 **Kafka 依赖冲突**: 解决了 Kafka 客户端版本冲突问题
- 🔧 **模式强制转换**: 修复了模式不匹配的 IllegalStateException
- 🔧 **DDL 并发问题**: 解决了 DSQL DDL 并发执行问题
- 🔧 **重复键处理**: 改进了重复键约束的处理机制
- 🔧 **模式注册**: 修复了模式注册超时问题
- 🔧 **协调器超时**: 增加了协调器通信的重试机制

### 优化改进 (Enhanced)
- ⚡ **错误处理**: 全面改进了错误处理和恢复机制
- ⚡ **重试逻辑**: 添加了指数退避重试策略
- ⚡ **超时配置**: 优化了各种超时配置
- ⚡ **字段映射**: 改进了字段名映射和类型转换
- ⚡ **批处理性能**: 优化了批处理的性能和稳定性

### 技术细节 (Technical Details)
- **基础版本**: Apache Flink CDC 3.5-SNAPSHOT
- **Flink 版本**: 1.20.1
- **Java 版本**: 11+
- **构建工具**: Maven 3.6+

### 配置示例 (Configuration Examples)
- `mysql-to-dsql.yaml`: MySQL 到 DSQL 的 CDC 配置
- `postgresql-to-dsql.yaml`: PostgreSQL 到 DSQL 的 CDC 配置
- `postgres-to-postgres.yaml`: PostgreSQL 到 PostgreSQL 的 CDC 配置

### 已知问题 (Known Issues)
- MySQL 连接需要唯一的 server-id 避免冲突
- PostgreSQL 需要启用逻辑复制
- DSQL 需要适当的 IAM 权限配置
