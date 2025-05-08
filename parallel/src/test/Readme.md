# 并行计算框架多节点大数据测试工具

## 概述

本测试工具用于在单机环境下模拟多节点运行并行计算框架，进行大数据量的性能测试。工具具有以下特点：

- **无需真实大数据**：内置数据生成器，可根据配置生成指定大小的测试数据
- **多节点模拟**：在单机上模拟多个计算节点的并行工作
- **全面性能统计**：收集任务执行时间、成功率、吞吐量等关键指标
- **丰富的报告**：生成HTML和文本格式的详细测试报告，包含图表展示
- **灵活配置**：通过命令行参数可调整各项测试参数

## 核心组件

测试框架由以下核心组件组成：

1. **数据生成器 (DataGenerator)**
   - 生成可配置大小的JSON格式测试数据
   - 支持文件压缩，减少占用空间

2. **多节点模拟器 (MultiNodeSimulator)**
   - 在单机上创建多个节点实例
   - 为每个节点配置独立环境

3. **性能测试客户端 (PerformanceTestClient)**
   - 任务分发与监控
   - 性能指标收集与计算

4. **测试报告生成器 (TestReportGenerator)**
   - HTML报告（带图表）
   - 文本报告（摘要信息）

5. **测试主类 (ParallelFrameworkTest)**
   - 整合以上组件
   - 提供命令行接口

## 使用方法

### 编译项目

确保已经编译项目，生成相关类文件。

### 运行测试

使用以下命令启动测试：

```bash
java -cp <classpath> pers.cclucky.parallel.test.ParallelFrameworkTest [选项]
```

### 主要选项

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `--nodes <数量>` | 模拟节点数量 | 3 |
| `--concurrent <数量>` | 并发任务数 | 10 |
| `--max-tasks <数量>` | 最大任务数量 | 1000 |
| `--timeout <分钟>` | 测试超时时间 | 10 |
| `--generate-data` | 自动生成测试数据 | - |
| `--file-count <数量>` | 生成的测试文件数量 | 5 |
| `--records-per-file <数量>` | 每个文件的记录数 | 200 |
| `--min-size <KB>` | 最小记录大小(KB) | 1 |
| `--max-size <KB>` | 最大记录大小(KB) | 10 |
| `--compress <true\|false>` | 是否压缩测试数据 | true |
| `--keep-files` | 测试后保留临时文件 | - |
| `--help` | 显示帮助信息 | - |

### 示例命令

1. 使用默认配置运行测试：
```bash
java -cp <classpath> pers.cclucky.parallel.test.ParallelFrameworkTest --generate-data
```

2. 自定义节点数量和并发任务：
```bash
java -cp <classpath> pers.cclucky.parallel.test.ParallelFrameworkTest --nodes 5 --concurrent 20 --generate-data
```

3. 生成更大的测试数据：
```bash
java -cp <classpath> pers.cclucky.parallel.test.ParallelFrameworkTest --generate-data --file-count 10 --records-per-file 500 --min-size 5 --max-size 50
```

## 测试报告

测试完成后将在指定目录（默认为 `~/parallel-test/reports`）生成两种格式的报告：

1. **HTML报告**：包含交互式图表，直观展示测试结果
2. **文本报告**：包含测试结果摘要，适合快速查看或日志记录

报告内容包括：

- 测试配置信息
- 任务统计（提交、完成、失败、取消）
- 性能指标（成功率、吞吐量）
- 任务处理时间统计（最小、最大、平均、中位数、95%分位、99%分位）
- 结果可视化图表（仅HTML报告）

## 测试数据

默认情况下，测试数据将生成在 `~/parallel-test/test-data` 目录中，测试完成后可根据需要保留或删除。

使用 `--generate-data` 选项可自动生成测试数据，数据格式为JSON，内容为随机生成的字段和值，可模拟真实业务数据。

## 注意事项

1. 运行测试前确保系统已安装并配置好Redis和ZooKeeper，这是并行计算框架的必要依赖
2. 测试过程可能会占用较多系统资源，请确保系统有足够的内存和CPU
3. 大数据量测试可能需要较长时间，可使用 `--timeout` 参数调整超时时间
4. 使用 `--keep-files` 选项可在测试完成后保留测试环境和数据，方便调试

## 测试结果解读

- **成功率**：成功完成的任务占总任务数的百分比，越高越好
- **吞吐量**：每秒处理的任务数，越高越好
- **处理时间**：任务处理耗时，越低越好
- **95%/99%分位时间**：95%/99%的任务处理时间都低于此值，反映系统稳定性

## 问题排查

如果测试过程中遇到问题：

1. 检查是否正确配置了Redis和ZooKeeper
2. 查看日志输出，定位具体错误
3. 使用 `--keep-files` 选项保留测试环境，便于检查配置文件和数据
4. 减小测试规模（节点数量、并发任务数、数据量）逐步排除问题 