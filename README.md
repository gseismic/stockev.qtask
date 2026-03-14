# stockev.qtask

基于 Redis Stream 的轻量级大载荷任务分布式队列处理框架。支持将超大 JSON 自动剥离缓存至 FastAPI 文件存储中，Worker 节点以拉取的方式实现消费与状态确认 (ACK)。

## 安装与配置

推荐在隔离的虚拟环境或您的项目环境中使用 `pip` 安装此本地包：

```bash
# 1. 安装核心运行库 (Worker 节点)
pip install -e .

# 2. 如果需要在同环境启动存储服务端，请安装 server 额外依赖
pip install -e ".[server]"

# 3. 如果需要运行测试，请安装 test 额外依赖
pip install -e ".[test]"
```

## 测试

在根目录执行：
```bash
pytest tests/ -v
```
它能够自动识别包结构并运行我们编写好的 `SmartQueue` 与 `Worker` 原理测试。
