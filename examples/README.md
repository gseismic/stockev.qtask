# qtask 运行示例指南 (Examples)

这下面是为了完整展示库功能（任务发送、分布式接收与大载荷对象拦截）而准备的脚本组。

## 前置环境准备

1. **启动基础存储 (Redis)**
为了支持 Redis Stream 协议和重试抢占功能，你需要运行一个支持该协议的 Redis 环境（建议 >= 5.0）。如果没有，可以在根目录下用 Docker 快捷启动：
```bash
sudo docker run -d --name redis-qtask -p 6379:6379 redis:alpine
```

2. **启动大载荷临时中转服务器 (File Storage Server)**
库针对大于 50KB 的 JSON payload 默认会提取出来落盘，通过内置的简易 FastAPI 服务处理，需要启动它作为所有计算节点的存储代理中心：
```bash
# 在项目根目录下打开一个新终端
python server/storage_server.py
```

## 测试流程演示

接下来请依次另开 3 个终端页面进行体验：

### 第 1 步：启动二级存储引擎 (Store Result Worker)
这是模拟大数据的最终落地去向（比如推送到统一的中心库，例如 Clickhouse 等）：
```bash
# 自动监听并处理 db:tasks 队列里的最终解析数据
python examples/run_store_result.py
```

### 第 2 步：启动爬虫节点群 (Scraper Worker)
代表分布式的算力节点，接收到单一任务后制造出超大返回载荷。由于载荷超大，`qtask` 会自动将其推往前面启动的 `storage_server.py` 而只在 Redis 里记录一个引用。
```bash
# 自动监听 spider:tasks 队列
python examples/run_scraper.py
```

### 第 3 步：生产者下发初始任务 (Task Generator)
向 `spider:tasks` 下发 3 个具体的股票任务，引发蝴蝶效应，整个链路便开始工作：
```bash
# 生成三只新股票的简单抓取任务
python examples/run_generator.py
```

执行后，您将能在前两个 Worker 的终端中看到实时的执行进度、Action 名称乃至消耗毫秒等完整日志。
