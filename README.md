# stockev.qtask

`stockev.qtask` 是一个基于 **Redis Stream** 的轻量级、健壮的分布式队列处理框架。支持将超大 JSON 自动剥离缓存至 FastAPI 文件存储中，Worker 节点以拉取的方式实现消费与状态确认 (ACK)、失败隔离 (DLQ) 及任务恢复。

## 产品架构特征

1. **优雅的接口**：使用类 Celery 的装饰器注册任务 (`@worker.on("action")`)。
2. **大载荷自动卸载 (Large Payload Offloading)**：队列自动拦截大于特定阈值 (默认 50KB) 的大载荷数据流并推向外置 FastAPI 大对象存储，在消费端透明反序列化，极大程度杜绝 OOM 问题。
3. **基于 Redis Stream 的健壮重传机制**：通过 Consumer Groups 和 `XCLAIM` 完成了彻底的去中心化节点状态追踪、任务重新分配和防丢失处理。
4. **内置管理 CLI**：提供了极其快捷的命令行终端工具检查数据积压状态。
5. **内建实时监控面板**：存储端携带了需要认证校验的 Web Dashboard UI，用于追踪全平台的任务运行流和宿主机硬件资源（CPU/内存）。

---

## 安装与配置

推荐在隔离的虚拟环境或您的项目环境中使用 `pip` 安装此本地包：

```bash
# 1. 安装核心运行库、CLI命令行工具等
pip install -e .

# 2. 如果需要在同环境启动存储与监控服务端，请安装 server 额外依赖
# 这一步会带入 FastAPI, Uvicorn, psutil 等
pip install -e ".[server]"

# 3. 如果需要运行测试，请安装 test 额外依赖
pip install -e ".[test]"
```

---

## 快速使用说明

### 1. 生产者节点 (Producer)

初始化 `SmartQueue` 并发送任务：

```python
from qtask import SmartQueue, RemoteStorage

storage = RemoteStorage("http://localhost:8000")
queue = SmartQueue(
    redis_url="redis://localhost:6379/0", 
    queue_name="spider:tasks", 
    storage=storage,
    worker_group="spider_group" # 推送任务时不严格要求，但须与消费者匹配
)

# 当传入的字典非常大时，SmartQueue 会自动剥去 payload 存到 storage_server，并在 Redis 里只压入存储键值。
queue.push({
    "action": "scrape_symbol", 
    "symbol": "AAPL",
    "metadata": "..." # 或者巨大的 JSON 对象
})
```

### 2. 消费者节点 (Worker)

启动 Worker 监听上述数据流。

```python
from qtask import Worker

worker = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="spider:tasks",
    result_url="redis://localhost:6379/0",  # 当 handler 返回数据时，会自动被推往该结果队列
    result_q_name="db:tasks",
    storage_url="http://localhost:8000",
    worker_group="spider_group"
)

@worker.on("scrape_symbol")
def do_scrape(task_payload):
    symbol = task_payload["symbol"]
    print(f"Scraping symbol: {symbol}")
    
    # 模拟产生了巨量数据
    large_market_data = [{"price": 100} for _ in range(100000)]
    
    # 返回的内容会自动被打包装载到 result_q_name (db:tasks) 队列
    return {
        "action": "store_data",
        "symbol": symbol,
        "payload": large_market_data
    }

worker.run()
```

---

## 外围生态套件：监控 & 运维工具

### 🍒 监控大屏 (Web Dashboard & Large Object Storage)

包含 HTTPBasic 白名单验证的 FastAPI 微服务。既负责存储 Worker 传来的重量级文件，又在 `/dashboard` 上搭载了监控面板。

**启动服务**：
```bash
# 默认占用 8000 端口
python server/storage_server.py
```

**访问面板**：
浏览器打开：`http://localhost:8000/dashboard`
* 默认用户名：`admin`
* 默认密码：`admin123`

*(如需修改账密，可以挂载系统环境变量 `QTASK_ADMIN_USER` 与 `QTASK_ADMIN_PASS`)*

### 🎮 诊断终端 (CLI 工具)

安装包环境后，终端自动注入执行命令 `qtask`：

1. **查看队列整体数据 (`index`)**
   ```bash
   qtask index spider:tasks
   ```
2. **查看挂靠在此队列上的微服务消费组 (`groups`)**
   ```bash
   qtask groups spider:tasks
   ```
3. **查阅死信（异常/被弃用）队列日志 (`dlq`)**
   ```bash
   # 查看被废弃了多少条数据包，并自动截取前 5 条作为预览报错排查
   qtask dlq spider:tasks --preview
   ```

---

## 运行单元测试
```bash
pytest tests/ -v
```
