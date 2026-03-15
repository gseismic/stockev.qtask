# qtask CLI 完整指南

本文件为 LLM agents 提供 qtask 任务队列的完整使用指南。

---

## 快速开始

### 1. 启动服务

```bash
# Redis (需要先启动)
redis-server

# 存储服务器 (需要设置环境变量)
export QTASK_ADMIN_USER="admin"
export QTASK_ADMIN_PASS="your_password"
export QTASK_ADMIN_TOKEN="your_token"
cd server
python storage_server.py
# 或 uvicorn storage_server:app --host 0.0.0.0 --port 8000

# 验证服务
curl http://127.0.0.1:8000/api/health
```

### 2. 生产者 - 推送任务

```python
from qtask import SmartQueue, RemoteStorage

# 初始化
storage = RemoteStorage("http://localhost:8000")
queue = SmartQueue(
    redis_url="redis://localhost:6379/0",
    queue_name="spider:tasks",
    namespace="my_project",  # 必填，与 Worker 保持一致
    storage=storage,         # 可选，大载荷自动卸载
)

# 推送任务
msg_id = queue.push({
    "action": "scrape_stock",
    "symbol": "AAPL",
    "data": {...}  # 超过 50KB 自动卸载到存储服务
})
print(f"已入队: {msg_id}")
```

### 3. 消费者 - Worker 处理任务

```python
from qtask import Worker

worker = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="spider:tasks",
    namespace="my_project",              # 必须与生产者一致
    storage_url="http://localhost:8000", # 大载荷自动还原
    result_url="redis://localhost:6379/0",# 可选，结果推送到其他队列
    result_q_name="db:tasks",
    auto_claim=True,                      # 自动认领僵尸任务
)

@worker.on("scrape_stock")
def handle_scrape(task):
    symbol = task["symbol"]
    print(f"处理: {symbol}")
    
    # 返回结果 -> 推送到 result_q
    # 不返回/返回 None -> 仅 ACK，不推送
    return {"action": "save_stock", "symbol": symbol, "data": [...]}

worker.run()  # 阻塞运行
```

---

## CLI 命令参考

### 队列操作

```bash
# 查看队列长度和信息
qtask index my_project:spider:tasks

# 查看消费者组状态
qtask groups my_project:spider:tasks

# 查看死信队列 (DLQ)
qtask dlq my_project:spider:tasks
qtask dlq my_project:spider:tasks --preview  # 预览前5条

# 重放 DLQ 消息到主队列
qtask requeue my_project:spider:tasks              # 重放全部
qtask requeue my_project:spider:tasks -t "123-0"  # 重放单条

# 认领僵尸任务 (Worker 宕机后)
qtask claim my_project:spider:tasks
qtask claim my_project:spider:tasks --idle-ms 60000  # 自定义超时(毫秒)

# 重置消费组游标 (危险)
qtask reset my_project:spider:tasks -f

# 清空队列 (危险)
qtask clear my_project:spider:tasks -f        # 保留历史
qtask clear my_project:spider:tasks -f --hard # 清除历史
```

### 任务历史

```bash
# 查看历史
qtask history my_project:spider:tasks

# 过滤状态
qtask history my_project:spider:tasks --status failed
qtask history my_project:spider:tasks --status completed

# 时间范围
qtask history my_project:spider:tasks --days 7

# 数量限制
qtask history my_project:spider:tasks -n 200
```

### Namespace 操作

```bash
# 列出所有 namespace
qtask ns list

# 查看 namespace 详情
qtask ns info my_project

# 清除整个 namespace (危险)
qtask ns purge my_project -f
```

### 全局设置

```bash
# 查看设置
qtask settings show

# 修改历史保留天数
qtask settings set --keep-days 30
```

---

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `QTASK_REDIS_URL` | redis://localhost:6379/0 | Redis 连接 |
| `QTASK_ADMIN_USER` | (必填) | Dashboard 用户名 |
| `QTASK_ADMIN_PASS` | (必填) | Dashboard 密码 |
| `QTASK_ADMIN_TOKEN` | (必填) | API Token |

---

## 队列命名规范

```
格式: [namespace:]queue_name

示例:
  spider:tasks              # 无 namespace
  myproj:spider:tasks       # 有 namespace
  
Redis Key:
  myproj:spider:tasks:stream     # 主队列
  myproj:spider:tasks:stream_dlq # 死信队列
```

---

## 常见错误处理

| 错误 | 原因 | 解决方案 |
|------|------|----------|
| NOGROUP | 消费者组不存在 | 自动创建，或检查 namespace |
| 无历史记录 | namespace 不一致 | 生产者和 Worker 使用相同 namespace |
| DLQ 积压 | 任务处理失败 | 检查 fail_reason，用 requeue 重试 |
| 僵尸任务 | Worker 崩溃未 ACK | 用 claim 认领或 reset 重置游标 |

---

## API 参考

### 存储服务 API

```bash
# 登录获取 token
curl -X POST http://localhost:8000/api/login \
  -d "username=admin&password=your_password"

# 上传大文件
curl -X POST http://localhost:8000/api/storage/upload \
  -F "file=@data.json"

# 下载文件
curl http://localhost:8000/api/storage/download/{key}

# 获取统计
curl -H "Authorization: Bearer {token}" \
  http://localhost:8000/api/stats

# 获取历史
curl -H "Authorization: Bearer {token}" \
  "http://localhost:8000/api/history/myproj:spider:tasks?status=failed&days=7"
```

---

## 完整示例

### 1. 爬虫任务流

```python
# run_generator.py - 产生任务
from qtask import SmartQueue, RemoteStorage

storage = RemoteStorage("http://localhost:8000")
q = SmartQueue("redis://localhost:6379/0", "spider:tasks", 
               namespace="stockev", storage=storage)

for symbol in ["AAPL", "MSFT", "GOOG"]:
    q.push({"action": "scrape_stock", "symbol": symbol})
```

```python
# run_scraper.py - 处理爬虫
from qtask import Worker

worker = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="spider:tasks",
    namespace="stockev",
    result_url="redis://localhost:6379/0",
    result_q_name="db:tasks",
    storage_url="http://localhost:8000",
)

@worker.on("scrape_stock")
def scrape(task):
    data = fetch_data(task["symbol"])  # 爬取
    return {"action": "save_stock", "symbol": task["symbol"], "data": data}

worker.run()
```

```python
# run_store_result.py - 存储结果
from qtask import Worker

worker = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="db:tasks",
    namespace="stockev",
)

@worker.on("save_stock")
def save(task):
    db.save(task["symbol"], task["data"])  # 存库

worker.run()
```

### 2. 运维命令

```bash
# 查看队列状态
qtask index stockev:spider:tasks
qtask groups stockev:spider:tasks

# 查看失败任务
qtask history stockev:spider:tasks --status failed

# 重试失败任务
qtask requeue stockev:spider:tasks

# 清理测试数据
qtask ns purge stockev -f

# 修改保留策略
qtask settings set --keep-days 7
```
