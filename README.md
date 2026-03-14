# qtask

> **基于 Redis Stream 的轻量级分布式任务队列框架**
> 支持大载荷自动卸载、消费确认（ACK）、死信队列（DLQ）、任务历史记录、Web 监控面板及 CLI 运维工具。

---

## 目录

1. [核心特性](#核心特性)
2. [架构总览](#架构总览)
3. [安装](#安装)
4. [快速开始](#快速开始)
   - [1. 启动存储与监控服务](#1-启动存储与监控服务)
   - [2. 生产者：推送任务](#2-生产者推送任务)
   - [3. 消费者：Worker 节点](#3-消费者worker-节点)
   - [4. 链式队列（结果队列）](#4-链式队列结果队列)
5. [任务历史记录](#任务历史记录)
6. [数据保留配置（K 天）](#数据保留配置k-天)
7. [Web Dashboard](#web-dashboard)
8. [CLI 工具参考](#cli-工具参考)
9. [API 参考](#api-参考)
10. [环境变量](#环境变量)
11. [运行测试](#运行测试)

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **大载荷自动卸载** | Payload > 50KB 时自动存入 FastAPI 文件存储，Redis 里只留指针，消费端透明还原 |
| **可靠的消费确认** | 基于 Redis Stream Consumer Group + XACK，天然防止消息丢失 |
| **死信队列 (DLQ)** | 处理失败自动移入 DLQ，支持单条/批量重放 |
| **Zombie 任务自动认领** | 长时间挂起的 Pending 消息被自动 XCLAIM，无需人工干预 |
| **任务历史记录** | 记录每条任务的状态（pending/completed/failed）、重试次数、耗时，按时间范围查询 |
| **K 天数据保留** | 可配置只保留最近 N 天历史，超期数据懒清理 |
| **Web 监控面板** | 实时展示队列长度、DLQ 数量、历史统计、服务器健康状态 |
| **服务器健康检查** | 独立心跳轮询，Server 宕机/Redis 异常时横幅提示，不刷屏弹窗 |
| **CLI 运维工具** | 命令行快速查看/操作队列、历史记录、全局设置 |

---

## 架构总览

```
Producer(s)           Redis Stream             Worker(s)
─────────────         ─────────────            ─────────────
SmartQueue.push()  →  queue:stream         ←   SmartQueue.pop_blocking()
                                               ↓
                      queue:stream_dlq  ←      [fail / DLQ]
                                               ↓
                  FastAPI Storage Server       TaskHistoryStore (同 Redis)
                  /api/storage/upload         qtask:hist:{queue}:{task_id}
                  /api/storage/download       qtask:hist_idx:{queue}
```

---

## 安装

```bash
# 克隆或进入项目目录后

# 核心库（SmartQueue, Worker, CLI）
pip install -e .

# 同时安装监控服务端依赖（FastAPI, Uvicorn, psutil 等）
pip install -e ".[server]"

# 运行测试需要
pip install -e ".[test]"
```

**最低环境要求**：Python ≥ 3.8，Redis ≥ 6.0

---

## 快速开始

### 1. 启动存储与监控服务

```bash
# 配置环境变量（按需修改）
export QTASK_REDIS_URL="redis://localhost:6379/0"
# 有密码：redis://:mypassword@localhost:6379/0
export QTASK_ADMIN_USER="admin"
export QTASK_ADMIN_PASS="admin123"

# 启动服务（默认 8000 端口）
cd server
uvicorn storage_server:app --host 0.0.0.0 --port 8000
# 或直接运行
python storage_server.py
```

服务启动后：
- **监控面板**：`http://localhost:8000/dashboard`
- **健康检查**：`http://localhost:8000/api/health`（无需登录）
- **API 文档**：`http://localhost:8000/docs`

---

### 2. 生产者：推送任务

```python
from qtask import SmartQueue, RemoteStorage

# 如果 Payload 可能超过 50KB，配置 storage；否则可省略
storage = RemoteStorage("http://localhost:8000")

queue = SmartQueue(
    redis_url="redis://localhost:6379/0",
    queue_name="spider:tasks",
    namespace="my_project",           # 推荐：加入项目级命名空间
    worker_group="spider_group",
    storage=storage,                  # 可选，大载荷卸载
    large_threshold_bytes=1024 * 50,  # 默认 50KB
)

# push() 返回 Redis Stream 消息 ID
msg_id = queue.push({
    "action": "scrape_stock",
    "symbol": "AAPL",
    # payload 超过 50KB 时自动卸载到 FastAPI storage
})
print(f"已入队: {msg_id}")
```

**批量推送示例** (`examples/run_generator.py`)：

```python
for symbol in ["AAPL", "MSFT", "TSLA"]:
    queue.push({"action": "scrape_stock", "symbol": symbol})
```

---

### 3. 消费者：Worker 节点

```python
from qtask import Worker

worker = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="spider:tasks",
    namespace="my_project",                # 必须与生产者一致！
    storage_url="http://localhost:8000",   # 对应 FastAPI 存储服务
    worker_group="spider_group",           # 不指定默认按 namespace 自动生成
    worker_id="worker-01",                 # 不指定则自动生成带 hostname 的 ID
    auto_claim=True,                       # 自动认领 Zombie 任务
    claim_interval=300,                    # 认领检查间隔（秒）
)

@worker.on("scrape_stock")
def handle_scrape(task):
    symbol = task["symbol"]
    print(f"处理: {symbol}")
    # ... 业务逻辑 ...
    # 不返回 / 返回 None → 标记 completed，不推结果队列
    # 抛出异常 → Worker 自动调用 fail()，移入 DLQ

worker.run()  # 阻塞运行
```

**关键行为：**
- 任务处理成功 → 自动 `ACK`，大载荷文件自动清理，历史记录标记 `completed`
- 抛出异常 → 自动 `fail()`，消息移入 DLQ，历史记录标记 `failed`，`retries +1`
- 未知 `action` → 直接 `fail()`

---

### 4. 链式队列（结果队列）

Worker 处理完成后可自动将结果推入下一个队列：

```python
# 上游 Worker（examples/run_scraper.py）
worker = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="spider:tasks",
    result_url="redis://localhost:6379/0",  # 结果推往此 URL
    result_q_name="db:tasks",               # 结果队列名
    namespace="my_project",
    storage_url="http://localhost:8000",
    worker_group="spider_group",
)

@worker.on("scrape_stock")
def handle_scrape(task):
    huge_data = [{"price": 100}] * 100000
    return {                   # ← 有返回值时自动推入 result_q
        "action": "save_stock",
        "symbol": task["symbol"],
        "data": huge_data,    # 超过阈值会被自动卸载到 FastAPI storage
    }

# 下游 Worker（examples/run_store_result.py）
worker2 = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="db:tasks",
    namespace="my_project",
    storage_url="http://localhost:8000",
    worker_group="db_group",
)

@worker2.on("save_stock")
def handle_save(task):
    # qtask 已自动从 FastAPI 拉取并反序列化大载荷
    data = task["data"]
    print(f"入库 {task['symbol']}，共 {len(data)} 条")
```

---

## 任务历史记录

任务历史由 `TaskHistoryStore` 自动维护（Worker 默认开启），**无需改动业务代码**。

数据存储在 Redis 中：
- `qtask:hist:{queue}:{task_id}` — 任务详情 Hash
- `qtask:hist_idx:{queue}` — 时间索引 SortedSet（用于范围查询和清理）

### 任务状态

| 状态 | 含义 |
|------|------|
| `pending` | 已入队，尚未 ACK |
| `completed` | 成功 ACK |
| `failed` | 已移入 DLQ |

### 查看历史（CLI）

```bash
# 查看所有历史（最近 15 天，最多 50 条）
qtask history spider:tasks

# 只看失败任务（最近 7 天）
qtask history spider:tasks --status failed --days 7

# 只看成功任务，显示 200 条
qtask history spider:tasks --status completed -n 200

# 输出示例：
# TASK ID                      ACTION               STATUS       RETRIES  DURATION  CREATED
# ──────────────────────────────────────────────────────────────────────────────────────────
# 1741940123456-0              scrape_stock         completed          0    0.312s  03-14 10:30:01
# 1741940111111-0              scrape_stock         failed             2       —    03-14 10:29:55
```

### 查看历史（Dashboard）

登录 Dashboard → 点击队列 → 点击 **Full History** 按钮，可按状态过滤、调整天数范围、查看重试次数和耗时。

### 直接使用 TaskHistoryStore

```python
import redis
from qtask.history import TaskHistoryStore

r = redis.from_url("redis://localhost:6379/0", decode_responses=True)
hist = TaskHistoryStore(r, "spider:tasks")

# 查询最近 7 天的失败任务
failed = hist.get_tasks(status="failed", limit=100, days=7)
for t in failed:
    print(t["task_id"], t["action"], t["retries"], t["fail_reason"])

# 统计各状态数量
counts = hist.count_by_status()
# {'pending': 3, 'completed': 142, 'failed': 5, 'total': 150}
```

---

## 数据保留配置（K 天）

默认保留最近 **15 天**的历史记录，可通过 CLI 或 Dashboard 修改。

### 用 CLI 配置

```bash
# 查看当前设置
qtask settings show
# ⚙️  qtask Global Settings
#    history_keep_days = 15 days

# 修改为 30 天
qtask settings set --keep-days 30

# 修改为 7 天（最短 1 天，最长 365 天）
qtask settings set -k 7
```

### 用 Dashboard 配置

侧边栏 → **Settings** → `Keep History (days)` → 输入数值 → **Save**

### 工作原理

- 配置存储在 Redis Hash `qtask:settings` 中，全局生效
- 每次 `push`/`ack`/`fail` 后触发一次懒清理（删除 SortedSet 中过期的 `task_id`）
- Redis Hash 本身也设置了 `TTL = keep_days + 1`，防止孤儿 key

---

## Web Dashboard

浏览器访问 `http://localhost:8000/dashboard` 并登录。

### 功能区

**System Overview**
- CPU / 内存使用率实时展示
- 各队列卡片：总消息数、Consumer Group 数量、Pending 数、历史状态摘要（Completed / Pending / Failed 三色数字）

**Queue Inspector**
- Stream 队列最近 100 条任务（Recent Tasks）
- 死信队列 DLQ 内容（Dead Letters）
- 单条 / 批量重放（Requeue）
- 危险操作：Reset Group Cursor、Purge Data

**Full History**（点击队列 → Full History 按钮）
- 按状态过滤：All / ✅ Completed / ⏳ Pending / ❌ Failed
- 调整时间范围（天数）
- 表格列：Task ID、Action、Status、🔁 Retries、Duration、Created、Fail Reason

**Settings**
- `Keep History (days)` 设置与保存
- CLI 命令速查

**Server Health Indicator**（顶栏）

| 状态 | 显示 |
|------|------|
| Server + Redis 正常 | 🟢 Online（闪烁） |
| Server 正常但 Redis 宕机 | 🟡 Degraded + 橙色横幅 |
| Server 宕机（首次） | 🔴 Offline + 红色横幅 + 1 条 Toast |
| Server 宕机（持续） | 🔴 Offline + 横幅显示失败次数，**不再弹窗** |
| Server 恢复 | 弹 1 条恢复通知 Toast |

---

## CLI 工具参考

```bash
# 帮助
qtask --help
qtask <command> --help
```

| 命令 | 说明 |
|------|------|
| `qtask index <queue>` | 查看队列长度和 Stream 信息 |
| `qtask groups <queue>` | 查看 Consumer Group 状态（pending 数等） |
| `qtask dlq <queue> [--preview]` | 查看死信队列；`--preview` 截取前 5 条 |
| `qtask requeue <queue> [--task-id ID]` | 将 DLQ 任务回放到主队列 |
| `qtask claim <queue> [--group G] [--idle-ms N]` | 强制认领超时 Zombie 任务 |
| `qtask reset <queue> [--group G] [-f]` | 重置消费组游标到最新（丢弃积压） |
| `qtask clear <queue> [-f]` | **危险**：清空队列及 DLQ 所有数据 |
| `qtask history <queue> [--status S] [--days D] [-n N]` | 查看任务历史记录 |
| `qtask settings show` | 查看全局设置（history_keep_days 等） |
| `qtask settings set --keep-days N` | 设置历史记录保留天数 |

所有命令支持 `--redis-url` 参数（默认 `redis://localhost:6379/0`）：

```bash
qtask history spider:tasks --status failed --redis-url redis://:pass@host:6379/0
```

---

## API 参考

### 无需认证

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/health` | 健康检查：server + redis 状态 |

### 需要 Bearer Token（`POST /api/login` 获取）

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/stats` | 系统/队列/历史统计 |
| `GET` | `/api/history/{queue}` | 任务历史列表（`?status=&limit=&days=`）|
| `GET` | `/api/history/{queue}/counts` | 快速状态计数 |
| `GET` | `/api/settings` | 读取全局设置 |
| `POST` | `/api/settings` | 更新全局设置（`{"history_keep_days": N}`）|
| `GET` | `/api/tasks/{queue}` | Stream / DLQ 消息列表 |
| `POST` | `/api/requeue/{queue}` | 重放 DLQ 消息（`{"task_id": "..."}` 可选）|
| `POST` | `/api/queues/{queue}/clear` | 清空队列（需提供 admin password）|
| `POST` | `/api/queues/{queue}/reset` | 重置消费组游标（需提供 admin password）|
| `POST` | `/api/storage/upload` | 上传大载荷文件 |
| `GET` | `/api/storage/download/{id}` | 下载大载荷文件 |
| `DELETE` | `/api/storage/delete/{id}` | 删除大载荷文件 |

---

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `QTASK_REDIS_URL` | `redis://localhost:6379/0` | Redis 连接 URL（支持密码：`redis://:pass@host:6379/db`） |
| `QTASK_ADMIN_USER` | `admin` | Dashboard 管理员用户名 |
| `QTASK_ADMIN_PASS` | `admin123` | Dashboard 管理员密码 |

---

## 运行测试

```bash
pytest tests/ -v
```

---

## 示例文件

| 文件 | 说明 |
|------|------|
| `examples/run_generator.py` | 批量推送任务到 `spider:tasks` |
| `examples/run_scraper.py` | 消费 `spider:tasks`，处理后推结果到 `db:tasks` |
| `examples/run_store_result.py` | 消费 `db:tasks`，处理最终结果 |
