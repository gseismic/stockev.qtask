"""
run_generator.py — 生产者示例

将任务推入队列，指定 namespace='stockev' 让所有任务归属同一项目。

namespace 的作用:
  - Redis key 自动加前缀: stockev:spider:tasks:stream
  - Dashboard 可按 namespace 过滤/统计
  - 项目结束后执行 `qtask ns purge stockev -f` 一键清除全部数据
"""

from qtask import SmartQueue, RemoteStorage

# namespace 标识"这批任务属于哪个项目"
# 多台主机的 Worker 共享同一 namespace，数据自动归组
NAMESPACE = "stockev"
REDIS_URL = "redis://localhost:6379/0"

storage = RemoteStorage("http://127.0.0.1:8000")
q = SmartQueue(
    REDIS_URL,
    "spider:tasks",             # 实际 key: stockev:spider:tasks:stream
    namespace=NAMESPACE,
    storage=storage,
    history_store=True,
)

for symbol in ["AAPL", "MSFT", "TSLA"]:
    q.push({"action": "scrape_stock", "symbol": symbol})
    print(f"Pushed: scrape_stock {symbol}")

print(f"\n查看任务状态:\n  qtask history {NAMESPACE}:spider:tasks")
print(f"  qtask index {NAMESPACE}:spider:tasks")