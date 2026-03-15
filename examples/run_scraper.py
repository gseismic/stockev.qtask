"""
run_scraper.py — 爬虫 Worker 示例

监听 spider:tasks 队列，将大对象结果推送到 db:tasks 队列。

提示: namespace 作为唯一需要的分组维度:
  namespace  → 标识"这批任务属于哪个项目" (如 "stockev")
               只需分配相同的 namespace，多台主机自动被编为同一消费者组协同处理任务。
               底层的 worker_group 和 worker_id 将自动推导和生成，对用户完全透明。

history 记录问题排查:
  history 依赖 enable_history=True（默认启用）且 namespace 参数与生产者一致。
  若不指定 namespace 或与生产者不一致，history key 会对不上，Dashboard 看不到记录。
"""

from qtask import Worker

NAMESPACE = "stockev"   # 与 run_generator.py 保持一致！
REDIS_URL = "redis://localhost:6379/0"

worker = Worker(
    listen_url=REDIS_URL,
    listen_q_name="spider:tasks",   # 实际 key: stockev:spider:tasks:stream
    result_url=REDIS_URL,
    result_q_name="db:tasks",       # 实际 key: stockev:db:tasks:stream
    storage_url="http://127.0.0.1:8000",
    namespace=NAMESPACE,            # ← 必须与生产者 namespace 保持一致
)


@worker.on("scrape_stock")
def handle_scrape(task):
    """处理爬虫任务，返回大对象由 qtask 自动卸载到 FastAPI 存储"""
    huge_data = [{"price": 100 + i, "symbol": task["symbol"]} for i in range(100000)]
    # 返回字典时，qtask 自动将 data 字段存入 FastAPI，在 result_q 留下指针
    return {
        "action": "save_stock",
        "symbol": task["symbol"],
        "data": huge_data,
    }


if __name__ == "__main__":
    print(f"Worker started: namespace={NAMESPACE}")
    print(f"  Listening: {NAMESPACE}:spider:tasks:stream")
    print(f"  Result  -> {NAMESPACE}:db:tasks:stream")
    print(f"  Monitor : qtask history {NAMESPACE}:spider:tasks")
    worker.run()