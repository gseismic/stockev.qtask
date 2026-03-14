"""
run_scraper.py — 爬虫 Worker 示例

监听 spider:tasks 队列，将大对象结果推送到 db:tasks 队列。

注意: namespace 与 worker_id 的区别:
  namespace  → 标识"这批任务属于哪个项目" (如 "stockev")
               同一 namespace 下多台主机可以并行处理，数据自动归组
  worker_id  → 标识"哪台机器/哪个进程在处理"，自动生成为 hostname-random
               无需手动设置，日志和监控中用于区分不同主机

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
    # worker_group 不传则自动为 {namespace}_group = "stockev_group"
    # worker_id   不传则自动为 {hostname}-{random}，多主机时日志中可识别来自哪台机器
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