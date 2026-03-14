"""
run_store_result.py — 数据库写入 Worker 示例

监听 db:tasks 队列，将爬虫结果入库。

namespace 正确配置后, history 会自动记录本队列的任务统计。

查看历史:
  qtask history stockev:db:tasks
  qtask ns info stockev
"""

from qtask import Worker

NAMESPACE = "stockev"   # 与 run_scraper.py / run_generator.py 保持一致！
REDIS_URL = "redis://localhost:6379/0"

worker = Worker(
    listen_url=REDIS_URL,
    listen_q_name="db:tasks",          # 实际 key: stockev:db:tasks:stream
    storage_url="http://127.0.0.1:8000",
    namespace=NAMESPACE,               # ← 必须与上游 Worker 的 namespace 保持一致
)


@worker.on("save_stock")
def handle_save(task):
    """处理入库任务，qtask 已自动从 FastAPI 拉取并反序列化了 data 字段"""
    data = task["data"]
    print(f"入库股票 {task['symbol']}，数据量 {len(data)}")
    # 方法返回后，qtask 自动 ACK 并通知 FastAPI 删除临时文件


if __name__ == "__main__":
    print(f"Worker started: namespace={NAMESPACE}")
    print(f"  Listening: {NAMESPACE}:db:tasks:stream")
    print(f"  Monitor : qtask history {NAMESPACE}:db:tasks")
    worker.run()