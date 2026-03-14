from qtask import SmartQueue, RemoteStorage

# 下发任务时配置 storage，以便于下发本身就是大载荷的任务（如果需要）
storage = RemoteStorage("http://192.168.1.100:8000")
q = SmartQueue("redis://localhost:6379/0", "spider:tasks", storage=storage)

for symbol in ["AAPL", "MSFT", "TSLA"]:
    q.push({"action": "scrape_stock", "symbol": symbol})