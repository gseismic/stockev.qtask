from qtask import SmartQueue, RemoteStorage

storage = RemoteStorage("http://127.0.0.1:8000")
q = SmartQueue(
    "redis://localhost:6379/0", 
    "spider:tasks", 
    worker_group="spider_group", 
    storage=storage
)

for symbol in ["AAPL", "MSFT", "TSLA"]:
    q.push({"action": "scrape_stock", "symbol": symbol})