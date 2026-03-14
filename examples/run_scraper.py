from qtask import Worker

worker = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="spider:tasks",
    result_url="redis://localhost:6379/0",
    result_q_name="db:tasks",
    storage_url="http://192.168.1.100:8000"  # 必须配置，否则无法卸载大对象
)

@worker.on("scrape_stock")
def handle_scrape(task):
    # 模拟抓取产生了巨大的字典结构
    huge_data = [{"price": 100} for _ in range(100000)] 
    
    # 只需要返回字典，qtask 会自动将其存入 FastAPI，并在 result_q 留下指针
    return {
        "action": "save_stock",
        "symbol": task["symbol"],
        "data": huge_data
    }

if __name__ == "__main__":
    worker.run()