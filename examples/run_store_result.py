from qtask import Worker

worker = Worker(
    listen_url="redis://localhost:6379/0",
    listen_q_name="db:tasks",
    storage_url="http://192.168.1.100:8000",
    worker_group="db_group"
)

@worker.on("save_stock")
def handle_save(task):
    # qtask 已经自动从 FastAPI 拉取并反序列化了数据
    data = task["data"] 
    print(f"入库股票 {task['symbol']}，数据量 {len(data)}")
    # 执行完毕后，qtask 会自动给 redis 发 ACK，并请求 FastAPI 删掉那个临时文件

if __name__ == "__main__":
    worker.run()