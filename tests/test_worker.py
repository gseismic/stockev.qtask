import pytest
from qtask import Worker, SmartQueue
import threading
import time
import redis

TEST_REDIS_URL = "redis://localhost:6379/0"
TEST_LISTEN_Q = "test_listen"
TEST_RESULT_Q = "test_result"

@pytest.fixture(autouse=True)
def clean_redis():
    client = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    client.delete(f"{TEST_LISTEN_Q}:stream", f"{TEST_RESULT_Q}:stream")
    yield
    client.delete(f"{TEST_LISTEN_Q}:stream", f"{TEST_RESULT_Q}:stream")

def test_worker_routing_and_execution():
    worker = Worker(
        listen_url=TEST_REDIS_URL,
        listen_q_name=TEST_LISTEN_Q,
        result_url=TEST_REDIS_URL,
        result_q_name=TEST_RESULT_Q,
        worker_group="test_workers"
    )
    
    @worker.on("compute_add")
    def handle_add(task):
        return {
            "action": "store_result",
            "result": task["a"] + task["b"]
        }
        
    # 推送一个测试任务
    worker.listen_q.push({"action": "compute_add", "a": 15, "b": 25})
    
    # 跑一次任务 (我们通过 hack `pop_blocking` 让它只跑一次后退出，或者让后台跑)
    # 因为 run 是个 while True 死循环，我们在测试里提取单次逻辑
    
    payload, msg_context = worker.listen_q.pop_blocking(timeout=1)
    action = payload.get("action")
    handler = worker.handlers.get(action)
    result_payload = handler(payload)
    worker.result_q.push(result_payload)
    worker.listen_q.ack(msg_context)
    
    # 验证提取到的结果
    assert result_payload["result"] == 40
    
    # 验证 result_q 收到数据
    result_data, result_ctx = worker.result_q.pop_blocking(timeout=1)
    assert result_data["result"] == 40
    worker.result_q.ack(result_ctx)
