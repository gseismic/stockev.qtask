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
    ns = "test_ns"
    worker = Worker(
        listen_url=TEST_REDIS_URL,
        listen_q_name=TEST_LISTEN_Q,
        result_url=TEST_REDIS_URL,
        result_q_name=TEST_RESULT_Q,
        namespace=ns
    )
    
    @worker.on("compute_add")
    def handle_add(task):
        return {
            "action": "store_result",
            "result": task["a"] + task["b"]
        }
        
    # 推送一个测试任务
    worker.listen_q.push({"action": "compute_add", "a": 15, "b": 25})
    
    # 模拟 Worker.run 中的单次执行逻辑
    # 使用 worker 实例持有的身份
    wg, wid = worker.worker_group, worker.worker_id
    
    payload, msg_context = worker.listen_q.pop_blocking(wg, wid, timeout=1)
    assert payload is not None
    
    action = payload.get("action")
    handler = worker.handlers.get(action)
    result_payload = handler(payload)
    
    # 结果推送到结果队列
    worker.result_q.push(result_payload)
    # 确认消息
    worker.listen_q.ack(msg_context)
    
    # 验证提取到的结果
    assert result_payload["result"] == 40
    
    # 验证 result_q 收到数据 (result_q 作为一个普通 handle，也需要指定身份消费)
    # 对于测试，我们临时造一个身份
    res_wg, res_wid = "res_group", "res_worker"
    result_data, result_ctx = worker.result_q.pop_blocking(res_wg, res_wid, timeout=1)
    assert result_data["result"] == 40
    worker.result_q.ack(result_ctx)
