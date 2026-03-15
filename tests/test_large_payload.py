import pytest
import requests
import time
import redis
from qtask import SmartQueue, RemoteStorage

TEST_REDIS_URL = "redis://localhost:6379/0"
TEST_STORAGE_URL = "http://localhost:8000"
TEST_Q_NAME = "test_large_q"

@pytest.fixture(scope="module")
def storage_server():
    # 假设测试运行时 storage_server 已经启动 (通常集成测试环境会预置)
    # 这里我们只检测是否可用
    try:
        resp = requests.get(f"{TEST_STORAGE_URL}/api/health", timeout=2)
        if resp.status_code != 200:
             pytest.skip("Storage server not healthy")
    except Exception:
        pytest.skip("Storage server not reachable")

@pytest.fixture(autouse=True)
def clean_redis():
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    r.delete(f"{TEST_Q_NAME}:stream", f"{TEST_Q_NAME}:stream_dlq")
    yield
    r.delete(f"{TEST_Q_NAME}:stream", f"{TEST_Q_NAME}:stream_dlq")

def test_large_payload_e2e(storage_server):
    storage = RemoteStorage(TEST_STORAGE_URL)
    # 设置极小的阈值强制卸载
    queue = SmartQueue(TEST_REDIS_URL, TEST_Q_NAME, storage=storage, large_threshold_bytes=100)
    
    wg, wid = "test_group", "test_worker"
    
    # 构建一个超过 100 字节的 payload
    large_data = {"data": "x" * 500, "action": "process_large"}
    
    # 1. Push
    msg_id = queue.push(large_data)
    assert msg_id is not None
    
    # 检查 Redis 中的原始数据，应该包含 _qtask_large_payload 标记
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    raw_msgs = r.xrange(f"{TEST_Q_NAME}:stream", min=msg_id, max=msg_id)
    assert "_qtask_large_payload" in raw_msgs[0][1]["payload"]
    
    # 2. Pop (应该自动还原)
    popped_data, msg_context = queue.pop_blocking(wg, wid, timeout=2)
    assert popped_data is not None
    assert popped_data["action"] == "process_large"
    assert len(popped_data["data"]) == 500
    
    # 3. Ack (清理由 SmartQueue 内部处理，依赖 storage_server 状态)
    queue.ack(msg_context)
