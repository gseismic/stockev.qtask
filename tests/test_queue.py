import pytest
import time
import redis
from qtask import SmartQueue

# Use a specific test queue name to avoid clashing with real data
TEST_REDIS_URL = "redis://localhost:6379/0"
TEST_QUEUE_NAME = "test_queue"

@pytest.fixture(autouse=True)
def clean_redis():
    """清理测试使用的 Redis 键"""
    client = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    client.delete(f"{TEST_QUEUE_NAME}:stream", f"{TEST_QUEUE_NAME}:stream_dlq")
    yield
    client.delete(f"{TEST_QUEUE_NAME}:stream", f"{TEST_QUEUE_NAME}:stream_dlq")

def test_push_and_pop():
    """测试基本的入队和出队逻辑"""
    queue = SmartQueue(TEST_REDIS_URL, TEST_QUEUE_NAME)
    wg, wid = "test_group", "test_worker_1"
    
    payload = {"action": "test_action", "data": "hello"}
    queue.push(payload)
    
    popped_payload, msg_context = queue.pop_blocking(wg, wid, timeout=1)
    
    assert popped_payload is not None
    assert popped_payload["action"] == "test_action"
    assert popped_payload["data"] == "hello"
    
    assert msg_context is not None
    assert "msg_id" in msg_context
    assert "raw_payload" in msg_context
    assert msg_context["worker_group"] == wg
    
    # ACK
    queue.ack(msg_context)
    
    # 确认队列已为空
    empty_payload, _ = queue.pop_blocking(wg, wid, timeout=1)
    assert empty_payload is None

def test_pending_claim():
    """测试挂起任务认领逻辑"""
    queue = SmartQueue(TEST_REDIS_URL, TEST_QUEUE_NAME)
    wg = "test_group"
    wid1, wid2 = "dead_worker", "alive_worker"
    
    queue.push({"action": "claim_test"})
    
    # wid1 弹出任务但不 ACK (模拟崩溃)
    payload, msg_context = queue.pop_blocking(wg, wid1, timeout=1)
    assert payload is not None
    
    # 立即用 wid2 去尝试认领，因为时间太短，不可能认领成功
    payload_early, _ = queue.pop_blocking(wg, wid2, timeout=1)
    assert payload_early is None
    
    # 等待时间直接从内部私有方法修改条件，强制认领 (测试只改用小超时参数)
    claimed_msg = queue._claim_pending_msgs(wg, wid2, idle_time_ms=0)
    assert claimed_msg is not None
    assert "claim_test" in claimed_msg[1]

def test_dlq_on_fail():
    """测试任务失败后进入死信队列"""
    queue = SmartQueue(TEST_REDIS_URL, TEST_QUEUE_NAME)
    wg, wid = "test_group", "test_worker"
    queue.push({"action": "fail_test"})
    
    payload, msg_context = queue.pop_blocking(wg, wid, timeout=1)
    
    # 主动调用 fail
    queue.fail(msg_context)
    
    # 检查流中是否还有这个任务（应该没有，也就是不应该被重复消费）
    payload2, msg_context2 = queue.pop_blocking(wg, wid, timeout=1)
    assert payload2 is None
    
    # 检查死信队列是否有数据
    client = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    dlq_data = client.xread({f"{TEST_QUEUE_NAME}:stream_dlq": "0-0"})
    assert len(dlq_data) > 0
