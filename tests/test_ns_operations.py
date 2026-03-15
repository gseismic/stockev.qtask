import pytest
import redis
from qtask import SmartQueue

TEST_REDIS_URL = "redis://localhost:6379/0"

@pytest.fixture(autouse=True)
def clean_redis():
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    # 清理测试可能用到的 ns
    SmartQueue.purge_namespace(r, "ns1")
    SmartQueue.purge_namespace(r, "ns2")
    yield
    SmartQueue.purge_namespace(r, "ns1")
    SmartQueue.purge_namespace(r, "ns2")

def test_namespace_isolation():
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    
    q1 = SmartQueue(TEST_REDIS_URL, "tasks", namespace="ns1")
    q2 = SmartQueue(TEST_REDIS_URL, "tasks", namespace="ns2")
    
    q1.push({"data": "ns1_task"})
    q2.push({"data": "ns2_task"})
    
    # 确认 Redis Key 物理隔离
    assert r.exists("ns1:tasks:stream")
    assert r.exists("ns2:tasks:stream")
    
    # 消费隔离
    wg, wid = "g", "w"
    msg1, _ = q1.pop_blocking(wg, wid, timeout=1)
    msg2, _ = q2.pop_blocking(wg, wid, timeout=1)
    
    assert msg1["data"] == "ns1_task"
    assert msg2["data"] == "ns2_task"

def test_purge_namespace():
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    q = SmartQueue(TEST_REDIS_URL, "tasks", namespace="ns1")
    q.push({"data": "to_be_purged"})
    
    # 模拟产生一些历史
    wg, wid = "g", "w"
    p, ctx = q.pop_blocking(wg, wid, timeout=1)
    q.ack(ctx)
    
    # 执行 Purge
    stats = SmartQueue.purge_namespace(r, "ns1")
    assert stats["stream_keys"] > 0
    assert stats["history_keys"] > 0
    
    # 检查 Key 是否由于 purge 消失
    assert not r.exists("ns1:tasks:stream")
    assert not r.exists("qtask:hist:ns1:tasks:idx") # 实际是 qtask:hist_idx:ns1:tasks
    # 或者通配检查所有 ns1 前缀
    assert len(r.keys("ns1:*")) == 0
    assert len(r.keys("qtask:hist:ns1:*")) == 0
    assert len(r.keys("qtask:hist_idx:ns1:*")) == 0
