import pytest
import redis
from qtask.history import TaskHistoryStore

TEST_REDIS_URL = "redis://localhost:6379/0"
TEST_Q_NAME = "test_hist_q"

@pytest.fixture(autouse=True)
def clean_redis():
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    # 清理历史相关的 key
    # qtask:hist:{q}:* 和 qtask:hist_idx:{q}
    keys = r.keys(f"qtask:hist:{TEST_Q_NAME}:*")
    if keys:
        r.delete(*keys)
    r.delete(f"qtask:hist_idx:{TEST_Q_NAME}")
    r.delete(f"qtask:hist_stat:{TEST_Q_NAME}")
    yield
    keys = r.keys(f"qtask:hist:{TEST_Q_NAME}:*")
    if keys:
        r.delete(*keys)
    r.delete(f"qtask:hist_idx:{TEST_Q_NAME}")
    r.delete(f"qtask:hist_stat:{TEST_Q_NAME}")

def test_history_flow():
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    hist = TaskHistoryStore(r, TEST_Q_NAME)
    
    tid = "12345-0"
    payload = '{"action": "test"}'
    
    # 1. 记录入队
    hist.record_push(tid, action="test", payload_preview=payload)
    
    tasks = hist.get_tasks(status="pending")
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == tid
    assert tasks[0]["status"] == "pending"
    
    # 2. 记录成功
    hist.record_ack(tid, duration_s=1.23)
    
    # 原 pending 应该没了
    assert len(hist.get_tasks(status="pending")) == 0
    # completed 应该有一个
    completed = hist.get_tasks(status="completed")
    assert len(completed) == 1
    assert completed[0]["task_id"] == tid
    assert completed[0]["status"] == "completed"
    # 确认没有 worker_id 等冗余字段
    assert "worker_id" not in completed[0]

def test_history_fail_and_dlq_status():
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    hist = TaskHistoryStore(r, TEST_Q_NAME)
    
    tid = "fail-1"
    hist.record_push(tid, action="fail_test")
    hist.record_fail(tid, reason="test error")
    
    failed_tasks = hist.get_tasks(status="failed")
    assert len(failed_tasks) == 1
    assert failed_tasks[0]["status"] == "failed"
    assert failed_tasks[0]["fail_reason"] == "test error"

def test_count_by_status():
    r = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    hist = TaskHistoryStore(r, TEST_Q_NAME)
    
    hist.record_push("t1", action="a1")
    hist.record_push("t2", action="a2")
    hist.record_ack("t1")
    
    counts = hist.count_by_status()
    assert counts["completed"] == 1
    assert counts["pending"] == 1
    assert counts["total"] == 2
