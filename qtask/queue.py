import json
import redis
from typing import Tuple, Dict, Any, Optional
from .storage import RemoteStorage

class SmartQueue:
    """支持大对象自动卸载和 ACK 机制的队列"""
    
    def __init__(
        self, 
        redis_url: str, 
        queue_name: str, 
        worker_id: str = "default", 
        storage: Optional[RemoteStorage] = None,
        large_threshold_bytes: int = 1024 * 50  # 默认 >50KB 存远程
    ):
        # decode_responses=True 确保 redis 返回 str 而非 bytes
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.queue_name = queue_name
        self.pending_q = f"{queue_name}:pending"
        self.processing_q = f"{queue_name}:processing:{worker_id}"
        self.dlq = f"{queue_name}:dead_letter"
        
        self.storage = storage
        self.large_threshold_bytes = large_threshold_bytes

    def push(self, payload: Dict[str, Any]):
        """入队：自动评估体积，拦截大对象"""
        data_str = json.dumps(payload)
        
        if self.storage and len(data_str.encode('utf-8')) > self.large_threshold_bytes:
            storage_key = self.storage.save(data_str)
            msg = json.dumps({"_qtask_large_payload": True, "storage_key": storage_key})
        else:
            msg = data_str
            
        self.redis.lpush(self.pending_q, msg) # 使用 lpush 配合 brpoplpush 实现 FIFO

    def pop_blocking(self, timeout: int = 0) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """出队：安全转移至 processing 队列，并自动还原大对象"""
        raw_msg = self.redis.brpoplpush(self.pending_q, self.processing_q, timeout=timeout)
        if not raw_msg:
            return None, None
            
        try:
            parsed = json.loads(raw_msg)
            if isinstance(parsed, dict) and parsed.get("_qtask_large_payload"):
                if not self.storage:
                    raise ValueError("检测到大对象，但未配置 Storage")
                real_data_str = self.storage.load(parsed["storage_key"])
                return json.loads(real_data_str), raw_msg
            return parsed, raw_msg
        except Exception as e:
            self._move_to_dlq(raw_msg)
            return None, None

    def ack(self, raw_msg: str):
        """确认成功：移除处理中状态，并清理关联的远程文件"""
        self.redis.lrem(self.processing_q, 1, raw_msg)
        
        parsed = json.loads(raw_msg)
        if isinstance(parsed, dict) and parsed.get("_qtask_large_payload") and self.storage:
            self.storage.delete(parsed["storage_key"])

    def fail(self, raw_msg: str):
        """确认失败：移入死信队列"""
        self._move_to_dlq(raw_msg)

    def _move_to_dlq(self, raw_msg: str):
        self.redis.lrem(self.processing_q, 1, raw_msg)
        self.redis.lpush(self.dlq, raw_msg)