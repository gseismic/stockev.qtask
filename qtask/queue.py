import json
import redis
from typing import Tuple, Dict, Any, Optional
from .storage import RemoteStorage

class SmartQueue:
    """基于 Redis Stream 支持大对象自动卸载和 ACK 机制的队列"""
    
    def __init__(
        self, 
        redis_url: str, 
        queue_name: str, 
        worker_group: str = "default_group",
        worker_id: str = "default_worker", 
        storage: Optional[RemoteStorage] = None,
        large_threshold_bytes: int = 1024 * 50  # 默认 >50KB 存远程
    ):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.queue_name = f"{queue_name}:stream"
        self.worker_group = worker_group
        self.worker_id = worker_id
        self.dlq = f"{queue_name}:stream_dlq"
        
        self.storage = storage
        self.large_threshold_bytes = large_threshold_bytes
        
        # 确保消费组存在
        self._ensure_consumer_group()

    def _ensure_consumer_group(self):
        try:
            self.redis.xgroup_create(self.queue_name, self.worker_group, id="0-0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" not in str(e):
                raise

    def push(self, payload: Dict[str, Any]):
        """入队：自动评估体积，拦截大对象，推入 Stream"""
        data_bytes = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        
        if self.storage and len(data_bytes) > self.large_threshold_bytes:
            storage_key = self.storage.save_bytes(data_bytes)
            msg = json.dumps({"_qtask_large_payload": True, "storage_key": storage_key})
        else:
            msg = data_bytes.decode('utf-8')
            
        self.redis.xadd(self.queue_name, {"payload": msg})

    def pop_blocking(self, timeout: int = 0) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        """出队：使用消费组拉取数据，并自动还原大对象"""
        claimed_msg = self._claim_pending_msgs()
        if claimed_msg:
            return self._process_raw_msg(claimed_msg[0], claimed_msg[1])
            
        # 设置 block=timeout_ms (timeout 转换为毫秒)
        block_ms = timeout * 1000 if timeout > 0 else 0
        
        streams = {self.queue_name: ">"}
        messages = self.redis.xreadgroup(
            self.worker_group, self.worker_id, streams, count=1, block=block_ms
        )
        
        if not messages:
            return None, None
            
        stream_name, stream_msgs = messages[0]
        if not stream_msgs:
            return None, None
            
        msg_id, msg_data = stream_msgs[0]
        raw_payload = msg_data.get("payload")
        
        return self._process_raw_msg(msg_id, raw_payload)
        
    def _claim_pending_msgs(self, idle_time_ms: int = 300000) -> Optional[Tuple[str, str]]:
        """认领超过 idle_time_ms (默认5分钟) 未处理的死节点任务"""
        pending_info = self.redis.xpending_range(
            self.queue_name, self.worker_group, min="-", max="+", count=1
        )
        if not pending_info:
            return None
            
        first_pending = pending_info[0]
        if first_pending['time_since_delivered'] > idle_time_ms:
            # 尝试认领
            msg_id = first_pending['message_id']
            claimed = self.redis.xclaim(
                self.queue_name, self.worker_group, self.worker_id, idle_time_ms, [msg_id]
            )
            if claimed:
                return claimed[0][0], claimed[0][1].get("payload")
        
        return None

    def _process_raw_msg(self, msg_id: str, raw_payload: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        msg_context = {"msg_id": msg_id, "raw_payload": raw_payload}
        try:
            parsed = json.loads(raw_payload)
            if isinstance(parsed, dict) and parsed.get("_qtask_large_payload"):
                if not self.storage:
                    raise ValueError("检测到大对象，但未配置 Storage")
                real_data_str = self.storage.load(parsed["storage_key"])
                return json.loads(real_data_str), msg_context
            return parsed, msg_context
        except Exception as e:
            self._move_to_dlq(msg_context)
            return None, None

    def ack(self, msg_context: Dict[str, str]):
        """确认成功：ACK 消息，并清理关联的远程文件"""
        msg_id = msg_context["msg_id"]
        raw_payload = msg_context["raw_payload"]
        
        # 确认消息
        self.redis.xack(self.queue_name, self.worker_group, msg_id)
        
        # 清除大对象文件
        parsed = json.loads(raw_payload)
        if isinstance(parsed, dict) and parsed.get("_qtask_large_payload") and self.storage:
            self.storage.delete(parsed["storage_key"])

    def fail(self, msg_context: Dict[str, str]):
        """确认失败：移入死信队列并 ACK 原消息，以防止循环消费"""
        self._move_to_dlq(msg_context)

    def _move_to_dlq(self, msg_context: Dict[str, str]):
        msg_id = msg_context.get("msg_id")
        raw_payload = msg_context.get("raw_payload")
        
        # 将失败消息投递到死信 Stream
        self.redis.xadd(self.dlq, {"payload": raw_payload, "original_id": msg_id})
        # ACK 移出待处理列表，防止陷入死循环重试
        self.redis.xack(self.queue_name, self.worker_group, msg_id)