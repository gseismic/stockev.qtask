import json
import redis
from loguru import logger
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
        large_threshold_bytes: int = 1024 * 50,  # 默认 >50KB 存远程
        auto_claim: bool = True,
        claim_interval: int = 300  # 自动认领检查的最小间隔(秒)
    ):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.queue_name = f"{queue_name}:stream"
        self.worker_group = worker_group
        self.worker_id = worker_id
        self.dlq = f"{queue_name}:stream_dlq"
        
        self.storage = storage
        self.large_threshold_bytes = large_threshold_bytes
        self.auto_claim = auto_claim
        self.claim_interval = claim_interval
        self._last_claim_check = 0
        
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
        data_len = len(data_bytes)
        
        if self.storage and data_len > self.large_threshold_bytes:
            storage_key = self.storage.save_bytes(data_bytes)
            logger.info(f"Intercepted large payload ({data_len / 1024:.2f} KB). Offloaded to storage with key: {storage_key}.")
            msg = json.dumps({"_qtask_large_payload": True, "storage_key": storage_key})
        else:
            msg = data_bytes.decode('utf-8')
            
        self.redis.xadd(self.queue_name, {"payload": msg})

    def pop_blocking(self, timeout: int = 0) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        """出队：使用消费组拉取数据，如果在 auto_claim 且到达检查时间则做一次抢占"""
        import time
        if self.auto_claim:
            now = time.time()
            if now - self._last_claim_check >= self.claim_interval:
                self._last_claim_check = now
                claimed_msg = self._claim_pending_msgs()
                if claimed_msg:
                    return self._process_raw_msg(claimed_msg[0], claimed_msg[1])
            
        # 设置 block=timeout_ms (timeout 转换为毫秒)
        block_ms = timeout * 1000 if timeout > 0 else 0
        
        streams = {self.queue_name: ">"}
        try:
            messages = self.redis.xreadgroup(
                self.worker_group, self.worker_id, streams, count=1, block=block_ms
            )
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                logger.warning(f"Consumer group {self.worker_group} missing. Attempting recovery...")
                self._ensure_consumer_group()
                return self.pop_blocking(timeout)
            raise
        
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

    def claim_all(self, idle_time_ms: int = 300000) -> int:
        """管理员命令：一次性大批量认领所有超时的遗留节点任务"""
        count = 0
        while True:
            pending_info = self.redis.xpending_range(
                self.queue_name, self.worker_group, min="-", max="+", count=100
            )
            if not pending_info:
                break
                
            claimed_ids = []
            for p in pending_info:
                if p['time_since_delivered'] > idle_time_ms:
                    claimed_ids.append(p['message_id'])
                    
            if not claimed_ids:
                break
                
            claimed = self.redis.xclaim(
                self.queue_name, self.worker_group, self.worker_id, idle_time_ms, claimed_ids
            )
            count += len(claimed)
            
            if len(pending_info) < 100:
                break
        return count

    def reset_group(self):
        """管理员命令：将消费组游标重置为最新$，丢弃堆积未读"""
        try:
            self.redis.xgroup_setid(self.queue_name, self.worker_group, id="$")
            return True
        except Exception as e:
            logger.error(f"Reset group failed: {e}")
            return False

    def clear_all(self):
        """管理员命令：排空主 Stream 和 DLQ 内所有数据 (保留组定义)"""
        try:
            # XTRIM MAXLEN 0 可以清空内容但保留 Stream 结构与 Groups
            self.redis.xtrim(self.queue_name, maxlen=0)
            self.redis.xtrim(self.dlq, maxlen=0)
            # 还是跑一下 ensure 确保万一没流时能创建好
            self._ensure_consumer_group()
            return True
        except Exception as e:
            logger.error(f"Clear queue failed: {e}")
            return False

    def _process_raw_msg(self, msg_id: str, raw_payload: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        msg_context = {"msg_id": msg_id, "raw_payload": raw_payload}
        try:
            parsed = json.loads(raw_payload)
            if isinstance(parsed, dict) and parsed.get("_qtask_large_payload"):
                if not self.storage:
                    logger.error("Detected large payload but RemoteStorage is not assigned.")
                    raise ValueError("检测到大对象，但未配置 Storage")
                real_data_str = self.storage.load(parsed["storage_key"])
                logger.info(f"[{msg_id}] Loaded large payload from remote storage successfully.")
                return json.loads(real_data_str), msg_context
            return parsed, msg_context
        except Exception as e:
            logger.error(f"Failed to process raw message {msg_id}: {e}")
            self._move_to_dlq(msg_context)
            return None, None

    def ack(self, msg_context: Dict[str, str]):
        """确认成功：ACK 消息，并清理关联的远程文件"""
        msg_id = msg_context["msg_id"]
        raw_payload = msg_context["raw_payload"]
        
        # 确认消息
        try:
            self.redis.xack(self.queue_name, self.worker_group, msg_id)
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                self._ensure_consumer_group()
                self.redis.xack(self.queue_name, self.worker_group, msg_id)
            else:
                raise
        
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
        
        if not msg_id:
            return

        try:
            # 将失败消息投递到死信 Stream
            self.redis.xadd(self.dlq, {"payload": raw_payload, "original_id": msg_id})
            # ACK 移出待处理列表，防止陷入死循环重试
            self.redis.xack(self.queue_name, self.worker_group, msg_id)
        except Exception as e:
            logger.error(f"Failed to move message {msg_id} to DLQ: {e}")