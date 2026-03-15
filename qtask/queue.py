try:
    import ujson as json
except ImportError:
    import json
import time
import redis
import socket
import uuid
from loguru import logger
from typing import Tuple, Dict, Any, Optional
from .storage import RemoteStorage
from .history import TaskHistoryStore

# Redis keys for namespace registry
_NS_SET_KEY = "qtask:namespaces"          # Set of all namespace names
_NS_QUEUES_KEY = "qtask:ns:{ns}:queues"  # Set of queue base-names in a namespace


class SmartQueue:
    """基于 Redis Stream 支持大对象自动卸载和 ACK 机制的队列

    namespace 参数：
        用于项目级分组（任务所属的命名空间）。指定后队列的 Redis Key 会以
        {namespace}: 为前缀，例如 namespace="proj_a", queue_name="spider:tasks"
        → Redis key: proj_a:spider:tasks:stream
        方便按项目统一查看和清除所有相关数据。
    """

    def __init__(
        self,
        redis_url: str,
        queue_name: str,
        storage: Optional[RemoteStorage] = None,
        large_threshold_bytes: int = 1024 * 50,
        auto_claim: bool = True,
        claim_interval: int = 300,
        history: Optional[TaskHistoryStore] = None,
        namespace: str = None,             # 项目/任务级命名空间
        history_store: bool = True,        # 默认自动开启历史记录
        redis_client: Optional[redis.Redis] = None, # 允许注入连接
    ):
        self.redis = redis_client if redis_client is not None else redis.from_url(redis_url, decode_responses=True)
        self.namespace = namespace or ""
        self._base_queue_name = queue_name  # 原始队列名（不含 ns 前缀）

        # 实际 Redis key 前缀
        _prefixed = f"{self.namespace}:{queue_name}" if self.namespace else queue_name
        self.queue_name = f"{_prefixed}:stream"
        self._base_name = _prefixed         # 用于构造 DLQ 等
        self.dlq = f"{_prefixed}:stream_dlq"

        # 注册 namespace 元数据（非阻塞失败不影响主流程）
        if self.namespace:
            self._register_namespace()

        self.storage = storage
        self.large_threshold_bytes = large_threshold_bytes
        self.auto_claim = auto_claim
        self.claim_interval = claim_interval
        self._last_claim_check = 0
        self._cleanup_counter = 0

        if history:
            self.history = history
        elif history_store:
            from .history import TaskHistoryStore
            self.history = TaskHistoryStore(self.redis, self._base_name)
        else:
            self.history = None

    def _register_namespace(self):
        """在 Redis 中注册 namespace 和队列的映射关系"""
        try:
            pipe = self.redis.pipeline()
            pipe.sadd(_NS_SET_KEY, self.namespace)
            pipe.sadd(_NS_QUEUES_KEY.format(ns=self.namespace), self._base_queue_name)
            pipe.execute()
        except Exception as e:
            logger.warning(f"Namespace registration failed (non-critical): {e}")

    def _ensure_group(self, group_name: str):
        """确保消费组存在"""
        try:
            self.redis.xgroup_create(self.queue_name, group_name, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "already exists" in str(e):
                return
            if "no such key" in str(e).lower() or "NOGROUP" in str(e):
                # 如果 mkstream=True 依然报错（通常是因为 Redis 版本或特殊配置）
                # 手动通过 XADD 创建流再建组
                self.redis.xadd(self.queue_name, {"_qtask_init": "1"}, id="*")
                self.redis.xgroup_create(self.queue_name, group_name, id="0", mkstream=True)
            else:
                raise

    def push(self, payload: Dict[str, Any]) -> str:
        """入队：自动评估体积，拦截大对象，推推入 Stream，返回 msg_id"""
        payload_json = json.dumps(payload, ensure_ascii=False)
        data_bytes = payload_json.encode('utf-8')
        data_len = len(data_bytes)

        if self.storage and data_len > self.large_threshold_bytes:
            storage_key = self.storage.save_bytes(data_bytes)
            logger.info(f"Intercepted large payload ({data_len / 1024:.2f} KB). Offloaded to storage key: {storage_key}.")
            msg = json.dumps({"_qtask_large_payload": True, "storage_key": storage_key})
        else:
            msg = payload_json

        msg_id = self.redis.xadd(self.queue_name, {"payload": msg})

        if self.history:
            try:
                action = payload.get("action", "unknown")
                preview = payload_json[:200]
                self.history.record_push(msg_id, action, preview)
                
                # 降频调用清理逻辑（每 100 次入队执行一次）
                self._cleanup_counter += 1
                if self._cleanup_counter % 100 == 0:
                    self.history.cleanup_old()
            except Exception as e:
                logger.warning(f"History record_push failed (non-critical): {e}")

        return msg_id

    def pop_blocking(self, worker_group: str, worker_id: str, timeout: int = 10) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        """出队：使用消费组拉取数据"""
        self._ensure_group(worker_group)

        # 1. 认领 Zombie 任务
        if self.auto_claim:
            now = time.time()
            if now - self._last_claim_check >= self.claim_interval:
                self._last_claim_check = now
                claimed_msg = self._claim_pending_msgs(worker_group, worker_id)
                if claimed_msg:
                    return self._process_raw_msg(claimed_msg[0], claimed_msg[1], worker_group=worker_group)

        # 2. 拉取新消息
        block_ms = timeout * 1000 if timeout > 0 else 0
        streams = {self.queue_name: ">"}
        
        # 使用单个 XREADGROUP 调用替代 while True 循环，
        # 并通过内部消息处理来维持阻塞行为。
        while True:
            try:
                messages = self.redis.xreadgroup(
                    worker_group, worker_id, streams, count=1, block=block_ms
                )
            except redis.exceptions.ResponseError as e:
                if "NOGROUP" in str(e):
                    self._ensure_group(worker_group)
                    continue
                raise

            if not messages:
                # 如果是非阻塞模式且没有消息，或者阻塞超时，则返回 None
                return None, None

            msg_id, msg_data = messages[0][1][0]
            # 过滤内部初始化消息
            if "_qtask_init" in msg_data:
                self.redis.xack(self.queue_name, worker_group, msg_id)
                # 如果是阻塞模式，继续循环以等待真实消息
                if timeout > 0:
                    continue
                else:
                    # 如果是非阻塞模式，且只收到 init 消息，则返回 None
                    return None, None
            
            raw_payload = msg_data.get("payload")
            return self._process_raw_msg(msg_id, raw_payload, worker_group=worker_group)

    def _claim_pending_msgs(self, worker_group: str, worker_id: str, idle_time_ms: int = 300000) -> Optional[Tuple[str, str]]:
        """认领僵尸消息。优化：批量扫描并批量认领，提高效率。"""
        # 虽然 Redis 6.0 没有 XAUTOCLAIM，但我们可以通过脚本或批量 XCLAIM 模拟
        pending_info = self.redis.xpending_range(
            self.queue_name, worker_group, min="-", max="+", count=100
        )
        if not pending_info:
            return None

        for pending in pending_info:
            if pending['time_since_delivered'] > idle_time_ms:
                msg_id = pending['message_id']
                # 认领单条并在此立即返回（Worker 每次只处理一条）
                # 这种方式最符合 Worker 的消费循环
                claimed = self.redis.xclaim(
                    self.queue_name, worker_group, worker_id, idle_time_ms, [msg_id]
                )
                if claimed:
                    if self.history:
                        try:
                            self.history.record_retry(msg_id)
                        except Exception:
                            pass
                    return claimed[0][0], claimed[0][1].get("payload")
        return None

    def claim_all(self, worker_group: str, worker_id: str, idle_time_ms: int = 300000) -> int:
        count = 0
        while True:
            pending_info = self.redis.xpending_range(
                self.queue_name, worker_group, min="-", max="+", count=100
            )
            if not pending_info:
                break

            claimed_ids = [p['message_id'] for p in pending_info if p['time_since_delivered'] > idle_time_ms]
            if not claimed_ids:
                break

            # 批量认领
            claimed = self.redis.xclaim(
                self.queue_name, worker_group, worker_id, idle_time_ms, claimed_ids
            )
            if self.history:
                for c in claimed:
                    try:
                        self.history.record_retry(c[0])
                    except Exception:
                        pass
            count += len(claimed)
            if len(pending_info) < 100:
                break
        return count

    def reset_group(self, worker_group: str):
        try:
            self.redis.xgroup_setid(self.queue_name, worker_group, id="$")
            return True
        except Exception as e:
            logger.error(f"Reset group failed: {e}")
            return False

    def clear_all(self, worker_group: str = "", clear_history: bool = False):
        """
        清空队列（Stream + DLQ）。
        clear_history=True 时同时删除该队列的所有历史记录
        （qtask:hist:{base}:* 和 qtask:hist_idx:{base}），不留任何残留。
        """
        try:
            pipe = self.redis.pipeline()
            pipe.delete(self.queue_name)
            pipe.delete(self.dlq)
            pipe.execute()
            
            if worker_group:
                self._ensure_group(worker_group)
            if clear_history:
                self._purge_history_for_base(self._base_name)
            return True
        except Exception as e:
            logger.error(f"Clear queue failed: {e}")
            return False

    def _purge_history_for_base(self, base_name: str):
        """删除单个队列的所有历史 key（Hash + SortedSet + Stat Hash）"""
        try:
            pipe = self.redis.pipeline()
            # 1. 删除 SortedSet（时间索引）和 Stat Hash
            pipe.delete(f"qtask:hist_idx:{base_name}")
            pipe.delete(f"qtask:hist_stat:{base_name}")
            pipe.execute()
            
            # 2. 删除所有任务 Hash（scan 防止阻塞）
            pattern = f"qtask:hist:{base_name}:*"
            cursor = 0
            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=200)
                if keys:
                    self.redis.delete(*keys)
                if cursor == 0:
                    break
            logger.debug(f"History purged for {base_name}")
        except Exception as e:
            logger.warning(f"_purge_history_for_base failed (non-critical): {e}")

    def _process_raw_msg(self, msg_id: str, raw_payload: str, worker_group: str = "") -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        msg_context = {"msg_id": msg_id, "raw_payload": raw_payload, "_pop_ts": time.time(), "worker_group": worker_group}
        try:
            parsed = json.loads(raw_payload)
            if isinstance(parsed, dict) and parsed.get("_qtask_large_payload"):
                if not self.storage:
                    raise ValueError("检测到大对象，但未配置 Storage")
                real_data_str = self.storage.load(parsed["storage_key"])
                logger.info(f"[{msg_id}] Loaded large payload from remote storage.")
                return json.loads(real_data_str), msg_context
            return parsed, msg_context
        except Exception as e:
            logger.error(f"Failed to process raw message {msg_id}: {e}")
            self._move_to_dlq(msg_context, worker_group=worker_group, reason=str(e))
            return None, None

    def ack(self, msg_context: Dict[str, str]):
        msg_id = msg_context["msg_id"]
        raw_payload = msg_context["raw_payload"]
        worker_group = msg_context.get("worker_group", "")
        pop_ts = msg_context.get("_pop_ts", time.time())

        if not worker_group:
            raise ValueError("ACK requires worker_group in msg_context")

        try:
            self.redis.xack(self.queue_name, worker_group, msg_id)
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                self._ensure_group(worker_group)
                self.redis.xack(self.queue_name, worker_group, msg_id)
            else:
                raise

        try:
            parsed = json.loads(raw_payload)
            if isinstance(parsed, dict) and parsed.get("_qtask_large_payload") and self.storage:
                self.storage.delete(parsed["storage_key"])
        except Exception:
            pass

        if self.history:
            try:
                self.history.record_ack(msg_id, time.time() - pop_ts)
            except Exception as e:
                logger.warning(f"History record_ack failed (non-critical): {e}")

    def fail(self, msg_context: Dict[str, str], reason: str = ""):
        worker_group = msg_context.get("worker_group", "")
        self._move_to_dlq(msg_context, reason=reason, worker_group=worker_group)

    def _move_to_dlq(self, msg_context: Dict[str, str], reason: str = "", worker_group: str = ""):
        msg_id = msg_context.get("msg_id")
        raw_payload = msg_context.get("raw_payload")
        if not msg_id:
            return
        
        target_group = worker_group or msg_context.get("worker_group", "")

        try:
            # DLQ 消息中增加更多上下文
            dlq_payload = {
                "payload": raw_payload,
                "original_id": msg_id,
                "fail_reason": reason[:1000],
                "failed_at": time.time(),
                "worker_group": target_group
            }
            self.redis.xadd(self.dlq, {"payload": json.dumps(dlq_payload, ensure_ascii=False)})
            
            if target_group:
                self.redis.xack(self.queue_name, target_group, msg_id)
        except Exception as e:
            logger.error(f"Failed to move message {msg_id} to DLQ: {e}")

        if self.history:
            try:
                self.history.record_fail(msg_id, reason or "moved to DLQ")
            except Exception as e:
                logger.warning(f"History record_fail failed (non-critical): {e}")

    @staticmethod
    def list_namespaces(redis_client) -> list:
        """列出 Redis 中注册的所有 namespace"""
        try:
            return sorted(redis_client.smembers(_NS_SET_KEY))
        except Exception:
            return []

    @staticmethod
    def get_namespace_queues(redis_client, namespace: str) -> list:
        """获取某 namespace 下注册的所有队列名（不含前缀）"""
        try:
            return sorted(redis_client.smembers(_NS_QUEUES_KEY.format(ns=namespace)))
        except Exception:
            return []

    @staticmethod
    def purge_namespace(redis_client, namespace: str) -> Dict[str, int]:
        """
        危险操作：彻底清除 namespace 下的所有数据。
        涵盖：Stream（含 DLQ）、历史记录 Hash、历史时间索引 SortedSet、状态计数器、注册元数据。
        """
        deleted = {"stream_keys": 0, "history_keys": 0, "meta_keys": 0}
        try:
            def _scan_delete(pattern):
                """扫描并批量删除匹配 pattern 的 key，返回删除数量"""
                cursor = 0
                total = 0
                while True:
                    cursor, keys = redis_client.scan(cursor, match=pattern, count=200)
                    if keys:
                        redis_client.delete(*keys)
                        total += len(keys)
                    if cursor == 0:
                        break
                return total

            # 1. Stream + DLQ key：{ns}:*
            deleted["stream_keys"] = _scan_delete(f"{namespace}:*")

            # 2. 历史 Hash：qtask:hist:{ns}:*
            n_hist = _scan_delete(f"qtask:hist:{namespace}:*")

            # 3. 历史时间索引和状态计数器：qtask:hist_idx:{ns}:* 和 qtask:hist_stat:{ns}:*
            idx_pattern = f"qtask:hist_idx:{namespace}:*"
            stat_pattern = f"qtask:hist_stat:{namespace}:*"
            n_idx = _scan_delete(idx_pattern)
            n_stat = _scan_delete(stat_pattern)

            deleted["history_keys"] = n_hist + n_idx + n_stat

            # 4. namespace 注册元数据
            pipe = redis_client.pipeline()
            pipe.delete(_NS_QUEUES_KEY.format(ns=namespace))
            pipe.srem(_NS_SET_KEY, namespace)
            pipe.execute()
            deleted["meta_keys"] = 1

        except Exception as e:
            logger.error(f"purge_namespace failed: {e}")
        return deleted