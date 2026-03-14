import json
import time
import redis
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
        可在多台主机的多个 Worker 上使用同一 namespace，worker_id/worker_group
        仍用于区分不同的 Worker 节点进程。
    """

    def __init__(
        self,
        redis_url: str,
        queue_name: str,
        worker_group: str = None,          # None 时按 namespace 自动生成
        worker_id: str = "default_worker",
        storage: Optional[RemoteStorage] = None,
        large_threshold_bytes: int = 1024 * 50,
        auto_claim: bool = True,
        claim_interval: int = 300,
        history: Optional[TaskHistoryStore] = None,
        namespace: str = None,             # 项目/任务级命名空间
        history_store: bool = False,       # 快捷开启历史记录
    ):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.namespace = namespace or ""
        self._base_queue_name = queue_name  # 原始队列名（不含 ns 前缀）

        # 实际 Redis key 前缀
        _prefixed = f"{self.namespace}:{queue_name}" if self.namespace else queue_name
        self.queue_name = f"{_prefixed}:stream"
        self._base_name = _prefixed         # 用于构造 DLQ 等
        self.dlq = f"{_prefixed}:stream_dlq"

        # worker_group 默认值：有 namespace 时用 {ns}_group，否则 default_group
        if worker_group is None:
            worker_group = f"{self.namespace}_group" if self.namespace else "default_group"
        self.worker_group = worker_group
        self.worker_id = worker_id

        self.storage = storage
        self.large_threshold_bytes = large_threshold_bytes
        self.auto_claim = auto_claim
        self.claim_interval = claim_interval
        self._last_claim_check = 0

        if history:
            self.history = history
        elif history_store:
            from .history import TaskHistoryStore
            self.history = TaskHistoryStore(self.redis, self._base_name)
        else:
            self.history = None

        self._ensure_consumer_group()
        # 注册 namespace 元数据（非阻塞失败不影响主流程）
        if self.namespace:
            self._register_namespace()

    def _register_namespace(self):
        """在 Redis 中注册 namespace 和队列的映射关系"""
        try:
            pipe = self.redis.pipeline()
            pipe.sadd(_NS_SET_KEY, self.namespace)
            pipe.sadd(_NS_QUEUES_KEY.format(ns=self.namespace), self._base_queue_name)
            pipe.execute()
        except Exception as e:
            logger.warning(f"Namespace registration failed (non-critical): {e}")

    def _ensure_consumer_group(self):
        try:
            self.redis.xgroup_create(self.queue_name, self.worker_group, id="0-0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" not in str(e):
                raise

    def push(self, payload: Dict[str, Any]) -> str:
        """入队：自动评估体积，拦截大对象，推入 Stream，返回 msg_id"""
        data_bytes = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        data_len = len(data_bytes)

        if self.storage and data_len > self.large_threshold_bytes:
            storage_key = self.storage.save_bytes(data_bytes)
            logger.info(f"Intercepted large payload ({data_len / 1024:.2f} KB). Offloaded to storage key: {storage_key}.")
            msg = json.dumps({"_qtask_large_payload": True, "storage_key": storage_key})
        else:
            msg = data_bytes.decode('utf-8')

        msg_id = self.redis.xadd(self.queue_name, {"payload": msg})

        if self.history:
            try:
                action = payload.get("action", "unknown")
                preview = json.dumps(payload, ensure_ascii=False)
                self.history.record_push(msg_id, action, preview)
                self.history.cleanup_old()
            except Exception as e:
                logger.warning(f"History record_push failed (non-critical): {e}")

        return msg_id

    def pop_blocking(self, timeout: int = 0) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        """出队：使用消费组拉取数据"""
        if self.auto_claim:
            now = time.time()
            if now - self._last_claim_check >= self.claim_interval:
                self._last_claim_check = now
                claimed_msg = self._claim_pending_msgs()
                if claimed_msg:
                    return self._process_raw_msg(claimed_msg[0], claimed_msg[1])

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
        pending_info = self.redis.xpending_range(
            self.queue_name, self.worker_group, min="-", max="+", count=1
        )
        if not pending_info:
            return None

        first_pending = pending_info[0]
        if first_pending['time_since_delivered'] > idle_time_ms:
            msg_id = first_pending['message_id']
            claimed = self.redis.xclaim(
                self.queue_name, self.worker_group, self.worker_id, idle_time_ms, [msg_id]
            )
            if claimed:
                if self.history:
                    try:
                        self.history.record_retry(msg_id)
                    except Exception:
                        pass
                return claimed[0][0], claimed[0][1].get("payload")
        return None

    def claim_all(self, idle_time_ms: int = 300000) -> int:
        count = 0
        while True:
            pending_info = self.redis.xpending_range(
                self.queue_name, self.worker_group, min="-", max="+", count=100
            )
            if not pending_info:
                break

            claimed_ids = [p['message_id'] for p in pending_info if p['time_since_delivered'] > idle_time_ms]
            if not claimed_ids:
                break

            claimed = self.redis.xclaim(
                self.queue_name, self.worker_group, self.worker_id, idle_time_ms, claimed_ids
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

    def reset_group(self):
        try:
            self.redis.xgroup_setid(self.queue_name, self.worker_group, id="$")
            return True
        except Exception as e:
            logger.error(f"Reset group failed: {e}")
            return False

    def clear_all(self, clear_history: bool = False):
        """
        清空队列（Stream + DLQ）。
        clear_history=True 时同时删除该队列的所有历史记录
        （qtask:hist:{base}:* 和 qtask:hist_idx:{base}），不留任何残留。
        """
        try:
            self.redis.xtrim(self.queue_name, maxlen=0)
            self.redis.xtrim(self.dlq, maxlen=0)
            self._ensure_consumer_group()
            if clear_history:
                self._purge_history_for_base(self._base_name)
            return True
        except Exception as e:
            logger.error(f"Clear queue failed: {e}")
            return False

    def _purge_history_for_base(self, base_name: str):
        """删除单个队列的所有历史 key（Hash + SortedSet）"""
        try:
            # 1. 删除 SortedSet（时间索引）
            self.redis.delete(f"qtask:hist_idx:{base_name}")
            # 2. 删除所有任务 Hash（scan 防止阻塞）
            pattern = f"qtask:hist:{base_name}:*"
            cursor = 0
            keys = []
            while True:
                cursor, batch = self.redis.scan(cursor, match=pattern, count=200)
                keys.extend(batch)
                if cursor == 0:
                    break
            if keys:
                self.redis.delete(*keys)
            logger.debug(f"History purged for {base_name}: idx + {len(keys)} hash keys")
        except Exception as e:
            logger.warning(f"_purge_history_for_base failed (non-critical): {e}")

    def _process_raw_msg(self, msg_id: str, raw_payload: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        msg_context = {"msg_id": msg_id, "raw_payload": raw_payload, "_pop_ts": time.time()}
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
            self._move_to_dlq(msg_context)
            return None, None

    def ack(self, msg_context: Dict[str, str]):
        msg_id = msg_context["msg_id"]
        raw_payload = msg_context["raw_payload"]
        pop_ts = msg_context.get("_pop_ts", time.time())

        try:
            self.redis.xack(self.queue_name, self.worker_group, msg_id)
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                self._ensure_consumer_group()
                self.redis.xack(self.queue_name, self.worker_group, msg_id)
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
        self._move_to_dlq(msg_context, reason=reason)

    def _move_to_dlq(self, msg_context: Dict[str, str], reason: str = ""):
        msg_id = msg_context.get("msg_id")
        raw_payload = msg_context.get("raw_payload")
        if not msg_id:
            return
        try:
            self.redis.xadd(self.dlq, {"payload": raw_payload, "original_id": msg_id})
            self.redis.xack(self.queue_name, self.worker_group, msg_id)
        except Exception as e:
            logger.error(f"Failed to move message {msg_id} to DLQ: {e}")

        if self.history:
            try:
                self.history.record_retry(msg_id)
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
        涵盖：Stream（含 DLQ）、历史记录 Hash、历史时间索引 SortedSet、注册元数据。
        返回各类被删除 key 数量的统计字典。
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

            # 2. 历史 Hash：qtask:hist:{ns}:*  (包含每条任务的 Hash)
            n_hist = _scan_delete(f"qtask:hist:{namespace}:*")

            # 3. 历史时间索引 SortedSet：qtask:hist_idx:{ns}:*  ← 关键补漏
            n_idx = _scan_delete(f"qtask:hist_idx:{namespace}:*")

            deleted["history_keys"] = n_hist + n_idx

            # 4. namespace 注册元数据
            redis_client.delete(_NS_QUEUES_KEY.format(ns=namespace))
            redis_client.srem(_NS_SET_KEY, namespace)
            deleted["meta_keys"] = 1

        except Exception as e:
            logger.error(f"purge_namespace failed: {e}")
        return deleted