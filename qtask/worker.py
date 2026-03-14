import uuid
import traceback
import time
import redis as redis_lib
from loguru import logger
from typing import Callable, Optional
from .queue import SmartQueue
from .storage import RemoteStorage
from .history import TaskHistoryStore

class Worker:
    """任务执行调度器

    namespace 参数：
        任务/项目级命名空间，与 worker_id/worker_group 正交。
        namespace 标识"这批任务属于哪个项目"，
        worker_id/worker_group 标识"哪台机器/哪个进程在处理任务"。
        多台主机的多个 Worker 可以共享同一 namespace，
        但拥有不同的 worker_id（默认自动生成唯一 ID）。
    """

    def __init__(
        self,
        listen_url: str,
        listen_q_name: str,
        result_url: Optional[str] = None,
        result_q_name: Optional[str] = None,
        storage_url: Optional[str] = None,
        worker_group: str = None,          # None → 由 SmartQueue 根据 namespace 自动生成
        worker_id: Optional[str] = None,
        auto_claim: bool = True,
        claim_interval: int = 300,
        enable_history: bool = True,
        namespace: str = None,             # 项目/任务级命名空间
    ):
        self.worker_id = worker_id or uuid.uuid4().hex[:8]
        self.namespace = namespace or ""
        # worker_group: 若未显式指定，交给 SmartQueue 按 namespace 自动推导
        self.worker_group = worker_group
        self.storage = RemoteStorage(storage_url) if storage_url else None

        # 初始化 TaskHistoryStore（共享同一个 Redis 连接）
        _history_client = None
        if enable_history:
            try:
                _history_client = redis_lib.from_url(listen_url, decode_responses=True)
            except Exception as e:
                logger.warning(f"Failed to init history Redis client: {e}")

        def _make_history(q_name_with_ns: str) -> Optional[TaskHistoryStore]:
            """q_name_with_ns 是加过 namespace 前缀的队列基础名"""
            if _history_client is None:
                return None
            return TaskHistoryStore(_history_client, q_name_with_ns)

        self.listen_q = SmartQueue(
            listen_url, listen_q_name,
            worker_group=self.worker_group,
            worker_id=self.worker_id,
            storage=self.storage,
            auto_claim=auto_claim,
            claim_interval=claim_interval,
            namespace=namespace,
            history=None,  # 先建队列让 ns 前缀生效，再用实际名创建 history
        )
        # 用加过 ns 前缀的实际基础名（不含 :stream）创建 history
        self.listen_q.history = _make_history(self.listen_q._base_name)

        if result_url and result_q_name:
            self.result_q = SmartQueue(
                result_url, result_q_name,
                worker_group=self.worker_group,
                worker_id=self.worker_id,
                storage=self.storage,
                auto_claim=auto_claim,
                claim_interval=claim_interval,
                namespace=namespace,
                history=None,
            )
            self.result_q.history = _make_history(self.result_q._base_name)
        else:
            self.result_q = None

        self.handlers = {}

    def on(self, action_name: str) -> Callable:
        """路由装饰器"""
        def decorator(func: Callable):
            self.handlers[action_name] = func
            return func
        return decorator

    def run(self):
        ns_tag = f"[ns={self.namespace}] " if self.namespace else ""
        logger.info(f"Worker [{self.worker_id}] {ns_tag}in Group [{self.listen_q.worker_group}] Started.")
        logger.info(f"Listening on Queue: {self.listen_q.queue_name}")
        while True:
            msg_context = None
            try:
                payload, msg_context = self.listen_q.pop_blocking()
                if not payload:
                    continue

                action = payload.get("action", "unknown_action")
                msg_id = msg_context.get("msg_id") if msg_context else "unknown_id"

                logger.info(f"[{msg_id}] Received Task -> Action: {action}")
                handler = self.handlers.get(action)

                if not handler:
                    logger.warning(f"[{msg_id}] Unknown action: {action}. Failing task.")
                    if msg_context:
                        self.listen_q.fail(msg_context, reason=f"No handler for action: {action}")
                    continue

                t_start = time.time()
                result_payload = handler(payload)
                t_cost = time.time() - t_start

                if result_payload and self.result_q:
                    self.result_q.push(result_payload)
                    logger.info(f"[{msg_id}] Task completed in {t_cost:.3f}s. Result pushed to {self.result_q.queue_name}.")
                else:
                    logger.info(f"[{msg_id}] Task completed in {t_cost:.3f}s. (No return payload)")

                self.listen_q.ack(msg_context)

            except Exception as e:
                logger.error(f"Task Failed: {e}\n{traceback.format_exc()}")
                if msg_context:
                    self.listen_q.fail(msg_context, reason=str(e))