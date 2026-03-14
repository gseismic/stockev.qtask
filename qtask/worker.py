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
    """任务执行调度器"""
    
    def __init__(
        self, 
        listen_url: str, 
        listen_q_name: str, 
        result_url: Optional[str] = None, 
        result_q_name: Optional[str] = None,
        storage_url: Optional[str] = None,
        worker_group: str = "default_group",
        worker_id: Optional[str] = None,
        auto_claim: bool = True,
        claim_interval: int = 300,
        enable_history: bool = True,   # 默认开启任务历史记录
    ):
        self.worker_id = worker_id or uuid.uuid4().hex[:8]
        self.worker_group = worker_group
        self.storage = RemoteStorage(storage_url) if storage_url else None

        # 初始化 TaskHistoryStore（共享同一个 Redis 连接）
        _history_client = None
        if enable_history:
            try:
                _history_client = redis_lib.from_url(listen_url, decode_responses=True)
            except Exception as e:
                logger.warning(f"Failed to init history Redis client: {e}")

        def _make_history(q_name: str) -> Optional[TaskHistoryStore]:
            if _history_client is None:
                return None
            return TaskHistoryStore(_history_client, q_name)

        self.listen_q = SmartQueue(
            listen_url, listen_q_name, self.worker_group, self.worker_id, self.storage,
            auto_claim=auto_claim, claim_interval=claim_interval,
            history=_make_history(listen_q_name),
        )
        
        if result_url and result_q_name:
            self.result_q = SmartQueue(
                result_url, result_q_name, self.worker_group, self.worker_id, self.storage,
                auto_claim=auto_claim, claim_interval=claim_interval,
                history=_make_history(result_q_name),
            )
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
        logger.info(f"Worker [{self.worker_id}] in Group [{self.worker_group}] Started.")
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

                # 执行业务逻辑
                t_start = time.time()
                result_payload = handler(payload)
                t_cost = time.time() - t_start
                
                # 如果有返回值且配置了结果队列，自动推送（超大载荷在此处会被拦截）
                if result_payload and self.result_q:
                    self.result_q.push(result_payload)
                    logger.info(f"[{msg_id}] Task completed in {t_cost:.3f}s. Result pushed to {self.result_q.queue_name}.")
                else:
                    logger.info(f"[{msg_id}] Task completed in {t_cost:.3f}s. (No return payload)")
                    
                # 成功后 ACK（history.record_ack 在 ack 方法内部调用）
                self.listen_q.ack(msg_context)
                
            except Exception as e:
                logger.error(f"Task Failed: {e}\n{traceback.format_exc()}")
                if msg_context:
                    self.listen_q.fail(msg_context, reason=str(e))