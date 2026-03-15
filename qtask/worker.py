import uuid
import socket
import traceback
import time
import signal
import redis as redis_lib
from loguru import logger
from typing import Callable, Optional
from .queue import SmartQueue
from .storage import RemoteStorage
from .history import TaskHistoryStore

class Worker:
    """任务执行调度器

    namespace 参数：
        项目/任务级命名空间。
        同一 namespace 的多个 Worker 会自动编为一个消费者组，共享任务负载。
        这是在不暴露底层技术概念的情况下，唯一需要配置的分组标识。
    """


    def __init__(
        self,
        listen_url: str,
        listen_q_name: str,
        result_url: Optional[str] = None,
        result_q_name: Optional[str] = None,
        storage_url: Optional[str] = None,
        auto_claim: bool = True,
        claim_interval: int = 300,
        enable_history: bool = True,
        namespace: str = None,             # 项目/任务级命名空间
    ):
        self.namespace = namespace or ""
        self.storage = RemoteStorage(storage_url) if storage_url else None

        # 核心：共享同一个 Redis 连接池
        self.redis_client = redis_lib.from_url(listen_url, decode_responses=True)

        # worker_group/worker_id 由 Worker 层管理
        self.worker_group = f"{self.namespace}_group" if self.namespace else "default_group"
        _hostname = socket.gethostname().split('.')[0][:12]
        self.worker_id = f"{_hostname}-{uuid.uuid4().hex[:6]}"

        self.listen_q = SmartQueue(
            listen_url, listen_q_name,
            storage=self.storage,
            auto_claim=auto_claim,
            claim_interval=claim_interval,
            namespace=namespace,
            history=None,
            history_store=enable_history,
            redis_client=self.redis_client, # 注入共享连接
        )

        if result_url and result_q_name:
            # 如果结果队列在同一个 Redis，复用连接；否则按需创建新连接
            res_client = self.redis_client if result_url == listen_url else None
            self.result_q = SmartQueue(
                result_url, result_q_name,
                storage=self.storage,
                auto_claim=auto_claim,
                claim_interval=claim_interval,
                namespace=namespace,
                history=None,
                history_store=enable_history,
                redis_client=res_client,
            )
        else:
            self.result_q = None

        self.handlers = {}
        
        # 优雅退出支持
        self.running = True
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def _handle_exit(self, sig, frame):
        logger.info(f"Signal {sig} received. Gracefully shutting down...")
        self.running = False

    def on(self, action_name: str) -> Callable:
        """路由装饰器"""
        def decorator(func: Callable):
            self.handlers[action_name] = func
            return func
        return decorator

    def run(self):
        ns_tag = f"[ns={self.namespace}] " if self.namespace else ""
        logger.info(f"Worker [{self.worker_id}] {ns_tag}in Group [{self.worker_group}] Started.")
        logger.info(f"Listening on Queue: {self.listen_q.queue_name}")
        
        while self.running:
            msg_context = None
            try:
                # 1. 尝试拉取任务
                payload, msg_context = self.listen_q.pop_blocking(self.worker_group, self.worker_id)
                if not payload:
                    logger.debug("No message received, continuing...")
                    continue

                # 2. 如果拉到了任务，即使此时收到退出信号，也必须完整处理完当前任务
                action = payload.get("action", "unknown_action")
                msg_id = msg_context.get("msg_id") if msg_context else "unknown_id"

                logger.debug(f"[{msg_id}] Received Task -> Action: {action}")
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
                    logger.debug(f"[{msg_id}] Task completed in {t_cost:.3f}s. Result pushed to {self.result_q.queue_name}.")
                else:
                    logger.debug(f"[{msg_id}] Task completed in {t_cost:.3f}s. (No return payload)")

                self.listen_q.ack(msg_context)

            except redis_lib.exceptions.ConnectionError as e:
                if not self.running:
                    logger.info(f"Worker stopping due to signal, connection closed: {e}")
                    break
                logger.error(f"Redis connection error: {e}\n{traceback.format_exc()}")
                if msg_context:
                    self.listen_q.fail(msg_context, reason=f"Connection error: {e}")
            except redis_lib.exceptions.TimeoutError as e:
                if not self.running:
                    logger.info(f"Worker stopping due to signal: {e}")
                    break
                logger.error(f"Redis timeout: {e}\n{traceback.format_exc()}")
                if msg_context:
                    self.listen_q.fail(msg_context, reason=f"Timeout: {e}")
            except Exception as e:
                if self.running:
                    logger.error(f"Task Failed: {e}\n{traceback.format_exc()}")
                    if msg_context:
                        self.listen_q.fail(msg_context, reason=str(e))
                else:
                    logger.info(f"Worker [{self.worker_id}] stopping due to signal.")
                    break
        
        logger.info(f"Worker [{self.worker_id}] stopped cleanly.")