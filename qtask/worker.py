import uuid
import traceback
from typing import Callable, Optional
from .queue import SmartQueue
from .storage import RemoteStorage

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
        worker_id: Optional[str] = None
    ):
        self.worker_id = worker_id or uuid.uuid4().hex[:8]
        self.worker_group = worker_group
        self.storage = RemoteStorage(storage_url) if storage_url else None
        
        self.listen_q = SmartQueue(
            listen_url, listen_q_name, self.worker_group, self.worker_id, self.storage
        )
        
        if result_url and result_q_name:
            self.result_q = SmartQueue(
                result_url, result_q_name, self.worker_group, self.worker_id, self.storage
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
        print(f"[*] Worker [{self.worker_id}] in Group [{self.worker_group}] Started. Listening: {self.listen_q.queue_name}")
        while True:
            try:
                payload, msg_context = self.listen_q.pop_blocking()
                if not payload:
                    continue
                    
                action = payload.get("action")
                handler = self.handlers.get(action)
                
                if not handler:
                    print(f"[!] Unknown action: {action}")
                    if msg_context:
                        self.listen_q.fail(msg_context)
                    continue

                # 执行业务逻辑
                result_payload = handler(payload)
                
                # 如果有返回值且配置了结果队列，自动推送（超大载荷在此处会被拦截）
                if result_payload and self.result_q:
                    self.result_q.push(result_payload)
                    
                # 成功后 ACK
                self.listen_q.ack(msg_context)
                
            except Exception as e:
                print(f"[-] Task Failed: {e}\n{traceback.format_exc()}")
                if msg_context:
                    self.listen_q.fail(msg_context)