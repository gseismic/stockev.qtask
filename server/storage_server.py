import os
import uuid
import psutil
import redis
import json
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import aiofiles

app = FastAPI(title="qtask Storage & Monitoring", description="qtask大对象存储服务与监控台")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/login")

ADMIN_USERNAME = os.environ.get("QTASK_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("QTASK_ADMIN_PASS", "admin123")
REDIS_URL = os.environ.get("QTASK_REDIS_URL", "redis://localhost:6379/0")
FAKE_TOKEN = "qtask_super_secret_token"

# ── Redis key constants (must match queue.py) ──
_NS_SET_KEY = "qtask:namespaces"
_NS_QUEUES_KEY = "qtask:ns:{ns}:queues"


@app.get("/api/health")
def health_check():
    """轻量健康检查（无需认证），前端心跳使用"""
    import time
    redis_ok = False
    redis_msg = ""
    try:
        r = get_redis_client()
        r.ping()
        redis_ok = True
        redis_msg = "ok"
    except Exception as e:
        redis_msg = str(e)[:120]
    return {
        "status": "ok" if redis_ok else "degraded",
        "server": "ok",
        "redis": redis_ok,
        "redis_msg": redis_msg,
        "ts": time.time(),
    }


@app.post("/api/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username == ADMIN_USERNAME and form_data.password == ADMIN_PASSWORD:
        return {"access_token": FAKE_TOKEN, "token_type": "bearer"}
    raise HTTPException(status_code=400, detail="Incorrect username or password")


def get_current_username(token: str = Depends(oauth2_scheme)):
    if token != FAKE_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return ADMIN_USERNAME


def get_redis_client():
    return redis.from_url(REDIS_URL, decode_responses=True)


def _detect_namespace(stream_key: str, known_namespaces: set) -> str:
    """从 stream key 推断其所属的 namespace（按最长前缀匹配）"""
    best = ""
    for ns in known_namespaces:
        if stream_key.startswith(f"{ns}:") and len(ns) > len(best):
            best = ns
    return best


# ============ Large Object Storage ============

STORAGE_DIR = "/tmp/qtask_storage"
os.makedirs(STORAGE_DIR, exist_ok=True)


@app.post("/api/storage/upload")
async def upload_file(file: UploadFile = File(...)):
    """上传大对象文件，返回唯一 Key"""
    file_id = uuid.uuid4().hex
    file_path = os.path.join(STORAGE_DIR, file_id)
    async with aiofiles.open(file_path, 'wb') as out_file:
        while content := await file.read(1024 * 1024):
            await out_file.write(content)
    return {"status": "uploaded", "key": file_id}


@app.get("/api/storage/download/{file_id}")
async def download_file(file_id: str):
    """下载大对象文件"""
    if ".." in file_id or "/" in file_id:
        raise HTTPException(status_code=400, detail="Invalid file_id")
    file_path = os.path.join(STORAGE_DIR, file_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)


@app.delete("/api/storage/delete/{file_id}")
async def delete_file(file_id: str):
    """删除大对象文件"""
    if ".." in file_id or "/" in file_id:
        raise HTTPException(status_code=400, detail="Invalid file_id")
    file_path = os.path.join(STORAGE_DIR, file_id)
    if os.path.exists(file_path):
        os.remove(file_path)
        return {"status": "deleted"}
    return {"status": "not_found"}


# ============ Namespace API ============

class NamespacePurgeRequest(BaseModel):
    password: str


@app.get("/api/namespaces", dependencies=[Depends(get_current_username)])
def list_namespaces():
    """
    列出所有已注册的 namespace，包含队列数和历史任务统计聚合。
    namespace 是任务/项目级的命名空间，由 SmartQueue/Worker 的 namespace 参数自动注册。
    """
    r = get_redis_client()
    result = []
    try:
        namespaces = sorted(r.smembers(_NS_SET_KEY))
        from qtask.history import TaskHistoryStore
        for ns in namespaces:
            queues = sorted(r.smembers(_NS_QUEUES_KEY.format(ns=ns)))
            agg = {"pending": 0, "completed": 0, "failed": 0, "total": 0}
            for q in queues:
                base_name = f"{ns}:{q}"
                try:
                    hist = TaskHistoryStore(r, base_name)
                    c = hist.count_by_status()
                    for k in agg:
                        agg[k] += c.get(k, 0)
                except Exception:
                    pass
            result.append({
                "namespace": ns,
                "queue_count": len(queues),
                "queues": list(queues),
                "history_counts": agg,
            })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"namespaces": result}


@app.delete("/api/namespaces/{ns}", dependencies=[Depends(get_current_username)])
def purge_namespace(ns: str, req: NamespacePurgeRequest):
    """
    危险操作：清除 namespace 下的所有 Redis 数据
    （Stream、DLQ、历史记录、注册元数据）。需提供管理员密码。
    """
    if req.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid admin password")
    try:
        from qtask.queue import SmartQueue
        deleted = SmartQueue.purge_namespace(get_redis_client(), ns)
        return {"status": "success", "namespace": ns, "deleted": deleted}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ Queue Control ============

class RequeueRequest(BaseModel):
    task_id: Optional[str] = None


@app.post("/api/requeue/{queue_name}", dependencies=[Depends(get_current_username)])
def requeue_dlq(queue_name: str, req: RequeueRequest = None):
    """将死信队列（DLQ）消息重放回主队列，支持单条 / 全量"""
    r = get_redis_client()
    base_name = queue_name.replace(":stream", "")
    q_name = f"{base_name}:stream"
    dlq_name = f"{base_name}:stream_dlq"
    try:
        target_id = req.task_id if req and req.task_id else None
        if target_id:
            msgs = r.xrange(dlq_name, min=target_id, max=target_id)
        else:
            msgs = r.xrange(dlq_name, min="-", max="+")
        if not msgs:
            return {"status": "ignored", "message": "No matching tasks found in DLQ"}
        pipe = r.pipeline()
        for msg_id, payload in msgs:
            orig_payload = payload.get("payload")
            if orig_payload:
                pipe.xadd(q_name, {"payload": orig_payload})
                pipe.xdel(dlq_name, msg_id)
        pipe.execute()
        try:
            from qtask.history import TaskHistoryStore
            hist = TaskHistoryStore(r, base_name)
            for msg_id, payload in msgs:
                hist.record_retry(payload.get("original_id", msg_id))
        except Exception:
            pass
        return {"status": "success", "requeued": len(msgs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class QueueAdminRequest(BaseModel):
    password: str
    group: Optional[str] = "default_group"


@app.post("/api/queues/{queue_name}/clear", dependencies=[Depends(get_current_username)])
def clear_queue(queue_name: str, req: QueueAdminRequest):
    """危险：彻底清空某个队列及其死信"""
    if req.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid admin password provided for decisive action.")
    from qtask.queue import SmartQueue
    q = SmartQueue(REDIS_URL, queue_name.replace(":stream", ""))
    if q.clear_all():
        return {"status": "success", "message": f"Queue {queue_name} has been cleared"}
    raise HTTPException(status_code=500, detail="Failed to clear queue")


@app.post("/api/queues/{queue_name}/reset", dependencies=[Depends(get_current_username)])
def reset_group(queue_name: str, req: QueueAdminRequest):
    """危险：重置消费组游标（放弃当前积压）"""
    if req.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid admin password provided for decisive action.")
    from qtask.queue import SmartQueue
    q = SmartQueue(REDIS_URL, queue_name.replace(":stream", ""), worker_group=req.group)
    if q.reset_group():
        return {"status": "success", "message": f"Cursor for group {req.group} reset"}
    raise HTTPException(status_code=500, detail="Failed to reset group cursor")


# ============ Task History API ============

@app.get("/api/history/{queue_name}", dependencies=[Depends(get_current_username)])
def get_task_history(queue_name: str, status: str = "all", limit: int = 100, days: Optional[int] = None):
    """
    获取任务历史记录（来自 TaskHistoryStore）。
    status: all | pending | completed | failed
    days: 最近 N 天，不传则使用全局 keep_days 设置
    """
    r = get_redis_client()
    base_name = queue_name.replace(":stream", "")
    try:
        from qtask.history import TaskHistoryStore
        hist = TaskHistoryStore(r, base_name)
        tasks = hist.get_tasks(status=status, limit=limit, days=days)
        counts = hist.count_by_status()
        keep_days = hist.get_keep_days()
        return {"tasks": tasks, "counts": counts, "keep_days": keep_days, "queue": base_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history/{queue_name}/counts", dependencies=[Depends(get_current_username)])
def get_task_history_counts(queue_name: str):
    """快速获取任务状态统计数（用于侧边栏 badge）"""
    r = get_redis_client()
    base_name = queue_name.replace(":stream", "")
    try:
        from qtask.history import TaskHistoryStore
        hist = TaskHistoryStore(r, base_name)
        return hist.count_by_status()
    except Exception:
        return {"pending": 0, "completed": 0, "failed": 0, "total": 0}


# ============ Global Settings API ============

class SettingsRequest(BaseModel):
    history_keep_days: int


@app.get("/api/settings", dependencies=[Depends(get_current_username)])
def get_settings():
    """读取全局设置（history_keep_days 等）"""
    r = get_redis_client()
    try:
        from qtask.history import TaskHistoryStore, DEFAULT_KEEP_DAYS, SETTINGS_KEY
        raw = r.hgetall(SETTINGS_KEY)
        keep_days = int(raw.get("history_keep_days", DEFAULT_KEEP_DAYS))
        return {"history_keep_days": keep_days}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/settings", dependencies=[Depends(get_current_username)])
def update_settings(req: SettingsRequest):
    """更新全局设置"""
    if req.history_keep_days < 1 or req.history_keep_days > 365:
        raise HTTPException(status_code=400, detail="history_keep_days must be between 1 and 365")
    r = get_redis_client()
    try:
        from qtask.history import TaskHistoryStore
        TaskHistoryStore.set_keep_days(r, req.history_keep_days)
        return {"status": "ok", "history_keep_days": req.history_keep_days}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ Task List / Stats API ============

@app.get("/api/tasks/{queue_name}", dependencies=[Depends(get_current_username)])
def get_task_details(queue_name: str, status: str = "all", limit: int = 50):
    """获取队列任务明细（优先 Stream，failed 时从 DLQ 读取）"""
    r = get_redis_client()
    base_name = queue_name.replace(":stream", "")
    q_name = f"{base_name}:stream"
    dlq_name = f"{base_name}:stream_dlq"
    res_tasks = []
    try:
        if status == "failed":
            msgs = r.xrevrange(dlq_name, max="+", min="-", count=limit)
            for msg_id, payload in msgs:
                try:
                    data = json.loads(payload.get("payload", "{}"))
                except Exception:
                    data = {"raw": payload.get("payload", "")}
                res_tasks.append({
                    "id": msg_id, "orig_id": payload.get("original_id"),
                    "status": "failed", "action": data.get("action", "unknown"), "payload": data,
                })
        else:
            msgs = r.xrevrange(q_name, max="+", min="-", count=limit)
            for msg_id, payload in msgs:
                try:
                    data = json.loads(payload.get("payload", "{}"))
                except Exception:
                    data = {"raw": payload.get("payload", "")}
                res_tasks.append({
                    "id": msg_id, "status": "queued",
                    "action": data.get("action", "unknown"), "payload": data,
                })
    except Exception:
        pass
    return {"tasks": res_tasks}


@app.get("/api/stats", dependencies=[Depends(get_current_username)])
def get_system_stats():
    """
    系统和 Redis 队列统计。
    每个队列携带 namespace 字段（空字符串表示无 namespace）。
    顶层 namespaces 列出注册的 namespace 及其队列列表。
    """
    cpu_percent = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()

    total_size = 0
    file_count = 0
    if os.path.exists(STORAGE_DIR):
        for f in os.listdir(STORAGE_DIR):
            fp = os.path.join(STORAGE_DIR, f)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)
                file_count += 1

    r = get_redis_client()
    queues = {}

    # 读取已注册的 namespace 集合，用于 namespace 推断
    try:
        known_namespaces = r.smembers(_NS_SET_KEY)
    except Exception:
        known_namespaces = set()

    try:
        keys = [k for k in r.keys("*:stream") if not k.endswith("_dlq")]
        from qtask.history import TaskHistoryStore
        for k in keys:
            dlq_k = f"{k.replace(':stream', '')}:stream_dlq"
            ns = _detect_namespace(k, known_namespaces)
            q_info = {
                "length": r.xlen(k),
                "dlq_length": 0,
                "groups": [],
                "history_counts": {},
                "namespace": ns,
            }
            try:
                q_info["dlq_length"] = r.xlen(dlq_k)
            except Exception:
                pass
            try:
                for g in r.xinfo_groups(k):
                    q_info["groups"].append({
                        "name": g.get("name"),
                        "consumers": g.get("consumers"),
                        "pending": g.get("pending"),
                    })
            except Exception:
                pass
            try:
                base_name = k.replace(":stream", "")
                q_info["history_counts"] = TaskHistoryStore(r, base_name).count_by_status()
            except Exception:
                pass
            queues[k] = q_info
    except Exception as e:
        queues = {"error": str(e)}

    # 读取全局设置
    settings = {"history_keep_days": 15}
    try:
        from qtask.history import DEFAULT_KEEP_DAYS, SETTINGS_KEY
        raw = r.hgetall(SETTINGS_KEY)
        settings["history_keep_days"] = int(raw.get("history_keep_days", DEFAULT_KEEP_DAYS))
    except Exception:
        pass

    # ── namespace 汇总 ──
    # 来源1: qtask:namespaces 注册 Set（Worker 带 namespace 参数时自动写入）
    # 来源2: 从 stream key 推断（key 格式 ns:queue:stream 有3段才认为有 ns）
    inferred_ns: set = set()
    for stream_key in queues:
        # 去掉末尾 :stream → 剩余部分如 "stockev:spider:tasks" (3段) 或 "spider:tasks" (2段)
        base = stream_key[:-7] if stream_key.endswith(":stream") else stream_key
        parts = base.split(":")
        if len(parts) >= 3:
            # 3段及以上：第一段是 namespace（如 stockev:spider:tasks）
            inferred_ns.add(parts[0])
        # 2段（如 spider:tasks）：无 namespace，跳过

    all_ns = sorted(known_namespaces | inferred_ns)

    return {
        "system": {
            "cpu_percent": cpu_percent,
            "memory_percent": mem.percent,
            "memory_used_mb": round(mem.used / (1024 * 1024), 2),
        },
        "storage": {"file_count": file_count, "size_mb": round(total_size / (1024 * 1024), 2)},
        "queues": queues,
        "settings": settings,
        "namespaces": all_ns,
    }



@app.get("/dashboard", response_class=HTMLResponse)
def read_dashboard():
    """返回 Vue SPA 监控面板"""
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Template missing</h1>", status_code=404)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)