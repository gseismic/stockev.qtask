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

# ============ 监控与认证配置 ============
ADMIN_USERNAME = os.environ.get("QTASK_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("QTASK_ADMIN_PASS", "admin123")
REDIS_URL = os.environ.get("QTASK_REDIS_URL", "redis://localhost:6379/0")

# 极简 Token 签发
FAKE_TOKEN = "qtask_super_secret_token"

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

# ============ 原有的存储相关配置 ============
STORAGE_DIR = "/tmp/qtask_storage"
os.makedirs(STORAGE_DIR, exist_ok=True)

@app.post("/api/storage/upload")
async def upload_file(file: UploadFile = File(...)):
    """接收 Worker 上传的大体积数据并返回唯一 Key"""
    file_id = uuid.uuid4().hex
    file_path = os.path.join(STORAGE_DIR, file_id)
    
    async with aiofiles.open(file_path, 'wb') as out_file:
        while content := await file.read(1024 * 1024):
            await out_file.write(content)
            
    return {"status": "uploaded", "key": file_id}

@app.get("/api/storage/download/{file_id}")
async def download_file(file_id: str):
    """处理 Worker 下载数据的请求"""
    if ".." in file_id or "/" in file_id:
        raise HTTPException(status_code=400, detail="Invalid file_id")
        
    file_path = os.path.join(STORAGE_DIR, file_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    return FileResponse(file_path)

@app.delete("/api/storage/delete/{file_id}")
async def delete_file(file_id: str):
    """处理中心节点入库后的文件清理请求"""
    if ".." in file_id or "/" in file_id:
        raise HTTPException(status_code=400, detail="Invalid file_id")
        
    file_path = os.path.join(STORAGE_DIR, file_id)
    if os.path.exists(file_path):
        os.remove(file_path)
        return {"status": "deleted"}
    return {"status": "not_found"}

# ============ Dashboard 及监控 API ============

class RequeueRequest(BaseModel):
    task_id: Optional[str] = None

@app.post("/api/requeue/{queue_name}", dependencies=[Depends(get_current_username)])
def requeue_dlq(queue_name: str, req: RequeueRequest = None):
    """通过 HTTP API 将死信重放为主队列任务，支持单笔重试"""
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
                new_id = pipe.xadd(q_name, {"payload": orig_payload})
                pipe.xdel(dlq_name, msg_id)
        
        pipe.execute()

        # 更新历史状态为 pending（requeue 后重新入队）
        try:
            from qtask.history import TaskHistoryStore
            hist = TaskHistoryStore(r, base_name)
            for msg_id, payload in msgs:
                orig_id = payload.get("original_id", msg_id)
                hist.record_retry(orig_id)
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
    """危险：重置消费组游标（放弃积压）"""
    if req.password != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid admin password provided for decisive action.")
    
    from qtask.queue import SmartQueue
    q = SmartQueue(REDIS_URL, queue_name.replace(":stream", ""), worker_group=req.group)
    if q.reset_group():
        return {"status": "success", "message": f"Cursor for group {req.group} reset"}
    raise HTTPException(status_code=500, detail="Failed to reset group cursor")


# ============ 任务历史 API ============

@app.get("/api/history/{queue_name}", dependencies=[Depends(get_current_username)])
def get_task_history(
    queue_name: str,
    status: str = "all",
    limit: int = 100,
    days: Optional[int] = None,
):
    """
    获取任务历史记录（来自 TaskHistoryStore）。
    status: all | pending | completed | failed
    days: 查询最近 N 天（不传则用全局 keep_days 设置）
    """
    r = get_redis_client()
    base_name = queue_name.replace(":stream", "")
    
    try:
        from qtask.history import TaskHistoryStore
        hist = TaskHistoryStore(r, base_name)
        tasks = hist.get_tasks(status=status, limit=limit, days=days)
        counts = hist.count_by_status()
        keep_days = hist.get_keep_days()
        return {
            "tasks": tasks,
            "counts": counts,
            "keep_days": keep_days,
            "queue": base_name,
        }
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
    except Exception as e:
        return {"pending": 0, "completed": 0, "failed": 0, "total": 0}


# ============ 全局 Settings API ============

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


# ============ 原有任务列表 API（保留兼容，增强展示） ============

@app.get("/api/tasks/{queue_name}", dependencies=[Depends(get_current_username)])
def get_task_details(queue_name: str, status: str = "all", limit: int = 50):
    """获取具体队列的任务明细列表（优先从历史读取，回退到 Stream）"""
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
                except:
                    data = {"raw": payload.get("payload", "")}
                res_tasks.append({
                    "id": msg_id, 
                    "orig_id": payload.get("original_id"),
                    "status": "failed", 
                    "action": data.get("action", "unknown"),
                    "payload": data
                })
        else:
            msgs = r.xrevrange(q_name, max="+", min="-", count=limit)
            for msg_id, payload in msgs:
                try:
                    data = json.loads(payload.get("payload", "{}"))
                except:
                    data = {"raw": payload.get("payload", "")}
                res_tasks.append({
                    "id": msg_id, 
                    "status": "queued",
                    "action": data.get("action", "unknown"),
                    "payload": data
                })
    except Exception as e:
        pass
        
    return {"tasks": res_tasks}

@app.get("/api/stats", dependencies=[Depends(get_current_username)])
def get_system_stats():
    """获取系统和 Redis 的统计信息用于绘图"""
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
                
    storage_stats = {
        "file_count": file_count,
        "size_mb": round(total_size / (1024 * 1024), 2)
    }
    
    r = get_redis_client()
    queues = {}
    
    try:
        keys = r.keys("*:stream")
        # 过滤掉 DLQ
        keys = [k for k in keys if not k.endswith("_dlq")]
        for k in keys:
            dlq_k = f"{k.replace(':stream', '')}:stream_dlq"
            q_info = {"length": r.xlen(k), "dlq_length": 0, "groups": [], "history_counts": {}}
            
            try:
                q_info["dlq_length"] = r.xlen(dlq_k)
            except:
                pass
                
            try:
                groups = r.xinfo_groups(k)
                for g in groups:
                    q_info["groups"].append({
                        "name": g.get("name"),
                        "consumers": g.get("consumers"),
                        "pending": g.get("pending"),
                    })
            except:
                pass

            # 附加历史统计
            try:
                from qtask.history import TaskHistoryStore
                base_name = k.replace(":stream", "")
                hist = TaskHistoryStore(r, base_name)
                q_info["history_counts"] = hist.count_by_status()
            except Exception:
                pass

            queues[k] = q_info
    except Exception as e:
        queues = {"error": str(e)}

    # 读取全局设置
    settings = {"history_keep_days": 15}
    try:
        from qtask.history import TaskHistoryStore, DEFAULT_KEEP_DAYS, SETTINGS_KEY
        raw = r.hgetall(SETTINGS_KEY)
        settings["history_keep_days"] = int(raw.get("history_keep_days", DEFAULT_KEEP_DAYS))
    except Exception:
        pass

    return {
        "system": {
            "cpu_percent": cpu_percent,
            "memory_percent": mem.percent,
            "memory_used_mb": round(mem.used / (1024 * 1024), 2)
        },
        "storage": storage_stats,
        "queues": queues,
        "settings": settings,
    }

@app.get("/dashboard", response_class=HTMLResponse)
def read_dashboard():
    """返回专业的 Vue SPA 面板"""
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Template missing</h1>", status_code=404)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)