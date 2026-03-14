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
# 实际生产中可以读取环境变量
ADMIN_USERNAME = os.environ.get("QTASK_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("QTASK_ADMIN_PASS", "admin123")
REDIS_URL = os.environ.get("QTASK_REDIS_URL", "redis://localhost:6379/0")

# 极简 Token 签发 (仅作演示, 未使用 pyjwt 以精简依赖)
FAKE_TOKEN = "qtask_super_secret_token"

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

@app.post("/api/storage/upload/{file_id}")
async def upload_file(file_id: str, file: UploadFile = File(...)):
    """接收 Worker 上传的大体积数据并返回唯一 Key"""
    # 基础的安全防御：防止目录穿越攻击 (Directory Traversal)
    if ".." in file_id or "/" in file_id:
        raise HTTPException(status_code=400, detail="Invalid file_id")

    file_path = os.path.join(STORAGE_DIR, file_id)
    
    # 异步分块写入，防止将大文件全部加载到内存中导致 OOM
    async with aiofiles.open(file_path, 'wb') as out_file:
        while content := await file.read(1024 * 1024):  # 每次读取 1MB
            await out_file.write(content)
            
    return {"status": "uploaded", "file_id": file_id}

@app.get("/api/storage/download/{file_id}")
async def download_file(file_id: str):
    """处理 Worker 下载数据的请求"""
    # 基础的安全防御：防止目录穿越攻击 (Directory Traversal)
    if ".." in file_id or "/" in file_id:
        raise HTTPException(status_code=400, detail="Invalid file_id")
        
    file_path = os.path.join(STORAGE_DIR, file_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    # FileResponse 会自动处理大文件的流式传输
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
        # 如果指定了单个 task_id，则仅拉取那条
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
        return {"status": "success", "requeued": len(msgs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tasks/{queue_name}", dependencies=[Depends(get_current_username)])
def get_task_details(queue_name: str, status: str = "all", limit: int = 50):
    """获取具体队列的任务明细列表"""
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
            # All or Pending etc... for simplicity, xrange the main stream
            # Note: A real implementation for 'processing' needs `XPENDING` iteration which is complex.
            # We will show the latest queue items.
            msgs = r.xrevrange(q_name, max="+", min="-", count=limit)
            for msg_id, payload in msgs:
                try:
                    data = json.loads(payload.get("payload", "{}"))
                except:
                    data = {"raw": payload.get("payload", "")}
                res_tasks.append({
                    "id": msg_id, 
                    "status": "queued", # or processing if in pending
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
    
    # 统计物理存储占用
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
    
    # 扫描 Redis 中可能相关的队列 (以 :stream 结尾)
    r = get_redis_client()
    queues = {}
    
    try:
        keys = r.keys("*:stream")
        for k in keys:
            dlq_k = f"{k.replace(':stream', '')}:stream_dlq"
            q_info = {"length": r.xlen(k), "dlq_length": 0, "groups": []}
            
            # 读取 dlq
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
            queues[k] = q_info
    except Exception as e:
        queues = {"error": str(e)}

    return {
        "system": {
            "cpu_percent": cpu_percent,
            "memory_percent": mem.percent,
            "memory_used_mb": round(mem.used / (1024 * 1024), 2)
        },
        "storage": storage_stats,
        "queues": queues
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