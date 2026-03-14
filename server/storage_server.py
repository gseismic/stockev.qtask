import os
import uuid
import psutil
import redis
import json
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Dict, Any
import aiofiles

app = FastAPI(title="qtask Storage & Monitoring", description="qtask大对象存储服务与监控台")
security = HTTPBasic()

# ============ 监控与认证配置 ============
# 实际生产中可以读取环境变量
ADMIN_USERNAME = os.environ.get("QTASK_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("QTASK_ADMIN_PASS", "admin123")
REDIS_URL = os.environ.get("QTASK_REDIS_URL", "redis://localhost:6379/0")

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != ADMIN_USERNAME or credentials.password != ADMIN_PASSWORD:
        raise HTTPException(
            status_code=401,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def get_redis_client():
    return redis.from_url(REDIS_URL, decode_responses=True)

# ============ 原有的存储相关配置 ============
STORAGE_DIR = "/tmp/qtask_storage"
os.makedirs(STORAGE_DIR, exist_ok=True)

@app.post("/upload/{file_id}")
async def upload_file(file_id: str = Form(...), file: UploadFile = File(...)):
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

@app.get("/download/{file_id}")
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

@app.delete("/delete/{file_id}")
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

@app.get("/api/stats", dependencies=[Depends(get_current_username)])
def get_system_stats():
    """获取系统和 Redis 的统计信息用于绘图"""
    cpu_percent = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()
    
    # 扫描 Redis 中可能相关的队列 (以 :stream 结尾)
    r = get_redis_client()
    queues = {}
    
    try:
        keys = r.keys("*:stream")
        for k in keys:
            q_info = {"length": r.xlen(k), "groups": []}
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
        "queues": queues
    }

@app.get("/dashboard", response_class=HTMLResponse, dependencies=[Depends(get_current_username)])
def read_dashboard():
    """纯手工匠心的极简 HTML 面板页面"""
    html_content = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>qtask Dashboard</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f4f5f7; margin: 0; padding: 20px; color: #333; }
            .header { background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center;}
            .header h1 { margin: 0; font-size: 24px; color: #2c3e50; }
            .badge { background: #3498db; color: white; padding: 4px 8px; border-radius: 12px; font-size: 12px; }
            .row { display: flex; gap: 20px; flex-wrap: wrap; }
            .card { background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); flex: 1; min-width: 300px; margin-bottom: 20px;}
            h2 { font-size: 18px; margin-top: 0; border-bottom: 1px solid #eee; padding-bottom: 10px; }
            .metric { font-size: 36px; font-weight: bold; color: #34495e; margin: 10px 0; }
            .metric span { font-size: 14px; color: #7f8c8d; font-weight: normal; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th, td { text-align: left; padding: 8px; border-bottom: 1px solid #eee; }
            th { color: #7f8c8d; font-weight: 500; }
            .refresh { background: none; border: none; color: #3498db; cursor: pointer; text-decoration: underline; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 qtask Monitoring Dashboard</h1>
            <span class="badge">Autorefreshing every 3s</span>
        </div>
        
        <div class="row">
            <div class="card">
                <h2>💻 Host CPU Usage</h2>
                <div class="metric" id="cpu-val">--%</div>
            </div>
            <div class="card">
                <h2>📈 Host Memory</h2>
                <div class="metric" id="mem-val">--% <span>(-- MB)</span></div>
            </div>
        </div>

        <div class="card">
            <h2>📨 Redis Task Stream Queues</h2>
            <div id="queues-container">Loading...</div>
        </div>

        <script>
            async function fetchStats() {
                try {
                    const res = await fetch('/api/stats');
                    const data = await res.json();
                    
                    document.getElementById('cpu-val').innerText = data.system.cpu_percent + '%';
                    document.getElementById('mem-val').innerHTML = data.system.memory_percent + '% <span>(' + data.system.memory_used_mb + ' MB)</span>';
                    
                    let qhtml = '';
                    if(Object.keys(data.queues).length === 0) {
                        qhtml = '<p>No Active Queues Found.</p>';
                    } else if (data.queues.error) {
                        qhtml = '<p style="color:red">Redis Error: ' + data.queues.error + '</p>';
                    } else {
                        qhtml = '<table><tr><th>Queue Name</th><th>Total Messages</th><th>Consumer Groups</th></tr>';
                        for (const [qname, qinfo] of Object.entries(data.queues)) {
                            let groupsHtml = '<ul>';
                            qinfo.groups.forEach(g => {
                                groupsHtml += `<li><b>${g.name}</b>: ${g.consumers} nodes, ${g.pending} pending</li>`;
                            });
                            groupsHtml += '</ul>';
                            
                            qhtml += `<tr>
                                <td><b>${qname}</b></td>
                                <td>${qinfo.length}</td>
                                <td>${groupsHtml}</td>
                            </tr>`;
                        }
                        qhtml += '</table>';
                    }
                    document.getElementById('queues-container').innerHTML = qhtml;
                } catch(e) {
                    console.error("Failed to fetch stats", e);
                }
            }
            
            fetchStats();
            setInterval(fetchStats, 3000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)