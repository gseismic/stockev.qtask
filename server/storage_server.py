import os
import uuid
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import aiofiles

app = FastAPI(title="LiteOSS", description="轻量级量化数据对象存储服务")

# 设定基础存储目录
BASE_STORE_DIR = "./data_store"
os.makedirs(BASE_STORE_DIR, exist_ok=True)

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """接收 Worker 上传的大体积数据并返回唯一 Key"""
    # 生成唯一文件名，防止冲突
    ext = file.filename.split('.')[-1] if '.' in file.filename else 'dat'
    file_key = f"{uuid.uuid4().hex}.{ext}"
    file_path = os.path.join(BASE_STORE_DIR, file_key)
    
    # 异步分块写入，防止将大文件全部加载到内存中导致 OOM
    async with aiofiles.open(file_path, 'wb') as out_file:
        while content := await file.read(1024 * 1024):  # 每次读取 1MB
            await out_file.write(content)
            
    return {"status": "success", "key": file_key}

@app.get("/download/{key}")
async def download_file(key: str):
    """处理 Worker 下载数据的请求"""
    # 基础的安全防御：防止目录穿越攻击 (Directory Traversal)
    if ".." in key or "/" in key:
        raise HTTPException(status_code=400, detail="Invalid file key")
        
    file_path = os.path.join(BASE_STORE_DIR, key)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    # FileResponse 会自动处理大文件的流式传输
    return FileResponse(file_path)

@app.delete("/delete/{key}")
async def delete_file(key: str):
    """处理中心节点入库后的文件清理请求"""
    if ".." in key or "/" in key:
        raise HTTPException(status_code=400, detail="Invalid file key")
        
    file_path = os.path.join(BASE_STORE_DIR, key)
    if os.path.exists(file_path):
        os.remove(file_path)
        return {"status": "success", "message": "Deleted"}
    return {"status": "ignored", "message": "File not found"}

if __name__ == "__main__":
    # 启动服务，绑定在 0.0.0.0 允许外部 Worker 访问
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)