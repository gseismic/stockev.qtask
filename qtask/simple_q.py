import requests
import json
import io

class FastAPIStorage:
    """基于自定义 FastAPI 服务的分布式存储客户端"""
    def __init__(self, api_base_url="http://你的中心服务器IP:8000"):
        self.api_base_url = api_base_url
        
    def save(self, data_str: str) -> str:
        """上传数据字符串到 FastAPI"""
        url = f"{self.api_base_url}/upload"
        
        # 将字符串转为类文件对象，直接流式上传，避免大字符串二次拷贝
        file_obj = io.BytesIO(data_str.encode('utf-8'))
        
        # 使用 multipart/form-data 格式上传
        files = {'file': ('data.json', file_obj, 'application/json')}
        response = requests.post(url, files=files)
        response.raise_for_status()
        
        return response.json()["key"]
        
    def load(self, key: str) -> str:
        """从 FastAPI 下载数据"""
        url = f"{self.api_base_url}/download/{key}"
        response = requests.get(url)
        response.raise_for_status()
        
        # 将返回的二进制内容解码为字符串
        return response.content.decode('utf-8')
            
    def delete(self, key: str):
        """通知 FastAPI 删除数据"""
        url = f"{self.api_base_url}/delete/{key}"
        try:
            requests.delete(url)
        except Exception as e:
            print(f"[Warning] 删除远程文件失败 {key}: {e}")

# 在 Worker 初始化时注入这个新的 Storage
# worker_storage = FastAPIStorage(api_base_url="http://192.168.1.100:8000")