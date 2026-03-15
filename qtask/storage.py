import io
import requests
from typing import Optional, Union

class RemoteStorage:
    """基于 FastAPI 的远程对象存储客户端"""
    
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url.rstrip('/')
        self.session = requests.Session()
        
        # 配置重试逻辑和连接池
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
    def save(self, data_str: str) -> str:
        """上传大字符串，返回唯一 Key"""
        return self.save_bytes(data_str.encode('utf-8'))
        
    def save_bytes(self, data_bytes: bytes) -> str:
        """上传二进制数据。Requests 内部支持直接发送 bytes，减少不必要的内存复制。"""
        url = f"{self.api_base_url}/api/storage/upload"
        # 直接传入 bytes 作为文件内容，不需要 io.BytesIO 包装
        files = {'file': ('data.json', data_bytes, 'application/json')}
        
        response = self.session.post(url, files=files, timeout=(3, 30))
        response.raise_for_status()
        return response.json()["key"]
        
    def load(self, key: str) -> str:
        """下载并读取内容"""
        url = f"{self.api_base_url}/api/storage/download/{key}"
        response = self.session.get(url, timeout=(3, 30))
        response.raise_for_status()
        return response.content.decode('utf-8')
            
    def delete(self, key: str) -> bool:
        """删除远程文件"""
        url = f"{self.api_base_url}/api/storage/delete/{key}"
        try:
            response = self.session.delete(url, timeout=(3, 30))
            return response.status_code == 200
        except requests.RequestException:
            return False