import io
import requests
from typing import Optional

class RemoteStorage:
    """基于 FastAPI 的远程对象存储客户端"""
    
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url.rstrip('/')
        
    def save(self, data_str: str) -> str:
        """上传大字符串，返回唯一 Key"""
        url = f"{self.api_base_url}/upload"
        file_obj = io.BytesIO(data_str.encode('utf-8'))
        files = {'file': ('data.json', file_obj, 'application/json')}
        
        response = requests.post(url, files=files)
        response.raise_for_status()
        return response.json()["key"]
        
    def load(self, key: str) -> str:
        """下载并读取内容"""
        url = f"{self.api_base_url}/download/{key}"
        response = requests.get(url)
        response.raise_for_status()
        return response.content.decode('utf-8')
            
    def delete(self, key: str) -> bool:
        """删除远程文件"""
        url = f"{self.api_base_url}/delete/{key}"
        try:
            response = requests.delete(url)
            return response.status_code == 200
        except requests.RequestException:
            return False