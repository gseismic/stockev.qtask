import requests

token = "qtask_super_secret_token"
res = requests.post("http://localhost:8000/api/requeue/spider:tasks", headers={"Authorization": f"Bearer " + token})
print(res.status_code, res.text)
