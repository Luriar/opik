import json, urllib.request

data = json.dumps({"message": "SK하이닉스 투자의견과 리스크 요인을 알려줘", "top_k": 3}).encode()
req = urllib.request.Request("http://localhost:8000/chat", data=data,
    headers={"Content-Type": "application/json"}, method="POST")
try:
    resp = urllib.request.urlopen(req, timeout=30)
    print(resp.read().decode()[:2000])
except Exception as e:
    print("Error:", e)
