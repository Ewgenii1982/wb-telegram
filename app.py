import os
import requests
from fastapi import FastAPI

app = FastAPI()

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

def tg_send(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {"error": "No TG_BOT_TOKEN or TG_CHAT_ID"}
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text}, timeout=10)
    return r.json()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/test-telegram")
def test_telegram():
    result = tg_send("✅ Тест: сообщение из облачного сервера Render")
    return {"telegram_result": result}
