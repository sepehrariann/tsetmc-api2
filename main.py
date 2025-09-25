from fastapi import FastAPI
from TSEDataGetter import market_fetcher, intraday_trade_details

app = FastAPI()

@app.get("/")
def home():
    return {"message": "API is working with TSETMC on Render!"}

# 📌 داده لحظه‌ای بازار
@app.get("/market")
def market():
    df = market_fetcher()
    return df.to_dict(orient="records")

# 📌 داده معاملات یک نماد خاص در یک روز مشخص
@app.get("/trade/{symbol_id}/{date}")
def trade(symbol_id: str, date: str):
    df = intraday_trade_details(symbol_id, date)
    return df.to_dict(orient="records")
