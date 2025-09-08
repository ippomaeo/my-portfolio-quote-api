from fastapi import FastAPI, Query, Header, HTTPException
from typing import List, Optional
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone

API_KEY = "change-me"  # デプロイ先では環境変数で上書き推奨

app = FastAPI(title="My Portfolio Quote API",
              description="Yahoo Finance(yfinance)ラッパー。終値/前日終値/高値/安値/出来高を返す。",
              version="1.0.0")

def require_key(x_api_key: Optional[str]):
    if API_KEY and (x_api_key != API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API Key")

def jp_tz_now():
    return datetime.now(timezone(timedelta(hours=9)))

def prev_trading_row(hist: pd.DataFrame) -> int:
    # 日足7日取得のうち末尾は最新、前営業日は末尾から2番目
    return -2 if len(hist) >= 2 else -1

@app.get("/batch_quotes")
def batch_quotes(
    symbols: List[str] = Query(..., description="例: 7203.T,6758.T,8306.T"),
    x_api_key: Optional[str] = Header(None)
):
    require_key(x_api_key)
    out = []
    tickers = " ".join(symbols)
    data = yf.download(tickers=tickers, period="7d", interval="1d",
                       group_by='ticker', auto_adjust=False, progress=False)

    def extract(df: pd.DataFrame):
        return df.dropna(how="all")

    for sym in symbols:
        try:
            hist = extract(data if len(symbols) == 1 else data[sym])
            if len(hist) == 0:
                out.append({"symbol": sym, "error": "no_data"})
                continue

            latest = hist.iloc[-1]
            prev = hist.iloc[prev_trading_row(hist)]
            result = {
                "symbol": sym,
                "date": hist.index[-1].strftime("%Y-%m-%d"),
                "close": None if pd.isna(latest.get("Close")) else float(latest["Close"]),
                "prev_close": None if pd.isna(prev.get("Close")) else float(prev["Close"]),
                "high": None if pd.isna(latest.get("High")) else float(latest["High"]),
                "low": None if pd.isna(latest.get("Low")) else float(latest["Low"]),
                "volume": None if pd.isna(latest.get("Volume")) else int(latest["Volume"]),
                "prev_volume": None if pd.isna(prev.get("Volume")) else int(prev["Volume"]),
            }
            out.append(result)
        except Exception as e:
            out.append({"symbol": sym, "error": str(e)})

    return {"quotes": out}

# 既存の /healthz はそのまま
@app.get("/healthz")
def healthz():
    return {"ok": True, "now_jst": jp_tz_now().strftime("%Y-%m-%d %H:%M:%S")}

# 追加：/health でも同じ結果を返す（ドキュメントには出さなくてOKなら include_in_schema=False）
@app.get("/health", include_in_schema=False)
def health():
    return {"ok": True, "now_jst": jp_tz_now().strftime("%Y-%m-%d %H:%M:%S")}

from pydantic import BaseModel  # 先頭の import 群の近くに既に無ければ追加

class QuoteResponse(BaseModel):
    symbol: str
    price: float

@app.get("/quote", response_model=QuoteResponse)
def quote(symbol: str, x_api_key: Optional[str] = Header(None)):
    # APIキー確認（Render の Environment Variables に設定した API_KEY を使用）
    require_key(x_api_key)

    # yfinance で当日(直近)の終値を取得
    tkr = yf.Ticker(symbol)
    hist = tkr.history(period="1d")
    if hist.empty:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}")

    price = float(hist["Close"].iloc[-1])
    return QuoteResponse(symbol=symbol.upper(), price=price)

# --- ここから追記 ---

from fastapi import Security
from fastapi.security.api_key import APIKeyHeader

# APIキーのヘッダースキーマを定義（SwaggerにAuthorizeボタンを出すため）
api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)

# 認証チェック用の関数
def require_key(x_api_key: str = Security(api_key_header)):
    from fastapi import HTTPException
    import os

    # Render の Environment に設定した API_KEY を読む
    API_KEY = os.getenv("API_KEY", "")

    if not API_KEY or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

# /batch_quotes に認証を付ける
@app.get("/batch_quotes")
def batch_quotes(
    symbols: List[str] = Query(..., description="例: 7203.T,6758.T,8306.T"),
    _: None = Depends(require_key)   # 👈 これでAPIキー必須になる
):
    # 既存の処理はそのまま
    ...

# /quote にも認証を付ける
@app.get("/quote")
def quote(
    symbol: str = Query(..., description="例: 6758.T"),
    _: None = Depends(require_key)
):
    # 既存の処理はそのまま
    ...
# --- ここまで追記 ---
