"""
AI Trading API Server - Cross-platform (Windows/Mac/Linux)
FastAPI backend with Google Sheets DB support via GAS REST API
"""
import os
import sys
import time
import threading
import datetime
import sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

try:
      from dotenv import load_dotenv
      load_dotenv()
except ImportError:
      pass

try:
      import yfinance as yf
      YF_AVAILABLE = True
except ImportError:
      YF_AVAILABLE = False

try:
      import psutil
      PSUTIL_AVAILABLE = True
except ImportError:
      PSUTIL_AVAILABLE = False

try:
      import requests
      REQUESTS_AVAILABLE = True
except ImportError:
      REQUESTS_AVAILABLE = False

# ─── Config ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.resolve()
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "data" / "trading.db"))
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
GAS_URL = os.environ.get("GAS_URL", "")
GAS_TIMEOUT = int(os.environ.get("GAS_TIMEOUT", "10"))
API_KEY = os.environ.get("API_KEY", "")
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")

# ─── App Setup ─────────────────────────────────────────────────────────────────
app = FastAPI(title="AI Trading API", version="1.0.0")

app.add_middleware(
      CORSMiddleware,
      allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
      allow_credentials=True,
      allow_methods=["*"],
      allow_headers=["*"],
)

# ─── API Key Middleware ────────────────────────────────────────────────────────
@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
      if not API_KEY:
                return await call_next(request)
            skip_paths = ["/docs", "/openapi.json", "/redoc"]
    if request.url.path in skip_paths:
              return await call_next(request)
          key = request.headers.get("X-API-Key", "")
    if key != API_KEY:
              return JSONResponse({"error": "Unauthorized"}, status_code=401)
          return await call_next(request)

# ─── DB Helper ────────────────────────────────────────────────────────────────
def get_db():
      db_path = Path(DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn

def db_query(sql, params=(), fetchall=True):
    try:
              conn = get_db()
              cur = conn.execute(sql, params)
              if fetchall:
                            rows = [dict(r) for r in cur.fetchall()]
    else:
            rows = dict(cur.fetchone()) if cur.fetchone() else {}
              conn.close()
        return rows
except Exception as e:
        return [{"error": str(e)}]

# ─── GAS Helper ───────────────────────────────────────────────────────────────
_gas_cache = {}

def gas_read(table, limit=0):
      if not GAS_URL:
                return []
            cache_key = f"{table}:{limit}"
    cached = _gas_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < 30:
              return cached["data"]
          try:
                    params = {"table": table, "action": "read"}
                    if limit:
                                  params["limit"] = limit
                              r = requests.get(GAS_URL, params=params, timeout=GAS_TIMEOUT)
        data = r.json().get("data", [])
        _gas_cache[cache_key] = {"data": data, "ts": time.time()}
        return data
except Exception as e:
        return [{"error": str(e)}]

def gas_post(payload):
      if not GAS_URL:
                return {"ok": False, "error": "GAS_URL not configured"}
    try:
              r = requests.post(GAS_URL, json=payload, timeout=GAS_TIMEOUT)
        return r.json()
except Exception as e:
        return {"ok": False, "error": str(e)}

# ─── yfinance Cache ───────────────────────────────────────────────────────────
_yf_cache = {}
_yf_lock = threading.Lock()

def yf_cached(symbol, period="1d", interval="1d"):
      if not YF_AVAILABLE:
                return None
    key = f"{symbol}:{period}:{interval}"
    with _yf_lock:
              cached = _yf_cache.get(key)
        if cached and time.time() - cached["ts"] < 300:
                      return cached["data"]
              try:
                        ticker = yf.Ticker(symbol)
                        hist = ticker.history(period=period, interval=interval)
                        info = ticker.info
                        data = {"hist": hist, "info": info}
                        with _yf_lock:
                                      _yf_cache[key] = {"data": data, "ts": time.time()}
                                  return data
except Exception:
        return None

def now_iso():
      return datetime.datetime.utcnow().isoformat()

def resp(data, **kwargs):
      return {"status": "ok", "ts": now_iso(), "data": data, **kwargs}

# ─── Static Files ─────────────────────────────────────────────────────────────
@app.get("/", response_class=JSONResponse)
async def root():
      return resp({"message": "AI Trading API is running"})

@app.get("/dashboard.html", response_class=HTMLResponse)
async def dashboard():
      html_file = BASE_DIR / "dashboard.html"
    if html_file.exists():
              return HTMLResponse(content=html_file.read_text(encoding="utf-8"))
      return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)

@app.get("/chart.min.js")
async def chart_js():
      f = BASE_DIR / "chart.min.js"
    if f.exists():
              return FileResponse(str(f))
    return JSONResponse({"error": "not found"}, status_code=404)

# ─── System ───────────────────────────────────────────────────────────────────
@app.get("/system/status")
async def system_status():
      models = []
    if REQUESTS_AVAILABLE:
              try:
                            r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=3)
                            models = [m["name"] for m in r.json().get("models", [])]
              except Exception:
            pass
    trades = db_query("SELECT COUNT(*) as c FROM trade_log WHERE DATE(ts)=DATE('now')")
    positions = db_query("SELECT COUNT(*) as c FROM t_position WHERE status='open'")
    confidence = db_query("SELECT * FROM ensemble_signal ORDER BY ts DESC LIMIT 1")
    return resp({
              "orchestrator_running": True,
              "last_cycle": None,
              "today_trades": trades[0].get("c", 0) if trades and "error" not in trades[0] else 0,
                        "open_positions": positions[0].get("c", 0) if positions and "error" not in positions[0] else 0,
              "ollama_models": models,
              "latest_confidence": confidence,
        "server_time": datetime.datetime.now().isoformat()
    })

@app.get("/system/resources")
async def system_resources():
      data = {
                "cpu": {"percent": 0, "cores": os.cpu_count() or 1},
                "gpu": {"name": "N/A", "cores": 0, "percent": 0, "vram_used_gb": 0, "vram_total_gb": 0},
                "ram": {"percent": 0, "used_gb": 0, "total_gb": 0, "cached_gb": 0},
                "swap": {"percent": 0, "used_gb": 0, "total_gb": 0},
                "ssd": {"percent": 0, "used_gb": 0, "total_gb": 0},
                "network": {"sent_mb": 0, "recv_mb": 0}
      }
    if PSUTIL_AVAILABLE:
              try:
                            data["cpu"]["percent"] = psutil.cpu_percent(interval=0.1)
                            vm = psutil.virtual_memory()
                            data["ram"] = {
                                "percent": vm.percent,
                                "used_gb": round(vm.used / 1e9, 1),
                                "total_gb": round(vm.total / 1e9, 1),
                                "cached_gb": round(getattr(vm, "cached", 0) / 1e9, 1)
                            }
                            sw = psutil.swap_memory()
                            data["swap"] = {
                                "percent": sw.percent,
                                "used_gb": round(sw.used / 1e9, 1),
                                "total_gb": round(sw.total / 1e9, 1)
                            }
                            disk = psutil.disk_usage(str(BASE_DIR))
                            data["ssd"] = {
                                "percent": disk.percent,
                                "used_gb": round(disk.used / 1e9, 1),
                                "total_gb": round(disk.total / 1e9, 1)
                            }
                            net = psutil.net_io_counters()
                            data["network"] = {
                                "sent_mb": round(net.bytes_sent / 1e6, 1),
                                "recv_mb": round(net.bytes_recv / 1e6, 1)
                            }
              except Exception:
            pass
    try:
              import subprocess
        result = subprocess.run(
                      ["nvidia-smi", "--query-gpu=name,utilization.gpu,memory.used,memory.total",
                                    "--format=csv,noheader,nounits"],
                      capture_output=True, text=True, timeout=3
        )
        if result.returncode == 0:
                      parts = result.stdout.strip().split(",")
            if len(parts) >= 4:
                              data["gpu"] = {
                                                    "name": parts[0].strip(),
                                                    "cores": 0,
                                                    "percent": float(parts[1].strip()),
                                                    "vram_used_gb": round(float(parts[2].strip()) / 1024, 2),
                                                    "vram_total_gb": round(float(parts[3].strip()) / 1024, 2)
}
except Exception:
        pass
    return resp(data)

@app.get("/system/models")
async def system_models():
      if not REQUESTS_AVAILABLE:
                return resp([])
    try:
              r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
                  return resp(r.json().get("models", []))
except Exception as e:
        return resp([], error=str(e))

# ─── Market ───────────────────────────────────────────────────────────────────
FX_SYMBOLS = {
      "EURUSD=X": ("EUR", "eu"), "JPY=X": ("JPY", "jp"), "GBPUSD=X": ("GBP", "gb"),
      "CNY=X": ("CNY", "cn"), "HKDUSD=X": ("HKD", "hk")
}
COMMODITY_SYMBOLS = {"GC=F": ("Gold", "$/oz"), "SI=F": ("Silver", "$/oz"),
                                          "CL=F": ("WTI Crude", "$/bbl"), "NG=F": ("Natural Gas", "$/MMBtu")}
INDEX_SYMBOLS = {"^VIX": "VIX Fear Index", "DX-Y.NYB": "Dollar Index", "^TNX": "US 10Y Treasury"}

_sidebar_cache = {"data": None, "ts": 0}

@app.get("/market/sidebar")
async def market_sidebar():
      global _sidebar_cache
    if _sidebar_cache["data"] and time.time() - _sidebar_cache["ts"] < 300:
              return resp(_sidebar_cache["data"])

    all_symbols = list(FX_SYMBOLS.keys()) + list(COMMODITY_SYMBOLS.keys()) + list(INDEX_SYMBOLS.keys())
    results = {}

    def fetch(sym):
              d = yf_cached(sym)
        if d and d["hist"] is not None and not d["hist"].empty:
            h = d["hist"]
            close = float(h["Close"].iloc[-1])
            prev = float(h["Close"].iloc[-2]) if len(h) > 1 else close
            chg = close - prev
            pct = (chg / prev * 100) if prev else 0
            results[sym] = {"close": close, "chg": chg, "pct": pct}

    if YF_AVAILABLE:
              with ThreadPoolExecutor(max_workers=8) as exe:
                            exe.map(fetch, all_symbols)

    fx = []
    for sym, (name, flag) in FX_SYMBOLS.items():
              r = results.get(sym, {})
        price = r.get("close", 0)
        unit = "100en" if "JPY" in sym else None
        entry = {"name": f"{name} ({sym.split('=')[0]})", "price": round(price * (100 if "JPY" in sym else 1), 2),
                                  "change": round(r.get("chg", 0), 2), "change_pct": round(r.get("pct", 0), 2), "flag": flag}
        if unit:
            entry["unit"] = unit
        fx.append(entry)
    fx.insert(0, {"name": "Dollar (USD)", "price": 0, "change": 0, "change_pct": 0, "flag": "us"})

    commodity = []
    for sym, (name, unit) in COMMODITY_SYMBOLS.items():
              r = results.get(sym, {})
        commodity.append({"name": name, "price": round(r.get("close", 0), 2),
                                                    "change": round(r.get("chg", 0), 2), "change_pct": round(r.get("pct", 0), 2), "unit": unit})

    index = []
    for sym, name in INDEX_SYMBOLS.items():
              r = results.get(sym, {})
        index.append({"name": name, "price": round(r.get("close", 0), 2),
                                            "change": round(r.get("chg", 0), 2), "change_pct": round(r.get("pct", 0), 2)})

    sidebar_data = {"fx": fx, "commodity": commodity, "index": index}
    _sidebar_cache = {"data": sidebar_data, "ts": time.time()}
    return resp(sidebar_data)

@app.get("/market/sentiment")
async def market_sentiment():
      return resp({
                "fear_greed": 50, "label": "Neutral",
                "vix": 20.0, "vix_change": 0,
                "put_call": 1.0, "breadth": 0.5,
                "ts": now_iso()
      })

@app.get("/market/news")
async def market_news():
      return resp([])

@app.get("/market/prices")
async def market_prices():
      return resp([])

@app.get("/market/index")
async def market_index():
      return resp([])

# ─── Portfolio / Positions / Trades ───────────────────────────────────────────
@app.get("/portfolio")
async def portfolio():
      rows = db_query("SELECT * FROM portfolio WHERE active=1 LIMIT 1")
    if rows and "error" not in rows[0]:
              return resp(rows[0])
    if GAS_URL:
              data = gas_read("portfolio", limit=1)
        return resp(data[0] if data else {})
    return resp({})

@app.get("/portfolio/all")
async def portfolio_all():
      rows = db_query("SELECT * FROM portfolio")
    if rows and "error" not in rows[0]:
              return resp(rows)
    if GAS_URL:
              return resp(gas_read("portfolio"))
    return resp([])

@app.get("/positions")
async def positions():
      rows = db_query("SELECT * FROM t_position WHERE status='open'")
    if rows and "error" not in rows[0]:
              return resp(rows)
    if GAS_URL:
              return resp(gas_read("t_position"))
    return resp([])

@app.get("/trades/today")
async def trades_today():
      rows = db_query("SELECT * FROM trade_log WHERE DATE(ts)=DATE('now') ORDER BY ts DESC")
    pnl = sum(r.get("pnl", 0) or 0 for r in rows if "error" not in r)
    comm = sum(r.get("commission", 0) or 0 for r in rows if "error" not in r)
    buy = sum(1 for r in rows if r.get("side") == "buy")
    sell = sum(1 for r in rows if r.get("side") == "sell")
    return resp(rows, meta={"today_count": len(rows), "buy": buy, "sell": sell, "pnl": pnl, "commission": comm})

# ─── Symbols ──────────────────────────────────────────────────────────────────
@app.get("/symbols")
async def symbols():
      rows = db_query("SELECT * FROM symbol_master WHERE active=1")
    if rows and "error" not in rows[0]:
              return resp(rows)
    if GAS_URL:
              return resp(gas_read("symbol_master"))
    return resp([])

@app.get("/symbols/alias")
async def symbols_alias():
      return resp({})

@app.post("/symbols/add")
async def symbols_add(request: Request):
      body = await request.json()
    symbol = body.get("symbol", "").upper()
    if not symbol:
              raise HTTPException(status_code=400, detail="symbol required")
    if GAS_URL:
              gas_post({"table": "symbol_master", "action": "upsert", "key": "symbol",
                                          "data": {"symbol": symbol, "name": body.get("name", symbol),
                                                                              "market": body.get("market", "US"), "active": True}})
    return resp({"added": symbol})

@app.post("/symbols/remove")
async def symbols_remove(request: Request):
      body = await request.json()
    symbol = body.get("symbol", "").upper()
    if not symbol:
              raise HTTPException(status_code=400, detail="symbol required")
    return resp({"removed": symbol})

# ─── Markets ──────────────────────────────────────────────────────────────────
@app.get("/markets")
async def markets():
      if GAS_URL:
                return resp(gas_read("cfg_market"))
    return resp([])

@app.get("/config/markets")
async def config_markets():
      return await markets()

# ─── Signals / Reasoning / Cycle ──────────────────────────────────────────────
@app.get("/signals/history")
async def signals_history():
      rows = db_query("SELECT * FROM ensemble_signal ORDER BY ts DESC LIMIT 50")
    if rows and "error" not in rows[0]:
              return resp(rows)
    if GAS_URL:
              return resp(gas_read("ensemble_signal", limit=50))
    return resp([])

@app.get("/reasoning/latest")
async def reasoning_latest():
      rows = db_query("SELECT * FROM reasoning_log ORDER BY ts DESC LIMIT 5")
    if rows and "error" not in rows[0]:
              return resp(rows)
    if GAS_URL:
              return resp(gas_read("reasoning_log", limit=5))
    return resp([])

@app.get("/cycle/latest")
async def cycle_latest():
      return resp({})

# ─── Performance / Report / Screener ──────────────────────────────────────────
@app.get("/performance/daily")
async def performance_daily():
      return resp([])

@app.get("/asset/daily")
async def asset_daily():
      return resp([])

@app.get("/asset/trend")
async def asset_trend():
      return resp([])

@app.get("/asset/trend/market")
async def asset_trend_market():
      return resp([])

@app.get("/report")
async def report():
      if GAS_URL:
                return resp(gas_read("trade_report", limit=1))
    return resp({})

@app.get("/screener/latest")
async def screener_latest():
      rows = db_query("SELECT * FROM screener_result ORDER BY ts DESC LIMIT 20")
      if rows and "error" not in rows[0]:
                return resp(rows)
    if GAS_URL:
              return resp(gas_read("screener_result", limit=20))
    return resp([])

# ─── Google Sheets Endpoints ──────────────────────────────────────────────────
@app.get("/sheets/status")
async def sheets_status():
      if not GAS_URL:
                return resp({"connected": False, "message": "GAS_URL not configured - mock mode"})
    try:
              r = requests.get(GAS_URL, params={"table": "cfg_market", "action": "read"}, timeout=GAS_TIMEOUT)
        data = r.json()
        return resp({"connected": data.get("ok", False), "message": "GAS connected"})
except Exception as e:
        return resp({"connected": False, "message": str(e)})

@app.get("/sheets/trades")
async def sheets_trades():
      return resp(gas_read("trade_log", limit=100) if GAS_URL else [])

@app.get("/sheets/signals")
async def sheets_signals():
      return resp(gas_read("ensemble_signal", limit=50) if GAS_URL else [])

@app.get("/sheets/portfolio")
async def sheets_portfolio():
      return resp(gas_read("portfolio") if GAS_URL else [])

@app.get("/sheets/positions")
async def sheets_positions():
    return resp(gas_read("t_position") if GAS_URL else [])

@app.get("/sheets/symbols")
async def sheets_symbols():
      return resp(gas_read("symbol_master") if GAS_URL else [])

@app.get("/sheets/screener")
async def sheets_screener():
      return resp(gas_read("screener_result", limit=20) if GAS_URL else [])

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
      import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api_server:app", host="0.0.0.0", port=port, reload=False)
