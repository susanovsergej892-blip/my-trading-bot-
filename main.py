""
Unified Trading Signal System v3.1
Crypto + US Equities engines with Quality Gate and Telegram alerts.
"""

import os
import time
import math
import json
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("trading-signals")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SIGNAL_WEBHOOK_SECRET = os.getenv("SIGNAL_WEBHOOK_SECRET", "")
DASHBOARD_WEBHOOK_URL = os.getenv("DASHBOARD_WEBHOOK_URL", "")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))

QUALITY_GATE_THRESHOLD = 12

MSK = timezone(timedelta(hours=3))

CRYPTO_SYMBOLS = [
    ("SOL/USDT", "SOLUSDT", "1m"),
    ("ETH/USDT", "ETHUSDT", "1m"),
    ("BNB/USDT", "BNBUSDT", "1m"),
    ("BTC/USDT", "BTCUSDT", "1m"),
    ("ADA/USDT", "ADAUSDT", "1m"),
    ("XRP/USDT", "XRPUSDT", "1m"),
    ("SOL/USDT", "SOLUSDT", "15m"),
    ("ETH/USDT", "ETHUSDT", "15m"),
    ("BNB/USDT", "BNBUSDT", "15m"),
    ("ADA/USDT", "ADAUSDT", "15m"),
    ("XRP/USDT", "XRPUSDT", "15m"),
]

EQUITY_SYMBOLS = [
    ("AAPL/USD", "AAPL"),
    ("MSFT/USD", "MSFT"),
    ("NVDA/USD", "NVDA"),
    ("TSLA/USD", "TSLA"),
    ("AMZN/USD", "AMZN"),
    ("META/USD", "META"),
    ("GOOGL/USD", "GOOGL"),
    ("AMD/USD", "AMD"),
]

def now_msk():
    return datetime.now(MSK)

def equities_engine_active():
    msk = now_msk()
    start = msk.replace(hour=16, minute=30, second=0, microsecond=0)
    end = msk.replace(hour=18, minute=30, second=0, microsecond=0)
    return start <= msk < end and msk.weekday() < 5

def fetch_binance_klines(symbol, interval, limit=100):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    rows = r.json()
    return [
        {"t": c[0], "open": float(c[1]), "high": float(c[2]),
         "low": float(c[3]), "close": float(c[4]), "volume": float(c[5])}
        for c in rows
    ]

def fetch_finnhub_quote(symbol):
    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": symbol, "token": FINNHUB_API_KEY}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def fetch_finnhub_candles(symbol, resolution="1", limit=100):
    to = int(time.time())
    frm = to - limit * 60
    url = "https://finnhub.io/api/v1/stock/candle"
    params = {
        "symbol": symbol, "resolution": resolution,
        "from": frm, "to": to, "token": FINNHUB_API_KEY,
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("s") != "ok":
        return []
    out = []
    for i in range(len(data["t"])):
        out.append({
            "t": data["t"][i], "open": float(data["o"][i]),
            "high": float(data["h"][i]), "low": float(data["l"][i]),
            "close": float(data["c"][i]), "volume": float(data["v"][i]),
        })
    return out

def ema(values, period):
    if not values:
        return 0.0
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e

def detect_bos(candles):
    if len(candles) < 20:
        return False, None
    window = candles[-21:-1]
    highs = [c["high"] for c in window]
    lows = [c["low"] for c in window]
    swing_high = max(highs)
    swing_low = min(lows)
    last = candles[-1]
    if last["close"] > swing_high:
        return True, "LONG"
    if last["close"] < swing_low:
        return True, "SHORT"
    return False, None

def volume_impulse(candles):
    if len(candles) < 21:
        return False, 0.0
    prior = [c["volume"] for c in candles[-21:-1]]
    avg = sum(prior) / len(prior) if prior else 0.0
    last_vol = candles[-1]["volume"]
    ratio = last_vol / avg if avg else 0.0
    return ratio >= 1.8, ratio

def trend_strength(candles):
    if len(candles) < 30:
        return 0
    closes = [c["close"] for c in candles[-30:]]
    e_now = ema(closes[-20:], 20)
    e_prev = ema(closes[-30:-10], 20)
    slope = (e_now - e_prev) / e_prev if e_prev else 0.0
    if abs(slope) < 0.0008:
        return 1
    if abs(slope) < 0.002:
        return 2
    return 3

def btc_regime():
    try:
        candles = fetch_binance_klines("BTCUSDT", "1m", 60)
    except Exception:
        return "NEUTRAL", 0.0
    closes = [c["close"] for c in candles]
    price = closes[-1]
    e = ema(closes[-20:], 20)
    if price > e * 1.0008:
        return "BULL", price
    if price < e * 0.9992:
        return "BEAR", price
    return "NEUTRAL", price

def btc_pump_aggressive():
    try:
        candles = fetch_binance_klines("BTCUSDT", "1m", 10)
    except Exception:
        return False
    if len(candles) < 6:
        return False
    start = candles[-6]["open"]
    end = candles[-1]["close"]
    gain = (end - start) / start
    return gain > 0.006

def qqq_direction():
    try:
        q = fetch_finnhub_quote("QQQ")
        if q.get("c") and q.get("pc"):
            return "UP" if q["c"] >= q["pc"] else "DOWN"
    except Exception:
        pass
    return "NEUTRAL"

def score_signal(broke, vol_impulse, aligned, trend):
    bos_score = 5 if broke else 0
    vol_score = 4 if vol_impulse else 0
    align_score = 3 if aligned else 0
    trend_score = trend
    total = bos_score + vol_score + align_score + trend_score
    return {"total": total, "bos": bos_score, "volume": vol_score, "alignment": align_score, "trend": trend_score}

def send_telegram(asset, direction, price, score, msk_time):
    arrow = "🟢" if direction == "LONG" else "🔴"
    msg = (
        f"📊 {arrow} {direction}\n"
        f"Asset: {asset}\n"
        f"Entry Price: {price}\n"
        f"Quality Score: {score}/15\n"
        f"Time: {msk_time}"
    )
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info("Telegram not configured. Would send:\n%s", msg)
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=15)
        return r.status_code == 200
    except Exception as e:
        log.error("Telegram send failed: %s", e)
        return False

def push_to_dashboard(payload):
    if not DASHBOARD_WEBHOOK_URL:
        return
    body = dict(payload)
    body["webhook_secret"] = SIGNAL_WEBHOOK_SECRET
    try:
        requests.post(DASHBOARD_WEBHOOK_URL, json=body, timeout=15)
    except Exception:
        pass

def evaluate_crypto(asset_label, binance_sym, timeframe):
    try:
        candles = fetch_binance_klines(binance_sym, timeframe, 60)
    except Exception:
        return
    if len(candles) < 30:
        return
    broke, bos_dir = detect_bos(candles)
    imp, _ = volume_impulse(candles)
    if not (broke and imp):
        return
    regime, _ = btc_regime()
    if bos_dir == "SHORT" and btc_pump_aggressive():
        return
    aligned = (bos_dir == "LONG" and regime == "BULL") or (bos_dir == "SHORT" and regime == "BEAR")
    trend = trend_strength(candles)
    sc = score_signal(broke, imp, aligned, trend)
    if sc["total"] < QUALITY_GATE_THRESHOLD:
        return
    price = candles[-1]["close"]
    msk_time = now_msk().strftime("%H:%M MSK")
    sent = send_telegram(asset_label, bos_dir, price, sc["total"], msk_time)
    payload = {
        "asset": asset_label, "direction": bos_dir, "entry_price": price,
        "quality_score": sc["total"], "timeframe": timeframe, "market": "crypto",
        "btc_regime": regime, "sent_to_telegram": sent,
    }
    push_to_dashboard(payload)

def evaluate_equity(asset_label, finnhub_sym):
    if not FINNHUB_API_KEY:
        return
    try:
        candles = fetch_finnhub_candles(finnhub_sym, "1", 60)
    except Exception:
        return
    if len(candles) < 30:
        return
    broke, bos_dir = detect_bos(candles)
    imp, _ = volume_impulse(candles)
    if not (broke and imp):
        return
    qqq = qqq_direction()
    aligned = (bos_dir == "LONG" and qqq == "UP") or (bos_dir == "SHORT" and qqq == "DOWN")
    trend = trend_strength(candles)
    sc = score_signal(broke, imp, aligned, trend)
    if sc["total"] < QUALITY_GATE_THRESHOLD:
        return
    quote = fetch_finnhub_quote(finnhub_sym)
    price = quote.get("c") or candles[-1]["close"]
    msk_time = now_msk().strftime("%H:%M MSK")
    sent = send_telegram(asset_label, bos_dir, price, sc["total"], msk_time)
    payload = {
        "asset": asset_label, "direction": bos_dir, "entry_price": price,
        "quality_score": sc["total"], "timeframe": "M1", "market": "equities",
        "btc_regime": "NEUTRAL", "sent_to_telegram": sent,
    }
    push_to_dashboard(payload)

def crypto_loop():
    log.info("Crypto engine started 24/7")
    while True:
        for asset_label, sym, tf in CRYPTO_SYMBOLS:
            try:
                evaluate_crypto(asset_label, sym, tf)
            except Exception:
                pass
        time.sleep(SCAN_INTERVAL)

def equities_loop():
    log.info("US Equities engine started (16:30-18:30 MSK)")
    while True:
        if equities_engine_active():
            for asset_label, sym in EQUITY_SYMBOLS:
                try:
                    evaluate_equity(asset_label, sym)
                except Exception:
                    pass
        time.sleep(SCAN_INTERVAL)

def main():
    log.info("Starting Unified Trading Signal System v3.1")
    t1 = threading.Thread(target=crypto_loop, daemon=True)
    t2 = threading.Thread(target=equities_loop, daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

if __name__ == "__main__":
    main()
