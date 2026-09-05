#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quality-First Signal Bot v2.0
=============================
Chain (strict order):
    H4 -> H1 -> M15 -> Sweep -> Displacement -> BOS -> ADX -> RSI -> Volume
    -> Session -> News -> RR -> Score -> Telegram

Hard rules (from spec v2.0):
  * Every step is a HARD GATE (mandatory). One mandatory X = NO SIGNAL.
    Score NEVER compensates a failed hard gate; it is computed ONLY after all gates pass.
  * Deterministic: non-repainting swings (3+3), Wilder RMA (ATR/ADX/RSI),
    UTC-aligned H4 buckets, closed-candle evaluation only.
  * Fail-safe: News API unavailable -> ALL signals blocked (no blind entries).
  * No FINNHUB_API_KEY -> Forex and Stocks are BLOCKED at startup.

Data sources (verified):
  Forex candles : GET /forex/candle   symbol=OANDA:EURUSD  resolution=15|60
  Stock candles : GET /stock/candle   symbol=AAPL
  Forex news    : GET /calendar/economic  (high-impact, +/-45 min)  <- correct source for FX
  Stock news    : GET /company-news       (last 45 min; NA companies only)
  Alerts        : POST https://api.telegram.org/bot<token>/sendMessage

Run:
  export FINNHUB_API_KEY=... TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=...
  python signal_bot_v2.py             # one live scan cycle
  python signal_bot_v2.py --dry-run   # scan, log, but do NOT send Telegram
  python signal_bot_v2.py --backtest  # walk history, print signal stats
"""

import json
import os
import sys
import time
import threading
from datetime import datetime, timezone

import requests

__version__ = "2.0.0"

# ---------------------------------------------------------------------------
# Configuration (env-driven, deterministic defaults)
# ---------------------------------------------------------------------------

def _env_bool(name, default):
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


class Config:
    def __init__(self):
        self.finnhub_api_key = os.environ.get("FINNHUB_API_KEY", "").strip()
        self.telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        self.telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

        # Markets
        self.enable_forex = _env_bool("ENABLE_FOREX", True)
        self.enable_stocks = _env_bool("ENABLE_STOCKS", True)
        
        # Обновленные списки из твоих скриншотов (Форекс и Акции)
        self.forex_pairs = [p.strip() for p in os.environ.get(
            "FOREX_PAIRS", "EURUSD,GBPUSD,NZDUSD,USDCAD,USDCHF,USDJPY,AUDUSD").split(",") if p.strip()]
        self.stock_symbols = [s.strip() for s in os.environ.get(
            "STOCK_SYMBOLS", "AAPL,AMZN,GOOGL,META,MSFT,NVDA,SPCX,TSLA").split(",") if s.strip()]

        # Timeframes / warm-up
        self.tf_m15 = "15"
        self.tf_h1 = "60"
        self.warmup_m15 = int(os.environ.get("WARMUP_M15", "600"))
        self.warmup_h4 = int(os.environ.get("WARMUP_H4", "250"))
        self.fetch_m15 = int(os.environ.get("FETCH_M15", "800"))
        self.fetch_h1 = int(os.environ.get("FETCH_H1", "1200"))  # 300 H4 bars

        # Session (GMT)
        self.session_start_h = int(os.environ.get("SESSION_START_H", "7"))
        self.session_end_h = int(os.environ.get("SESSION_END_H", "20"))
        self.session_min_left = int(os.environ.get("SESSION_MIN_LEFT", "90"))

        # News
        self.news_block_min = int(os.environ.get("NEWS_BLOCK_MIN", "45"))

        # Indicators / hard-gate thresholds
        self.adx_min = float(os.environ.get("ADX_MIN", "22"))
        self.adx_bonus = float(os.environ.get("ADX_BONUS", "30"))
        self.disp_min = float(os.environ.get("DISP_MIN", "1.25"))
        self.disp_bonus = float(os.environ.get("DISP_BONUS", "1.8"))
        self.rsi_period = int(os.environ.get("RSI_PERIOD", "14"))
        self.rsi_long_min = float(os.environ.get("RSI_LONG_MIN", "50"))
        self.rsi_long_max = float(os.environ.get("RSI_LONG_MAX", "80"))
        self.rsi_short_min = float(os.environ.get("RSI_SHORT_MIN", "20"))
        self.rsi_short_max = float(os.environ.get("RSI_SHORT_MAX", "50"))
        self.vol_period = int(os.environ.get("VOL_PERIOD", "20"))
        self.vol_mult_min = float(os.environ.get("VOL_MULT_MIN", "1.0"))
        self.vol_mult_bonus = float(os.environ.get("VOL_MULT_BONUS", "1.5"))

        # Risk / Reward
        self.rr_min = float(os.environ.get("RR_MIN", "1.5"))
        self.rr_bonus = float(os.environ.get("RR_BONUS", "2.0"))
        self.atr_period = int(os.environ.get("ATR_PERIOD", "14"))
        self.sl_atr_min = float(os.environ.get("SL_ATR_MIN", "0.8"))
        self.sl_atr_max = float(os.environ.get("SL_ATR_MAX", "1.5"))
        self.tp1_r = float(os.environ.get("TP1_R", "1.0"))
        self.tp2_r = float(os.environ.get("TP2_R", "2.0"))
        self.be_r = float(os.environ.get("BE_R", "0.8"))
        self.max_hold_h = float(os.environ.get("MAX_HOLD_H", "8"))

        # Score / class
        self.score_min = int(os.environ.get("SCORE_MIN", "16"))
        self.score_max = 20

        # Cooldown (pair-wide, persisted across restarts)
        self.cooldown_min = int(os.environ.get("COOLDOWN_MIN", "180"))
        self.cooldown_file = os.environ.get("COOLDOWN_FILE", "cooldowns.json")

        # Misc
        self.dry_run = _env_bool("DRY_RUN", False)
        self.rate_min_interval = float(os.environ.get("RATE_MIN_INTERVAL", "1.1"))
        self.http_timeout = float(os.environ.get("HTTP_TIMEOUT", "10"))
        self.retries = int(os.environ.get("HTTP_RETRIES", "3"))

    def validate_startup(self):
        """Fail-safe: without FINNHUB_API_KEY, Forex/Stocks are blocked."""
        if not self.finnhub_api_key:
            if self.enable_forex or self.enable_stocks:
                print("[STARTUP] NO FINNHUB_API_KEY -> Forex/Stocks BLOCKED "
                      "(fail-safe, no blind entries). Set FINNHUB_API_KEY to enable.",
                      file=sys.stderr)
                return False
        if self.telegram_token and not self.telegram_chat_id:
            print("[STARTUP] TELEGRAM_BOT_TOKEN set but TELEGRAM_CHAT_ID missing.",
                  file=sys.stderr)
        return True


# Pair -> base/quote currencies for the economic-calendar filter
PAIR_CURRENCIES = {
    "EURUSD": ("EUR", "USD"), "GBPUSD": ("GBP", "USD"), "USDJPY": ("USD", "JPY"),
    "AUDUSD": ("AUD", "USD"), "GBPJPY": ("GBP", "JPY"), "USDCAD": ("USD", "CAD"),
    "NZDUSD": ("NZD", "USD"), "EURGBP": ("EUR", "GBP"), "EURJPY": ("EUR", "JPY"),
    "AUDJPY": ("AUD", "JPY"), "USDCHF": ("USD", "CHF"),
}

# ---------------------------------------------------------------------------
# Indicators (Wilder / deterministic)
# ---------------------------------------------------------------------------

def ema(values, period):
    """Exponential moving average, SMA-seeded (ta-lib compatible)."""
    n = len(values)
    out = [None] * n
    if n < period:
        return out
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    k = 2.0 / (period + 1.0)
    for i in range(period, n):
        out[i] = values[i] * k + out[i - 1] * (1.0 - k)
    return out


def true_ranges(highs, lows, closes):
    n = len(closes)
    tr = [0.0] * n
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]))
    return tr


def atr_wilder(highs, lows, closes, period=14):
    n = len(closes)
    out = [None] * n
    if n <= period:
        return out
    tr = true_ranges(highs, lows, closes)
    out[period - 1] = sum(tr[:period]) / period
    for i in range(period, n):
        out[i] = (out[i - 1] * (period - 1) + tr[i]) / period
    return out


def adx_wilder(highs, lows, closes, period=14):
    n = len(closes)
    out = [None] * n
    if n < 2 * period:
        return out
    up = [0.0] * n
    dn = [0.0] * n
    tr = [0.0] * n
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        u = highs[i] - highs[i - 1]
        d = lows[i - 1] - lows[i]
        up[i] = u if (u > d and u > 0) else 0.0
        dn[i] = d if (d > u and d > 0) else 0.0
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    atr = [0.0] * n
    pdi = [0.0] * n
    mdi = [0.0] * n
    dx = [0.0] * n
    atr[period] = sum(tr[1:period + 1]) / period
    spdm = sum(up[1:period + 1])
    smdm = sum(dn[1:period + 1])
    pdi[period] = 100.0 * spdm / atr[period] if atr[period] else 0.0
    mdi[period] = 100.0 * smdm / atr[period] if atr[period] else 0.0
    for i in range(period + 1, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
        spdm = (spdm * (period - 1) + up[i]) / period
        smdm = (smdm * (period - 1) + dn[i]) / period
        pdi[i] = 100.0 * spdm / atr[i] if atr[i] else 0.0
        mdi[i] = 100.0 * smdm / atr[i] if atr[i] else 0.0
        s = pdi[i] + mdi[i]
        dx[i] = 100.0 * abs(pdi[i] - mdi[i]) / s if s else 0.0
    start = 2 * period - 1
    if n > start:
        out[start] = sum(dx[period:2 * period]) / period
        for i in range(start + 1, n):
            out[i] = (out[i - 1] * (period - 1) + dx[i]) / period
    return out


def rsi_wilder(closes, period=14):
    n = len(closes)
    out = [None] * n
    if n <= period:
        return out
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        ch = closes[i] - closes[i - 1]
        gains[i] = max(ch, 0.0)
        losses[i] = max(-ch, 0.0)
    ag = sum(gains[1:period + 1]) / period
    al = sum(losses[1:period + 1]) / period
    out[period] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    for i in range(period + 1, n):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        out[i] = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    return out


def find_swings(highs, lows, left=3, right=3):
    """Confirmed pivots. A swing at index i is CONFIRMED only when bar i+right
    has closed (non-repainting). Returns list of (index, price, 'H'|'L')."""
    n = len(highs)
    swings = []
    for i in range(left, n - right):
        if all(highs[i] > highs[i - j] for j in range(1, left + 1)) and \
           all(highs[i] > highs[i + j] for j in range(1, right + 1)):
            swings.append((i, highs[i], "H"))
        if all(lows[i] < lows[i - j] for j in range(1, left + 1)) and \
           all(lows[i] < lows[i + j] for j in range(1, right + 1)):
            swings.append((i, lows[i], "L"))
    return swings


def resample_h4(h1):
    """Aggregate 1h bars into UTC-aligned 4h bars (buckets 0,4,8,12,16,20 UTC)."""
    h4 = []
    cur = None
    for c in h1:
        bucket = int(c["t"]) // 3600
        if bucket % 4 == 0:
            if cur is not None:
                h4.append(cur)
            cur = {"t": c["t"], "o": c["o"], "h": c["h"], "l": c["l"], "c": c["c"], "v": c["v"]}
        elif cur is not None:
            cur["h"] = max(cur["h"], c["h"])
            cur["l"] = min(cur["l"], c["l"])
            cur["c"] = c["c"]
            cur["v"] += c["v"]
    if cur is not None:
        h4.append(cur)
    return h4

# ---------------------------------------------------------------------------
# API clients (Finnhub + Telegram) with throttling and retries
# ---------------------------------------------------------------------------

class FinnhubClient:
    BASE = "https://finnhub.io/api/v1"

    def __init__(self, api_key, min_interval=1.1, timeout=10, retries=3):
        self.api_key = api_key
        self.min_interval = min_interval  # ~55 calls/min (free tier = 60/min)
        self.timeout = timeout
        self.retries = retries
        self._lock = threading.Lock()
        self._last = 0.0

    def _throttle(self):
        with self._lock:
            wait = self.min_interval - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()

    def _get(self, path, params):
        last_err = None
        for attempt in range(self.retries):
            self._throttle()
            try:
                r = requests.get(self.BASE + path,
                                 params={**params, "token": self.api_key},
                                 timeout=self.timeout)
                if r.status_code == 429:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                if r.status_code >= 500:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                last_err = e
                time.sleep(2.0 * (attempt + 1))
        raise RuntimeError(f"Finnhub {path} failed after {self.retries} tries: {last_err}")

    def candles(self, market, symbol, resolution, frm, to):
        if market == "forex":
            return self._get("/forex/candle",
                             {"symbol": symbol, "resolution": resolution,
                              "from": frm, "to": to})
        return self._get("/stock/candle",
                         {"symbol": symbol, "resolution": resolution,
                          "from": frm, "to": to})

    def economic_calendar(self, frm, to):
        return self._get("/calendar/economic", {"from": frm, "to": to})

    def company_news(self, symbol, frm, to):
        return self._get("/company-news", {"symbol": symbol, "from": frm, "to": to})


def send_telegram(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=15)
    r.raise_for_status()
    return r.json()


def parse_candles(payload):
    """Finnhub candle payload -> list of dicts, or [] if s != 'ok'."""
    if not payload or payload.get("s") != "ok":
        return []
    out = []
    for i in range(len(payload["t"])):
        out.append({
            "t": int(payload["t"][i]),
            "o": float(payload["o"][i]),
            "h": float(payload["h"][i]),
            "l": float(payload["l"][i]),
            "c": float(payload["c"][i]),
            "v": float(payload["v"][i]) if payload.get("v") else 0.0,
        })
    return out


def drop_open(candles, tf_sec, now_ts):
    """Drop the last candle if it is still forming (determinism: closed bars only)."""
    out = list(candles)
    if out and now_ts < out[-1]["t"] + tf_sec:
        out = out[:-1]
    return out


def _parse_ev_time(s):
    if not s:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    try:
        return int(datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S")
                   .replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        return None


def classify_session(now_dt):
    h = now_dt.hour
    if 7 <= h < 12:
        return "London"
    if 12 <= h < 16:
        return "Overlap"
    if 16 <= h < 20:
        return "NY"
    return "Other"


# ---------------------------------------------------------------------------
# News filter (hard gate, fail-safe)
# ---------------------------------------------------------------------------

def check_news(cfg, symbol, market, calendar_events, stock_news, now_ts):
    """Returns (ok, detail). Forex: high-impact events +/-45 min for the pair's
    currencies. Stocks: any company news within the window (NA companies only)."""
    if market == "forex":
        currs = PAIR_CURRENCIES.get(symbol, ())
        if not currs:
            return False, "no currency map"
        for ev in calendar_events or []:
            if ev.get("impact") != "high":
                continue
            if ev.get("country") not in currs:
                continue
            t = ev.get("time")
            if t is None:
                continue
            if abs(t - now_ts) <= cfg.news_block_min * 60:
                return False, f"{ev.get('event')} {ev.get('country')} high-impact"
        return True, "clear"
    for it in stock_news or []:
        dt = it.get("datetime")
        if dt is None:
            continue
        if now_ts - cfg.news_block_min * 60 <= dt <= now_ts + cfg.news_block_min * 60:
            return False, (it.get("headline") or "news")[:80]
    return True, "clear"


# ---------------------------------------------------------------------------
# Core pipeline (pure, no I/O) — returns (signal|None, reason)
# ---------------------------------------------------------------------------

def _rej(reason):
    return None, reason


def run_pipeline(cfg, symbol, market, m15, h1, calendar_events, stock_news, now_ts):
    """Evaluate the full chain on closed candles.
    m15/h1: lists of candles (oldest -> newest). now_ts: unix seconds (UTC)."""
    # ---- warm-up ----------------------------------------------------------
    if len(m15) < cfg.warmup_m15:
        return _rej(f"warmup M15 {len(m15)}<{cfg.warmup_m15}")
    h4 = resample_h4(h1)
    if len(h4) < cfg.warmup_h4:
        return _rej(f"warmup H4 {len(h4)}<{cfg.warmup_h4}")

    closes = [c["c"] for c in m15]
    highs = [c["h"] for c in m15]
    lows = [c["l"] for c in m15]
    vols = [c["v"] for c in m15]
    n = len(m15)
    N = n - 2      # sweep candle (closed)
    N1 = n - 1     # displacement/BOS candle (closed, latest)

    # ---- H4 / H1 trend (hard gate) ---------------------------------------
    h4c = [c["c"] for c in h4]
    h1c = [c["c"] for c in h1]
    h4e50 = ema(h4c, 50)
    h4e200 = ema(h4c, 200)
    h1e50 = ema(h1c, 50)
    h1e200 = ema(h1c, 200)
    if h4e200[-1] is None or h1e200[-1] is None:
        return _rej("trend: not enough bars")
    last = m15[-1]["c"]
    h4_above = last > h4e50[-1] and last > h4e200[-1]
    h4_below = last < h4e50[-1] and last < h4e200[-1]
    h1_above = last > h1e50[-1] and last > h1e200[-1]
    h1_below = last < h1e50[-1] and last < h1e200[-1]
    if not (h4_above or h4_below):
        return _rej("trend H4: mixed")
    if not (h1_above or h1_below):
        return _rej("trend H1: mixed")
    if h4_above != h1_above:
        return _rej("trend H4/H1 mismatch")
    direction = "LONG" if h4_above else "SHORT"

    # ---- swings (confirmed as of candle N) --------------------------------
    swings = find_swings(highs, lows, 3, 3)
    confirmed = [s for s in swings if s[0] + 3 <= N]
    swing_highs = [s for s in confirmed if s[2] == "H" and s[0] < N]
    swing_lows = [s for s in confirmed if s[2] == "L" and s[0] < N]
    if not swing_highs or not swing_lows:
        return _rej("no confirmed swings")

    # ---- Liquidity Sweep on candle N (hard gate) --------------------------
    prev_swl = swing_lows[-1][1]
    prev_swh = swing_highs[-1][1]
    cn = m15[N]
    if direction == "LONG":
        swept = cn["l"] < prev_swl and cn["c"] > prev_swl
        swept_level = prev_swl
    else:
        swept = cn["h"] > prev_swh and cn["c"] < prev_swh
        swept_level = prev_swh
    if not swept:
        return _rej("sweep: no")

    # ---- Displacement + BOS on candle N+1 (hard gates) --------------------
    c1 = m15[N1]
    body = abs(c1["c"] - c1["o"])
    avg_body = sum(abs(m15[i]["c"] - m15[i]["o"]) for i in range(N - 20, N)) / 20.0
    disp_ratio = body / avg_body if avg_body else 0.0
    if disp_ratio < cfg.disp_min:
        return _rej(f"displacement {disp_ratio:.2f}<{cfg.disp_min}")

    confirmed1 = [s for s in swings if s[0] + 3 <= N1]
    if direction == "LONG":
        bos_level = max((s[1] for s in confirmed1 if s[2] == "H" and s[0] < N1), default=None)
        bos_ok = bos_level is not None and c1["c"] > bos_level
    else:
        bos_level = min((s[1] for s in confirmed1 if s[2] == "L" and s[0] < N1), default=None)
        bos_ok = bos_level is not None and c1["c"] < bos_level
    if not bos_ok:
        return _rej("BOS: no")

    # ---- ADX (hard gate) --------------------------------------------------
    adx = adx_wilder(highs, lows, closes, cfg.atr_period)
    adx_val = adx[N1]
    if adx_val is None or adx_val < cfg.adx_min:
        return _rej(f"ADX {adx_val}<{cfg.adx_min}")

    # ---- RSI (hard gate) --------------------------------------------------
    rsi = rsi_wilder(closes, cfg.rsi_period)
    rsi_val = rsi[N1]
    if rsi_val is None:
        return _rej("RSI: n/a")
    if direction == "LONG" and not (cfg.rsi_long_min <= rsi_val <= cfg.rsi_long_max):
        return _rej(f"RSI {rsi_val:.1f} out of long band")
    if direction == "SHORT" and not (cfg.rsi_short_min <= rsi_val <= cfg.rsi_short_max):
        return _rej(f"RSI {rsi_val:.1f} out of short band")

    # ---- Volume (hard gate) ----------------------------------------------
    avg_vol = sum(vols[N - cfg.vol_period:N]) / cfg.vol_period
    vol_ratio = vols[N1] / avg_vol if avg_vol else 0.0
    if vol_ratio < cfg.vol_mult_min:
        return _rej(f"volume {vol_ratio:.2f}<{cfg.vol_mult_min}")

    # ---- Session (hard gate) ----------------------------------------------
    now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    if now_dt.weekday() >= 5:
        return _rej("session: weekend")
    hh = now_dt.hour + now_dt.minute / 60.0
    if not (cfg.session_start_h <= hh < cfg.session_end_h):
        return _rej("session: outside window")
    if cfg.session_end_h - hh < cfg.session_min_left / 60.0:
        return _rej("session: <90min left")
    session = classify_session(now_dt)

    # ---- News (hard gate, fail-safe) --------------------------------------
    news_ok, news_detail = check_news(cfg, symbol, market, calendar_events,
                                      stock_news, now_ts)
    if not news_ok:
        return _rej(f"news blocked: {news_detail}")

    # ---- Risk / Reward (hard gate) ----------------------------------------
    atr = atr_wilder(highs, lows, closes, cfg.atr_period)
    atr_val = atr[N1]
    if atr_val is None or atr_val <= 0:
        return _rej("ATR: n/a")
    entry = c1["c"]
    if direction == "LONG":
        sl = max(swept_level - 0.1 * atr_val, entry - cfg.sl_atr_max * atr_val)
        sl = min(sl, entry - cfg.sl_atr_min * atr_val)
    else:
        sl = min(swept_level + 0.1 * atr_val, entry + cfg.sl_atr_max * atr_val)
        sl = max(sl, entry + cfg.sl_atr_min * atr_val)
    risk = abs(entry - sl)
    if risk <= 0:
        return _rej("RR: risk<=0")
    tp1 = entry + cfg.tp1_r * atr_val if direction == "LONG" else entry - cfg.tp1_r * atr_val
    tp2 = entry + cfg.tp2_r * atr_val if direction == "LONG" else entry - cfg.tp2_r * atr_val
    rr = (cfg.tp2_r * atr_val) / risk   # full 2R reward vs risk
    if rr < cfg.rr_min:
        return _rej(f"RR {rr:.2f}<{cfg.rr_min}")

    # ---- Score (ONLY after all hard gates pass) ---------------------------
    score = 10  # all hard gates passed
    if disp_ratio >= cfg.disp_bonus:
        score += 2
    if adx_val >= cfg.adx_bonus:
        score += 2
    if (direction == "LONG" and 55 <= rsi_val <= 70) or \
       (direction == "SHORT" and 30 <= rsi_val <= 45):
        score += 2
    if vol_ratio >= cfg.vol_mult_bonus:
        score += 2
    if session == "Overlap":
        score += 2
    if rr >= cfg.rr_bonus:
        score += 2
    score = min(score, cfg.score_max)

    if score < cfg.score_min:
        return _rej(f"score {score}<{cfg.score_min}")

    klass = "A+" if score >= 18 else ("A" if score >= 16 else "B")
    be_price = entry + cfg.be_r * atr_val if direction == "LONG" else entry - cfg.be_r * atr_val

    sig = {
        "timestamp_utc": now_dt.isoformat(),
        "symbol": symbol,
        "market": market,
        "direction": direction,
        "score": score,
        "class": klass,
        "entry_price": round(entry, 5),
        "stop_loss": round(sl, 5),
        "tp1": round(tp1, 5),
        "tp2": round(tp2, 5),
        "be_price": round(be_price, 5),
        "atr_value": round(atr_val, 5),
        "adx_value": round(adx_val, 2),
        "rsi_value": round(rsi_val, 2),
        "displacement_ratio": round(disp_ratio, 3),
        "volume_ratio": round(vol_ratio, 3),
        "rr": round(rr, 3),
        "session": session,
        "swept_level": round(swept_level, 5),
        "bos_level": round(bos_level, 5),
        "news": news_detail,
        "tp1_r": cfg.tp1_r,
        "tp2_r": cfg.tp2_r,
        "be_r": cfg.be_r,
        "cooldown_min": cfg.cooldown_min,
        "max_hold_h": cfg.max_hold_h,
        "filters_passed": {
            "trend_h4_h1": True, "liquidity_sweep": True, "displacement": True,
            "bos_after_sweep": True, "adx": True, "rsi": True, "volume": True,
            "session": True, "news_clear": True, "rr": True,
        },
        "status": "OPEN",
    }
    return sig, None


# ---------------------------------------------------------------------------
# Engine: data fetching, cooldown, priority, emission
# ---------------------------------------------------------------------------

class Engine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.finnhub = FinnhubClient(cfg.finnhub_api_key, cfg.rate_min_interval,
                                     cfg.http_timeout, cfg.retries)
        self.cooldowns = self._load_cooldowns()

    def _load_cooldowns(self):
        try:
            with open(self.cfg.cooldown_file) as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_cooldowns(self):
        try:
            with open(self.cfg.cooldown_file, "w") as f:
                json.dump(self.cooldowns, f)
        except Exception as e:
            print(f"[WARN] cooldown save failed: {e}", file=sys.stderr)

    def in_cooldown(self, symbol, now_ts):
        last = self.cooldowns.get(symbol)
        if last is None:
            return False
        return (now_ts - last) < self.cfg.cooldown_min * 60

    def fetch_all(self, market, symbol, now_ts):
        to = int(now_ts)
        if market == "forex":
            sym = f"OANDA:{symbol}"
            m15 = parse_candles(self.finnhub.candles("forex", sym, self.cfg.tf_m15,
                                                     to - self.cfg.fetch_m15 * 900, to))
            h1 = parse_candles(self.finnhub.candles("forex", sym, self.cfg.tf_h1,
                                                     to - self.cfg.fetch_h1 * 3600, to))
        else:
            m15 = parse_candles(self.finnhub.candles("stock", symbol, self.cfg.tf_m15,
                                                     to - self.cfg.fetch_m15 * 900, to))
            h1 = parse_candles(self.finnhub.candles("stock", symbol, self.cfg.tf_h1,
                                                    to - self.cfg.fetch_h1 * 3600, to))
        m15 = drop_open(m15, 900, now_ts)
        h1 = drop_open(h1, 3600, now_ts)
        return m15, h1

    def fetch_news_all(self, now_ts):
        """Returns (calendar_events, stock_news). Raises on forex calendar failure
        (fail-safe -> caller blocks everything)."""
        cfg = self.cfg
        cal = None
        stock_news = {}
        if cfg.enable_forex:
            frm = int(now_ts) - 2 * 3600
            to = int(now_ts) + 12 * 3600
            payload = self.finnhub.economic_calendar(frm, to)  # raises on failure
            cal = []
            for ev in payload.get("economicCalendar", []) or []:
                cal.append({
                    "impact": (ev.get("impact") or "").lower(),
                    "country": ev.get("country") or "",
                    "event": ev.get("event") or "",
                    "time": _parse_ev_time(ev.get("time")),
                })
        if cfg.enable_stocks:
            frm = int(now_ts) - 2 * 3600
            to = int(now_ts)
            for s in cfg.stock_symbols:
                try:
                    stock_news[s] = self.finnhub.company_news(s, frm, to) or []
                except Exception as e:
                    print(f"[WARN] company-news {s} failed: {e} "
                          f"(fail-safe: symbol blocked)", file=sys.stderr)
                    stock_news[s] = None
        return cal, stock_news

    def scan(self, now_ts=None, dry_run=None):
        cfg = self.cfg
        now_ts = int(now_ts or time.time())
        dry_run = cfg.dry_run if dry_run is None else dry_run

        # Fail-safe news fetch: any error -> block ALL signals
        try:
            cal_events, stock_news = self.fetch_news_all(now_ts)
        except Exception as e:
            print(f"[NEWS-FAIL] {e} -> ALL SIGNALS BLOCKED (fail-safe)", file=sys.stderr)
            return []

        candidates = []
        if cfg.enable_forex:
            for sym in cfg.forex_pairs:
                if self.in_cooldown(sym, now_ts):
                    print(f"[SKIP] {sym}: cooldown")
                    continue
                try:
                    m15, h1 = self.fetch_all("forex", sym, now_ts)
                    sig, reason = run_pipeline(cfg, sym, "forex", m15, h1,
                                               cal_events, {}, now_ts)
                except Exception as e:
                    print(f"[ERR] {sym}: {e}", file=sys.stderr)
                    continue
                if sig:
                    candidates.append(sig)
                else:
                    print(f"[SKIP] {sym}: {reason}")
        if cfg.enable_stocks:
            for sym in cfg.stock_symbols:
                if self.in_cooldown(sym, now_ts):
                    print(f"[SKIP] {sym}: cooldown")
                    continue
                if stock_news.get(sym) is None:
                    print(f"[SKIP] {sym}: news fetch failed (fail-safe)")
                    continue
                try:
                    m15, h1 = self.fetch_all("stock", sym, now_ts)
                    sig, reason = run_pipeline(cfg, sym, "stock", m15, h1,
                                               None, stock_news.get(sym, []), now_ts)
                except Exception as e:
                    print(f"[ERR] {sym}: {e}", file=sys.stderr)
                    continue
                if sig:
                    candidates.append(sig)
                else:
                    print(f"[SKIP] {sym}: {reason}")

        if not candidates:
            return []

        # Priority: highest score first, then configured pair order
        def prio(sig):
            if sig["market"] == "forex":
                idx = cfg.forex_pairs.index(sig["symbol"]) if sig["symbol"] in cfg.forex_pairs else 999
            else:
                idx = cfg.stock_symbols.index(sig["symbol"]) if sig["symbol"] in cfg.stock_symbols else 999
            return (-sig["score"], idx)
        candidates.sort(key=prio)
        best = candidates[0]

        self.cooldowns[best["symbol"]] = now_ts
        self._save_cooldowns()
        return [best]


# ---------------------------------------------------------------------------
# Output: Telegram payload + JSON journal
# ---------------------------------------------------------------------------

def build_telegram(sig):
    d = sig["direction"]
    arrow = "🟢 LONG" if d == "LONG" else "🔴 SHORT"
    lines = [
        f"🚨 СИГНАЛ v2.0: {sig['symbol']} {arrow} ({sig['class']})",
        "",
        f"📍 Вход по рынку: {sig['entry_price']}",
        f"🛑 Stop: {sig['stop_loss']}",
        f"🎯 TP1 ({sig['tp1_r']}R · 50%): {sig['tp1']}",
        f"🎯 TP2 ({sig['tp2_r']}R · 50%): {sig['tp2']}",
        f"🔁 BE на {sig['be_r']}R: {sig['be_price']}",
        "",
        f"⚙️ РЕЖИМ (Score: {sig['score']}/20 · {sig['class']}):",
        "• Trend H4+H1: ✅",
        "• Liquidity Sweep: ✅",
        f"• Displacement: ✅ {sig['displacement_ratio']}x",
        "• BOS (после Sweep): ✅",
        f"• ADX(14): ✅ {sig['adx_value']}",
        f"• RSI(14): ✅ {sig['rsi_value']}",
        f"• Volume: ✅ {sig['volume_ratio']}x",
        f"• Session: ✅ {sig['session']}",
        "• News: ✅ clear",
        f"• RR: ✅ {sig['rr']}",
        "",
        f"⏱️ Cooldown: {sig['cooldown_min']} мин по паре",
        f"⏱️ Max Hold: {sig['max_hold_h']} ч",
    ]
    return "\n".join(lines)


def journal_signal(sig, path="journal.jsonl"):
    with open(path, "a") as f:
        f.write(json.dumps(sig, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Backtest hook (walk history, print signal stats)
# ---------------------------------------------------------------------------

def backtest(cfg):
    eng = Engine(cfg)
    now_ts = int(time.time())
    start_ts = now_ts - 250 * 86400
    print(f"[BT] window {datetime.fromtimestamp(start_ts, tz=timezone.utc)} -> "
          f"{datetime.fromtimestamp(now_ts, tz=timezone.utc)}")

    cal = []
    if cfg.enable_forex:
        try:
            payload = eng.finnhub.economic_calendar(start_ts, now_ts)
            for ev in payload.get("economicCalendar", []) or []:
                cal.append({"impact": (ev.get("impact") or "").lower(),
                            "country": ev.get("country") or "",
                            "event": ev.get("event") or "",
                            "time": _parse_ev_time(ev.get("time"))})
        except Exception as e:
            print(f"[BT-WARN] calendar fetch failed: {e} "
                  f"(news filter disabled in backtest)", file=sys.stderr)

    total = 0
    per_day = {}
    for market, symbols in (("forex", cfg.forex_pairs), ("stock", cfg.stock_symbols)):
        if market == "forex" and not cfg.enable_forex:
            continue
        if market == "stock" and not cfg.enable_stocks:
            continue
        for sym in symbols:
            try:
                m15, h1 = eng.fetch_all(market, sym, now_ts)
            except Exception as e:
                print(f"[BT-ERR] {sym}: {e}", file=sys.stderr)
                continue
            if len(m15) < cfg.warmup_m15:
                print(f"[BT-SKIP] {sym}: warmup")
                continue
            news = {}
            if market == "stock":
                try:
                    news[sym] = eng.finnhub.company_news(sym, start_ts, now_ts) or []
                except Exception:
                    news[sym] = []
            for i in range(cfg.warmup_m15, len(m15)):
                ts = m15[i]["t"]
                sub15 = m15[:i + 1]
                subh1 = [c for c in h1 if c["t"] <= ts]
                if len(subh1) < cfg.warmup_h4 * 4:
                    continue
                sig, _ = run_pipeline(cfg, sym, market, sub15, subh1,
                                      cal, news.get(sym, []), ts)
                if sig:
                    total += 1
                    day = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
                    per_day[day] = per_day.get(day, 0) + 1
                    print(f"[BT-SIGNAL] {day} {sym} {sig['direction']} "
                          f"score={sig['score']} {sig['class']}")
    days = len(per_day)
    print(f"[BT] signals={total} days_with_signals={days} "
          f"avg_per_day={total / max(days, 1):.2f}")
    print("[BT] NOTE: news filter uses calendar fetched once; free-tier history "
          "may be limited.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    cfg = Config()
    if not cfg.validate_startup():
        sys.exit(1)
    if "--backtest" in sys.argv:
        backtest(cfg)
        return
    dry = "--dry-run" in sys.argv or cfg.dry_run
    eng = Engine(cfg)
    sigs = eng.scan(dry_run=dry)
    if not sigs:
        print(f"[{datetime.now(timezone.utc).isoformat()}] NO SIGNAL")
        return
    for sig in sigs:
        text = build_telegram(sig)
        print(text)
        journal_signal(sig)
        if dry:
            print("[DRY-RUN] Telegram NOT sent")
        elif not cfg.telegram_token or not cfg.telegram_chat_id:
            print("[WARN] Telegram not configured — signal logged only", file=sys.stderr)
        else:
            send_telegram(cfg.telegram_token, cfg.telegram_chat_id, text)
            print("[OK] Telegram sent")


if __name__ == "__main__":
    main()
