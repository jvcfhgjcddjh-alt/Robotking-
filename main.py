"""
╔══════════════════════════════════════════════════════════════════════════╗
║         SMC SIGNAL ENGINE  v3  — Smart Money Concepts ELITE             ║
║                                                                          ║
║  NOUVEAUTÉS v3 :                                                         ║
║  ✦ AMD  Accumulation → Manipulation → Distribution  (H4)                ║
║  ✦ SEPTUPLE TRACTION H4  — 7 bougies momentum institutionnel            ║
║  ✦ SUPPLY & DEMAND ZONES  — zones institutionnelles vraies              ║
║  ✦ LIQUIDITY MAP AVANCÉE  — EQH/EQL · BSL/SSL · intra-range            ║
║  ✦ BTC UNIQUEMENT pour le crypto                                        ║
║  ✦ CONFIRMATION H4 → M15 → M5  (3 TF institutionnels)                  ║
║  ✦ BREAKER BLOCK amélioré                                               ║
║  ✦ BOUGIES D'ENTRÉE institutionnelles  (Displacement · OFS · Imb.)     ║
║                                                                          ║
║   FOREX  |  BTC  |  GOLD  |  INDICES                                    ║
╚══════════════════════════════════════════════════════════════════════════╝

Installation :
    pip install yfinance pandas numpy colorama flask requests

Usage :
    python smc_signals_v3.py                    # scan complet live
    python smc_signals_v3.py --cat forex        # forex seulement
    python smc_signals_v3.py --cat btc          # BTC seulement
    python smc_signals_v3.py --cat priority     # Gold + BTC
    python smc_signals_v3.py --symbol BTC-USD   # symbole unique
    python smc_signals_v3.py --scan             # scan unique (test)
    python smc_signals_v3.py --min-score 80     # filtre score
"""

import argparse
import threading
import time
import os
import requests
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

import logging
import sys

import numpy as np
import pandas as pd
import yfinance as yf

# ── Flask — serveur HTTP pour Render ─────────────────────────
from flask import Flask, jsonify

flask_app = Flask(__name__)

_STATUS: dict = {
    "started_at"   : None,
    "last_scan"    : None,
    "cycle"        : 0,
    "symbols_count": 0,
    "last_signals" : [],
    "scan_running" : False,
}
_STATUS_LOCK = threading.Lock()


@flask_app.route("/")
def index():
    with _STATUS_LOCK:
        st = dict(_STATUS)

    signals_html = ""
    for s in reversed(st["last_signals"][-15:]):
        color    = "#e74c3c" if s["direction"] == "SHORT" else "#2ecc71"
        mode_col = {"AMD": "#9b59b6", "PRE-BOS": "#f39c12",
                    "SMC": "#58a6ff", "SWEEP_SHIFT": "#e67e22",
                    "CHOCH_LIQ": "#1abc9c", "BREAKER_HTF": "#e74c3c",
                    "OFS": "#27ae60", "SMC_TRADER": "#f1c40f"}.get(s.get("mode", "SMC"), "#58a6ff")
        signals_html += (
            f"<tr>"
            f"<td>{s['ts']}</td>"
            f"<td><b>{s['market']}</b></td>"
            f"<td style='color:{color};font-weight:bold'>{s['direction']}</td>"
            f"<td><span style='background:{mode_col};color:#000;padding:2px 6px;"
            f"border-radius:4px;font-size:.8em;font-weight:bold'>{s.get('mode','SMC')}</span></td>"
            f"<td>{s['entry']}</td>"
            f"<td style='color:#e74c3c'>{s['sl']}</td>"
            f"<td style='color:#2ecc71'>{s['tp']}</td>"
            f"<td>1:{s['rr']}</td>"
            f"<td>{s['score']}/100</td>"
            f"<td>{s['lot']} lot</td>"
            f"</tr>"
        )

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="refresh" content="30">
  <title>SMC Signal Engine v3</title>
  <style>
    body  {{ font-family: monospace; background:#0d1117; color:#c9d1d9; margin:2em; }}
    h1    {{ color:#58a6ff; }}
    h2    {{ color:#8b949e; border-bottom:1px solid #30363d; padding-bottom:.3em; }}
    table {{ border-collapse:collapse; width:100%; }}
    th    {{ background:#161b22; color:#8b949e; padding:.5em 1em; text-align:left; }}
    td    {{ padding:.4em 1em; border-bottom:1px solid #21262d; }}
    .badge{{ display:inline-block; padding:.2em .6em; border-radius:4px; font-size:.85em; font-weight:bold; }}
    .live {{ background:#2ecc71; color:#000; }}
    .idle {{ background:#f39c12; color:#000; }}
  </style>
</head>
<body>
  <h1>⚡ SMC Signal Engine v3 — AMD · Supply/Demand · Septuple Traction</h1>
  <p>
    Statut : <span class="badge {'live' if st['scan_running'] else 'idle'}">
      {'🟢 SCAN ACTIF' if st['scan_running'] else '🟡 EN ATTENTE'}
    </span>
    &nbsp;|&nbsp; Démarré : <b>{st['started_at'] or '—'}</b>
    &nbsp;|&nbsp; Cycle : <b>#{st['cycle']}</b>
    &nbsp;|&nbsp; Marchés : <b>{st['symbols_count']}</b>
    &nbsp;|&nbsp; Dernier scan : <b>{st['last_scan'] or '—'}</b>
  </p>
  <p style="color:#8b949e;font-size:.85em">⟳ Rafraîchissement toutes les 30s</p>
  <h2>📋 Derniers signaux</h2>
  {'<p style="color:#f39c12">Aucun signal validé pour le moment.</p>' if not st['last_signals'] else f"""
  <table>
    <tr>
      <th>Heure UTC</th><th>Marché</th><th>Direction</th><th>Mode</th>
      <th>Entrée</th><th>SL 🔴</th><th>TP 🟢</th><th>R:R</th><th>Score</th><th>Lot</th>
    </tr>{signals_html}
  </table>"""}
  <h2>⚙️ Configuration</h2>
  <table>
    <tr><th>Paramètre</th><th>Valeur</th></tr>
    <tr><td>Score minimum</td><td>{SCORE_THRESHOLD}/100</td></tr>
    <tr><td>RR minimum</td><td>1:{MIN_RR}</td></tr>
    <tr><td>Risque/trade</td><td>${RISK_USD}</td></tr>
    <tr><td>Timeframes</td><td>H4 → H1 → M15</td></tr>
    <tr><td>Modes</td><td>SMC + AMD + Septuple + Supply/Demand + 4H Sweep+5M Shift + CHoCH+EQL</td></tr>
    <tr><td>BTC</td><td>🟢 Scan 24/7 (weekends inclus)</td></tr>
    <tr><td>Intervalle scan</td><td>5 minutes</td></tr>
  </table>
</body>
</html>"""
    return html


@flask_app.route("/status")
def status_json():
    with _STATUS_LOCK:
        return jsonify(_STATUS)


def start_flask(port: int = 10000) -> None:
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


def start_self_ping(port: int = 10000) -> None:
    url = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{port}")
    ping_url = f"{url}/status"

    def _ping_loop():
        time.sleep(30)
        while True:
            try:
                r = requests.get(ping_url, timeout=10)
                if r.status_code != 200:
                    log.warning(f"  ⚠ Self-ping HTTP {r.status_code}")
            except Exception as e:
                log.warning(f"  ⚠ Self-ping échoué : {e}")
            time.sleep(240)

    t = threading.Thread(target=_ping_loop, daemon=True, name="self-ping")
    t.start()
    log.info(f"  ✓ Self-ping actif → {ping_url}")


try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    COLOR = True
except ImportError:
    COLOR = False

# ═════════════════════════════════════════════════════════════
#  CONFIGURATION GLOBALE
# ═════════════════════════════════════════════════════════════
HTF             = "4h"    # ← H4 : biais institutionnel + AMD + Septuple Traction
MTF             = "1h"    # H1  : confirmation structure
LTF             = "15m"   # M15 : entrée précise

FVG_MIN_RATIO   = 0.0002
OB_LOOKBACK     = 5
LIQ_THRESHOLD   = 0.0004
SCORE_THRESHOLD = 80
MIN_RR          = 3.0
RISK_USD        = 100.0

# ── Septuple Traction : N bougies consécutives minimum ───────
SEPTUPLE_MIN_CANDLES = 5   # 5 suffisent en practice (7 = très rare)

# ── AMD Phase Detection ────────────────────────────────────
AMD_LOOKBACK = 50   # bougies H4 pour détecter les 3 phases

# ── Supply & Demand Zones ─────────────────────────────────
SD_MIN_IMPULSE_RATIO = 1.5  # corps bougie ≥ 1.5× ATR pour qualifier une zone S/D
SD_ZONE_BUFFER       = 0.15  # tolérance 15% de l'ATR pour "dans la zone"

# ─────────────────────────────────────────────────────────────
#  SESSIONS ACTIVES (UTC)
# ─────────────────────────────────────────────────────────────
SESSION_WINDOWS_UTC: list[tuple[int, int]] = [
    (0,  5),    # Tokyo/Asian session
    (6,  17),   # Pré-London + London open → NY close
]


def is_session_active() -> bool:
    hour = datetime.now(timezone.utc).hour
    return any(start <= hour < end for start, end in SESSION_WINDOWS_UTC)


def is_weekend() -> bool:
    """Retourne True si on est samedi ou dimanche (UTC)."""
    return datetime.now(timezone.utc).weekday() >= 5   # 5=Sat, 6=Sun


def is_crypto_symbol(symbol: str) -> bool:
    """BTC et autres crypto tradent 24/7, y compris le weekend."""
    return symbol in ("BTC-USD", "ETH-USD", "BTC-USDT", "ETH-USDT")

GOLD_SYMBOLS = {"GC=F", "SI=F", "CL=F", "BZ=F"}

def is_gold_session_active() -> bool:
    """Gold trade aussi le dimanche soir dès 23h00 UTC."""
    now = datetime.now(timezone.utc)
    if now.weekday() == 5:   # samedi → fermé
        return False
    if now.weekday() == 6:   # dimanche → ouvert à partir de 23h UTC
        return now.hour >= 23
    return True  # lundi–vendredi toujours ouvert


# ─────────────────────────────────────────────────────────────
#  ATR MINIMUM PAR INSTRUMENT
# ─────────────────────────────────────────────────────────────
ATR_MIN: dict[str, float] = {
    # Forex majeurs — calibrés sur ATR M15 observé (session pré-London incluse)
    "EURUSD=X": 0.00035, "GBPUSD=X": 0.00040, "USDJPY=X": 0.035,
    "USDCHF=X": 0.00035, "AUDUSD=X": 0.00022, "NZDUSD=X": 0.00018,
    "USDCAD=X": 0.00035, "GBPJPY=X": 0.070,   "EURJPY=X": 0.050,
    "GBPAUD=X": 0.00080, "GBPCAD=X": 0.00080, "GBPNZD=X": 0.00100,
    "EURGBP=X": 0.00020, "EURAUD=X": 0.00060, "EURCAD=X": 0.00060,
    "AUDJPY=X": 0.045,   "CADJPY=X": 0.040,   "CHFJPY=X": 0.060,
    "NZDJPY=X": 0.040,
    # Matières premières
    "GC=F"    : 1.20,    "SI=F"    : 0.04,
    "CL=F"    : 0.25,    "BZ=F"    : 0.25,
    # Crypto
    "BTC-USD" : 150.0,   "ETH-USD" : 8.0,
    # Indices US — pas de filtre ATR strict (pas de spread toxique)
    "^GSPC"   : 5.0,     "^NDX"    : 20.0,    "^DJI"    : 50.0,
    # Indices EU — même logique
    "^GDAXI"  : 30.0,    "^FCHI"   : 10.0,    "^FTSE"   : 15.0,
}
ATR_MIN_DEFAULT = 0.00050
MAX_SPREAD_ATR_RATIO = 0.35   # yfinance spreads légèrement > brokers réels


def check_volatility(symbol: str, df_ltf: pd.DataFrame,
                     df_mtf: pd.DataFrame | None = None) -> tuple[bool, str]:
    if df_ltf.empty or len(df_ltf) < 14:
        return False, "données insuffisantes"

    atr_ltf = (df_ltf["high"] - df_ltf["low"]).rolling(14).mean().iloc[-1]

    # Spread/ATR calculé sur M15 (plus stable que M5 pour yfinance)
    # Si M15 dispo, on l'utilise pour le ratio ; sinon fallback M5
    if df_mtf is not None and not df_mtf.empty and len(df_mtf) >= 14:
        atr_for_spread = (df_mtf["high"] - df_mtf["low"]).rolling(14).mean().iloc[-1]
    else:
        atr_for_spread = atr_ltf

    # ATR minimum dynamique : moyenne 100 bougies M5 × 0.5
    atr_mean = (df_ltf["high"] - df_ltf["low"]).rolling(100).mean().iloc[-1]
    if not pd.isna(atr_mean) and atr_mean > 0:
        atr_min = atr_mean * 0.5
    else:
        close = df_ltf["close"].iloc[-1]
        if is_crypto_symbol(symbol) and close > 0:
            atr_min = close * 0.0012
        else:
            atr_min = ATR_MIN.get(symbol, ATR_MIN_DEFAULT) * 0.7

    # ── Facteur pré-session (05h–09h UTC) ────────────────────
    hour_utc = datetime.now(timezone.utc).hour
    if 5 <= hour_utc < 9:
        atr_min *= 0.60

    spread = get_spread(symbol)
    if atr_ltf < atr_min:
        return False, f"ATR trop faible ({round(atr_ltf, 5)} < {round(atr_min, 5)})"
    ratio = spread / atr_for_spread if atr_for_spread > 0 else 1.0
    if ratio > MAX_SPREAD_ATR_RATIO:
        return False, f"spread/ATR={round(ratio*100,1)}% > {int(MAX_SPREAD_ATR_RATIO*100)}%"
    # Les cryptos (BTC) tradent 24/7 — pas de filtre session
    if is_crypto_symbol(symbol):
        return True, ""
    # Gold/matières premières : filtre session spécifique (dim soir ok)
    if symbol in GOLD_SYMBOLS:
        if not is_gold_session_active():
            return False, "weekend — Gold fermé (sam + dim avant 23h UTC)"
        return True, ""
    if is_weekend():
        return False, "weekend — marché fermé (Forex)"
    if not is_session_active():
        return False, "hors session (London/NY)"
    return True, ""


# ─────────────────────────────────────────────────────────────
#  SPREADS
# ─────────────────────────────────────────────────────────────
SPREAD_TABLE: dict[str, float] = {
    "EURUSD=X": 0.00008, "GBPUSD=X": 0.00010, "USDJPY=X": 0.009,
    "USDCHF=X": 0.00010, "AUDUSD=X": 0.00010, "NZDUSD=X": 0.00013,
    "USDCAD=X": 0.00012, "EURGBP=X": 0.00013, "EURJPY=X": 0.012,
    "EURCHF=X": 0.00018, "EURAUD=X": 0.00020, "EURCAD=X": 0.00020,
    "EURNZD=X": 0.00025, "GBPJPY=X": 0.018,   "GBPCHF=X": 0.00022,
    "GBPAUD=X": 0.00025, "GBPCAD=X": 0.00025, "GBPNZD=X": 0.00030,
    "AUDJPY=X": 0.012,   "CADJPY=X": 0.015,   "CHFJPY=X": 0.015,
    "NZDJPY=X": 0.015,   "AUDCAD=X": 0.00018, "AUDCHF=X": 0.00018,
    "AUDNZD=X": 0.00020, "NZDCAD=X": 0.00020, "NZDCHF=X": 0.00020,
    "CADCHF=X": 0.00018, "USDMXN=X": 0.003,   "USDZAR=X": 0.005,
    "USDTRY=X": 0.010,   "USDSEK=X": 0.004,   "USDNOK=X": 0.004,
    "USDSGD=X": 0.00020, "USDHKD=X": 0.00030,
    "GC=F"    : 0.30,    "SI=F"    : 0.015,
    "CL=F"    : 0.03,    "BZ=F"    : 0.04,    "NG=F"    : 0.003,
    "BTC-USD" : 15.0,    "ETH-USD" : 0.80,
    "^GSPC"   : 0.30,    "^NDX"    : 0.50,    "^DJI"    : 2.00,
    "^GDAXI"  : 1.00,    "^FCHI"   : 1.00,    "^FTSE"   : 1.00,
    "^N225"   : 5.00,    "^HSI"    : 5.00,
}


def get_spread(symbol: str) -> float:
    return SPREAD_TABLE.get(symbol, 0.00015)


# ─────────────────────────────────────────────────────────────
#  CORRÉLATION GUARD
# ─────────────────────────────────────────────────────────────
_CORR_GROUPS: dict[str, str] = {
    "EURUSD=X": "USD", "GBPUSD=X": "USD", "AUDUSD=X": "USD", "NZDUSD=X": "USD",
    "USDJPY=X": "USD", "USDCHF=X": "USD", "USDCAD=X": "USD",
    "GBPJPY=X": "JPY", "EURJPY=X": "JPY", "AUDJPY=X": "JPY",
    "CADJPY=X": "JPY", "CHFJPY=X": "JPY", "NZDJPY=X": "JPY",
    "GBPAUD=X": "GBP", "GBPCAD=X": "GBP", "GBPNZD=X": "GBP", "EURGBP=X": "GBP",
    "EURAUD=X": "EUR", "EURCAD=X": "EUR", "EURNZD=X": "EUR",
    "GC=F"    : "GOLD", "SI=F"    : "GOLD",
    "CL=F"    : "OIL",  "BZ=F"    : "OIL",
    "BTC-USD" : "BTC",
    "^GSPC"   : "US_IDX", "^NDX"  : "US_IDX", "^DJI"  : "US_IDX",
    "^GDAXI"  : "EU_IDX", "^FCHI" : "EU_IDX",
}

_active_corr_groups: dict[str, float] = {}
CORR_TTL = 900


def correlation_guard_reset() -> None:
    _active_corr_groups.clear()


def correlation_guard(symbol: str, direction: str) -> tuple[bool, str]:
    group = _CORR_GROUPS.get(symbol)
    if group is None:
        return True, ""
    key    = f"{group}:{direction}"
    now_ts = time.time()
    if key in _active_corr_groups:
        if now_ts - _active_corr_groups[key] > CORR_TTL:
            del _active_corr_groups[key]
        else:
            return False, f"corrélation {group} {direction} active"
    _active_corr_groups[key] = now_ts
    return True, ""


# ─────────────────────────────────────────────────────────────
#  TELEGRAM
# ─────────────────────────────────────────────────────────────
_TG_TOKEN_ENV = os.environ.get("TG_TOKEN", "")
# TG_ENABLED : true automatiquement si TG_TOKEN est défini, sauf si explicitement désactivé
_TG_ENABLED   = bool(_TG_TOKEN_ENV) if os.environ.get("TG_ENABLED", "") == "" else \
                os.environ.get("TG_ENABLED", "false").lower() == "true"

if not _TG_TOKEN_ENV:
    print("  [TG] ⚠  TG_TOKEN absent — envoi Telegram désactivé")

TELEGRAM_TOKEN     = _TG_TOKEN_ENV
TELEGRAM_CHAT_ID   = None
TELEGRAM_GROUP_ID  = "-1002335466840"
TELEGRAM_LEADER_ID = os.environ.get("TG_LEADER_ID", "6982051442")

SIGNAL_COOLDOWN = 600
_signal_cache: dict[str, float] = {}
_setup_sent: dict[str, bool] = {}


def _setup_key(symbol: str, direction: str, score: int) -> str:
    bucket = (score // 5) * 5
    return f"{symbol}:{direction}:{bucket}"


def is_setup_already_sent(symbol: str, direction: str, score: int) -> bool:
    return _setup_sent.get(_setup_key(symbol, direction, score), False)


def mark_setup_sent(symbol: str, direction: str, score: int) -> None:
    _setup_sent[_setup_key(symbol, direction, score)] = True


def reset_setup(symbol: str) -> None:
    keys_to_del = [k for k in _setup_sent if k.startswith(f"{symbol}:")]
    for k in keys_to_del:
        del _setup_sent[k]


def _tg_url(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def tg_get_chat_id() -> Optional[str]:
    global TELEGRAM_GROUP_ID
    try:
        r = requests.get(_tg_url("getUpdates"), timeout=10)
        updates = r.json().get("result", [])
        personal_id = None
        for upd in reversed(updates):
            msg = upd.get("message") or upd.get("channel_post", {})
            if not msg:
                continue
            chat      = msg.get("chat", {})
            chat_type = chat.get("type", "")
            cid       = str(chat.get("id", ""))
            if chat_type in ("group", "supergroup") and not TELEGRAM_GROUP_ID:
                TELEGRAM_GROUP_ID = cid
            elif chat_type == "private" and not personal_id:
                personal_id = cid
        return personal_id
    except Exception:
        pass
    return None


def tg_send(text: str, chat_id: str) -> bool:
    try:
        r = requests.post(
            _tg_url("sendMessage"),
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        print(c(f"  [TG] Erreur : {e}", "red"))
        return False


# ── Compteur de signaux ────────────────────────────────────────────────────
_SIGNAL_COUNTER_FILE = "/tmp/smc_signal_count.txt"

def _next_signal_number() -> int:
    try:
        with open(_SIGNAL_COUNTER_FILE, "r") as f:
            n = int(f.read().strip()) + 1
    except Exception:
        n = 1
    try:
        with open(_SIGNAL_COUNTER_FILE, "w") as f:
            f.write(str(n))
    except Exception:
        pass
    return n

_signal_number_cache: dict[str, int] = {}


def tg_format_signal(sig: "Signal", tier: str = "", mode: str = "SMC",
                     signal_num: int = 0) -> str:
    """Format Telegram correspondant au screenshot du groupe SMC SIGNALS PRO."""
    dec = 2 if sig.entry > 100 else 5

    risk = abs(sig.entry - sig.sl)

    # RR réels calculés sur les cibles structurelles
    def _rr(tp_val: float) -> str:
        if risk <= 0:
            return "—"
        if sig.direction == "LONG":
            r = (tp_val - sig.entry) / risk
        else:
            r = (sig.entry - tp_val) / risk
        return f"1:{round(r, 1)}"

    def _pct(tp_val: float) -> str:
        if sig.entry <= 0:
            return "—"
        v = (tp_val - sig.entry) / sig.entry * 100
        return f"+{round(v,2)}%" if v > 0 else f"{round(v,2)}%"

    def _sl_pct() -> str:
        if sig.entry <= 0:
            return "—"
        v = (sig.sl - sig.entry) / sig.entry * 100
        return f"+{round(v,2)}%" if v > 0 else f"{round(v,2)}%"

    # Lot basé sur SL distance réelle pour $100 de risque
    base_lot = compute_lot(sig.symbol, sig.entry, sig.sl, risk_usd=100.0)

    # TP2 et TP3 structurels (depuis sig, sinon fallback mathématique)
    tp2 = sig.tp2 if sig.tp2 and sig.tp2 != sig.tp else (
        round(sig.entry + 3 * risk, dec) if sig.direction == "LONG"
        else round(sig.entry - 3 * risk, dec)
    )
    tp3 = sig.tp3 if sig.tp3 and sig.tp3 != sig.tp else (
        round(sig.entry + 6 * risk, dec) if sig.direction == "LONG"
        else round(sig.entry - 6 * risk, dec)
    )

    # Gain potentiel par TP en $
    def _gain(tp_val: float) -> str:
        if risk <= 0 or base_lot <= 0:
            return "—"
        if sig.direction == "LONG":
            r = (tp_val - sig.entry) / risk
        else:
            r = (sig.entry - tp_val) / risk
        gain = round(r * 100.0, 0)
        return f"+${int(gain)}"

    if sig.direction == "LONG":
        mom    = "haussier"
        struct = "haussière"
    else:
        mom    = "baissier"
        struct = "baissière"

    # Nom affichage
    sym_map = {"GC=F": "XAUUSD / GOLD", "SI=F": "XAGUSD / SILVER",
               "CL=F": "USOIL", "BZ=F": "UKOIL", "BTC-USD": "BTCUSD / Bitcoin",
               "^GSPC": "S&P 500", "^NDX": "Nasdaq 100", "^DJI": "Dow Jones"}
    sym_display = sym_map.get(sig.symbol,
        sig.symbol.replace("=X", "").replace("-USD", "").replace("^", ""))

    dir_arrow = "🟢 BUY / LONG" if sig.direction == "LONG" else "🔴 SELL / SHORT"
    num_str   = f"#{signal_num}" if signal_num else ""

    msg = (
        f"<b>⭐ SMC SIGNALS PRO</b>\n"
        f"🟢 <b>NOUVEAU SIGNAL {num_str}</b>\n"
        f"<b>{sym_display}</b>\n"
        f"{'─'*30}\n"
        f"💎  <b>SETUP :</b> {mode}\n"
        f"🎯  <b>DIRECTION :</b> <b>{dir_arrow}</b>\n"
        f"💰  <b>ENTRY :</b> <code>{sig.entry}</code>\n"
        f"📦  <b>LOT :</b> <code>{base_lot}</code>  <i>(risque $100)</i>\n"
        f"{'─'*30}\n"
        f"🎯  <b>TP1 :</b> <code>{sig.tp}</code>  {_rr(sig.tp)}  {_pct(sig.tp)}  <b>{_gain(sig.tp)}</b>\n"
        f"🚀  <b>TP2 :</b> <code>{tp2}</code>  {_rr(tp2)}  {_pct(tp2)}  <b>{_gain(tp2)}</b>\n"
        f"💎  <b>TP3 :</b> <code>{tp3}</code>  {_rr(tp3)}  {_pct(tp3)}  <b>{_gain(tp3)}</b>\n"
        f"🔴  <b>SL :</b> <code>{sig.sl}</code>  {_sl_pct()}  <b>-$100</b>\n"
        f"{'─'*30}\n"
        f"✅ <b>CONFLUENCE SMC VALIDÉE</b>\n"
        f"📈 Momentum {mom} + Structure {struct}\n"
        f"🧠 Patience • Discipline • Liquidité\n\n"
        f"<i>@smcsignalspro</i>"
    )
    return msg


# ── Génération du graphique SMC ────────────────────────────────────────────
def generate_chart_image(sig: "Signal") -> Optional[str]:
    """Génère un graphique SMC dark-theme 1280×720 optimisé Telegram et retourne le chemin /tmp/*.png."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
        from matplotlib.patches import Rectangle
        from matplotlib.lines  import Line2D

        df = sig.df_chart
        if df is None or len(df) < 10:
            return None

        df = df.tail(80).reset_index(drop=True)
        n  = len(df)

        # ── Palette dark theme ────────────────────────────────
        BG     = "#0a0c10"; BG2 = "#0d1117"
        GREEN  = "#22c55e"; RED = "#ef4444"
        BLUE   = "#3b82f6"; PURPLE = "#a855f7"
        GOLD   = "#f59e0b"; ORANGE = "#f97316"
        GRAY   = "#64748b"; LGRAY  = "#94a3b8"
        MONO   = "DejaVu Sans Mono"

        # ── Figure 1280×720 — optimisé Telegram sendPhoto ────
        fig, ax = plt.subplots(figsize=(13.33, 7.5), dpi=96, facecolor=BG)
        ax.set_facecolor(BG2)
        for s in ax.spines.values():
            s.set_color("#1e293b"); s.set_linewidth(0.4)

        prices = pd.concat([df["high"], df["low"]])
        p_min  = prices.min() * 0.9992
        p_max  = prices.max() * 1.0008

        # ── Grid ─────────────────────────────────────────────
        import numpy as _np
        for p in _np.linspace(p_min, p_max, 12):
            ax.axhline(p, color="#1e293b", lw=0.5, ls="--", alpha=0.6)

        # ── FVG ───────────────────────────────────────────────
        fvg = sig.fvg_chart
        if fvg and p_min <= fvg.top <= p_max:
            x0 = max(0, fvg.index - 2)
            ax.add_patch(Rectangle((x0, fvg.bottom), n - x0, fvg.top - fvg.bottom,
                facecolor=BLUE, alpha=0.15, zorder=1))
            ax.add_patch(Rectangle((x0, fvg.bottom), n - x0, fvg.top - fvg.bottom,
                edgecolor=BLUE, facecolor="none", lw=1.0, ls="--", alpha=0.7, zorder=2))
            ax.text((x0 + min(x0 + 15, n)) / 2, (fvg.top + fvg.bottom) / 2, "FVG",
                color=BLUE, fontsize=7, fontweight="bold", ha="center", va="center",
                fontfamily=MONO, bbox=dict(fc=BG2, ec=BLUE, boxstyle="round,pad=0.3", alpha=0.9))

        # ── OB ────────────────────────────────────────────────
        ob = sig.ob_chart
        if ob and p_min <= ob.top <= p_max:
            x0 = max(0, ob.index - 2); x1 = min(n, ob.index + 12)
            ax.add_patch(Rectangle((x0, ob.bottom), x1 - x0, ob.top - ob.bottom,
                facecolor=PURPLE, alpha=0.18, zorder=1))
            ax.add_patch(Rectangle((x0, ob.bottom), x1 - x0, ob.top - ob.bottom,
                edgecolor=PURPLE, facecolor="none", lw=1.0, zorder=2))
            ax.text((x0 + x1) / 2, (ob.top + ob.bottom) / 2, "OB",
                color=PURPLE, fontsize=7, fontweight="bold", ha="center", va="center",
                fontfamily=MONO, bbox=dict(fc=BG2, ec=PURPLE, boxstyle="round,pad=0.3", alpha=0.9))

        # ── BOS / CHoCH ───────────────────────────────────────
        if sig.bos_lv and p_min <= sig.bos_lv <= p_max:
            ax.axhline(sig.bos_lv, color=RED, lw=0.8, ls="--",
                       xmin=0.0, xmax=0.55, zorder=3)
            ax.text(n * 0.25, sig.bos_lv * (1 + 0.00012), "BOS",
                color=RED, fontsize=6, fontweight="bold", fontfamily=MONO)

        if sig.choch_lv and p_min <= sig.choch_lv <= p_max:
            ax.axhline(sig.choch_lv, color=ORANGE, lw=0.8, ls=":",
                       xmin=0.50, xmax=0.80, zorder=3)
            ax.text(n * 0.62, sig.choch_lv * (1 + 0.00012), "CHoCH",
                color=ORANGE, fontsize=6, fontweight="bold", fontfamily=MONO)

        # ── TP / SL / Entry — niveaux structurels réels ───────
        dec  = 2 if sig.entry > 100 else 5
        risk = abs(sig.entry - sig.sl)

        # TP2 / TP3 : structurels depuis sig, fallback math
        tp2 = sig.tp2 if (sig.tp2 and sig.tp2 != sig.tp and sig.tp2 > 0) else (
            round(sig.entry + 3 * risk, dec) if sig.direction == "LONG"
            else round(sig.entry - 3 * risk, dec))
        tp3 = sig.tp3 if (sig.tp3 and sig.tp3 != sig.tp and sig.tp3 > 0) else (
            round(sig.entry + 6 * risk, dec) if sig.direction == "LONG"
            else round(sig.entry - 6 * risk, dec))

        def _rr_label(tp_val: float) -> str:
            if risk <= 0: return ""
            r = (tp_val - sig.entry) / risk if sig.direction == "LONG" \
                else (sig.entry - tp_val) / risk
            return f"  1:{round(r,1)}"

        levels = [
            (tp3,       f"TP3 {tp3}{_rr_label(tp3)}",        GREEN),
            (tp2,       f"TP2 {tp2}{_rr_label(tp2)}",        GREEN),
            (sig.tp,    f"TP1 {sig.tp}{_rr_label(sig.tp)}",  "#86efac"),
            (sig.entry, f"ENTRY {sig.entry}",                  GOLD),
            (sig.sl,    f"SL   {sig.sl}   -$100",             RED),
        ]
        for price, lbl, col in levels:
            if p_min <= price <= p_max:
                ax.axhline(price, color=col, lw=0.9, ls="--", alpha=0.9,
                           xmin=0.45, zorder=2)
                ax.text(n - 0.2, price, lbl, color=col, fontsize=5.5,
                    va="center", ha="right", fontfamily=MONO,
                    bbox=dict(fc=BG2, alpha=0.88, pad=1.5, ec="none"))

        # ── Flèche d'entrée ───────────────────────────────────
        entry_x  = max(n - 14, n // 2)
        dist     = abs(sig.entry - sig.sl)
        arr_start = sig.entry - dist * 0.6 if sig.direction == "LONG" \
                    else sig.entry + dist * 0.6
        ax.annotate("", xy=(entry_x, sig.entry), xytext=(entry_x, arr_start),
            arrowprops=dict(arrowstyle="->", color=GREEN if sig.direction == "LONG"
                            else RED, lw=1.5))
        ax.text(entry_x, arr_start - (p_max - p_min) * 0.004,
            sig.direction, color=GREEN if sig.direction == "LONG" else RED,
            fontsize=7, ha="center", fontweight="bold", fontfamily=MONO)

        # ── Bougies ───────────────────────────────────────────
        w = 0.38  # largeur corps
        for i, row in df.iterrows():
            o, h, l, cl = row["open"], row["high"], row["low"], row["close"]
            up  = cl >= o
            col = GREEN if up else RED
            bh  = max(abs(cl - o), (p_max - p_min) * 0.0005)
            ax.plot([i, i], [l, h], color=col, lw=0.8, zorder=4)
            ax.add_patch(Rectangle((i - w, min(cl, o)), w * 2, bh,
                fc=col if up else "none", ec=col, lw=0.8, zorder=5))

        # ── Titre & watermark ─────────────────────────────────
        sym_display = ({"GC=F": "XAUUSD", "SI=F": "XAGUSD", "BTC-USD": "BTCUSD",
                        "CL=F": "USOIL",  "BZ=F": "UKOIL"}
                       .get(sig.symbol,
                            sig.symbol.replace("=X","").replace("-USD","").replace("^","")))
        ax.text(0.013, 0.975, f"{sym_display}  •  M15  •  SMC v3",
            transform=ax.transAxes, color=LGRAY, fontsize=9,
            va="top", fontfamily=MONO, fontweight="bold")
        ax.text(0.013, 0.935, f"Score {sig.score}/100  •  {sig.mode}  •  {sig.direction}",
            transform=ax.transAxes, color=GRAY, fontsize=7, va="top", fontfamily=MONO)
        ax.text(0.99, 0.015, "@smcsignalspro",
            transform=ax.transAxes, color="#334155", fontsize=6,
            va="bottom", ha="right", fontfamily=MONO)

        ax.set_xlim(-1, n + 2)
        ax.set_ylim(p_min, p_max)
        ax.tick_params(colors=GRAY, labelsize=5, length=2, width=0.4)
        ax.yaxis.set_visible(False)
        ax.set_xticks([])

        plt.tight_layout(pad=0.4)
        safe = (sig.symbol.replace("=X","").replace("-","")
                          .replace("^","").replace(".",""))
        path = f"/tmp/smc_{safe}_{int(time.time())}.png"
        fig.savefig(path, dpi=96, bbox_inches="tight", facecolor=BG,
                    metadata={"Software": "SMC Signal Engine v3"})
        plt.close(fig)
        import gc as _gc; _gc.collect()
        return path

    except Exception as e:
        print(f"  [CHART] Erreur génération graphique : {e}")
        return None


# ── Envoi photo Telegram ───────────────────────────────────────────────────
def tg_send_photo(image_path: str, caption: str, chat_id: str) -> bool:
    """Envoie une image avec caption HTML via sendPhoto."""
    try:
        with open(image_path, "rb") as img:
            r = requests.post(
                _tg_url("sendPhoto"),
                data={"chat_id": chat_id, "caption": caption,
                      "parse_mode": "HTML"},
                files={"photo": img},
                timeout=30,
            )
        if r.status_code != 200:
            print(f"  [TG] sendPhoto HTTP {r.status_code} : {r.text[:200]}")
        return r.status_code == 200
    except Exception as e:
        print(f"  [TG] sendPhoto erreur : {e}")
        return False


def tg_notify(sig: "Signal", tier: str = "", mode: str = "SMC",
              chat_id: Optional[str] = None) -> None:
    global TELEGRAM_CHAT_ID, TELEGRAM_LEADER_ID

    # ── FLAG GLOBAL — mettre TG_ENABLED = True pour activer l'envoi ──
    TG_ENABLED = _TG_ENABLED
    if not TG_ENABLED:
        # Log local uniquement — aucun appel API Telegram
        num = _next_signal_number()
        msg = tg_format_signal(sig, tier, mode, signal_num=num)
        print(c(f"\n  [TG] 🔕 Envoi désactivé (TG_ENABLED=false) — signal #{num} prêt", "yellow"))
        print(f"  [TG] Preview message :\n{msg[:300]}...")
        # Génère quand même le graphique pour vérification locale
        chart_path = generate_chart_image(sig)
        if chart_path:
            print(c(f"  [TG] 📊 Graphique 1280×720 généré : {chart_path}", "cyan"))
        return

    # Récupérer l'ID leader si pas encore connu
    if not TELEGRAM_LEADER_ID:
        TELEGRAM_LEADER_ID = tg_get_chat_id() or ""
    if not TELEGRAM_CHAT_ID:
        TELEGRAM_CHAT_ID = TELEGRAM_LEADER_ID

    # Numéro du signal
    num = _next_signal_number()
    msg = tg_format_signal(sig, tier, mode, signal_num=num)

    # Générer le graphique
    chart_path = generate_chart_image(sig)

    # ── Envoi en DM au leader — TOUJOURS (pas de filtre doublon) ─────
    if TELEGRAM_LEADER_ID:
        if chart_path:
            ok_dm = tg_send_photo(chart_path, msg, TELEGRAM_LEADER_ID)
        else:
            ok_dm = tg_send(msg, TELEGRAM_LEADER_ID)
        print(c(f"  [TG] {'✓ DM leader' if ok_dm else '✗ DM leader échoué'}", "green" if ok_dm else "red"))
    else:
        print(c("  [TG] ⚠ Aucun ID leader — ajoute TG_LEADER_ID dans Render", "yellow"))

    # ── Envoi au GROUPE — filtre doublon actif ────────────────────────
    if is_setup_already_sent(sig.symbol, sig.direction, sig.score):
        print(c(f"  [TG] ⏭ Groupe — setup déjà envoyé ({sig.symbol} {sig.direction})", "yellow"))
    else:
        mark_setup_sent(sig.symbol, sig.direction, sig.score)
        if TELEGRAM_GROUP_ID:
            if chart_path:
                ok_grp = tg_send_photo(chart_path, msg, TELEGRAM_GROUP_ID)
                print(c(f"  [TG] {'✓ Groupe (photo)' if ok_grp else '✗ Groupe photo échoué'}", "green" if ok_grp else "red"))
            else:
                ok_grp = tg_send(msg, TELEGRAM_GROUP_ID)
                print(c(f"  [TG] {'✓ Groupe (texte)' if ok_grp else '✗ Groupe texte échoué'}", "green" if ok_grp else "red"))
        else:
            print(c("  [TG] ⚠ TELEGRAM_GROUP_ID non défini", "red"))

    # Nettoyage fichier temporaire
    if chart_path:
        try:
            import os as _os
            _os.remove(chart_path)
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════
#  DATA CLASSES
# ═════════════════════════════════════════════════════════════

@dataclass
class FVG:
    direction: str
    top:       float
    bottom:    float
    index:     int
    filled:    bool = False


@dataclass
class OrderBlock:
    direction: str
    top:       float
    bottom:    float
    index:     int
    mitigated: bool = False


@dataclass
class SupplyDemandZone:
    """Zone Supply ou Demand institutionnelle."""
    zone_type:  str    # "supply" | "demand"
    top:        float
    bottom:     float
    index:      int
    impulse_size: float  # taille de la bougie impulsive (force de la zone)
    tested:     bool = False


@dataclass
class AmdPhase:
    """
    Résultat de l'analyse de phase AMD.
    phase : "accumulation" | "manipulation" | "distribution" | "unknown"
    sub_phase : "bull_manipulation" | "bear_manipulation" | etc.
    """
    phase:       str
    sub_phase:   str
    direction:   str    # "LONG" | "SHORT" — direction attendue après AMD
    confidence:  int    # 0–100
    range_high:  float
    range_low:   float
    sweep_level: Optional[float] = None
    reasons:     list = field(default_factory=list)


@dataclass
class Signal:
    symbol:    str
    direction: str
    entry:     float
    sl:        float
    tp:        float
    rr:        float
    score:     int
    timestamp: datetime
    htf_bias:  str
    lot:       float = 0.0
    risk_usd:  float = RISK_USD
    mode:      str   = "SMC"   # "SMC" | "AMD" | "SEPTUPLE" | "SD" | "PRE-BOS"
    reasons:   list  = field(default_factory=list)
    # Champs pour la génération du graphique
    df_chart:   object = field(default=None, repr=False)   # pd.DataFrame M5
    fvg_chart:  object = field(default=None, repr=False)   # FVG | None
    ob_chart:   object = field(default=None, repr=False)   # OrderBlock | None
    bos_lv:     float  = 0.0
    choch_lv:   float  = 0.0
    tp2:        float  = 0.0   # cible structurelle RR5-6 (swing suivant)
    tp3:        float  = 0.0   # extension max RR8-10 (liquidité majeure)


# ═════════════════════════════════════════════════════════════
#  HELPERS
# ═════════════════════════════════════════════════════════════

def c(text: str, color: str = "green") -> str:
    if not COLOR:
        return text
    colors = {
        "green": Fore.GREEN, "red": Fore.RED, "yellow": Fore.YELLOW,
        "cyan": Fore.CYAN,   "white": Fore.WHITE, "magenta": Fore.MAGENTA,
        "blue": Fore.BLUE,
    }
    return colors.get(color, "") + text + Style.RESET_ALL


def compute_lot(symbol: str, entry: float, sl: float,
                risk_usd: float = RISK_USD) -> float:
    sl_distance = abs(entry - sl)
    if sl_distance == 0:
        return 0.0
    sym = symbol.upper().replace("=X", "").replace("-", "").replace("^", "")

    if symbol in ("GC=F",):
        lot = risk_usd / (sl_distance * 100.0)
    elif symbol in ("SI=F",):
        lot = risk_usd / (sl_distance * 50.0)
    elif symbol in ("CL=F", "BZ=F"):
        lot = risk_usd / (sl_distance * 1000.0)
    elif symbol in ("NG=F", "HG=F", "PL=F", "PA=F"):
        lot = risk_usd / (sl_distance * 100.0)
    elif sym in ("BTCUSD", "ETHUSD") or symbol in ("BTC-USD", "ETH-USD"):
        return round(risk_usd / sl_distance, 6)
    elif sym in ("GSPC", "NDX", "DJI", "GDAXI", "FCHI", "FTSE", "N225", "HSI"):
        lot = risk_usd / (sl_distance * 10.0)
    elif sym.endswith("JPY"):
        sl_pips = sl_distance / 0.01
        pip_val = 1000.0 / entry
        lot = risk_usd / (sl_pips * pip_val)
    elif sym.startswith("USD"):
        sl_pips = sl_distance / 0.0001
        pip_val = 10.0 / entry
        lot = risk_usd / (sl_pips * pip_val)
    else:
        sl_pips = sl_distance / 0.0001
        lot = risk_usd / (sl_pips * 10.0)

    return max(0.01, round(lot, 2))


def fetch(symbol: str, interval: str, period: str = "5d",
          retries: int = 3, retry_delay: int = 15) -> pd.DataFrame:
    # Fallback sur plusieurs périodes si yfinance échoue
    periods_fallback = list(dict.fromkeys([period, "5d", "10d", "1mo"]))
    for p in periods_fallback:
        for attempt in range(1, retries + 1):
            try:
                try:
                    df = yf.download(symbol, interval=interval, period=p,
                                     auto_adjust=True, progress=False,
                                     threads=False,
                                     multi_level_index=False)
                except TypeError:
                    df = yf.download(symbol, interval=interval, period=p,
                                     auto_adjust=True, progress=False,
                                     threads=False)
                if not df.empty:
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0).str.lower()
                    else:
                        df.columns = df.columns.str.lower()
                    # Normalise les colonnes minimales requises
                    for col in ("open", "high", "low", "close", "volume"):
                        if col not in df.columns:
                            df[col] = float("nan")
                    df.dropna(subset=["close", "high", "low"], inplace=True)
                    df = df[pd.to_numeric(df["close"], errors="coerce").notna()]
                    if not df.empty:
                        return df
                time.sleep(retry_delay * attempt)
            except Exception as e:
                err_str = str(e).lower()
                if ("rate" in err_str or "too many" in err_str or "429" in err_str) \
                        and attempt < retries:
                    time.sleep(retry_delay * attempt)
                    continue
                time.sleep(retry_delay)
        # Période échouée → on tente la suivante
    return pd.DataFrame()


def swing_highs(df: pd.DataFrame) -> list[tuple[int, float]]:
    """Retourne les swing highs (index, valeur) dans les n dernières bougies."""
    result = []
    for i in range(1, len(df) - 1):
        if df["high"].iloc[i] > df["high"].iloc[i-1] and df["high"].iloc[i] > df["high"].iloc[i+1]:
            result.append((i, df["high"].iloc[i]))
    return result


def swing_lows(df: pd.DataFrame) -> list[tuple[int, float]]:
    """Retourne les swing lows (index, valeur)."""
    result = []
    for i in range(1, len(df) - 1):
        if df["low"].iloc[i] < df["low"].iloc[i-1] and df["low"].iloc[i] < df["low"].iloc[i+1]:
            result.append((i, df["low"].iloc[i]))
    return result


# ═════════════════════════════════════════════════════════════
#  ★ SETUP PRIORITAIRE — SÉQUENCE SMC TRADER (H4 → M15 → M5)
#  BOS H4 → Sweep (bougie X) → MSS M15 → FVG/OB M5
# ═════════════════════════════════════════════════════════════

def next_liquidity_target(df_h4: pd.DataFrame, direction: str, price_now: float) -> float:
    """Retourne la prochaine liquidité BSL (LONG) ou SSL (SHORT) sur H4."""
    if len(df_h4) < 5:
        return price_now
    if direction == "LONG":
        candidates = [v for _, v in swing_highs(df_h4) if v > price_now]
        return min(candidates) if candidates else df_h4["high"].iloc[-20:].max()
    else:
        candidates = [v for _, v in swing_lows(df_h4) if v < price_now]
        return max(candidates) if candidates else df_h4["low"].iloc[-20:].min()


@dataclass
class SmcTraderResult:
    detected:      bool
    direction:     str
    sweep_low:     float
    sweep_high:    float
    mss_level:     float
    entry_top:     float
    entry_bottom:  float
    tp_liquidity:  float
    score:         int
    reasons:       list


def detect_smc_trader(
    df_h4:  pd.DataFrame,   # H4  — BOS biais + sweep
    df_m15: pd.DataFrame,   # M15 — MSS confirmation
    df_m5:  pd.DataFrame,   # M5  — FVG/OB entrée précise
    direction: str,
) -> SmcTraderResult:
    """
    Séquence SMC Trader adaptée H4 :
    ① BOS H4 → ② Sweep (bougie X) → ③ MSS M15 → ④ FVG/OB M5
    """
    empty = SmcTraderResult(False, direction, 0, 0, 0, 0, 0, 0, 0, [])
    if len(df_h4) < 20 or len(df_m15) < 15 or len(df_m5) < 10:
        return empty

    atr_h4 = (df_h4["high"] - df_h4["low"]).rolling(14).mean().iloc[-1]
    atr_m5 = (df_m5["high"] - df_m5["low"]).rolling(14).mean().iloc[-1]
    if pd.isna(atr_h4) or atr_h4 == 0:
        return empty

    price_now = df_m5["close"].iloc[-1]
    bos_type  = "bullish" if direction == "LONG" else "bearish"
    reasons   = []
    score     = 0

    # ── ① BOS H4 — biais confirmé ─────────────────────────────
    bos_h4 = detect_bos(df_h4)
    recent_bos = [b for b in bos_h4[-6:] if b["type"] == bos_type]
    if not recent_bos:
        return empty
    score += 15
    reasons.append(f"✅ BOS {bos_type.upper()} H4 confirmé → biais {direction}  (+15)")

    # ── ② SWEEP de liquidité — bougie X ──────────────────────
    sweep_found = False
    sweep_low   = 0.0
    sweep_high  = 0.0

    for i in range(-20, -1):
        abs_i = len(df_h4) + i
        if abs_i < 12:
            continue
        lookback = df_h4.iloc[abs_i - 12: abs_i]
        if len(lookback) < 5:
            continue
        h  = df_h4["high"].iloc[i]
        l  = df_h4["low"].iloc[i]
        cl = df_h4["close"].iloc[i]
        if direction == "LONG":
            prev_low = lookback["low"].min()
            if l < prev_low - atr_h4 * 0.03 and cl > prev_low:
                sweep_found = True
                sweep_low   = l
                sweep_high  = h
                break
        else:
            prev_high = lookback["high"].max()
            if h > prev_high + atr_h4 * 0.03 and cl < prev_high:
                sweep_found = True
                sweep_low   = l
                sweep_high  = h
                break

    if not sweep_found:
        return empty

    score += 25
    sl_anchor = sweep_low if direction == "LONG" else sweep_high
    dec = 2 if price_now > 100 else 5
    reasons.append(
        f"🔥 Sweep {'SSL' if direction == 'LONG' else 'BSL'} — bougie X "
        f"@ {round(sl_anchor, dec)}  → SL anchor  (+25)"
    )

    # ── ③ MSS — Market Structure Shift (M15) ─────────────────
    bos_m15 = detect_bos(df_m15)
    mss_candidates = [b for b in bos_m15[-8:] if b["type"] == bos_type]
    if not mss_candidates:
        return empty

    mss = mss_candidates[-1]
    score += 20
    reasons.append(
        f"📐 MSS M15 — BOS {bos_type} @ {round(mss['level'], dec)}  (+20)"
    )

    # ── ④ FVG ou OB M5 — zone d'entrée ──────────────────────
    fvgs_m5 = detect_fvg(df_m5)
    bos_m5  = detect_bos(df_m5)
    obs_m5  = detect_order_blocks(df_m5, bos_m5)

    entry_top    = 0.0
    entry_bottom = 0.0

    fvg_active = active_fvg(df_m5, fvgs_m5, bos_type)
    if fvg_active:
        entry_top    = max(fvg_active.top, fvg_active.bottom)
        entry_bottom = min(fvg_active.top, fvg_active.bottom)
        score += 15
        reasons.append(f"📍 FVG M5 actif [{round(entry_bottom, dec)} — {round(entry_top, dec)}]  (+15)")
    else:
        ob_match = next((o for o in reversed(obs_m5) if o.direction == bos_type), None)
        if ob_match:
            entry_top    = ob_match.top
            entry_bottom = ob_match.bottom
            score += 12
            reasons.append(f"🧱 OB M5 [{round(entry_bottom, dec)} — {round(entry_top, dec)}]  (+12)")
        else:
            entry_top    = price_now + atr_m5 * 0.3
            entry_bottom = price_now - atr_m5 * 0.3
            score += 5
            reasons.append("⚠️ Entrée au marché (pas de FVG/OB M5)  (+5)")

    # ── TP — prochaine liquidité BSL/SSL ──────────────────────
    tp_liq = next_liquidity_target(df_h4, direction, price_now)
    reasons.append(
        f"🎯 TP → Liquidité {'BSL' if direction == 'LONG' else 'SSL'} "
        f"@ {round(tp_liq, dec)}"
    )

    return SmcTraderResult(
        detected=True,
        direction=direction,
        sweep_low=sweep_low,
        sweep_high=sweep_high,
        mss_level=mss["level"],
        entry_top=entry_top,
        entry_bottom=entry_bottom,
        tp_liquidity=tp_liq,
        score=score,
        reasons=reasons,
    )


# ═════════════════════════════════════════════════════════════
#  ① AMD — ACCUMULATION · MANIPULATION · DISTRIBUTION
#
#  Logique institutionnelle (Wyckoff + SMC moderne) :
#
#  ACCUMULATION  = range comprimé (faible volatilité H4)
#                  Les institutions accumulent des positions long
#                  → Range high/low clairement défini
#
#  MANIPULATION  = faux mouvement qui chasse les stops
#                  • Bull Manipulation : spike sous le range low (BSL sweep)
#                    → institutions achètent les stops des bears
#                  • Bear Manipulation : spike au-dessus du range high
#                    → institutions vendent les stops des bulls
#
#  DISTRIBUTION  = mouvement directionnel APRÈS la manipulation
#                  → C'est là qu'on trade : dans le sens institutionnel
#
#  Signal AMD :
#    Accumulation identifiée + Manipulation validée (sweep) → Long/Short
#    dans la phase Distribution avec confluence H4 + M15
# ═════════════════════════════════════════════════════════════

def detect_amd_phase(df_h4: pd.DataFrame) -> AmdPhase:
    """
    Détecte la phase AMD courante sur H4.

    Algorithme :
    1. Identifie le range des 20 dernières bougies H4
    2. Vérifie si le range est "comprimé" (ATR faible = accumulation)
    3. Détecte le sweep (manipulation) : spike H/L hors range + retour dedans
    4. Si sweep détecté → phase Distribution confirmée
    5. Direction : bullish si sweep du range LOW (chasse bears), bearish si sweep du HIGH

    Score de confiance (0–100) :
      • Range bien défini (plusieurs tests) : +30
      • Sweep clair (clôture dans le range après spike) : +30
      • Volume/impulsion post-sweep : +20
      • Biais H4 aligné : +20
    """
    if len(df_h4) < AMD_LOOKBACK + 5:
        return AmdPhase("unknown", "", "LONG", 0, 0, 0)

    # Fenêtre d'analyse : 50 dernières bougies H4
    window    = df_h4.iloc[-AMD_LOOKBACK:]
    atr_full  = (df_h4["high"] - df_h4["low"]).rolling(14).mean()
    atr_now   = atr_full.iloc[-1]

    # ── 1. RANGE (Accumulation zone) ──────────────────────────
    # On cherche la zone de range : les 20 dernières bougies AVANT les 10 dernières
    # (les 10 dernières = zone récente où manipulation/distribution peut se produire)
    range_window  = window.iloc[:30]   # bougies "historiques" du range
    recent_window = window.iloc[30:]   # bougies récentes (manipulation + distribution)

    range_high = range_window["high"].quantile(0.80)   # 80e percentile des hauts
    range_low  = range_window["low"].quantile(0.20)    # 20e percentile des bas
    range_size = range_high - range_low

    if range_size <= 0:
        return AmdPhase("unknown", "", "LONG", 0, 0, 0)

    # ── 2. COMPRESSION ATR (signature accumulation) ───────────
    atr_range    = (range_window["high"] - range_window["low"]).mean()
    atr_recent   = (recent_window["high"] - recent_window["low"]).mean() if len(recent_window) > 0 else atr_range
    is_compressed = atr_range < atr_now * 0.85   # volatilité du range < ATR actuel

    # ── 3. SWEEP DÉTECTION (Manipulation) ─────────────────────
    # Un sweep = dernières bougies H4 qui piquent hors du range PUIS reviennent
    sweep_up   = False  # spike au-dessus du range high → bear manipulation
    sweep_down = False  # spike en-dessous du range low → bull manipulation
    sweep_level = None

    for i in range(len(recent_window) - 1, max(len(recent_window) - 8, 0), -1):
        h = recent_window["high"].iloc[i]
        l = recent_window["low"].iloc[i]
        cl = recent_window["close"].iloc[i]

        # Bull Manipulation : spike sous range_low + retour au-dessus
        if l < range_low - atr_now * 0.1 and cl > range_low:
            sweep_down  = True
            sweep_level = range_low
            break
        # Bear Manipulation : spike au-dessus range_high + retour en-dessous
        if h > range_high + atr_now * 0.1 and cl < range_high:
            sweep_up    = True
            sweep_level = range_high
            break

    # ── 4. PHASE et DIRECTION ──────────────────────────────────
    if not sweep_up and not sweep_down:
        # Pas encore de manipulation détectée → accumulation en cours
        phase     = "accumulation"
        sub_phase = "range_forming"
        direction = "LONG"  # neutre pour l'instant
        confidence = 30 if is_compressed else 15
        reasons = ["📦 Accumulation en cours — range comprimé" if is_compressed
                   else "📦 Range en formation — accumulation possible"]
        return AmdPhase(phase, sub_phase, direction, confidence,
                        range_high, range_low, None, reasons)

    # Manipulation détectée !
    phase     = "distribution"
    direction = "LONG" if sweep_down else "SHORT"
    sub_phase = "bull_manipulation_complete" if sweep_down else "bear_manipulation_complete"

    # ── 5. CONFIANCE (scoring AMD) ────────────────────────────
    confidence = 0
    reasons    = []

    # Range bien défini
    if range_size > atr_now * 2:
        confidence += 30
        reasons.append(f"📦 Range AMD bien défini ({round(range_size, 5)})  (+30)")

    # Sweep propre
    if sweep_up or sweep_down:
        confidence += 35
        sweep_type = "Bull (sweep du bas)" if sweep_down else "Bear (sweep du haut)"
        reasons.append(f"🔥 Manipulation {sweep_type} complète @ {round(sweep_level, 5)}  (+35)")

    # Compression ATR = accumulation authentique
    if is_compressed:
        confidence += 20
        reasons.append("📊 ATR comprimé = accumulation institutionnelle  (+20)")

    # Post-sweep : impulsion (distribution en cours ?)
    last_close  = df_h4["close"].iloc[-1]
    last_open   = df_h4["open"].iloc[-1]
    post_impulse = abs(last_close - last_open) > atr_now * 0.8
    if post_impulse:
        confidence += 15
        reasons.append("⚡ Impulsion post-sweep détectée  (+15)")

    confidence = min(confidence, 100)

    return AmdPhase(
        phase=phase, sub_phase=sub_phase, direction=direction,
        confidence=confidence, range_high=range_high, range_low=range_low,
        sweep_level=sweep_level, reasons=reasons
    )


# ═════════════════════════════════════════════════════════════
#  ② SEPTUPLE TRACTION H4
#
#  N bougies consécutives dans la même direction sur H4
#  = momentum institutionnel fort = "train en marche"
#
#  Les institutions utilisent les retracements dans ce trend
#  pour rentrer, PAS contre le mouvement.
#
#  Setup : Septuple Traction H4 + retracement M15 50–61.8%
#          + FVG M5 dans la zone → ENTRÉE
#
#  Score bonus : +25 si 5+ bougies, +35 si 7+ bougies
# ═════════════════════════════════════════════════════════════

def detect_septuple_traction(df_h4: pd.DataFrame) -> dict:
    """
    Détecte le momentum institutionnel H4 (Septuple Traction).

    Critères STRICTS (institutionnel) :
    • Corps ≥ 50% de la bougie (peu de mèches = conviction)
    • Bougies consécutives (pas d'interruption)
    • Direction uniforme (all bullish ou all bearish)
    • Momentum croissant (chaque bougie ≥ 80% de la précédente)

    Retourne dict {detected, direction, count, strength, first_open, last_close}
    """
    if len(df_h4) < 10:
        return {"detected": False, "count": 0}

    atr = (df_h4["high"] - df_h4["low"]).rolling(14).mean().iloc[-1]

    # Cherche depuis la dernière bougie clôturée vers l'arrière
    max_streak      = 0
    best_direction  = None
    best_first_open = None
    best_last_close = None

    # Teste les 20 dernières bougies
    search_end = min(len(df_h4) - 1, 20)  # dernière bougie en cours = exclue

    for start in range(1, search_end):
        direction = None
        count     = 0
        momentum_ok = True
        prev_body = None

        for i in range(start, search_end + 1):
            idx = -(i + 1)  # bougie clôturée (on évite la courante)
            if abs(idx) > len(df_h4):
                break

            o  = df_h4["open"].iloc[idx]
            h  = df_h4["high"].iloc[idx]
            l  = df_h4["low"].iloc[idx]
            cl = df_h4["close"].iloc[idx]

            body      = abs(cl - o)
            rng       = h - l
            is_bull   = cl > o
            body_ratio = body / rng if rng > 0 else 0

            # Corps minimum : 50% de la bougie
            if body_ratio < 0.50:
                break

            cur_direction = "LONG" if is_bull else "SHORT"

            if direction is None:
                direction = cur_direction
            elif cur_direction != direction:
                break

            # Momentum : corps ≥ 80% du précédent (pas de ralentissement brutal)
            if prev_body is not None and body < prev_body * 0.60:
                momentum_ok = False
                break

            count    += 1
            prev_body = body

            if count > max_streak:
                max_streak      = count
                best_direction  = direction
                best_first_open = df_h4["open"].iloc[-(start + count)]
                best_last_close = df_h4["close"].iloc[-(start + 1)]

        if count >= SEPTUPLE_MIN_CANDLES:
            break

    if max_streak < SEPTUPLE_MIN_CANDLES:
        return {"detected": False, "count": max_streak}

    # Force du mouvement
    strength = "EXTREME" if max_streak >= 7 else ("FORT" if max_streak >= 6 else "MODÉRÉ")

    return {
        "detected"   : True,
        "direction"  : best_direction,
        "count"      : max_streak,
        "strength"   : strength,
        "first_open" : best_first_open,
        "last_close" : best_last_close,
    }


# ═════════════════════════════════════════════════════════════
#  ③ SUPPLY & DEMAND ZONES
#
#  Une vraie zone Supply/Demand institutionnelle n'est PAS
#  simplement un Order Block. Elle est créée par :
#
#  DEMAND ZONE = base d'un mouvement haussier impulsif
#    → Dernière consolidation AVANT la grande bougie haussière
#    → Prix revient tester cette zone → acheteurs institutionnels
#
#  SUPPLY ZONE = base d'un mouvement baissier impulsif
#    → Dernière consolidation AVANT la grande bougie baissière
#    → Prix revient tester cette zone → vendeurs institutionnels
#
#  Critères de qualité :
#    • Taille de la bougie impulsive ≥ ATR × 1.5
#    • La zone n'a pas encore été "mitigée" (prix n'est pas revenu)
#    • Fresh zone (testée 0 fois) > Tested once > Tested twice (trop faible)
# ═════════════════════════════════════════════════════════════

def detect_supply_demand_zones(df: pd.DataFrame, direction: str) -> list[SupplyDemandZone]:
    """
    Détecte les zones Supply (bearish) et Demand (bullish) institutionnelles.

    Algorithme :
    1. Cherche les bougies impulsives (corps ≥ ATR × 1.5)
    2. La zone = corps de la DERNIÈRE PETITE bougie avant l'impulsion
       (c'est là que les institutions ont placé leurs ordres)
    3. Vérifie que la zone n'est pas mitigée (prix n'y est pas revenu)
    4. Retourne les zones actives dans le sens du biais
    """
    if len(df) < 20:
        return []

    atr = (df["high"] - df["low"]).rolling(14).mean()
    zones: list[SupplyDemandZone] = []
    zone_type = "supply" if direction == "SHORT" else "demand"

    for i in range(2, len(df) - 2):
        o  = df["open"].iloc[i]
        cl = df["close"].iloc[i]
        body = abs(cl - o)
        atr_i = atr.iloc[i]

        if atr_i <= 0 or np.isnan(atr_i):
            continue

        # Bougie impulsive ?
        if body < atr_i * SD_MIN_IMPULSE_RATIO:
            continue

        is_bull_impulse = cl > o
        # Direction correcte ?
        if direction == "LONG"  and not is_bull_impulse:
            continue
        if direction == "SHORT" and is_bull_impulse:
            continue

        # Zone = corps de la bougie JUSTE AVANT l'impulsion (base institutionnelle)
        base_idx = i - 1
        if base_idx < 0:
            continue

        base_o  = df["open"].iloc[base_idx]
        base_cl = df["close"].iloc[base_idx]
        base_h  = df["high"].iloc[base_idx]
        base_l  = df["low"].iloc[base_idx]

        zone_top    = max(base_o, base_cl, base_h)
        zone_bottom = min(base_o, base_cl, base_l)

        # La zone ne doit pas être mitigée (prix n'est pas REVENU dedans après l'impulsion)
        mitigated = False
        for j in range(i + 1, len(df)):
            close_j = df["close"].iloc[j]
            if zone_bottom <= close_j <= zone_top:
                mitigated = True
                break

        if not mitigated:
            zones.append(SupplyDemandZone(
                zone_type=zone_type,
                top=zone_top,
                bottom=zone_bottom,
                index=base_idx,
                impulse_size=body / atr_i,   # ratio force de l'impulsion
                tested=False
            ))

    # Tri par proximité au prix actuel
    current_price = df["close"].iloc[-1]
    zones.sort(key=lambda z: abs(current_price - (z.top + z.bottom) / 2))

    return zones[:5]   # retourne les 5 zones les plus proches


def price_in_sd_zone(price: float, zones: list[SupplyDemandZone],
                     atr: float) -> Optional[SupplyDemandZone]:
    """
    Retourne la première zone S/D dans laquelle le prix se trouve.
    Tolérance : ±15% de l'ATR autour de la zone.
    """
    buf = atr * SD_ZONE_BUFFER
    for zone in zones:
        if (zone.bottom - buf) <= price <= (zone.top + buf):
            return zone
    return None


# ═════════════════════════════════════════════════════════════
#  ④ LIQUIDITY MAP AVANCÉE
#
#  Les institutions traquent les liquidités RÉELLES, pas juste
#  les swing highs/lows. On identifie :
#
#  BSL (Buy Side Liquidity)  = stops des SHORTS au-dessus des highs
#  SSL (Sell Side Liquidity) = stops des LONGS en-dessous des lows
#
#  Equal Highs (EQH) = double top = pool de liquidité visé par les institutions
#  Equal Lows  (EQL) = double bottom = idem en sens inverse
#
#  Liquidity Void = zone de déséquilibre (FVG = liquidity void)
#
#  Intraday Liquidity = high/low du jour précédent (PDH/PDL)
#                       = première cible intraday des institutions
# ═════════════════════════════════════════════════════════════

@dataclass
class LiquidityMap:
    bsl_levels:  list[float]   # Buy Side Liquidity (above highs)
    ssl_levels:  list[float]   # Sell Side Liquidity (below lows)
    eqh_levels:  list[float]   # Equal Highs
    eql_levels:  list[float]   # Equal Lows
    pdh:         Optional[float]  # Previous Day High
    pdl:         Optional[float]  # Previous Day Low
    swept_bsl:   bool          # BSL récemment sweepée (signal SHORT)
    swept_ssl:   bool          # SSL récemment sweepée (signal LONG)
    nearest_bsl: Optional[float]
    nearest_ssl: Optional[float]


def build_liquidity_map(df_h4: pd.DataFrame, df_ltf: pd.DataFrame) -> LiquidityMap:
    """
    Construit la carte complète de liquidité sur H4 + LTF.

    BSL/SSL : 5 derniers swing H/L H4 significatifs
    EQH/EQL : niveaux proches à ±0.02% (institutionnellement = "même niveau")
    PDH/PDL  : high/low de la session précédente H4 (4 bougies = 1 jour environ)
    Swept    : spike + retour dans la dernière bougie LTF
    """
    # ── SWING HIGHS / LOWS H4 ─────────────────────────────────
    shs = swing_highs(df_h4)
    sls = swing_lows(df_h4)

    bsl_levels = [v for _, v in shs[-8:]]
    ssl_levels = [v for _, v in sls[-8:]]

    # ── EQUAL HIGHS / LOWS (EQH/EQL) ──────────────────────────
    # Deux niveaux sont "égaux" si leur écart est < 0.025%
    eqh_levels = []
    eql_levels = []
    tolerance  = 0.00025   # 2.5 pips sur forex

    for i, h1 in enumerate(bsl_levels):
        for h2 in bsl_levels[i+1:]:
            if abs(h1 - h2) / max(h1, 0.0001) < tolerance:
                eqh_levels.append((h1 + h2) / 2)

    for i, l1 in enumerate(ssl_levels):
        for l2 in ssl_levels[i+1:]:
            if abs(l1 - l2) / max(l1, 0.0001) < tolerance:
                eql_levels.append((l1 + l2) / 2)

    # ── PDH / PDL (Previous Day High/Low) ─────────────────────
    # H4 : 6 bougies ≈ 24h. On prend les 6 bougies précédentes.
    pdh, pdl = None, None
    if len(df_h4) >= 12:
        pd_window = df_h4.iloc[-12:-6]
        pdh = pd_window["high"].max()
        pdl = pd_window["low"].min()

    # ── SWEPT BSL/SSL ? ───────────────────────────────────────
    price    = df_ltf["close"].iloc[-1]
    last_h   = df_ltf["high"].iloc[-2]   # dernière bougie clôturée
    last_l   = df_ltf["low"].iloc[-2]
    last_c   = df_ltf["close"].iloc[-2]
    atr      = (df_ltf["high"] - df_ltf["low"]).rolling(14).mean().iloc[-1]

    swept_bsl = False
    swept_ssl = False

    for level in bsl_levels[-5:]:
        if last_h > level + atr * 0.05 and last_c < level:
            swept_bsl = True
            break

    for level in ssl_levels[-5:]:
        if last_l < level - atr * 0.05 and last_c > level:
            swept_ssl = True
            break

    # ── NEAREST BSL/SSL ───────────────────────────────────────
    above_bsl = [l for l in bsl_levels if l > price]
    below_ssl = [l for l in ssl_levels if l < price]
    nearest_bsl = min(above_bsl) if above_bsl else None
    nearest_ssl = max(below_ssl) if below_ssl else None

    return LiquidityMap(
        bsl_levels=bsl_levels, ssl_levels=ssl_levels,
        eqh_levels=eqh_levels, eql_levels=eql_levels,
        pdh=pdh, pdl=pdl,
        swept_bsl=swept_bsl, swept_ssl=swept_ssl,
        nearest_bsl=nearest_bsl, nearest_ssl=nearest_ssl,
    )


# ═════════════════════════════════════════════════════════════
#  ⑤ BOUGIES D'ENTRÉE INSTITUTIONNELLES
#
#  Les grandes institutions n'entrent PAS sur n'importe quelle bougie.
#  Les setups d'entrée qui donnent le meilleur timing :
#
#  1. DISPLACEMENT CANDLE
#     Corps ≥ ATR × 2 + clôture dans le sens du trade
#     = bougie de "déplacement institutionnel" qui efface le désordre
#
#  2. ORDER FLOW SHIFT (OFS)
#     Série de 3 bougies : bear → bull → close above bear high (LONG)
#     = renversement micro-structure = premier signal d'intent
#
#  3. IMBALANCE CANDLE (FVG micro)
#     Gap entre bougie[i-2].low et bougie[i].high > 0 (bullish)
#     = déséquilibre = les institutions ont acheté agressivement
#
#  4. REJECTION WICK
#     Mèche ≥ corps × 3 dans le sens opposé = rejet institutionnel
#     Seule une mèche AVEC volume = authentique (pas de fakeout)
#
#  5. ENGULFING INSTITUTIONNEL
#     Close > open + close > prev_high (bullish engulfing fort)
#     = absorbe TOUT le mouvement précédent = conviction totale
# ═════════════════════════════════════════════════════════════

@dataclass
class EntryCandle:
    candle_type: str    # "displacement" | "ofs" | "imbalance" | "rejection" | "engulfing"
    quality:     str    # "premium" | "standard"
    score_bonus: int
    index:       int


def detect_institutional_entry_candles(df: pd.DataFrame,
                                        direction: str) -> list[EntryCandle]:
    """
    Détecte les bougies d'entrée institutionnelles sur les 5 dernières bougies
    CLÔTURÉES (on évite la bougie courante = données incomplètes).

    Retourne toutes les bougies valides détectées (peut en avoir plusieurs).
    """
    if len(df) < 6:
        return []

    atr     = (df["high"] - df["low"]).rolling(14).mean().iloc[-1]
    entries = []

    for i in range(-2, -6, -1):   # [-2, -3, -4, -5] = 4 dernières clôturées
        if abs(i) > len(df) - 1:
            break

        o   = df["open"].iloc[i]
        h   = df["high"].iloc[i]
        l   = df["low"].iloc[i]
        cl  = df["close"].iloc[i]
        body        = abs(cl - o)
        full_range  = h - l
        upper_wick  = h - max(o, cl)
        lower_wick  = min(o, cl) - l
        is_bull     = cl > o

        # ── 1. DISPLACEMENT ───────────────────────────────────
        if body >= atr * 1.8:
            if (direction == "LONG" and is_bull) or (direction == "SHORT" and not is_bull):
                quality = "premium" if body >= atr * 2.5 else "standard"
                entries.append(EntryCandle("displacement", quality, 20 if quality == "premium" else 15, i))
                continue

        # ── 2. ORDER FLOW SHIFT ───────────────────────────────
        if abs(i) < len(df) - 2:
            prev_o  = df["open"].iloc[i - 1]
            prev_cl = df["close"].iloc[i - 1]
            if direction == "LONG":
                # Bear → Bull → close above prev_high
                prev_is_bear = prev_cl < prev_o
                if prev_is_bear and is_bull and cl > df["high"].iloc[i - 1]:
                    entries.append(EntryCandle("ofs", "premium", 18, i))
                    continue
            elif direction == "SHORT":
                prev_is_bull = prev_cl > prev_o
                if prev_is_bull and not is_bull and cl < df["low"].iloc[i - 1]:
                    entries.append(EntryCandle("ofs", "premium", 18, i))
                    continue

        # ── 3. IMBALANCE (Micro-FVG) ──────────────────────────
        if abs(i) >= 2 and abs(i) < len(df) - 2:
            if direction == "LONG":
                prev2_low = df["low"].iloc[i - 2]
                if h > prev2_low and (h - prev2_low) / max(h, 0.0001) > 0.0001:
                    entries.append(EntryCandle("imbalance", "standard", 12, i))
                    continue
            elif direction == "SHORT":
                prev2_high = df["high"].iloc[i - 2]
                if l < prev2_high and (prev2_high - l) / max(l, 0.0001) > 0.0001:
                    entries.append(EntryCandle("imbalance", "standard", 12, i))
                    continue

        # ── 4. REJECTION WICK ─────────────────────────────────
        if body > 0:
            if direction == "LONG" and lower_wick >= body * 2.5 and is_bull:
                quality = "premium" if lower_wick >= body * 4 else "standard"
                entries.append(EntryCandle("rejection", quality, 15 if quality == "premium" else 10, i))
                continue
            elif direction == "SHORT" and upper_wick >= body * 2.5 and not is_bull:
                quality = "premium" if upper_wick >= body * 4 else "standard"
                entries.append(EntryCandle("rejection", quality, 15 if quality == "premium" else 10, i))
                continue

        # ── 5. ENGULFING INSTITUTIONNEL ───────────────────────
        if abs(i) < len(df) - 1:
            prev_h = df["high"].iloc[i - 1]
            prev_l = df["low"].iloc[i - 1]
            if direction == "LONG" and is_bull and cl > prev_h:
                entries.append(EntryCandle("engulfing", "premium", 20, i))
            elif direction == "SHORT" and not is_bull and cl < prev_l:
                entries.append(EntryCandle("engulfing", "premium", 20, i))

    return entries


# ═════════════════════════════════════════════════════════════
#  DÉTECTEURS CLASSIQUES (BOS, FVG, OB, Breaker, Liquidité)
#  — Conservés et améliorés depuis v2
# ═════════════════════════════════════════════════════════════

def htf_bias(df: pd.DataFrame) -> str:
    """
    Biais H4 via scoring multi-facteurs (plus robuste que EMA seul).

    Facteurs BULLISH (+1 chacun) :
      • Close > EMA8
      • Close > EMA21
      • Dernier HH (high[-1] > max des 3 bougies précédentes)
      • Dernier HL (low[-1] > low[-5])
      • 3+ closes haussiers sur les 5 dernières bougies

    Facteurs BEARISH (-1 chacun) — inverse.

    Score ≥ +2  → BULLISH
    Score ≤ -2  → BEARISH
    Sinon       → NEUTRAL (marché latéral, on attend)
    """
    if len(df) < 25:
        return "NEUTRAL"

    closes = df["close"].values
    highs  = df["high"].values
    lows   = df["low"].values

    ema8  = np.convolve(closes, np.ones(8)  / 8,  mode="valid")
    ema21 = np.convolve(closes, np.ones(21) / 21, mode="valid")

    c_last  = closes[-1]
    score   = 0

    # EMA
    if c_last > ema8[-1]:  score += 1
    else:                  score -= 1
    if c_last > ema21[-1]: score += 1
    else:                  score -= 1

    # Higher High / Lower Low
    if highs[-1] > highs[-4:-1].max():  score += 1   # HH
    else:                               score -= 1   # LH
    if lows[-1] > lows[-6:-1].min():    score += 1   # HL
    else:                               score -= 1   # LL

    # Momentum : nombre de bougies haussières sur les 5 dernières
    bull_candles = sum(closes[-5:] > closes[-6:-1])
    if bull_candles >= 3:   score += 1
    elif bull_candles <= 2: score -= 1

    if   score >=  2: return "BULLISH"
    elif score <= -2: return "BEARISH"
    return "NEUTRAL"


def detect_bos(df: pd.DataFrame) -> list[dict]:
    bos_list = []
    lookback = 10
    for i in range(lookback, len(df)):
        window     = df.iloc[i - lookback:i]
        close      = df["close"].iloc[i]
        swing_low  = window["low"].min()
        swing_high = window["high"].max()
        if close < swing_low:
            bos_list.append({"index": i, "type": "bearish", "level": swing_low})
        elif close > swing_high:
            bos_list.append({"index": i, "type": "bullish", "level": swing_high})
    return bos_list


def detect_fvg(df: pd.DataFrame) -> list[FVG]:
    fvgs = []
    for i in range(2, len(df)):
        mid_price = df["close"].iloc[i]
        top    = df["high"].iloc[i - 2]
        bottom = df["low"].iloc[i]
        if bottom > top and (bottom - top) / mid_price > FVG_MIN_RATIO:
            fvgs.append(FVG("bearish", bottom, top, i))
        top    = df["high"].iloc[i]
        bottom = df["low"].iloc[i - 2]
        if top > bottom and (top - bottom) / mid_price > FVG_MIN_RATIO:
            fvgs.append(FVG("bullish", top, bottom, i))
    return fvgs


def detect_order_blocks(df: pd.DataFrame, bos_list: list[dict]) -> list[OrderBlock]:
    obs = []
    for bos in bos_list[-5:]:
        idx = bos["index"]
        if idx < OB_LOOKBACK:
            continue
        if bos["type"] == "bearish":
            for j in range(idx - 1, idx - OB_LOOKBACK - 1, -1):
                if df["close"].iloc[j] > df["open"].iloc[j]:
                    obs.append(OrderBlock("bearish", df["high"].iloc[j], df["low"].iloc[j], j))
                    break
        elif bos["type"] == "bullish":
            for j in range(idx - 1, idx - OB_LOOKBACK - 1, -1):
                if df["close"].iloc[j] < df["open"].iloc[j]:
                    obs.append(OrderBlock("bullish", df["high"].iloc[j], df["low"].iloc[j], j))
                    break
    return obs


def detect_breaker_blocks(df: pd.DataFrame, bos_list: list[dict]) -> list[dict]:
    """Breaker Block = OB mitiqué qui flippe de direction (amélioré v3)."""
    breakers = []
    for bos in bos_list[-6:]:
        idx = bos["index"]
        if idx < OB_LOOKBACK + 2 or idx + 3 >= len(df):
            continue
        for j in range(idx - 1, max(idx - OB_LOOKBACK - 1, 0), -1):
            is_bull = df["close"].iloc[j] > df["open"].iloc[j]
            ob_hi   = df["high"].iloc[j]
            ob_lo   = df["low"].iloc[j]
            if bos["type"] == "bearish" and is_bull:
                post_high = df["high"].iloc[idx: min(idx + 15, len(df))].max()
                if ob_lo <= post_high <= ob_hi * 1.001:
                    breakers.append({"direction": "bearish", "top": ob_hi,
                                      "bottom": ob_lo, "index": j})
                    break
            elif bos["type"] == "bullish" and not is_bull:
                post_low = df["low"].iloc[idx: min(idx + 15, len(df))].min()
                if ob_lo * 0.999 <= post_low <= ob_hi:
                    breakers.append({"direction": "bullish", "top": ob_hi,
                                      "bottom": ob_lo, "index": j})
                    break
    return breakers


# ═════════════════════════════════════════════════════════════
#  ORDER FLOW SHIFT STRUCTUREL (M15 / H4)
#
#  Différent du micro-OFS sur bougie unique.
#  Ici on détecte un CHANGEMENT D'INTENT institutionnel sur M15 :
#
#  LONG  : série de HL montants (higher lows) + BOS haussier récent
#           = les institutions accumulent → demande structurelle
#
#  SHORT : série de LH descendants (lower highs) + BOS baissier récent
#           = les institutions distribuent → pression vendeuse
#
#  Bonus : +12 si OFS structurel aligné avec le biais H4
# ═════════════════════════════════════════════════════════════

def detect_order_flow_structural(df: pd.DataFrame, direction: str,
                                  bos_list: list[dict]) -> dict:
    """
    Détecte un Order Flow Shift structurel sur M15 ou H4.

    Critères LONG :
      • Au moins 3 Higher Lows consécutifs sur les 20 dernières bougies
      • BOS haussier récent (dans les 10 dernières bougies)
      • Pas de cassure baissière après le dernier HL

    Critères SHORT :
      • Au moins 3 Lower Highs consécutifs sur les 20 dernières bougies
      • BOS baissier récent (dans les 10 dernières bougies)
      • Pas de cassure haussière après le dernier LH

    Retourne dict {detected, hl_count/lh_count, bos_aligned, score_bonus, reason}
    """
    empty = {"detected": False, "count": 0, "bos_aligned": False,
             "score_bonus": 0, "reason": ""}

    if len(df) < 20:
        return empty

    window = df.iloc[-25:]

    # ── Détecter Higher Lows ou Lower Highs ────────────────────
    if direction == "LONG":
        # Higher Lows : chaque swing low > swing low précédent
        lows = []
        for i in range(1, len(window) - 1):
            if window["low"].iloc[i] < window["low"].iloc[i-1] and \
               window["low"].iloc[i] < window["low"].iloc[i+1]:
                lows.append(window["low"].iloc[i])

        # Compte les HL consécutifs depuis la fin
        hl_count = 0
        for k in range(len(lows) - 1, 0, -1):
            if lows[k] > lows[k-1]:
                hl_count += 1
            else:
                break

        if hl_count < 2:
            return empty

        # BOS haussier récent aligné
        bos_aligned = any(
            b["type"] == "bullish"
            for b in bos_list[-10:]
        )

        score_bonus = 0
        if hl_count >= 3:
            score_bonus = 12
        elif hl_count >= 2:
            score_bonus = 8

        if bos_aligned:
            score_bonus += 3

        reason = (
            f"📈 Order Flow Shift LONG — {hl_count} Higher Lows structurels"
            f"{'  + BOS haussier aligné' if bos_aligned else ''}  (+{score_bonus})"
        )
        return {"detected": True, "count": hl_count, "bos_aligned": bos_aligned,
                "score_bonus": score_bonus, "reason": reason}

    else:  # SHORT
        # Lower Highs : chaque swing high < swing high précédent
        highs = []
        for i in range(1, len(window) - 1):
            if window["high"].iloc[i] > window["high"].iloc[i-1] and \
               window["high"].iloc[i] > window["high"].iloc[i+1]:
                highs.append(window["high"].iloc[i])

        lh_count = 0
        for k in range(len(highs) - 1, 0, -1):
            if highs[k] < highs[k-1]:
                lh_count += 1
            else:
                break

        if lh_count < 2:
            return empty

        bos_aligned = any(
            b["type"] == "bearish"
            for b in bos_list[-10:]
        )

        score_bonus = 0
        if lh_count >= 3:
            score_bonus = 12
        elif lh_count >= 2:
            score_bonus = 8

        if bos_aligned:
            score_bonus += 3

        reason = (
            f"📉 Order Flow Shift SHORT — {lh_count} Lower Highs structurels"
            f"{'  + BOS baissier aligné' if bos_aligned else ''}  (+{score_bonus})"
        )
        return {"detected": True, "count": lh_count, "bos_aligned": bos_aligned,
                "score_bonus": score_bonus, "reason": reason}


def detect_breaker_block_htf(df_htf: pd.DataFrame, df_mtf: pd.DataFrame,
                              direction: str) -> dict:
    """
    Breaker Block MULTI-TIMEFRAME (H4 + M15).

    Un Breaker Block H4 est le setup le plus puissant :
    • OB haussier H4 cassé vers le bas (BOS bearish H4) → devient Supply (Breaker bearish)
    • Prix revient tester ce niveau sur M15 → entrée SHORT de haute conviction
    • Inverse pour LONG (OB baissier H4 cassé → Demand Breaker)

    Score bonus :
      +10 si Breaker M15 seul
      +15 si Breaker H4 (niveau institutionnel)
      +18 si Breaker H4 + prix revient tester sur M15

    Retourne dict {detected, level_top, level_bottom, tf, score_bonus, reason}
    """
    empty = {"detected": False, "level_top": None, "level_bottom": None,
             "tf": None, "score_bonus": 0, "reason": ""}

    # ── Breaker Block H4 ──────────────────────────────────────
    bos_htf = detect_bos(df_htf)
    bkr_htf = detect_breaker_blocks(df_htf, bos_htf)

    htf_bias_dir = "bearish" if direction == "SHORT" else "bullish"
    htf_breakers = [b for b in bkr_htf if b["direction"] == htf_bias_dir]

    if htf_breakers:
        bb = htf_breakers[-1]  # le plus récent
        price_now = df_mtf["close"].iloc[-1]
        atr_mtf   = (df_mtf["high"] - df_mtf["low"]).rolling(14).mean().iloc[-1]

        # Le prix reteste-t-il le Breaker H4 sur M15 ?
        in_bb = (bb["bottom"] - atr_mtf * 0.2) <= price_now <= (bb["top"] + atr_mtf * 0.2)

        if in_bb:
            reason = (
                f"🔥 Breaker Block H4 retesté sur M15 @ "
                f"{round((bb['top']+bb['bottom'])/2, 5)}  (+18)"
            )
            return {"detected": True, "level_top": bb["top"], "level_bottom": bb["bottom"],
                    "tf": "H4+M15", "score_bonus": 18, "reason": reason}
        else:
            reason = (
                f"🔥 Breaker Block H4 actif @ "
                f"{round((bb['top']+bb['bottom'])/2, 5)}  (+15)"
            )
            return {"detected": True, "level_top": bb["top"], "level_bottom": bb["bottom"],
                    "tf": "H4", "score_bonus": 15, "reason": reason}

    return empty


# ═════════════════════════════════════════════════════════════
#  CHART PATTERNS DETECTION  (Images 1 & 2)
#
#  Bullish Continuation  : Ascending Triangle · Bull Flag · Bull Wedge · Sym Triangle
#  Bearish Continuation  : Descending Triangle · Bear Flag · Bear Wedge · Sym Triangle
#  Bullish Reversal      : Double Bottom · Triple Bottom · Inverted H&S · Falling Wedge
#  Bearish Reversal      : Double Top   · Triple Top    · H&S           · Rising Wedge
#
#  OB Retest (3 types)   : Continuation Pattern · Consolidation · BSL/PDL Retest
# ═════════════════════════════════════════════════════════════

@dataclass
class PatternResult:
    detected:      bool
    pattern_name:  str
    direction:     str   # "LONG" | "SHORT"
    score_bonus:   int
    description:   str


@dataclass
class OBRetestResult:
    detected:     bool
    retest_type:  str    # "continuation" | "consolidation" | "bsl_retest"
    direction:    str
    score_bonus:  int
    description:  str


def _swing_points(df: pd.DataFrame, col_high: bool = True) -> list[tuple[int, float]]:
    """Retourne les swing highs ou lows (index, valeur)."""
    result = []
    col = "high" if col_high else "low"
    for i in range(1, len(df) - 1):
        v  = df[col].iloc[i]
        v1 = df[col].iloc[i - 1]
        v2 = df[col].iloc[i + 1]
        if col_high and v > v1 and v > v2:
            result.append((i, v))
        elif not col_high and v < v1 and v < v2:
            result.append((i, v))
    return result


def detect_double_top_bottom(df: pd.DataFrame, direction: str) -> PatternResult:
    """Double Top (SHORT) / Double Bottom (LONG)."""
    empty = PatternResult(False, "", direction, 0, "")
    if len(df) < 30:
        return empty
    window = df.iloc[-50:]
    atr = (window["high"] - window["low"]).rolling(14).mean().iloc[-1]
    tol = atr * 1.5
    if direction == "SHORT":
        pts = _swing_points(window, col_high=True)
        for i in range(len(pts) - 1, 0, -1):
            idx1, h1 = pts[i]
            for j in range(i - 1, max(i - 8, 0), -1):
                idx2, h2 = pts[j]
                if abs(h1 - h2) < tol and (idx1 - idx2) >= 5:
                    neckline = window["low"].iloc[idx2:idx1].min()
                    if window["close"].iloc[-1] < neckline + atr * 0.5:
                        return PatternResult(True, "Double Top", "SHORT", 18,
                            f"Double Top @ {round((h1+h2)/2,5)} | Neckline {round(neckline,5)}")
    else:
        pts = _swing_points(window, col_high=False)
        for i in range(len(pts) - 1, 0, -1):
            idx1, l1 = pts[i]
            for j in range(i - 1, max(i - 8, 0), -1):
                idx2, l2 = pts[j]
                if abs(l1 - l2) < tol and (idx1 - idx2) >= 5:
                    neckline = window["high"].iloc[idx2:idx1].max()
                    if window["close"].iloc[-1] > neckline - atr * 0.5:
                        return PatternResult(True, "Double Bottom", "LONG", 18,
                            f"Double Bottom @ {round((l1+l2)/2,5)} | Neckline {round(neckline,5)}")
    return empty


def detect_triple_top_bottom(df: pd.DataFrame, direction: str) -> PatternResult:
    """Triple Top (SHORT) / Triple Bottom (LONG)."""
    empty = PatternResult(False, "", direction, 0, "")
    if len(df) < 40:
        return empty
    window = df.iloc[-60:]
    atr = (window["high"] - window["low"]).rolling(14).mean().iloc[-1]
    tol = atr * 1.5
    if direction == "SHORT":
        pts = _swing_points(window, col_high=True)
        for i in range(len(pts) - 1, 1, -1):
            idx1, h1 = pts[i]
            for j in range(i - 1, max(i - 6, 1), -1):
                idx2, h2 = pts[j]
                if abs(h1 - h2) > tol:
                    continue
                for k in range(j - 1, max(j - 6, 0), -1):
                    idx3, h3 = pts[k]
                    if abs(h1 - h3) < tol and (idx2 - idx3) >= 4:
                        neckline = window["low"].iloc[idx3:idx1].min()
                        if window["close"].iloc[-1] < neckline + atr * 0.5:
                            return PatternResult(True, "Triple Top", "SHORT", 22,
                                f"Triple Top @ {round((h1+h2+h3)/3,5)}")
    else:
        pts = _swing_points(window, col_high=False)
        for i in range(len(pts) - 1, 1, -1):
            idx1, l1 = pts[i]
            for j in range(i - 1, max(i - 6, 1), -1):
                idx2, l2 = pts[j]
                if abs(l1 - l2) > tol:
                    continue
                for k in range(j - 1, max(j - 6, 0), -1):
                    idx3, l3 = pts[k]
                    if abs(l1 - l3) < tol and (idx2 - idx3) >= 4:
                        neckline = window["high"].iloc[idx3:idx1].max()
                        if window["close"].iloc[-1] > neckline - atr * 0.5:
                            return PatternResult(True, "Triple Bottom", "LONG", 22,
                                f"Triple Bottom @ {round((l1+l2+l3)/3,5)}")
    return empty


def detect_head_shoulders(df: pd.DataFrame, direction: str) -> PatternResult:
    """Head & Shoulders (SHORT) / Inverted H&S (LONG)."""
    empty = PatternResult(False, "", direction, 0, "")
    if len(df) < 40:
        return empty
    window = df.iloc[-60:]
    atr = (window["high"] - window["low"]).rolling(14).mean().iloc[-1]
    if direction == "SHORT":
        pts = _swing_points(window, col_high=True)
        if len(pts) < 3:
            return empty
        for i in range(len(pts) - 1, 1, -1):
            idx_r, h_r = pts[i]
            idx_h, h_h = pts[i - 1]
            idx_l, h_l = pts[i - 2]
            if (h_h > h_r and h_h > h_l
                    and abs(h_r - h_l) < atr * 2.5
                    and (idx_h - idx_l) >= 4 and (idx_r - idx_h) >= 4):
                neckline = window["low"].iloc[idx_l:idx_r].min()
                if window["close"].iloc[-1] < neckline + atr:
                    return PatternResult(True, "Head & Shoulders", "SHORT", 25,
                        f"H&S tête={round(h_h,5)} épaules≈{round((h_l+h_r)/2,5)}")
    else:
        pts = _swing_points(window, col_high=False)
        if len(pts) < 3:
            return empty
        for i in range(len(pts) - 1, 1, -1):
            idx_r, l_r = pts[i]
            idx_h, l_h = pts[i - 1]
            idx_l, l_l = pts[i - 2]
            if (l_h < l_r and l_h < l_l
                    and abs(l_r - l_l) < atr * 2.5
                    and (idx_h - idx_l) >= 4 and (idx_r - idx_h) >= 4):
                neckline = window["high"].iloc[idx_l:idx_r].max()
                if window["close"].iloc[-1] > neckline - atr:
                    return PatternResult(True, "Inverted H&S", "LONG", 25,
                        f"Inv H&S tête={round(l_h,5)} épaules≈{round((l_l+l_r)/2,5)}")
    return empty


def detect_wedge(df: pd.DataFrame, direction: str) -> PatternResult:
    """Falling Wedge (LONG reversal) / Rising Wedge (SHORT reversal)."""
    empty = PatternResult(False, "", direction, 0, "")
    if len(df) < 20:
        return empty
    window = df.iloc[-30:]
    atr    = (window["high"] - window["low"]).rolling(14).mean().iloc[-1]
    x      = np.arange(len(window))
    slope_h = np.polyfit(x, window["high"].values, 1)[0]
    slope_l = np.polyfit(x, window["low"].values,  1)[0]
    # Falling Wedge : deux pentes négatives, lows moins négatifs que highs → convergent vers le bas
    if slope_h < -atr * 0.004 and slope_l < -atr * 0.004 and slope_l > slope_h and direction == "LONG":
        return PatternResult(True, "Falling Wedge 📐", "LONG", 16,
            f"Falling Wedge haussier — pentes H={round(slope_h,5)} L={round(slope_l,5)}")
    # Rising Wedge : deux pentes positives, highs moins positifs que lows → convergent vers le haut
    if slope_h > atr * 0.004 and slope_l > atr * 0.004 and slope_h < slope_l and direction == "SHORT":
        return PatternResult(True, "Rising Wedge 📐", "SHORT", 16,
            f"Rising Wedge baissier — pentes H={round(slope_h,5)} L={round(slope_l,5)}")
    return empty


def detect_triangle(df: pd.DataFrame, direction: str) -> PatternResult:
    """Ascending / Descending / Symmetrical Triangle."""
    empty = PatternResult(False, "", direction, 0, "")
    if len(df) < 20:
        return empty
    window  = df.iloc[-35:]
    atr     = (window["high"] - window["low"]).rolling(14).mean().iloc[-1]
    x       = np.arange(len(window))
    slope_h = np.polyfit(x, window["high"].values, 1)[0]
    slope_l = np.polyfit(x, window["low"].values,  1)[0]
    if abs(slope_h) < atr * 0.003 and slope_l > atr * 0.004 and direction == "LONG":
        return PatternResult(True, "Ascending Triangle 📐", "LONG", 15,
            f"Ascending Triangle — résistance plate + lows ascendants")
    if abs(slope_l) < atr * 0.003 and slope_h < -atr * 0.004 and direction == "SHORT":
        return PatternResult(True, "Descending Triangle 📐", "SHORT", 15,
            f"Descending Triangle — support plat + highs descendants")
    if slope_h < -atr * 0.003 and slope_l > atr * 0.003:
        name = "Sym. Triangle 📐 (haussier)" if direction == "LONG" else "Sym. Triangle 📐 (baissier)"
        return PatternResult(True, name, direction, 12,
            f"Triangle symétrique — convergence imminente")
    return empty


def detect_flag(df: pd.DataFrame, direction: str) -> PatternResult:
    """Bull Flag (LONG continuation) / Bear Flag (SHORT continuation)."""
    empty = PatternResult(False, "", direction, 0, "")
    if len(df) < 20:
        return empty
    pole_window = df.iloc[-20:-8]
    flag_window = df.iloc[-8:]
    atr         = (df["high"] - df["low"]).rolling(14).mean().iloc[-1]
    pole_move   = abs(pole_window["close"].iloc[-1] - pole_window["open"].iloc[0])
    if pole_move < atr * 3:
        return empty
    pole_bull  = pole_window["close"].iloc[-1] > pole_window["open"].iloc[0]
    flag_slope = np.polyfit(np.arange(len(flag_window)), flag_window["close"].values, 1)[0]
    if pole_bull and flag_slope < 0 and abs(flag_slope) < atr * 0.08 and direction == "LONG":
        return PatternResult(True, "Bull Flag 🚩", "LONG", 14,
            f"Bull Flag — mât={round(pole_move,5)} / drapeau légèrement baissier")
    if not pole_bull and flag_slope > 0 and abs(flag_slope) < atr * 0.08 and direction == "SHORT":
        return PatternResult(True, "Bear Flag 🚩", "SHORT", 14,
            f"Bear Flag — mât={round(pole_move,5)} / drapeau légèrement haussier")
    return empty


def detect_all_patterns(df_h4: pd.DataFrame, df_ltf: pd.DataFrame,
                         direction: str) -> list[PatternResult]:
    """Lance tous les détecteurs de patterns sur H4 + LTF."""
    results = []
    for df in (df_h4, df_ltf):
        results.append(detect_double_top_bottom(df, direction))
        results.append(detect_triple_top_bottom(df, direction))
        results.append(detect_head_shoulders(df, direction))
        results.append(detect_wedge(df, direction))
        results.append(detect_triangle(df, direction))
        results.append(detect_flag(df, direction))
    return [p for p in results if p.detected]


def detect_ob_retest(df_h4: pd.DataFrame, df_ltf: pd.DataFrame,
                     direction: str, sd_zones: list) -> OBRetestResult:
    """
    Détecte les 3 types de retest OB/Demand Zone (Image 2) :
    1. CONTINUATION PATTERN  — canal descendant / bear flag sur la zone
    2. CONSOLIDATION         — range serré au-dessus/dessous de la zone
    3. BSL RETEST (PDL/PDH)  — chasse de liquidité puis rebond sur la zone
    """
    empty = OBRetestResult(False, "", direction, 0, "")
    if len(df_ltf) < 20 or not sd_zones:
        return empty
    atr    = (df_ltf["high"] - df_ltf["low"]).rolling(14).mean().iloc[-1]
    price  = df_ltf["close"].iloc[-1]
    zone   = sd_zones[0]
    in_zone = (zone.bottom - atr * 0.3) <= price <= (zone.top + atr * 0.5)
    if not in_zone:
        return empty
    zone_mid = (zone.top + zone.bottom) / 2
    recent   = df_ltf.iloc[-12:]
    x        = np.arange(len(recent))
    slope_h  = np.polyfit(x, recent["high"].values, 1)[0]
    slope_l  = np.polyfit(x, recent["low"].values,  1)[0]
    # 1. Continuation pattern (canal / flag vers la zone)
    if direction == "LONG" and slope_h < -atr * 0.002 and slope_l < -atr * 0.002:
        return OBRetestResult(True, "continuation", direction, 15,
            f"🔁 Continuation Pattern sur Demand Zone @ {round(zone_mid,5)}")
    if direction == "SHORT" and slope_h > atr * 0.002 and slope_l > atr * 0.002:
        return OBRetestResult(True, "continuation", direction, 15,
            f"🔁 Continuation Pattern sur Supply Zone @ {round(zone_mid,5)}")
    # 2. Consolidation
    recent_range = (recent["high"] - recent["low"]).mean()
    if recent_range < atr * 0.6:
        return OBRetestResult(True, "consolidation", direction, 12,
            f"📦 Consolidation sur zone @ {round(zone_mid,5)} — range={round(recent_range,5)}")
    # 3. BSL/PDL Retest
    if len(df_h4) >= 12:
        pd_window = df_h4.iloc[-12:-6]
        pdl = pd_window["low"].min()
        pdh = pd_window["high"].max()
        last_low   = df_ltf["low"].iloc[-3:-1].min()
        last_high  = df_ltf["high"].iloc[-3:-1].max()
        last_close = df_ltf["close"].iloc[-1]
        if direction == "LONG" and last_low < pdl and last_close > pdl:
            return OBRetestResult(True, "bsl_retest", direction, 18,
                f"💧 BSL Retest — PDL swept @ {round(pdl,5)} → rebond Demand Zone")
        if direction == "SHORT" and last_high > pdh and last_close < pdh:
            return OBRetestResult(True, "bsl_retest", direction, 18,
                f"💧 BSL Retest — PDH swept @ {round(pdh,5)} → rebond Supply Zone")
    return empty


def best_pattern(patterns: list[PatternResult]) -> Optional[PatternResult]:
    """Retourne le pattern avec le score bonus le plus élevé."""
    return max(patterns, key=lambda p: p.score_bonus) if patterns else None


def detect_liquidity_sweep(df: pd.DataFrame) -> dict:
    result  = {"bullish_sweep": False, "bearish_sweep": False, "level": None}
    window  = df.iloc[-30:]
    swing_high = window["high"].max()
    swing_low  = window["low"].min()
    last_high  = df["high"].iloc[-1]
    last_low   = df["low"].iloc[-1]
    last_close = df["close"].iloc[-1]
    if last_high > swing_high * (1 + LIQ_THRESHOLD) and last_close < swing_high:
        result["bearish_sweep"] = True
        result["level"]         = swing_high
    if last_low < swing_low * (1 - LIQ_THRESHOLD) and last_close > swing_low:
        result["bullish_sweep"] = True
        result["level"]         = swing_low
    return result


def active_fvg(df: pd.DataFrame, fvgs: list[FVG], direction: str) -> Optional[FVG]:
    price = df["close"].iloc[-1]
    for fvg in reversed(fvgs):
        if fvg.direction != direction:
            continue
        lo, hi = min(fvg.top, fvg.bottom), max(fvg.top, fvg.bottom)
        if lo <= price <= hi:
            return fvg
    return None


def is_fvg_unmitigated(df: pd.DataFrame, fvg: FVG) -> bool:
    if fvg.index + 1 >= len(df):
        return True
    lo = min(fvg.top, fvg.bottom)
    hi = max(fvg.top, fvg.bottom)
    for i in range(fvg.index + 1, len(df)):
        if lo <= df["close"].iloc[i] <= hi:
            return False
    return True


def detect_confirmation_candle(df: pd.DataFrame, direction: str) -> bool:
    if len(df) < 4:
        return False
    for i in range(-2, -5, -1):
        o  = df["open"].iloc[i]
        h  = df["high"].iloc[i]
        l  = df["low"].iloc[i]
        cl = df["close"].iloc[i]
        body       = abs(cl - o)
        if body == 0:
            continue
        upper_wick = h - max(o, cl)
        lower_wick = min(o, cl) - l
        if direction == "LONG":
            if cl > o and i > -3:
                prev_o = df["open"].iloc[i - 1]
                prev_c = df["close"].iloc[i - 1]
                if prev_c < prev_o and cl > prev_o and o < prev_c:
                    return True
            if lower_wick >= body * 2 and cl > o:
                return True
        elif direction == "SHORT":
            if cl < o and i > -3:
                prev_o = df["open"].iloc[i - 1]
                prev_c = df["close"].iloc[i - 1]
                if prev_c > prev_o and cl < prev_o and o > prev_c:
                    return True
            if upper_wick >= body * 2 and cl < o:
                return True
    return False


# ═════════════════════════════════════════════════════════════
#  ⑦ SETUP : 4H SWEEP + 5M SHIFT + TARGET 4H HIGH/LOW
#
#  Logique (Image 1 — Instagram Reel) :
#  ─────────────────────────────────────
#  H4  : Le prix casse brièvement un swing Low/High récent
#         (sweep de la liquidité SSL ou BSL), puis CLÔTURE de retour
#         dans le range → manipulation institutionnelle confirmée.
#
#  5M  : Dans les bougies suivant le sweep H4, un BOS/CHoCH bullish
#         (ou bearish) se forme → shift de structure = confirmation
#         que les institutionnels ont inversé la direction.
#
#  TP  : Prochain High H4 (LONG) ou prochain Low H4 (SHORT)
#         = la "distribution" institutionnelle vise le côté opposé.
#
#  Score bonus : +22 si sweep + shift confirmés
# ═════════════════════════════════════════════════════════════

def detect_h4_sweep_5m_shift(
    df_h4: pd.DataFrame,
    df_m5: pd.DataFrame,
    direction: str,
) -> dict:
    """
    Détecte le setup 4H Sweep + 5M Shift.

    Retourne :
      detected      : bool
      sweep_level   : float | None   — niveau sweepé sur H4
      target_h4     : float | None   — prochain H/L H4 visé
      score_bonus   : int
      reasons       : list[str]
    """
    empty = {"detected": False, "sweep_level": None,
             "target_h4": None, "score_bonus": 0, "reasons": []}

    if len(df_h4) < 20 or len(df_m5) < 20:
        return empty

    atr_h4 = (df_h4["high"] - df_h4["low"]).rolling(14).mean().iloc[-1]
    if pd.isna(atr_h4) or atr_h4 == 0:
        return empty

    sweep_level = None
    sweep_found = False

    # ── Cherche un sweep dans les 6 dernières bougies H4 ────────
    for i in range(-6, -1):
        abs_i = len(df_h4) + i
        if abs_i < 15:
            continue

        h   = df_h4["high"].iloc[i]
        l   = df_h4["low"].iloc[i]
        cl  = df_h4["close"].iloc[i]

        lookback = df_h4.iloc[abs_i - 15: abs_i]
        if len(lookback) < 5:
            continue

        prev_low  = lookback["low"].min()
        prev_high = lookback["high"].max()

        if direction == "LONG":
            # SSL sweep : mèche basse sous prev_low, clôture au-dessus
            if l < prev_low - atr_h4 * 0.05 and cl > prev_low:
                sweep_level = prev_low
                sweep_found = True
                break
        else:
            # BSL sweep : mèche haute au-dessus prev_high, clôture en-dessous
            if h > prev_high + atr_h4 * 0.05 and cl < prev_high:
                sweep_level = prev_high
                sweep_found = True
                break

    if not sweep_found or sweep_level is None:
        return empty

    # ── Vérifie le Shift M5 (BOS aligné) dans les 10 dernières bougies ─
    bos_m5 = detect_bos(df_m5)
    target_bos_type = "bullish" if direction == "LONG" else "bearish"
    recent_bos = [b for b in bos_m5[-10:] if b["type"] == target_bos_type]

    if not recent_bos:
        return empty   # Pas de shift M5 → setup invalide

    # ── Target : prochain High/Low H4 non cassé ─────────────────
    window_h4 = df_h4.iloc[-30:]
    if direction == "LONG":
        # Vise le plus récent swing High H4 au-dessus du prix actuel
        price_now = df_m5["close"].iloc[-1]
        candidates = [
            window_h4["high"].iloc[k]
            for k in range(1, len(window_h4) - 1)
            if window_h4["high"].iloc[k] > window_h4["high"].iloc[k-1]
               and window_h4["high"].iloc[k] > window_h4["high"].iloc[k+1]
               and window_h4["high"].iloc[k] > price_now
        ]
        target_h4 = min(candidates) if candidates else round(window_h4["high"].max(), 2)
    else:
        price_now = df_m5["close"].iloc[-1]
        candidates = [
            window_h4["low"].iloc[k]
            for k in range(1, len(window_h4) - 1)
            if window_h4["low"].iloc[k] < window_h4["low"].iloc[k-1]
               and window_h4["low"].iloc[k] < window_h4["low"].iloc[k+1]
               and window_h4["low"].iloc[k] < price_now
        ]
        target_h4 = max(candidates) if candidates else round(window_h4["low"].min(), 2)

    sweep_type = "SSL (bas de range) → LONG" if direction == "LONG" \
                 else "BSL (haut de range) → SHORT"
    reasons = [
        f"🔄 4H Sweep {sweep_type} @ {round(sweep_level, 5)}  (+15)",
        f"📐 5M Shift confirmé (BOS {target_bos_type})  (+7)",
        f"🎯 Target H4 : {round(target_h4, 5)}",
    ]

    return {
        "detected"   : True,
        "sweep_level": sweep_level,
        "target_h4"  : target_h4,
        "score_bonus": 22,
        "reasons"    : reasons,
    }


# ═════════════════════════════════════════════════════════════
#  ⑧ SETUP : EQUAL HIGHS/LOWS + CHoCH + FVG + OB RETEST
#             (SMC Liquidity School — Image 2)
#
#  Logique :
#  ─────────
#  1. Equal Highs (EQH) ou Equal Lows (EQL) = pool de liquidité
#     Les institutionnels SAVENT que les stops sont là.
#
#  2. Sweep/Manipulation : le prix dépasse brièvement l'EQH ou EQL
#     puis revient → liquidity grab ("draw on liquidity").
#
#  3. Change of Character (CHoCH) : premier BOS CONTRAIRE après
#     le sweep = les institutionnels ont pris la liquidité et
#     inversent maintenant → signal de retournement.
#
#  4. Liquidity void / FVG formé après le CHoCH = zone de valeur.
#
#  5. Entrée : retest de l'OB baissier (ou haussier) ≈ 50% OB.
#     Target : prochain OB institutionnel de l'autre côté.
#
#  Score bonus : +25 si tous les critères sont réunis
# ═════════════════════════════════════════════════════════════

def detect_choch_eql_setup(
    df_h4:    pd.DataFrame,
    df_m5:    pd.DataFrame,
    liq_map:  "LiquidityMap",
    direction: str,
) -> dict:
    """
    Détecte le setup Equal Liq + CHoCH + FVG + OB.

    Retourne :
      detected      : bool
      choch_level   : float | None
      fvg_present   : bool
      score_bonus   : int
      reasons       : list[str]
    """
    empty = {"detected": False, "choch_level": None,
             "fvg_present": False, "score_bonus": 0, "reasons": []}

    if len(df_h4) < 20 or len(df_m5) < 20:
        return empty

    # ── 1. Equal Highs/Lows présents (liquidité institutionnelle) ─
    has_eqh = bool(liq_map.eqh_levels)
    has_eql = bool(liq_map.eql_levels)

    if direction == "SHORT" and not has_eqh:
        return empty   # SHORT : il faut des EQH pour sweeper
    if direction == "LONG" and not has_eql:
        return empty   # LONG : il faut des EQL pour sweeper

    eq_levels = liq_map.eqh_levels if direction == "SHORT" else liq_map.eql_levels
    eq_level  = eq_levels[0] if eq_levels else None

    # ── 2. Le prix a-t-il sweepé le niveau EQH/EQL ? ─────────────
    price_now = df_m5["close"].iloc[-1]
    atr_m5    = (df_m5["high"] - df_m5["low"]).rolling(14).mean().iloc[-1]
    if pd.isna(atr_m5) or atr_m5 == 0 or eq_level is None:
        return empty

    if direction == "SHORT":
        # Prix a dépassé l'EQH puis est redescendu
        swept = any(
            df_m5["high"].iloc[i] > eq_level + atr_m5 * 0.05
            and df_m5["close"].iloc[i] < eq_level
            for i in range(-10, -1)
            if abs(i) <= len(df_m5)
        )
    else:
        # Prix a cassé l'EQL puis est remonté
        swept = any(
            df_m5["low"].iloc[i] < eq_level - atr_m5 * 0.05
            and df_m5["close"].iloc[i] > eq_level
            for i in range(-10, -1)
            if abs(i) <= len(df_m5)
        )

    if not swept:
        return empty

    # ── 3. CHoCH (premier BOS contraire après sweep) ──────────────
    bos_m5 = detect_bos(df_m5)
    choch_type   = "bullish" if direction == "LONG" else "bearish"
    choch_recent = [b for b in bos_m5[-8:] if b["type"] == choch_type]
    choch_level  = choch_recent[-1]["level"] if choch_recent else None

    if choch_level is None:
        return empty

    # ── 4. FVG post-CHoCH ─────────────────────────────────────────
    fvgs_m5     = detect_fvg(df_m5)
    fvg_dir     = "bullish" if direction == "LONG" else "bearish"
    fvg_present = any(f.direction == fvg_dir for f in fvgs_m5[-10:])

    # ── Score ─────────────────────────────────────────────────────
    score  = 15   # base : EQL sweep + CHoCH
    score += 5 if fvg_present else 0
    score += 5 if (has_eqh and direction == "SHORT") or (has_eql and direction == "LONG") else 0

    eq_type_str = "EQH (equal highs)" if direction == "SHORT" else "EQL (equal lows)"
    choch_str   = "bearish CHoCH" if direction == "SHORT" else "bullish CHoCH"

    reasons = [
        f"💰 {eq_type_str} = pool de liquidité sweepé @ {round(eq_level, 5)}  (+15)",
        f"🔃 {choch_str} confirmé @ {round(choch_level, 5)}  (+5)",
    ]
    if fvg_present:
        reasons.append(f"🕳️ Liquidity void / FVG post-CHoCH présent  (+5)")

    return {
        "detected"    : True,
        "choch_level" : choch_level,
        "fvg_present" : fvg_present,
        "score_bonus" : min(score, 25),
        "reasons"     : reasons,
    }



#  Architecture :
#    Base H4 (biais + AMD + Septuple)        → 0–45 pts
#    Structure M15 (BOS + OB + Liquidité)    → 0–30 pts
#    Entrée M5 (FVG + S/D Zone + Bougie)     → 0–25 pts
#    Total max = 100 pts
# ═════════════════════════════════════════════════════════════

def compute_score_v3(
    # H4
    bias_aligned:       bool = False,
    amd_detected:       bool = False,
    amd_confidence:     int  = 0,
    septuple_detected:  bool = False,
    septuple_count:     int  = 0,
    # M15
    mtf_bos:            bool = False,
    mtf_ob:             bool = False,
    liquidity_taken:    bool = False,
    breaker_block:      bool = False,
    bsl_ssl_swept:      bool = False,   # BSL ou SSL sweepée (liquidity map)
    eqh_eql_present:    bool = False,   # Equal Highs/Lows = pool de liquidité
    # M5 Entry
    ltf_fvg:            bool = False,
    fvg_unmitigated:    bool = False,
    sd_zone_active:     bool = False,   # Supply/Demand zone active
    entry_candle_score: int  = 0,       # bonus des bougies institutionnelles
    older_block_htf:    bool = False,   # OB H4 actif
    # Chart Patterns + OB Retest
    pattern_bonus:      int  = 0,
    pattern_name:       str  = "",
    ob_retest_bonus:    int  = 0,
    ob_retest_desc:     str  = "",
    # ── Nouveaux setups ──────────────────────────────────────
    sweep_shift_bonus:  int  = 0,    # Setup 4H Sweep + 5M Shift
    choch_eql_bonus:    int  = 0,    # Setup CHoCH + Equal Liq
    # ── Order Flow + Breaker HTF ─────────────────────────────
    ofs_structural_bonus: int = 0,   # Order Flow Shift structurel M15
    ofs_structural_reason: str = "", # Raison OFS
    breaker_htf_bonus:  int  = 0,   # Breaker Block H4 (niveau institutionnel)
    breaker_htf_reason: str  = "",  # Raison Breaker HTF
) -> tuple[int, list[str]]:
    score   = 0
    reasons = []

    # ── H4 BASE (45 pts max) ──────────────────────────────────
    if bias_aligned:
        score += 15
        reasons.append("✅ Biais H4 aligné  (+15)")

    if amd_detected:
        amd_pts = min(20, int(amd_confidence * 0.20))
        score  += amd_pts
        reasons.append(f"🔮 AMD confirmé (confiance {amd_confidence}%)  (+{amd_pts})")

    if septuple_detected:
        sep_pts = 10 if septuple_count >= 7 else (8 if septuple_count >= 6 else 6)
        score  += sep_pts
        reasons.append(f"⚡ Septuple Traction H4 ({septuple_count} bougies)  (+{sep_pts})")

    # ── M15 STRUCTURE (30 pts max) ────────────────────────────
    if mtf_bos:
        score += 10
        reasons.append("✅ BOS M15 confirmé  (+10)")

    if mtf_ob:
        score += 7
        reasons.append("✅ Order Block M15 validé  (+7)")

    if liquidity_taken:
        score += 8
        reasons.append("✅ Liquidité M15 prise (stop hunt)  (+8)")

    if breaker_block:
        score += 10
        reasons.append("🔥 Breaker Block M15 détecté  (+10)")

    if bsl_ssl_swept:
        score += 8
        reasons.append("💧 BSL/SSL sweepée — pool de liquidité visé  (+8)")

    if eqh_eql_present:
        score += 4
        reasons.append("⚡ Equal High/Low (EQH/EQL) = liquidité institutionnelle  (+4)")

    # ── ORDER FLOW SHIFT STRUCTUREL (max 15 pts) ──────────────
    if ofs_structural_bonus > 0 and ofs_structural_reason:
        ofs_pts = min(ofs_structural_bonus, 15)
        score  += ofs_pts
        reasons.append(ofs_structural_reason)

    # ── BREAKER BLOCK H4 (max 18 pts) ─────────────────────────
    if breaker_htf_bonus > 0 and breaker_htf_reason:
        bb_pts = min(breaker_htf_bonus, 18)
        score += bb_pts
        reasons.append(breaker_htf_reason)

    # ── M5 ENTRÉE (25 pts max) ────────────────────────────────
    if sd_zone_active:
        score += 12
        reasons.append("🏛️ Prix dans zone Supply/Demand institutionnelle  (+12)")

    if ltf_fvg:
        score += 6
        reasons.append("📍 FVG M5 actif — zone de valeur  (+6)")

    if fvg_unmitigated:
        score += 3
        reasons.append("✅ FVG valid non mitiqué  (+3)")

    if older_block_htf:
        score += 5
        reasons.append("🏛️ Older Block H4 actif — confluence HTF  (+5)")

    # Bonus bougies institutionnelles (max 20 pts plafonné)
    if entry_candle_score > 0:
        ec_pts = min(entry_candle_score, 20)
        score += ec_pts
        reasons.append(f"🕯️ Bougie d'entrée institutionnelle  (+{ec_pts})")

    # ── CHART PATTERNS (max 25 pts) ───────────────────────────
    if pattern_bonus > 0 and pattern_name:
        p_pts = min(pattern_bonus, 25)
        score += p_pts
        reasons.append(f"📐 Pattern détecté : {pattern_name}  (+{p_pts})")

    # ── OB RETEST (max 18 pts) ────────────────────────────────
    if ob_retest_bonus > 0 and ob_retest_desc:
        r_pts = min(ob_retest_bonus, 18)
        score += r_pts
        reasons.append(f"{ob_retest_desc}  (+{r_pts})")

    # ── 4H SWEEP + 5M SHIFT (max 22 pts) ─────────────────────
    if sweep_shift_bonus > 0:
        ss_pts = min(sweep_shift_bonus, 22)
        score += ss_pts
        # raisons déjà dans reasons via detect_h4_sweep_5m_shift

    # ── CHoCH + EQUAL LIQ (max 25 pts) ───────────────────────
    if choch_eql_bonus > 0:
        ce_pts = min(choch_eql_bonus, 25)
        score += ce_pts
        # raisons déjà dans reasons via detect_choch_eql_setup

    return min(score, 100), reasons


# ─────────────────────────────────────────────────────────────
#  CALCUL NIVEAUX — ENTRY / SL / TP
# ─────────────────────────────────────────────────────────────

def compute_sl_tp_v3(
    df_m5:     pd.DataFrame,
    df_m15:    pd.DataFrame,
    direction: str,
    ob:        Optional[OrderBlock],
    fvg:       Optional[FVG],
    sd_zone:   Optional[SupplyDemandZone],
    liq_map:   Optional[LiquidityMap],
    symbol:    str = "",
) -> tuple[float, float, float, float, float, float]:
    """
    Entry / SL / TP1 / TP2 / TP3 v3+ — cibles structurelles réelles.

    ENTRÉE (priorité décroissante) :
      1. Milieu de la zone Supply/Demand
      2. Milieu du FVG M5
      3. Close M5 courant

    STOP LOSS :
      LONG  : sous le bas de la Demand Zone / OB / FVG  + buffer ATR×0.4
      SHORT : au-dessus du haut de la Supply Zone / OB / FVG  + buffer ATR×0.4

    TAKE PROFIT (3 niveaux structurels) :
      TP1 : RR3 min — BSL/SSL nearest ou PDH/PDL ou swing M15
      TP2 : RR5-6  — swing H/L M15 suivant au-delà de TP1
      TP3 : RR8-10 — liquidité majeure H4 / swing H4 / extension max
    """
    atr    = (df_m5["high"] - df_m5["low"]).rolling(14).mean().iloc[-1]
    close  = df_m5["close"].iloc[-1]
    spread = get_spread(symbol) if symbol else 0.0
    dec    = 2 if close > 100 else 5
    buf    = max(atr * 0.45, spread * 3.0)

    # ── 1. ENTRÉE ─────────────────────────────────────────────
    if sd_zone is not None:
        entry = round((sd_zone.top + sd_zone.bottom) / 2, dec)
    elif fvg is not None:
        entry = round((max(fvg.top, fvg.bottom) + min(fvg.top, fvg.bottom)) / 2, dec)
    else:
        entry = round(close, dec)

    # ── 2. STOP LOSS ──────────────────────────────────────────
    if direction == "LONG":
        if sd_zone:
            sl = round(sd_zone.bottom - buf, dec)
        elif ob:
            sl = round(ob.bottom - buf, dec)
        elif fvg:
            sl = round(min(fvg.top, fvg.bottom) - buf, dec)
        else:
            sl = round(entry - atr * 1.8, dec)
    else:  # SHORT
        if sd_zone:
            sl = round(sd_zone.top + buf, dec)
        elif ob:
            sl = round(ob.top + buf, dec)
        elif fvg:
            sl = round(max(fvg.top, fvg.bottom) + buf, dec)
        else:
            sl = round(entry + atr * 1.8, dec)

    risk = abs(entry - sl)
    if risk <= 0:
        return entry, sl, entry, entry, entry, 0.0

    # ── 3. COLLECTE des cibles structurelles réelles ──────────
    # Tous les niveaux au-delà de entry dans la bonne direction
    targets: list[float] = []

    # BSL/SSL depuis la liquidity map
    if liq_map is not None:
        if direction == "LONG":
            if liq_map.nearest_bsl and liq_map.nearest_bsl > entry + risk:
                targets.append(liq_map.nearest_bsl)
            if liq_map.pdh and liq_map.pdh > entry + risk:
                targets.append(liq_map.pdh)
            for lvl in (liq_map.bsl_levels or []):
                if lvl > entry + risk:
                    targets.append(lvl)
        else:
            if liq_map.nearest_ssl and liq_map.nearest_ssl < entry - risk:
                targets.append(liq_map.nearest_ssl)
            if liq_map.pdl and liq_map.pdl < entry - risk:
                targets.append(liq_map.pdl)
            for lvl in (liq_map.ssl_levels or []):
                if lvl < entry - risk:
                    targets.append(lvl)

    # Swing highs/lows M15
    window_m15 = df_m15.iloc[-80:]
    if direction == "LONG":
        for i in range(1, len(window_m15) - 1):
            h = window_m15["high"].iloc[i]
            if (h > window_m15["high"].iloc[i-1]
                    and h > window_m15["high"].iloc[i+1]
                    and h > entry + risk):
                targets.append(h)
    else:
        for i in range(1, len(window_m15) - 1):
            lo = window_m15["low"].iloc[i]
            if (lo < window_m15["low"].iloc[i-1]
                    and lo < window_m15["low"].iloc[i+1]
                    and lo < entry - risk):
                targets.append(lo)

    # Trier les cibles du plus proche au plus loin
    if direction == "LONG":
        targets = sorted(set(round(t, dec) for t in targets if t > entry + risk))
    else:
        targets = sorted(set(round(t, dec) for t in targets if t < entry - risk), reverse=True)

    # ── 4. ASSIGNATION TP1 / TP2 / TP3 ───────────────────────
    # TP1 = premier niveau structurel réel (RR1 minimum, pas de RR3 imposé)
    rr1_min = entry + risk * 1.0 if direction == "LONG" else entry - risk * 1.0
    rr3_min = entry + risk * 3.0 if direction == "LONG" else entry - risk * 3.0
    rr6_min = entry + risk * 6.0 if direction == "LONG" else entry - risk * 6.0

    if targets:
        # TP1 : première cible structurelle ≥ RR1 (la plus proche réelle)
        tp1_cands = [t for t in targets if (t >= rr1_min if direction == "LONG" else t <= rr1_min)]
        tp1 = round(tp1_cands[0], dec) if tp1_cands else round(rr1_min, dec)
    else:
        tp1 = round(rr1_min, dec)

    # TP2 : cible structurelle suivante ≥ RR3, sinon RR3 mathématique
    if targets:
        tp2_cands = [t for t in targets if (t >= rr3_min if direction == "LONG" else t <= rr3_min)
                     and t != tp1]
        tp2 = round(tp2_cands[0], dec) if tp2_cands else round(rr3_min, dec)
    else:
        tp2 = round(rr3_min, dec)

    # TP3 : extension max ≥ RR6 — swing H4 ou liquidité lointaine
    window_h4 = df_m15.iloc[-200:] if len(df_m15) >= 200 else df_m15
    tp3 = round(rr6_min, dec)  # fallback RR6
    if direction == "LONG":
        far_highs = [
            window_h4["high"].iloc[i]
            for i in range(1, len(window_h4) - 1)
            if window_h4["high"].iloc[i] > window_h4["high"].iloc[i-1]
               and window_h4["high"].iloc[i] > window_h4["high"].iloc[i+1]
               and window_h4["high"].iloc[i] >= rr6_min
        ]
        if far_highs:
            tp3 = round(max(far_highs), dec)
    else:
        far_lows = [
            window_h4["low"].iloc[i]
            for i in range(1, len(window_h4) - 1)
            if window_h4["low"].iloc[i] < window_h4["low"].iloc[i-1]
               and window_h4["low"].iloc[i] < window_h4["low"].iloc[i+1]
               and window_h4["low"].iloc[i] <= rr6_min
        ]
        if far_lows:
            tp3 = round(min(far_lows), dec)

    # ── 5. RR net sur TP1 ─────────────────────────────────────
    if direction == "LONG":
        gain_net = (tp1 - entry) - spread
    else:
        gain_net = (entry - tp1) - spread

    rr_net = round(gain_net / risk, 2) if gain_net > 0 and risk > 0 else 0.0
    return entry, sl, tp1, rr_net, tp2, tp3


# ═════════════════════════════════════════════════════════════
#  MOTEUR PRINCIPAL v3 — H4 → M15 → M5
# ═════════════════════════════════════════════════════════════

def analyse(symbol: str, htf: str = HTF, ltf: str = LTF,
            silent: bool = False) -> Optional[Signal]:
    mtf = MTF

    if not silent:
        print(f"\n{c('═'*65, 'cyan')}")
        print(f"  {c('SMC ENGINE v5+3 FUSION', 'yellow')}  —  {c(symbol, 'white')}  "
              f"{c(datetime.now(timezone.utc).strftime('%H:%M UTC'), 'cyan')}")
        print(f"  {c('H4 → M15 → M5  |  SMC_TRADER ★ + AMD + Septuple + S/D', 'cyan')}")
        print(c("═" * 65, "cyan"))

    # ── Téléchargement H4 / M15 / M5 ────────────────────────
    # Les indices EU (DAX, CAC, FTSE) ont moins de bougies M15 disponibles
    # → on force une période plus longue pour eux
    _idx_eu = {"^GDAXI", "^FCHI", "^FTSE", "^GSPC", "^NDX", "^DJI"}
    _ltf_period = "5d" if symbol in _idx_eu else "2d"
    _mtf_period = "10d" if symbol in _idx_eu else "5d"

    df_htf = fetch(symbol, htf, period="30d")   # H4 = 30j pour AMD
    df_mtf = fetch(symbol, mtf, period=_mtf_period)
    df_ltf = fetch(symbol, ltf, period=_ltf_period)

    if df_htf.empty or df_mtf.empty or df_ltf.empty:
        if not silent:
            print(c("  ✗ Données indisponibles.", "red"))
        return None

    # ── Filtre volatilité ────────────────────────────────────
    vol_ok, vol_reason = check_volatility(symbol, df_ltf, df_mtf)
    if not vol_ok:
        if not silent:
            print(c(f"  ⛔ Skip : {vol_reason}", "yellow"))
        return None

    # ─────────────────────────────────────────────────────────
    #  H4 : Biais + AMD + Septuple Traction
    # ─────────────────────────────────────────────────────────
    bias      = htf_bias(df_htf)
    direction = "SHORT" if bias == "BEARISH" else ("LONG" if bias == "BULLISH" else None)

    if not silent:
        col = "red" if bias == "BEARISH" else ("green" if bias == "BULLISH" else "yellow")
        print(f"\n  {'📊 Biais H4':<28} {c(bias, col)}")

    if direction is None:
        if not silent:
            print(c("  ✗ Biais NEUTRAL — ignoré.", "yellow"))
        return None

    # ── AMD ───────────────────────────────────────────────────
    amd = detect_amd_phase(df_htf)
    amd_ok    = (amd.phase == "distribution" and amd.direction == direction
                 and amd.confidence >= 60)
    amd_conf  = amd.confidence if amd_ok else 0

    if not silent:
        amd_col = "green" if amd_ok else "yellow"
        print(f"  {'🔮 AMD Phase':<28} {c(amd.phase.upper(), amd_col)}"
              f"  {c(amd.sub_phase, 'white') if amd.sub_phase else ''}"
              f"  conf={amd.confidence}%")

    # ── Septuple Traction ─────────────────────────────────────
    sept = detect_septuple_traction(df_htf)
    sept_ok    = sept["detected"] and sept.get("direction") == direction
    sept_count = sept.get("count", 0)

    if not silent:
        sept_col = "green" if sept_ok else "white"
        if sept_ok:
            sept_strength = sept.get("strength", "")
            print(f"  {'⚡ Septuple Traction H4':<28} {c(str(sept_count) + ' bougies — ' + sept_strength, sept_col)}")
        else:
            print(f"  {'⚡ Septuple Traction H4':<28} {c('Non détecté', 'white')}")

    # ── Liquidity Map ─────────────────────────────────────────
    liq_map = build_liquidity_map(df_htf, df_ltf)
    bsl_ssl_swept = (liq_map.swept_bsl and direction == "SHORT") or \
                    (liq_map.swept_ssl and direction == "LONG")
    eqh_eql_ok = bool(liq_map.eqh_levels or liq_map.eql_levels)

    if not silent:
        print(f"  {'💧 BSL/SSL Sweepée':<28} {c('✓', 'green') if bsl_ssl_swept else c('✗', 'red')}")
        print(f"  {'⚡ EQH/EQL présent':<28} {c('✓', 'green') if eqh_eql_ok else c('✗', 'red')}")

    # ─────────────────────────────────────────────────────────
    #  M15 : Structure, BOS, OB, Breaker
    # ─────────────────────────────────────────────────────────
    bos_mtf    = detect_bos(df_mtf)
    obs_mtf    = detect_order_blocks(df_mtf, bos_mtf)
    liq_mtf    = detect_liquidity_sweep(df_mtf)
    bkr_mtf    = detect_breaker_blocks(df_mtf, bos_mtf)

    last_bos_mtf  = bos_mtf[-1] if bos_mtf else None
    mtf_bos_ok    = last_bos_mtf is not None and last_bos_mtf["type"] == bias.lower()
    mtf_ob_ok     = any(o.direction == bias.lower() for o in obs_mtf)
    liq_taken     = liq_mtf["bearish_sweep"] if direction == "SHORT" else liq_mtf["bullish_sweep"]
    breaker_ok    = any(b["direction"] == bias.lower() for b in bkr_mtf)

    ob_mtf_match = next((o for o in reversed(obs_mtf) if o.direction == bias.lower()), None)

    if not silent:
        def tick(v): return c("✓", "green") if v else c("✗", "red")
        print(f"\n  {'BOS M15':<28} {tick(mtf_bos_ok)}")
        print(f"  {'OB M15':<28} {tick(mtf_ob_ok)}")
        print(f"  {'Liquidité prise M15':<28} {tick(liq_taken)}")
        print(f"  {'Breaker Block M15':<28} {tick(breaker_ok)}")

    # ─────────────────────────────────────────────────────────
    #  M5 : FVG, OB, Supply/Demand Zones, Bougies institutionnelles
    # ─────────────────────────────────────────────────────────
    bos_ltf  = detect_bos(df_ltf)
    fvgs_ltf = detect_fvg(df_ltf)
    obs_ltf  = detect_order_blocks(df_ltf, bos_ltf)

    fvg_active    = active_fvg(df_ltf, fvgs_ltf, bias.lower())
    ltf_fvg_ok    = fvg_active is not None
    fvg_unmit_ok  = is_fvg_unmitigated(df_ltf, fvg_active) if fvg_active else False
    ob_ltf_match  = next((o for o in reversed(obs_ltf) if o.direction == bias.lower()), None)

    # Supply & Demand Zones M5
    sd_zones   = detect_supply_demand_zones(df_ltf, direction)
    price_now  = df_ltf["close"].iloc[-1]
    atr_m5     = (df_ltf["high"] - df_ltf["low"]).rolling(14).mean().iloc[-1]
    sd_active  = price_in_sd_zone(price_now, sd_zones, atr_m5)
    sd_ok      = sd_active is not None

    # Older Block H4
    bos_htf    = detect_bos(df_htf)
    obs_htf    = detect_order_blocks(df_htf, bos_htf)
    older_block = any(
        o.direction == bias.lower() and
        min(o.top, o.bottom) <= price_now <= max(o.top, o.bottom)
        for o in obs_htf
    )

    # Bougies d'entrée institutionnelles
    entry_candles = detect_institutional_entry_candles(df_ltf, direction)
    ec_total_score = sum(ec.score_bonus for ec in entry_candles[:3])  # top 3 seulement

    # ── Chart Patterns (H4 + M5) ──────────────────────────────
    detected_patterns = detect_all_patterns(df_htf, df_ltf, direction)
    top_pattern       = best_pattern(detected_patterns)
    pat_bonus  = top_pattern.score_bonus if top_pattern else 0
    pat_name   = top_pattern.pattern_name if top_pattern else ""
    pat_desc   = top_pattern.description  if top_pattern else ""

    # ── OB Retest (3 types) ───────────────────────────────────
    ob_retest     = detect_ob_retest(df_htf, df_ltf, direction, sd_zones)
    ob_ret_bonus  = ob_retest.score_bonus if ob_retest.detected else 0
    ob_ret_desc   = ob_retest.description if ob_retest.detected else ""

    # ── Order Flow Shift structurel M15 ──────────────────────
    ofs_mtf       = detect_order_flow_structural(df_mtf, direction, bos_mtf)
    ofs_bonus     = ofs_mtf["score_bonus"] if ofs_mtf["detected"] else 0
    ofs_reason    = ofs_mtf["reason"] if ofs_mtf["detected"] else ""

    # ── Breaker Block H4 (niveau institutionnel) ──────────────
    bb_htf        = detect_breaker_block_htf(df_htf, df_mtf, direction)
    bb_htf_bonus  = bb_htf["score_bonus"] if bb_htf["detected"] else 0
    bb_htf_reason = bb_htf["reason"] if bb_htf["detected"] else ""

    # ── Setup ⑦ : 4H Sweep + 5M Shift ────────────────────────
    sweep_shift   = detect_h4_sweep_5m_shift(df_htf, df_ltf, direction)
    ss_bonus      = sweep_shift["score_bonus"] if sweep_shift["detected"] else 0

    # ── Setup ⑧ : CHoCH + Equal Liq ──────────────────────────
    choch_eql     = detect_choch_eql_setup(df_htf, df_ltf, liq_map, direction)
    ce_bonus      = choch_eql["score_bonus"] if choch_eql["detected"] else 0

    if not silent:
        print(f"\n  {'FVG M5 actif':<28} {tick(ltf_fvg_ok)}")
        print(f"  {'FVG non mitiqué':<28} {tick(fvg_unmit_ok)}")
        print(f"  {'Supply/Demand Zone':<28} {tick(sd_ok)}"
              + (f"  ratio_force={round(sd_active.impulse_size, 1)}×ATR" if sd_ok else ""))
        print(f"  {'Older Block H4':<28} {tick(older_block)}")
        if entry_candles:
            ec_names = " + ".join(f"{ec.candle_type}({ec.quality})" for ec in entry_candles[:3])
            print(f"  {'Bougies institutionnelles':<28} {c(ec_names, 'green')}  (+{ec_total_score})")
        else:
            print(f"  {'Bougies institutionnelles':<28} {c('Aucune détectée', 'white')}")
        if detected_patterns:
            pat_names_str = " · ".join(p.pattern_name for p in detected_patterns[:3])
            print(f"  {'📐 Chart Patterns':<28} {c(pat_names_str, 'magenta')}  (+{pat_bonus})")
        else:
            print(f"  {'📐 Chart Patterns':<28} {c('Aucun', 'white')}")
        if ob_retest.detected:
            print(f"  {'🔁 OB Retest':<28} {c(ob_retest.retest_type, 'yellow')}  (+{ob_ret_bonus})")
        else:
            print(f"  {'🔁 OB Retest':<28} {c('Non détecté', 'white')}")
        # Nouveaux setups
        if sweep_shift["detected"]:
            print(f"  {'🔄 4H Sweep+5M Shift':<28} {c('✓ DÉTECTÉ', 'green')}  (+{ss_bonus})")
        else:
            print(f"  {'🔄 4H Sweep+5M Shift':<28} {c('Non détecté', 'white')}")
        if choch_eql["detected"]:
            print(f"  {'💰 CHoCH+Equal Liq':<28} {c('✓ DÉTECTÉ', 'green')}  (+{ce_bonus})")
        else:
            print(f"  {'💰 CHoCH+Equal Liq':<28} {c('Non détecté', 'white')}")
        # Order Flow + Breaker HTF
        if ofs_mtf["detected"]:
            _ofs_cnt = ofs_mtf["count"]
            _ofs_lbl = c(f"{_ofs_cnt} HL/LH structurels", "green")
            print(f"  {'📈 Order Flow Shift M15':<28} {_ofs_lbl}  (+{ofs_bonus})")
        else:
            print(f"  {'📈 Order Flow Shift M15':<28} {c('Non détecté', 'white')}")
        if bb_htf["detected"]:
            _bb_tf  = bb_htf["tf"]
            _bb_lbl = c(f"{_bb_tf} ✓", "yellow")
            print(f"  {'🔥 Breaker Block H4':<28} {_bb_lbl}  (+{bb_htf_bonus})")
        else:
            print(f"  {'🔥 Breaker Block H4':<28} {c('Non détecté', 'white')}")

    # ─────────────────────────────────────────────────────────
    #  SCORING v3
    # ─────────────────────────────────────────────────────────
    score, reasons = compute_score_v3(
        bias_aligned      = True,
        amd_detected      = amd_ok,
        amd_confidence    = amd_conf,
        septuple_detected = sept_ok,
        septuple_count    = sept_count,
        mtf_bos           = mtf_bos_ok,
        mtf_ob            = mtf_ob_ok,
        liquidity_taken   = liq_taken,
        breaker_block     = breaker_ok,
        bsl_ssl_swept     = bsl_ssl_swept,
        eqh_eql_present   = eqh_eql_ok,
        ltf_fvg           = ltf_fvg_ok,
        fvg_unmitigated   = fvg_unmit_ok,
        sd_zone_active    = sd_ok,
        entry_candle_score= ec_total_score,
        older_block_htf   = older_block,
        pattern_bonus     = pat_bonus,
        pattern_name      = pat_name,
        ob_retest_bonus   = ob_ret_bonus,
        ob_retest_desc    = ob_ret_desc,
        sweep_shift_bonus = ss_bonus,
        choch_eql_bonus   = ce_bonus,
        ofs_structural_bonus  = ofs_bonus,
        ofs_structural_reason = ofs_reason,
        breaker_htf_bonus     = bb_htf_bonus,
        breaker_htf_reason    = bb_htf_reason,
    )

    # Ajout des raisons AMD
    if amd_ok:
        reasons = amd.reasons + reasons

    # Ajout des raisons Sweep+Shift et CHoCH+EQL
    if sweep_shift["detected"]:
        reasons = sweep_shift["reasons"] + reasons
    if choch_eql["detected"]:
        reasons = choch_eql["reasons"] + reasons

    # ─────────────────────────────────────────────────────────
    #  FILTRE ZONE D'ENTRÉE — au moins UNE zone valide
    # ─────────────────────────────────────────────────────────
    in_sd    = sd_ok
    in_fvg   = ltf_fvg_ok
    in_ob_m15 = False
    if ob_mtf_match:
        ob_lo = min(ob_mtf_match.top, ob_mtf_match.bottom)
        ob_hi = max(ob_mtf_match.top, ob_mtf_match.bottom)
        tol   = (ob_hi - ob_lo) * 0.10
        in_ob_m15 = (ob_lo - tol) <= price_now <= (ob_hi + tol)

    # Fibonacci 50–78.6% sur M15
    in_fib = False
    if len(df_mtf) >= 20:
        mw = df_mtf.iloc[-50:]
        sh = mw["high"].max()
        sl_v = mw["low"].min()
        if direction == "SHORT":
            f50 = sh - (sh - sl_v) * 0.500
            f786 = sh - (sh - sl_v) * 0.214
            in_fib = f50 <= price_now <= f786
        else:
            f50  = sl_v + (sh - sl_v) * 0.500
            f786 = sl_v + (sh - sl_v) * 0.786
            in_fib = f50 <= price_now <= f786

    price_in_zone = in_sd or in_fvg or in_ob_m15 or in_fib

    if not silent:
        print(f"\n  {c('── Filtre Zone d\'Entrée ──', 'cyan')}")
        print(f"  {'S/D Zone':<28} {tick(in_sd)}")
        print(f"  {'FVG M5':<28} {tick(in_fvg)}")
        print(f"  {'OB M15':<28} {tick(in_ob_m15)}")
        print(f"  {'Fibonacci 50–78.6%':<28} {tick(in_fib)}")
        zone_col = "green" if price_in_zone else "red"
        print(f"  {'→ Zone valide':<28} {c('OUI ✓' if price_in_zone else 'NON ✗ — rejeté', zone_col)}")

    if not price_in_zone:
        if not silent:
            print(c("\n  ✗ Prix hors zone structurelle — signal rejeté.", "red"))
        return None

    # Bonus multi-zone
    zone_count = sum([in_sd, in_fvg, in_ob_m15, in_fib])
    if zone_count >= 2:
        score = min(score + 5, 100)
        reasons.append(f"📐 Confluence {zone_count} zones  (+5)")

    # Affichage score
    if not silent:
        bar_filled = int(score / 5)
        bar = "█" * bar_filled + "░" * (20 - bar_filled)
        sc  = "green" if score >= 80 else ("yellow" if score >= 60 else "red")
        print(f"\n  Score  [{c(bar, sc)}]  {c(str(score) + '/100', sc)}")

    if score < SCORE_THRESHOLD:
        if not silent:
            print(c(f"\n  ✗ Score {score} < {SCORE_THRESHOLD} — insuffisant.", "yellow"))
        return None

    # ─────────────────────────────────────────────────────────
    #  NIVEAUX — Entry / SL / TP
    # ─────────────────────────────────────────────────────────
    ob_for_sl = ob_ltf_match or ob_mtf_match

    entry, sl, tp, rr, tp2, tp3 = compute_sl_tp_v3(
        df_m5     = df_ltf,
        df_m15    = df_mtf,
        direction = direction,
        ob        = ob_for_sl,
        fvg       = fvg_active,
        sd_zone   = sd_active,
        liq_map   = liq_map,
        symbol    = symbol,
    )

    if rr < MIN_RR:
        if not silent:
            print(c(f"\n  ✗ RR {rr} < {MIN_RR} — rejeté.", "yellow"))
        return None

    # ── PRE-BOS : prix à moins de 0.3 × ATR d'un swing H/L clé ──
    # Le signal PRE-BOS précède une cassure de structure imminente :
    # le prix approche le dernier swing H (SHORT) ou swing L (LONG)
    # sans l'avoir encore cassé → setup d'anticipation.
    pre_bos_detected = False
    atr_m5_val = (df_ltf["high"] - df_ltf["low"]).rolling(14).mean().iloc[-1]
    if not pd.isna(atr_m5_val) and atr_m5_val > 0:
        if direction == "SHORT":
            recent_highs = [v for _, v in swing_highs(df_mtf)[-5:]]
            if recent_highs:
                nearest_key = min(recent_highs, key=lambda h: abs(h - price_now))
                gap = nearest_key - price_now
                if 0 < gap < atr_m5_val * 0.3:
                    pre_bos_detected = True
                    reasons.append(
                        f"⚠️ PRE-BOS SHORT — swing high clé à {round(gap, 5)} "
                        f"({round(gap / atr_m5_val * 100, 0)}% ATR)  (+5)"
                    )
                    score = min(score + 5, 100)
        else:
            recent_lows = [v for _, v in swing_lows(df_mtf)[-5:]]
            if recent_lows:
                nearest_key = max(recent_lows, key=lambda l: l if l < price_now else -1)
                gap = price_now - nearest_key
                if 0 < gap < atr_m5_val * 0.3:
                    pre_bos_detected = True
                    reasons.append(
                        f"⚠️ PRE-BOS LONG — swing low clé à {round(gap, 5)} "
                        f"({round(gap / atr_m5_val * 100, 0)}% ATR)  (+5)"
                    )
                    score = min(score + 5, 100)

    # ─────────────────────────────────────────────────────────
    #  ★ SETUP PRIORITAIRE — SÉQUENCE SMC TRADER H4
    # ─────────────────────────────────────────────────────────
    smc_trader = detect_smc_trader(df_htf, df_mtf, df_ltf, direction)

    if not silent:
        if smc_trader.detected:
            sl_ref = smc_trader.sweep_low if direction == "LONG" else smc_trader.sweep_high
            print(f"  {'★ SMC Trader H4':<28} {c('✓ COMPLET', 'green')}  score={smc_trader.score}")
            print(f"    └ Bougie X (SL anchor) : {c(str(round(sl_ref, dec if 'dec' in dir() else 5)), 'red')}")
            print(f"    └ MSS M15 @ {round(smc_trader.mss_level, 5)}")
        else:
            print(f"  {'★ SMC Trader H4':<28} {c('✗ Incomplet', 'yellow')}")

    # Détermine le mode du signal — SMC_TRADER en priorité absolue
    if smc_trader.detected and smc_trader.score >= 60:
        mode = "SMC_TRADER"
        # Boost score avec les raisons SMC_TRADER
        score = min(score + smc_trader.score // 4, 100)
        reasons = smc_trader.reasons + reasons
    elif bb_htf["detected"] and bb_htf_bonus >= 15:
        mode = "BREAKER_HTF"
    elif amd_ok and amd_conf >= 75:
        mode = "AMD"
    elif sept_ok and sept_count >= 5:
        mode = "SEPTUPLE"
    elif ofs_mtf["detected"] and ofs_bonus >= 10:
        mode = "OFS"
    elif sweep_shift["detected"] and ss_bonus >= 20:
        mode = "SWEEP_SHIFT"
    elif choch_eql["detected"] and ce_bonus >= 20:
        mode = "CHOCH_LIQ"
    elif pre_bos_detected:
        mode = "PRE-BOS"
    elif pat_bonus >= 18:
        mode = "PATTERN"
    elif ob_retest.detected and ob_ret_bonus >= 15:
        mode = "OB_RETEST"
    elif sd_ok:
        mode = "SD"
    else:
        mode = "SMC"

    lot = compute_lot(symbol, entry, sl)

    # Récupérer le niveau BOS depuis les raisons (MTF)
    _bos_lv = 0.0
    for r in reasons:
        if "BOS" in r:
            try:
                import re as _re
                nums = _re.findall(r"[\d.]+", r)
                if nums:
                    _bos_lv = float(nums[-1])
                    break
            except Exception:
                pass

    # Niveau CHoCH depuis choch_eql si disponible
    _choch_lv = float(choch_eql.get("choch_level", 0.0)) if isinstance(choch_eql, dict) else 0.0

    signal = Signal(
        symbol    = symbol,
        direction = direction,
        entry     = entry,
        sl        = sl,
        tp        = tp,
        rr        = rr,
        score     = score,
        timestamp = datetime.now(timezone.utc),
        htf_bias  = bias,
        lot       = lot,
        risk_usd  = RISK_USD,
        mode      = mode,
        reasons   = reasons,
        # Données graphique
        df_chart  = df_ltf,
        fvg_chart = fvg_active,
        ob_chart  = ob_for_sl,
        bos_lv    = _bos_lv,
        choch_lv  = _choch_lv,
        tp2       = tp2,
        tp3       = tp3,
    )

    if not silent:
        dec    = 2 if entry > 100 else 5
        d_col  = "red" if direction == "SHORT" else "green"
        rr_col = "green" if rr >= 3 else "yellow"
        sc_col = "green" if score >= 80 else "yellow"
        print(f"\n  {c('━'*60, 'cyan')}")
        print(f"  {c('⚡ SIGNAL ÉLITE v3', 'yellow')}  [{c(mode, 'magenta')}]  →  {c(direction, d_col)}")
        print(f"  {c('━'*60, 'cyan')}")
        print(f"  {'Symbole':<22} {c(signal.symbol, 'white')}")
        print(f"  {'Direction':<22} {c(signal.direction, d_col)}")
        print(f"  {'Mode':<22} {c(mode, 'magenta')}")
        print(f"  {'─'*45}")
        print(f"  {'📍 Entrée':<22} {c(str(signal.entry), 'white')}")
        print(f"  {'🔴 Stop Loss':<22} {c(str(signal.sl), 'red')}   risk={round(abs(entry-sl),dec)}")
        print(f"  {'🟢 Take Profit':<22} {c(str(signal.tp), 'green')}   gain={round(abs(tp-entry),dec)}")
        print(f"  {'⚖  R : R':<22} {c('1:'+str(rr), rr_col)}")
        print(f"  {'Score':<22} {c(str(score)+'/100', sc_col)}")
        print(f"  {'💰 LOT':<22} {c(str(lot)+' lot', 'magenta')}")
        print(f"  {'─'*45}")
        print(f"  Confluence :")
        for r in reasons:
            print(f"    • {r}")
        print(f"  {c('━'*60, 'cyan')}\n")

    return signal


# ═════════════════════════════════════════════════════════════
#  WATCHLIST v3 — BTC UNIQUEMENT pour crypto
# ═════════════════════════════════════════════════════════════

TIER_1_PRIORITY: list[tuple[str, str]] = [
    ("GC=F",    "Gold"),
    ("SI=F",    "Silver"),
    ("CL=F",    "Oil WTI"),
    ("BZ=F",    "Oil Brent"),
    ("BTC-USD", "Bitcoin"),    # ← BTC UNIQUEMENT (pas ETH, pas autres crypto)
]

TIER_2_FOREX: list[tuple[str, str]] = [
    ("EURUSD=X", "EUR/USD"),
    ("GBPUSD=X", "GBP/USD"),
    ("USDJPY=X", "USD/JPY"),
    ("USDCHF=X", "USD/CHF"),
    ("AUDUSD=X", "AUD/USD"),
    ("NZDUSD=X", "NZD/USD"),
    ("USDCAD=X", "USD/CAD"),
]

TIER_3_EXTRA: list[tuple[str, str]] = [
    ("EURGBP=X", "EUR/GBP"), ("EURJPY=X", "EUR/JPY"), ("GBPJPY=X", "GBP/JPY"),
    ("EURAUD=X", "EUR/AUD"), ("GBPAUD=X", "GBP/AUD"), ("AUDJPY=X", "AUD/JPY"),
    ("CADJPY=X", "CAD/JPY"), ("CHFJPY=X", "CHF/JPY"), ("EURCAD=X", "EUR/CAD"),
    ("GBPCAD=X", "GBP/CAD"), ("NZDJPY=X", "NZD/JPY"), ("GBPCHF=X", "GBP/CHF"),
    ("EURCHF=X", "EUR/CHF"), ("EURNZD=X", "EUR/NZD"), ("GBPNZD=X", "GBP/NZD"),
    ("AUDCAD=X", "AUD/CAD"), ("AUDNZD=X", "AUD/NZD"), ("AUDCHF=X", "AUD/CHF"),
    ("NZDCAD=X", "NZD/CAD"), ("NZDCHF=X", "NZD/CHF"), ("CADCHF=X", "CAD/CHF"),
    ("USDMXN=X", "USD/MXN"), ("USDZAR=X", "USD/ZAR"), ("USDTRY=X", "USD/TRY"),
    ("USDSEK=X", "USD/SEK"), ("USDNOK=X", "USD/NOK"), ("USDSGD=X", "USD/SGD"),
    ("NG=F",     "Gaz Naturel"), ("HG=F",   "Cuivre"),
    ("^GSPC",    "S&P 500"), ("^NDX",   "Nasdaq 100"), ("^DJI",   "Dow Jones"),
    ("^GDAXI",   "DAX"),     ("^FCHI",  "CAC 40"),     ("^FTSE",  "FTSE 100"),
]

CATEGORY_MAP: dict[str, list[tuple[str, str]]] = {
    "priority"  : TIER_1_PRIORITY,
    "btc"       : [("BTC-USD", "Bitcoin")],
    # forex_all inclut toujours Gold + BTC + toutes les paires forex
    "forex"     : [("GC=F", "Gold"), ("BTC-USD", "Bitcoin")] + TIER_2_FOREX + [s for s in TIER_3_EXTRA if "=X" in s[0]],
    "forex_all" : [("GC=F", "Gold"), ("BTC-USD", "Bitcoin")] + TIER_2_FOREX + [s for s in TIER_3_EXTRA if "=X" in s[0]],
    "all"       : TIER_1_PRIORITY + TIER_2_FOREX + TIER_3_EXTRA,
}


def get_symbols(cat: str) -> list[tuple[str, str]]:
    return CATEGORY_MAP.get(cat, TIER_1_PRIORITY + TIER_2_FOREX + TIER_3_EXTRA)


# ─────────────────────────────────────────────────────────────
#  AFFICHAGE WATCHLIST
# ─────────────────────────────────────────────────────────────

def print_market_list(symbols: list[tuple[str, str]]) -> None:
    tier1_set = {s[0] for s in TIER_1_PRIORITY}
    tier2_set = {s[0] for s in TIER_2_FOREX}
    groups = [
        ("🥇  TIER 1  —  Gold / BTC / Commodités", []),
        ("🥈  TIER 2  —  Forex Majeures",           []),
        ("🥉  TIER 3  —  Croisées / Indices",        []),
    ]
    for sym, name in symbols:
        if sym in tier1_set:
            groups[0][1].append((sym, name))
        elif sym in tier2_set:
            groups[1][1].append((sym, name))
        else:
            groups[2][1].append((sym, name))

    W   = 72
    sep = "╔" + "═" * W + "╗"
    mid = "╠" + "═" * W + "╣"
    bot = "╚" + "═" * W + "╝"

    def row(text):
        return "║  " + text + " " * max(0, W - 2 - len(text)) + "║"

    print(f"\n{sep}")
    print(row(f"📋  SMC ENGINE v3  —  {len(symbols)} MARCHÉS  —  H4 · AMD · S/D"))
    print(row(f"   Score min : {SCORE_THRESHOLD}/100   |   RR min : 1:{MIN_RR}   |   Crypto : BTC only"))
    print(mid)

    grand_i = 1
    for tier_name, group in groups:
        if not group:
            continue
        print(row(f"{tier_name}  ({len(group)} marchés)"))
        print(row("─" * (W - 4)))
        for j in range(0, len(group), 2):
            sym1, name1 = group[j]
            col1 = f"{grand_i:>2}. {name1:<18}  {sym1:<13}"
            grand_i += 1
            col2 = ""
            if j + 1 < len(group):
                sym2, name2 = group[j + 1]
                col2 = f"{grand_i:>2}. {name2:<18}  {sym2}"
                grand_i += 1
            print(row(col1 + col2))
        print(row(""))
    print(bot + "\n")


# ─────────────────────────────────────────────────────────────
#  LOGGING + SESSIONS
# ─────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("smc_v3")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    # Pas de FileHandler sur Render (économise disque + I/O)
    if not os.environ.get("RENDER"):
        fh = logging.FileHandler("smc_v3.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


log = setup_logging()

SESSIONS = {"London": (7, 16), "New York": (12, 21)}

MAX_SIGNALS_PER_DAY = 5
_daily_count: dict[str, int] = {}
_daily_date:  str = ""
_last_bias:   dict[str, str] = {}


def check_daily_limit(symbol: str) -> bool:
    global _daily_count, _daily_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != _daily_date:
        _daily_date  = today
        _daily_count = {}
    return _daily_count.get(symbol, 0) < MAX_SIGNALS_PER_DAY


def increment_daily_count(symbol: str) -> None:
    _daily_count[symbol] = _daily_count.get(symbol, 0) + 1


def startup_check() -> bool:
    log.info("=" * 65)
    log.info("  SMC SIGNAL ENGINE v3  —  AMD · Septuple · Supply/Demand")
    log.info(f"  HTF={HTF}  MTF={MTF}  LTF={LTF}")
    log.info(f"  Score min : {SCORE_THRESHOLD}/100   RR min : {MIN_RR}   Risque : ${RISK_USD}")
    log.info(f"  Crypto    : BTC-USD 24/7 (y compris weekends)")
    log.info(f"  Setups    : SMC · AMD · Septuple · S/D · 4H Sweep+5M Shift · CHoCH+EQL")
    log.info("=" * 65)

    try:
        r = requests.get(_tg_url("getMe"), timeout=10)
        if r.status_code == 200:
            bot_name = r.json()["result"]["username"]
            log.info(f"  ✓ Bot Telegram : @{bot_name}")
        else:
            log.warning(f"  ⚠ Telegram répond {r.status_code} — démarrage en mode dégradé")
    except Exception as e:
        log.warning(f"  ⚠ Telegram injoignable : {e} — démarrage en mode dégradé")

    try:
        r2 = requests.get(_tg_url("getChat"),
                          params={"chat_id": TELEGRAM_GROUP_ID}, timeout=10)
        log.info("  ✓ Groupe Telegram OK" if r2.status_code == 200
                 else f"  ⚠ Groupe : {r2.text[:60]}")
    except Exception as e:
        log.warning(f"  ⚠ Groupe : {e}")

    yf_ok = False
    for attempt in range(1, 4):
        try:
            df = fetch("GC=F", "5m", period="1d")
            if not df.empty:
                log.info("  ✓ yfinance OK (Gold M5)")
                yf_ok = True
                break
        except Exception:
            pass
        time.sleep(20 * attempt)

    if not yf_ok:
        log.warning("  ⚠ yfinance indisponible — démarrage quand même.")

    # ── Message de démarrage Telegram ────────────────────────
    ts_start = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    startup_msg = (
        f"🟢 <b>SMC Signal Engine v3 — DÉMARRÉ</b>\n"
        f"{'─'*30}\n"
        f"<b>🕐 Heure :</b> <code>{ts_start}</code>\n"
        f"<b>📊 Timeframes :</b> H4 → H1 → M15\n"
        f"<b>⚙️ Score min :</b> {SCORE_THRESHOLD}/100\n"
        f"<b>⚖️ RR min :</b> 1:{MIN_RR}\n"
        f"<b>💰 Risque/trade :</b> ${RISK_USD}\n"
        f"<b>🔮 Modes :</b> SMC · AMD · Septuple · Supply/Demand\n"
        f"<b>🔄 Nouveau :</b> 4H Sweep+5M Shift · CHoCH+Equal Liq\n"
        f"<b>₿ BTC :</b> Scan 24/7 (weekends inclus) ✅\n"
        f"{'─'*30}\n"
        f"✅ Bot connecté · yfinance {'OK' if yf_ok else '⚠ indispo'}\n"
        f"🔍 Scan actif toutes les 5 minutes"
    )
    # ── Message de démarrage → DM leader uniquement (pas dans le groupe) ────
    try:
        if TELEGRAM_LEADER_ID:
            requests.post(_tg_url("sendMessage"), json={
                "chat_id": TELEGRAM_LEADER_ID,
                "text": startup_msg,
                "parse_mode": "HTML",
            }, timeout=10)
            log.info("  ✓ Message de démarrage envoyé en DM au leader")
    except Exception as e:
        log.warning(f"  ⚠ Envoi startup Telegram échoué : {e}")

    log.info("  ✓ Démarrage scan live\n")
    return True


def _reasons_flags(reasons: list[str]) -> tuple[str, str, str, str, str]:
    has_amd  = any("AMD" in r or "Accumulation" in r or "Manipulation" in r for r in reasons)
    has_bos  = any("BOS" in r for r in reasons)
    has_sd   = any("Supply" in r or "Demand" in r or "S/D" in r for r in reasons)
    has_liq  = any("Liquidit" in r or "BSL" in r or "SSL" in r or "Hunt" in r for r in reasons)
    has_sept = any("Septuple" in r or "Traction" in r for r in reasons)
    return (
        c("A", "magenta") if has_amd  else c("·", "white"),
        c("B", "green")   if has_bos  else c("·", "white"),
        c("S", "yellow")  if has_sd   else c("·", "white"),
        c("L", "cyan")    if has_liq  else c("·", "white"),
        c("7", "red")     if has_sept else c("·", "white"),
    )


# ─────────────────────────────────────────────────────────────
#  BOUCLE LIVE PRINCIPALE
# ─────────────────────────────────────────────────────────────

def run_live(cat: str = "all", min_score: int = SCORE_THRESHOLD,
             min_rr: float = MIN_RR, interval: int = 300) -> None:
    """
    Boucle principale VPS — scan continu toutes les {interval}s.
    Affiche un tableau de statut pour chaque marché :
      AMD(A) | BOS(B) | S/D(S) | Liquidité(L) | Septuple(7) | Score | RR | Statut
    """
    if not startup_check():
        log.error("  Startup check échoué — arrêt.")
        return

    symbols = get_symbols(cat)
    print_market_list(symbols)

    with _STATUS_LOCK:
        _STATUS["started_at"]    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        _STATUS["symbols_count"] = len(symbols)
        _STATUS["scan_running"]  = True

    consecutive_errors = 0
    cycle_n = 0

    while True:
        try:
            cycle_n += 1
            now_utc  = datetime.now(timezone.utc)
            now_str  = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

            if is_weekend():
                # ── Weekend : BTC 24/7 + Gold si dimanche soir ≥ 23h ──
                always_on = [
                    (s, m) for s, m in symbols
                    if is_crypto_symbol(s) or (s in GOLD_SYMBOLS and is_gold_session_active())
                ]
                if always_on:
                    symbols_to_scan = always_on
                    if cycle_n % 10 == 1:
                        names = ", ".join(s for s, _ in always_on)
                        log.info(f"  🌙 [{cycle_n}] {now_utc.strftime('%H:%M UTC')} "
                                 f"— Weekend  |  Scan actif : {names}")
                else:
                    if cycle_n % 10 == 1:
                        log.info(f"  💤 [{cycle_n}] {now_utc.strftime('%H:%M UTC')} — Weekend — attente")
                    with _STATUS_LOCK:
                        _STATUS["cycle"] = cycle_n
                        _STATUS["last_scan"] = now_str
                        _STATUS["scan_running"] = False
                    time.sleep(interval)
                    continue
            elif not is_session_active():
                # ── Hors session (nuit semaine) : BTC uniquement ───────
                btc_only = [(s, m) for s, m in symbols if is_crypto_symbol(s)]
                if btc_only:
                    symbols_to_scan = btc_only
                    if cycle_n % 10 == 1:
                        log.info(f"  💤 [{cycle_n}] {now_utc.strftime('%H:%M UTC')} "
                                 f"— Hors session — attente")
                else:
                    if cycle_n % 10 == 1:
                        log.info(f"  💤 [{cycle_n}] {now_utc.strftime('%H:%M UTC')} — Hors session — attente")
                    with _STATUS_LOCK:
                        _STATUS["cycle"] = cycle_n
                        _STATUS["last_scan"] = now_str
                        _STATUS["scan_running"] = False
                    time.sleep(interval)
                    continue
            else:
                symbols_to_scan = symbols

            with _STATUS_LOCK:
                _STATUS["scan_running"] = True
                _STATUS["cycle"]        = cycle_n
                _STATUS["last_scan"]    = now_str

            log.info(f"  🔍 [{cycle_n}] {now_utc.strftime('%H:%M UTC')} — Scan {len(symbols_to_scan)} paires")
            correlation_guard_reset()

            W = 95
            print(f"\n{'╔' + '═'*W + '╗'}")
            print(f"║  🔍  CYCLE #{cycle_n}  [{now_str}]  {len(symbols_to_scan)} marchés  "
                  + " " * max(0, W - 4 - 8 - len(now_str) - len(str(len(symbols_to_scan))) - 20) + "║")
            print(f"║  A=AMD · B=BOS · S=Supply/Demand · L=Liquidité · 7=Septuple · R=Sweep/CHoCH  "
                  + " " * max(0, W - 83) + "║")
            print(f"{'╠' + '═'*W + '╣'}")
            print(f"  {'N°':<4} {'Tier':<4} {'Marché':<14} {'Symbole':<12}"
                  f"  {'Prix':>14}  {'Biais':>6}"
                  f"  A  B  S  L  7  {'Score':>6}  {'RR':>5}  Statut")
            print(f"  {'─'*93}")

            signals_found: list[tuple[str, str, Signal, str]] = []

            for i, (sym, mkt) in enumerate(symbols_to_scan, 1):
                t1 = {s[0] for s in TIER_1_PRIORITY}
                t2 = {s[0] for s in TIER_2_FOREX}
                tier = c("T1🥇", "yellow") if sym in t1 else (
                       c("T2🥈", "cyan")   if sym in t2 else c("T3🥉", "white"))
                prefix = f"  {i:<4} {tier}  {mkt:<14} {c(sym, 'cyan'):<12}"

                print(prefix + "  … ", end="", flush=True)

                try:
                    sig = analyse(sym, silent=True)

                    if sig is not None:
                        new_bias = sig.htf_bias
                        if _last_bias.get(sym) and _last_bias[sym] != new_bias:
                            reset_setup(sym)
                        _last_bias[sym] = new_bias

                    if sig is None:
                        try:
                            df_peek = fetch(sym, LTF, period="1d")
                            px_s = str(round(df_peek["close"].iloc[-1],
                                             2 if df_peek["close"].iloc[-1] > 100 else 5)) \
                                   if not df_peek.empty else "—"
                            vol_ok, vol_reason = check_volatility(sym, df_peek) \
                                                  if not df_peek.empty else (True, "")
                            skip = f"⛔ {vol_reason}" if not vol_ok else "⚪ Pas de setup"
                        except Exception:
                            px_s = "—"
                            skip = "⚪ Pas de setup"
                        print(f"\r{prefix}  {px_s:>14}  {'—':>6}"
                              f"  ·  ·  ·  ·  ·  {'—':>6}  {'—':>5}  {skip}")
                    else:
                        px_s   = str(round(sig.entry, 2 if sig.entry > 100 else 5))
                        a_, b_, s_, l_, s7_ = _reasons_flags(sig.reasons)
                        sc_col = "green" if sig.score >= 80 else ("yellow" if sig.score >= 60 else "red")
                        rr_col = "green" if sig.rr >= 3 else "yellow"
                        d_col  = "red" if sig.direction == "SHORT" else "green"

                        if sig.score >= min_score and sig.rr >= min_rr:
                            corr_ok, corr_reason = correlation_guard(sym, sig.direction)
                            if not corr_ok:
                                status = c(f"🟠 Corrélé", "yellow")
                            else:
                                tier_lbl = next(
                                    (lbl for lbl, grp in [
                                        ("TIER 1 🥇  GOLD + BTC", TIER_1_PRIORITY),
                                        ("TIER 2 🥈  FOREX MAJEURES", TIER_2_FOREX),
                                        ("TIER 3 🥉  CROISÉES + EXTRA", TIER_3_EXTRA),
                                    ] if any(s == sym for s, _ in grp)),
                                    "TIER 2 🥈  FOREX MAJEURES"
                                )
                                signals_found.append((mkt, sym, sig, tier_lbl))
                                status = c(f"⚡ {sig.direction} [{sig.mode}]", d_col)
                        elif sig.score >= int(min_score * 0.75):
                            status = c(f"🟡 Proche ({sig.score})", "yellow")
                        else:
                            status = c(f"🔵 Attente ({sig.score})", "white")

                        print(f"\r{prefix}  {px_s:>14}  "
                              f"{c(sig.htf_bias[:4], d_col):>6}  "
                              f"{a_}  {b_}  {s_}  {l_}  {s7_}  "
                              f"{c(str(sig.score), sc_col):>6}  "
                              f"{c('1:'+str(sig.rr), rr_col):>5}  {status}")

                    time.sleep(1)

                except Exception as e:
                    print(f"\r{prefix}  {'—':>14}  {'—':>6}"
                          f"  ·  ·  ·  ·  ·  {'—':>6}  {'—':>5}  "
                          + c(f"⚠ {str(e)[:35]}", "red"))

            # ── Limite 2 meilleurs signaux / cycle ──────────
            print(f"  {'─'*93}")
            if len(signals_found) > 2:
                signals_found = sorted(signals_found, key=lambda x: x[2].score, reverse=True)[:2]

            if signals_found:
                print(c(f"\n  ⚡ {len(signals_found)} SIGNAL(S) — Envoi Telegram en cours…", "yellow"))

            for mkt, sym, sig, tier_lbl in signals_found:
                # ── Limite journalière par symbole ───────────────
                if not check_daily_limit(sym):
                    log.info(f"  ⏭ {sym} — limite {MAX_SIGNALS_PER_DAY} signaux/jour atteinte")
                    continue
                increment_daily_count(sym)

                log.info(f"  ⚡ {sig.direction} {mkt}  mode={sig.mode}  score={sig.score}  "
                         f"RR=1:{sig.rr}  lot={sig.lot}")
                tg_notify(sig, tier=tier_lbl, mode=sig.mode)

                with _STATUS_LOCK:
                    _STATUS["last_signals"].append({
                        "ts"       : datetime.now(timezone.utc).strftime("%d/%m %H:%M"),
                        "market"   : mkt,
                        "direction": sig.direction,
                        "entry"    : sig.entry,
                        "sl"       : sig.sl,
                        "tp"       : sig.tp,
                        "rr"       : sig.rr,
                        "score"    : sig.score,
                        "lot"      : sig.lot,
                        "mode"     : sig.mode,
                    })
                    _STATUS["last_signals"] = _STATUS["last_signals"][-20:]

            if not signals_found:
                print(c(f"  ℹ️  Aucun signal valide (score≥{min_score} + RR≥{min_rr})", "white"))

            print(f"{'╚' + '═'*W + '╝'}")
            consecutive_errors = 0
            log.info(f"  ⏳ Prochain scan dans {interval}s\n")
            time.sleep(interval)

        except KeyboardInterrupt:
            log.info("\n  Session live terminée.")
            try:
                requests.post(_tg_url("sendMessage"), json={
                    "chat_id": TELEGRAM_GROUP_ID,
                    "text": "🔴 <b>SMC Signal Engine v3 arrêté</b>",
                    "parse_mode": "HTML",
                }, timeout=5)
            except Exception:
                pass
            break

        except Exception as e:
            consecutive_errors += 1
            log.error(f"  ✗ Erreur critique : {e}")
            wait = min(60 * consecutive_errors, 300)
            log.info(f"  ⏳ Reprise dans {wait}s (erreur #{consecutive_errors})")
            time.sleep(wait)


# ─────────────────────────────────────────────────────────────
#  SCAN UNIQUE (test local)
# ─────────────────────────────────────────────────────────────

def scan_watchlist(symbols: list[tuple[str, str]], htf: str, ltf: str,
                   min_score: int = SCORE_THRESHOLD, min_rr: float = MIN_RR):
    all_results = []
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = len(symbols)

    print(f"\n{c('╔' + '═'*66 + '╗', 'cyan')}")
    print(f"{c('║', 'cyan')}  {c('SMC v3 SCAN — AMD · Septuple · S/D · Liquidity Map', 'yellow'):<65}{c('║', 'cyan')}")
    print(f"{c('║', 'cyan')}  H4={htf}  LTF={ltf}  score≥{min_score}  RR≥{min_rr}   {ts:<27}{c('║', 'cyan')}")
    print(f"{c('╚' + '═'*66 + '╝', 'cyan')}")

    for i, (sym, mkt) in enumerate(symbols, 1):
        print(f"  [{i:>2}/{total}]  {mkt:<16} {c(sym, 'cyan')} … ", end="", flush=True)
        try:
            sig = analyse(sym, htf, ltf, silent=True)
            if sig and sig.score >= min_score and sig.rr >= min_rr:
                all_results.append((mkt, sig))
                d_color = "red" if sig.direction == "SHORT" else "green"
                print(c(f"⚡ {sig.direction} [{sig.mode}]  score={sig.score}  RR=1:{sig.rr}", d_color))
                tg_notify(sig, tier="TIER 1", mode=sig.mode)
            else:
                print(c("—", "white"))
        except Exception as e:
            print(c(f"err: {e}", "red"))

    if all_results:
        print(f"\n{c('═'*70, 'yellow')}")
        print(c(f"  ⚡ {len(all_results)} signal(s) validé(s)", "yellow"))
        for mkt, s in sorted(all_results, key=lambda x: -x[1].score):
            d = "red" if s.direction == "SHORT" else "green"
            print(f"  {mkt:<16} {c(s.direction, d)}  [{s.mode}]  "
                  f"score={s.score}  RR=1:{s.rr}  lot={s.lot}")
    else:
        print(c(f"\n  Aucun signal score≥{min_score} RR≥{min_rr}", "yellow"))

    return all_results


# ═════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SMC Signal Engine v3 — AMD · Septuple Traction · Supply/Demand",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--symbol",    default=None,
                        help="Symbole unique  (ex: BTC-USD, GC=F, EURUSD=X)")
    parser.add_argument("--cat",       default="all",
                        choices=["priority", "btc", "forex", "forex_all", "all"],
                        help=(
                            "priority  = Gold + BTC\n"
                            "btc       = BTC uniquement\n"
                            "forex     = Forex + Gold + BTC\n"
                            "forex_all = Forex complet\n"
                            "all       = Tout scanner (défaut)\n"
                        ))
    parser.add_argument("--scan",      action="store_true",
                        help="Scan unique (test local)")
    parser.add_argument("--min-score", type=int,   default=SCORE_THRESHOLD)
    parser.add_argument("--min-rr",    type=float, default=MIN_RR)
    parser.add_argument("--interval",  type=int,   default=300,
                        help="Intervalle scan secondes (défaut: 300 = 5 minutes)")
    args = parser.parse_args()

    # ── Flask dashboard ───────────────────────────────────────
    flask_port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=start_flask, args=(flask_port,),
                     daemon=True, name="flask").start()
    time.sleep(3)
    log.info(f"  ✓ Flask dashboard port {flask_port}")

    # ── Self-ping anti-veille Render ──────────────────────────
    start_self_ping(flask_port)

    # ── Mode selon arguments ──────────────────────────────────
    if args.symbol:
        sig = analyse(args.symbol)
        if sig:
            tg_notify(sig, tier="TIER 1", mode=sig.mode)

    elif args.scan:
        symbols = get_symbols(args.cat)
        scan_watchlist(symbols, HTF, LTF, args.min_score, args.min_rr)

    else:
        run_live(cat=args.cat, min_score=args.min_score,
                 min_rr=args.min_rr, interval=args.interval)

