
"""
╔══════════════════════════════════════════════════════════════════════╗
║  BACKTEST HISTORIQUE 3 MOIS — SMC Signal Engine v7                 ║
║                                                                      ║
║  Ce script :                                                         ║
║   1. Télécharge 3 mois de données (yfinance) pour tous les marchés  ║
║   2. Rejoue le moteur SMC bougie par bougie (walk-forward)          ║
║   3. Simule chaque signal (entry / SL / TP1 / TP2 / TP3)           ║
║   4. Calcule winrate, PnL, drawdown, Sharpe (approximation)         ║
║   5. Envoie le rapport complet sur Telegram                         ║
║                                                                      ║
║  Usage :                                                             ║
║    export TG_TOKEN="7403481925:AAEDticdpHEhdCrVbwmopMG7QIi31bWxrwA"   # token du bot Telegram                  ║
║    export TG_CHAT_ID="6982051442" # chat_id du groupe ou DM                ║
║    python backtest_3mois.py                                          ║
║    python backtest_3mois.py --months 2 --cat forex --min-score 75   ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# ─────────────────────────────────────────────────────────────
#  PARAMÈTRES
# ─────────────────────────────────────────────────────────────
RISK_USD        = 100.0   # risque fixe par trade ($)
MIN_RR          = 2.5
SCORE_THRESHOLD = 72
AMD_LOOKBACK    = 30
FVG_MIN_RATIO   = 0.0002
OB_LOOKBACK     = 5
SD_MIN_IMPULSE  = 1.5

# ─────────────────────────────────────────────────────────────
#  MARCHÉS — miroir exact de main-v7.py
# ─────────────────────────────────────────────────────────────
TIER_1 = [
    ("GC=F",    "Gold"),
    ("BTC-USD", "Bitcoin"),
    ("SI=F",    "Silver"),
    ("CL=F",    "Oil WTI"),
    ("BZ=F",    "Oil Brent"),
]
TIER_2 = [
    ("EURUSD=X", "EUR/USD"), ("GBPUSD=X", "GBP/USD"),
    ("USDJPY=X", "USD/JPY"), ("USDCHF=X", "USD/CHF"),
    ("AUDUSD=X", "AUD/USD"), ("NZDUSD=X", "NZD/USD"),
    ("USDCAD=X", "USD/CAD"), ("^GDAXI",   "DAX"),
]
TIER_3 = [
    ("EURGBP=X","EUR/GBP"), ("EURJPY=X","EUR/JPY"), ("GBPJPY=X","GBP/JPY"),
    ("GBPAUD=X","GBP/AUD"), ("AUDJPY=X","AUD/JPY"), ("EURCAD=X","EUR/CAD"),
    ("GBPCAD=X","GBP/CAD"), ("GBPNZD=X","GBP/NZD"),
    ("^GSPC","S&P 500"),    ("^NDX","Nasdaq 100"),
]

MARKET_LISTS = {
    "priority": TIER_1,
    "forex":    TIER_1 + TIER_2,
    "all":      TIER_1 + TIER_2 + TIER_3,
}


# ─────────────────────────────────────────────────────────────
#  DATACLASSES
# ─────────────────────────────────────────────────────────────
@dataclass
class BacktestTrade:
    symbol:    str
    market:    str
    setup:     str
    direction: str
    entry:     float
    sl:        float
    tp1:       float
    tp2:       float
    tp3:       float
    rr:        float
    score:     int
    timestamp: datetime
    # Résultat simulé
    result:    str   = ""   # "TP1" | "TP2" | "TP3" | "SL" | "OPEN"
    pnl_usd:   float = 0.0
    bars_held: int   = 0


# ─────────────────────────────────────────────────────────────
#  UTILITAIRES (extraits de main-v7.py)
# ─────────────────────────────────────────────────────────────

def fetch_ohlc(symbol: str, interval: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Télécharge OHLC yfinance sur une plage de dates."""
    try:
        df = yf.download(
            symbol, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
            interval=interval, auto_adjust=True, progress=False,
            multi_level_index=False,
        )
        if df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0).str.lower()
        else:
            df.columns = df.columns.str.lower()
        for col in ("open", "high", "low", "close"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(subset=["open","high","low","close"], inplace=True)
        return df
    except Exception as e:
        print(f"  [FETCH] {symbol} {interval} : {e}")
        return pd.DataFrame()


def htf_bias(df: pd.DataFrame) -> str:
    if len(df) < 25:
        return "NEUTRAL"
    closes = df["close"].values
    highs  = df["high"].values
    lows   = df["low"].values
    ema8  = np.convolve(closes, np.ones(8) / 8,  mode="valid")
    ema21 = np.convolve(closes, np.ones(21)/ 21, mode="valid")
    c_last = closes[-1]
    score  = 0
    if c_last > ema8[-1]:  score += 1
    else:                  score -= 1
    if c_last > ema21[-1]: score += 1
    else:                  score -= 1
    if highs[-1] > highs[-4:-1].max():  score += 1
    else:                               score -= 1
    if lows[-1] > lows[-6:-1].min():    score += 1
    else:                               score -= 1
    bull = sum(closes[-5:] > closes[-6:-1])
    if bull >= 3:   score += 1
    elif bull <= 2: score -= 1
    if   score >=  2: return "BULLISH"
    elif score <= -2: return "BEARISH"
    return "NEUTRAL"


def detect_bos(df: pd.DataFrame) -> list[dict]:
    bos_list = []
    lookback = 10
    for i in range(lookback, len(df)):
        window = df.iloc[i - lookback:i]
        close  = df["close"].iloc[i]
        sl     = window["low"].min()
        sh     = window["high"].max()
        if close < sl:
            bos_list.append({"index": i, "type": "bearish", "level": sl})
        elif close > sh:
            bos_list.append({"index": i, "type": "bullish", "level": sh})
    return bos_list


def detect_fvg(df: pd.DataFrame) -> list[dict]:
    fvgs = []
    for i in range(2, len(df)):
        mid = df["close"].iloc[i]
        # Bullish FVG
        top    = df["high"].iloc[i]
        bottom = df["low"].iloc[i - 2]
        if top > bottom and (top - bottom) / mid > FVG_MIN_RATIO:
            fvgs.append({"dir": "bullish", "top": top, "bottom": bottom, "idx": i})
        # Bearish FVG
        top2   = df["high"].iloc[i - 2]
        bottom2= df["low"].iloc[i]
        if bottom2 > top2 and (bottom2 - top2) / mid > FVG_MIN_RATIO:
            fvgs.append({"dir": "bearish", "top": bottom2, "bottom": top2, "idx": i})
    return fvgs


def detect_order_blocks(df: pd.DataFrame, bos_list: list[dict]) -> list[dict]:
    obs = []
    for bos in bos_list[-5:]:
        idx = bos["index"]
        if idx < OB_LOOKBACK:
            continue
        if bos["type"] == "bearish":
            for j in range(idx - 1, idx - OB_LOOKBACK - 1, -1):
                if df["close"].iloc[j] > df["open"].iloc[j]:
                    obs.append({"dir": "bearish", "top": df["high"].iloc[j],
                                "bottom": df["low"].iloc[j], "idx": j})
                    break
        else:
            for j in range(idx - 1, idx - OB_LOOKBACK - 1, -1):
                if df["close"].iloc[j] < df["open"].iloc[j]:
                    obs.append({"dir": "bullish", "top": df["high"].iloc[j],
                                "bottom": df["low"].iloc[j], "idx": j})
                    break
    return obs


def compute_lot(symbol: str, entry: float, sl: float) -> float:
    risk = abs(entry - sl)
    if risk == 0:
        return 0.01
    sym = symbol.upper().replace("=X","").replace("-","").replace("^","")
    if symbol == "GC=F":
        lot = RISK_USD / (risk * 100.0)
    elif symbol == "SI=F":
        lot = RISK_USD / (risk * 50.0)
    elif symbol in ("CL=F","BZ=F"):
        lot = RISK_USD / (risk * 1000.0)
    elif sym in ("BTCUSD","ETHUSD") or symbol in ("BTC-USD","ETH-USD"):
        return round(RISK_USD / risk, 6)
    elif sym in ("GSPC","NDX","DJI","GDAXI","FCHI","FTSE"):
        lot = RISK_USD / (risk * 10.0)
    elif sym.endswith("JPY"):
        pip_val = 1000.0 / entry
        lot = RISK_USD / ((risk / 0.01) * pip_val)
    else:
        lot = RISK_USD / ((risk / 0.0001) * 10.0)
    return max(0.01, round(lot, 2))


# ─────────────────────────────────────────────────────────────
#  MOTEUR DE DÉTECTION (simplifié — reproduit le scoring v7)
# ─────────────────────────────────────────────────────────────

def _atr(df: pd.DataFrame, n: int = 14) -> float:
    val = (df["high"] - df["low"]).rolling(n).mean().iloc[-1]
    return float(val) if not pd.isna(val) else 0.0


def detect_signals_on_window(
    symbol: str, df_h4: pd.DataFrame, df_m15: pd.DataFrame,
    direction: str, min_score: int
) -> list[dict]:
    """
    Rejoue les 7 modules SMC sur la fenêtre courante.
    Retourne une liste de signaux candidats avec entry/sl/tp.
    """
    signals = []
    if len(df_h4) < 20 or len(df_m15) < 20:
        return signals

    atr_h4  = _atr(df_h4)
    atr_m15 = _atr(df_m15)
    if atr_h4 == 0 or atr_m15 == 0:
        return signals

    price   = df_m15["close"].iloc[-1]
    bos_m15 = detect_bos(df_m15)
    bos_h4  = detect_bos(df_h4)
    obs_m15 = detect_order_blocks(df_m15, bos_m15)
    fvg_m15 = detect_fvg(df_m15)
    expected = "bullish" if direction == "LONG" else "bearish"
    dec = 2 if price > 100 else 5

    def _sl_tp(setup: str) -> Optional[tuple]:
        """Calcule entry/sl/tp1/tp2 selon le setup."""
        if direction == "LONG":
            ob_match = next((o for o in reversed(obs_m15) if o["dir"] == "bullish"), None)
            fvg_match = next((f for f in reversed(fvg_m15) if f["dir"] == "bullish"), None)
            if ob_match:
                entry = round(ob_match["top"], dec)
                sl    = round(ob_match["bottom"] - atr_m15 * 0.3, dec)
            elif fvg_match:
                entry = round((fvg_match["top"] + fvg_match["bottom"]) / 2, dec)
                sl    = round(fvg_match["bottom"] - atr_m15 * 0.3, dec)
            else:
                entry = round(price, dec)
                sl    = round(price - atr_m15 * 1.5, dec)
            risk = entry - sl
            if risk <= 0:
                return None
            tp1 = round(entry + risk * 2.5, dec)
            tp2 = round(entry + risk * 4.0, dec)
            tp3 = round(entry + risk * 6.0, dec)
            rr  = round((tp1 - entry) / risk, 1)
        else:
            ob_match = next((o for o in reversed(obs_m15) if o["dir"] == "bearish"), None)
            fvg_match = next((f for f in reversed(fvg_m15) if f["dir"] == "bearish"), None)
            if ob_match:
                entry = round(ob_match["bottom"], dec)
                sl    = round(ob_match["top"] + atr_m15 * 0.3, dec)
            elif fvg_match:
                entry = round((fvg_match["top"] + fvg_match["bottom"]) / 2, dec)
                sl    = round(fvg_match["top"] + atr_m15 * 0.3, dec)
            else:
                entry = round(price, dec)
                sl    = round(price + atr_m15 * 1.5, dec)
            risk = sl - entry
            if risk <= 0:
                return None
            tp1 = round(entry - risk * 2.5, dec)
            tp2 = round(entry - risk * 4.0, dec)
            tp3 = round(entry - risk * 6.0, dec)
            rr  = round((entry - tp1) / risk, 1)

        if rr < MIN_RR:
            return None
        return entry, sl, tp1, tp2, tp3, rr

    # ── T1 BREAKER ────────────────────────────────────────────
    has_bos_h4  = any(b["type"] == expected for b in bos_h4[-6:])
    has_bos_m15 = any(b["type"] == expected for b in bos_m15[-5:])
    if has_bos_h4 and has_bos_m15:
        score = 72 + min(len([b for b in bos_m15[-5:] if b["type"] == expected]) * 5, 15)
        lvls = _sl_tp("BREAKER")
        if lvls and score >= min_score:
            signals.append({"setup": "BREAKER", "tier": 1, "score": score,
                            "direction": direction, "levels": lvls})

    # ── T3 ORDER BLOCK ────────────────────────────────────────
    ob_aligned = [o for o in obs_m15 if o["dir"] == expected]
    if ob_aligned and has_bos_m15:
        ob = ob_aligned[-1]
        in_ob = ob["bottom"] <= price <= ob["top"]
        score = 70 + (10 if in_ob else 0)
        lvls = _sl_tp("OB")
        if lvls and score >= min_score:
            signals.append({"setup": "OB", "tier": 3, "score": score,
                            "direction": direction, "levels": lvls})

    # ── T4 BOS RETEST ─────────────────────────────────────────
    if has_bos_m15:
        fvg_aligned = [f for f in fvg_m15 if f["dir"] == expected]
        if fvg_aligned:
            fvg = fvg_aligned[-1]
            in_fvg = fvg["bottom"] <= price <= fvg["top"]
            score  = 68 + (8 if in_fvg else 0)
            lvls = _sl_tp("BOS")
            if lvls and score >= min_score:
                signals.append({"setup": "BOS", "tier": 4, "score": score,
                                "direction": direction, "levels": lvls})

    # ── T6 FVG ────────────────────────────────────────────────
    fvg_aligned = [f for f in fvg_m15[-10:] if f["dir"] == expected]
    if fvg_aligned:
        fvg = fvg_aligned[-1]
        in_fvg = fvg["bottom"] <= price <= fvg["top"]
        if in_fvg and has_bos_m15:
            score = 72
            lvls = _sl_tp("FVG")
            if lvls and score >= min_score:
                signals.append({"setup": "FVG", "tier": 6, "score": score,
                                "direction": direction, "levels": lvls})

    # ── T7 AMD ────────────────────────────────────────────────
    if len(df_h4) >= AMD_LOOKBACK + 5:
        window = df_h4.iloc[-AMD_LOOKBACK:]
        atr_full = (df_h4["high"] - df_h4["low"]).rolling(14).mean()
        atr_now  = atr_full.iloc[-1]
        split    = AMD_LOOKBACK * 2 // 3
        rw       = window.iloc[:split]
        recent   = window.iloc[split:]
        rh = rw["high"].quantile(0.80)
        rl = rw["low"].quantile(0.20)
        sweep_down = sweep_up = False
        for i in range(len(recent) - 1, max(len(recent) - 8, 0), -1):
            h_ = recent["high"].iloc[i]
            l_ = recent["low"].iloc[i]
            cl_= recent["close"].iloc[i]
            if not pd.isna(atr_now) and atr_now > 0:
                if l_ < rl - atr_now * 0.1 and cl_ > rl:
                    sweep_down = True; break
                if h_ > rh + atr_now * 0.1 and cl_ < rh:
                    sweep_up   = True; break
        amd_dir = None
        if sweep_down and direction == "LONG":  amd_dir = "LONG"
        if sweep_up   and direction == "SHORT": amd_dir = "SHORT"
        if amd_dir and has_bos_m15:
            score = 75
            lvls = _sl_tp("AMD")
            if lvls and score >= min_score:
                signals.append({"setup": "AMD", "tier": 7, "score": score,
                                "direction": direction, "levels": lvls})

    return signals


# ─────────────────────────────────────────────────────────────
#  SIMULATEUR DE SORTIE (walk-forward sur bougies futures)
# ─────────────────────────────────────────────────────────────

def simulate_trade(
    df_future: pd.DataFrame,   # bougies M15 APRÈS le signal
    direction: str,
    entry: float, sl: float, tp1: float, tp2: float
) -> tuple[str, float, int]:
    """
    Parcourt les bougies futures bougie par bougie.
    Règle de gestion :
      - TP1 touché → ferme 50%, déplace SL à entry (BE)
      - TP2 touché (ou TP1 manqué) → ferme le reste
      - SL touché → perte pleine
    Retourne (résultat, pnl_usd, nb_bougies).
    """
    if df_future.empty:
        return "OPEN", 0.0, 0

    risk = abs(entry - sl)
    if risk == 0:
        return "OPEN", 0.0, 0

    be_triggered = False

    for i, (_, row) in enumerate(df_future.iterrows()):
        h = row["high"]
        l = row["low"]

        if direction == "LONG":
            # TP2 atteint
            if h >= tp2:
                pnl = RISK_USD * ((tp2 - entry) / risk)
                return "TP2", round(pnl, 2), i + 1
            # TP1 atteint
            if h >= tp1 and not be_triggered:
                be_triggered = True
                # Déplace SL à entry (BE) — continue à traquer TP2
            # SL touché (ou BE)
            sl_now = entry if be_triggered else sl
            if l <= sl_now:
                if be_triggered:
                    # 50% déjà fermé en TP1 — reste fermé à BE
                    partial_pnl = RISK_USD * ((tp1 - entry) / risk) * 0.5
                    return "TP1", round(partial_pnl, 2), i + 1
                else:
                    return "SL", round(-RISK_USD, 2), i + 1
        else:
            if l <= tp2:
                pnl = RISK_USD * ((entry - tp2) / risk)
                return "TP2", round(pnl, 2), i + 1
            if l <= tp1 and not be_triggered:
                be_triggered = True
            sl_now = entry if be_triggered else sl
            if h >= sl_now:
                if be_triggered:
                    partial_pnl = RISK_USD * ((entry - tp1) / risk) * 0.5
                    return "TP1", round(partial_pnl, 2), i + 1
                else:
                    return "SL", round(-RISK_USD, 2), i + 1

    # Ni SL ni TP2 touché dans la fenêtre
    last = df_future["close"].iloc[-1]
    if direction == "LONG":
        pnl = RISK_USD * ((last - entry) / risk)
    else:
        pnl = RISK_USD * ((entry - last) / risk)
    return "OPEN", round(pnl, 2), len(df_future)


# ─────────────────────────────────────────────────────────────
#  BOUCLE PRINCIPALE DE BACKTEST
# ─────────────────────────────────────────────────────────────

def run_backtest(
    symbols: list[tuple[str, str]],
    months:  int  = 3,
    min_score: int = SCORE_THRESHOLD,
    step_bars: int = 4,   # réévaluation toutes les N bougies M15 (≈1h)
) -> list[BacktestTrade]:
    """
    Walk-forward complet :
      - Télécharge MONTHS mois de données M15 et H4
      - Avance pas à pas (step_bars bougies M15)
      - Lance le moteur de détection sur la fenêtre glissante
      - Simule chaque signal sur les 200 bougies M15 suivantes
    """
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=months * 31)

    all_trades: list[BacktestTrade] = []
    _sent_keys: set = set()   # évite les doublons (même signal)

    print(f"\n  📅 Période : {start.strftime('%d/%m/%Y')} → {end.strftime('%d/%m/%Y')}")
    print(f"  🔍 Marchés : {len(symbols)}  |  Score min : {min_score}  |  RR min : {MIN_RR}\n")

    for sym, mkt in symbols:
        print(f"  ⏳ {mkt} ({sym}) …", end="", flush=True)
        df_h4  = fetch_ohlc(sym, "4h",  start, end)
        df_m15 = fetch_ohlc(sym, "15m", start, end)

        if df_h4.empty or df_m15.empty or len(df_m15) < 60:
            print(" ⛔ données vides")
            continue

        trades_sym = 0
        # Fenêtre glissante : on commence à i=50 pour avoir assez de contexte
        for i in range(50, len(df_m15) - 20, step_bars):
            win_h4  = df_h4.iloc[:max(5, i // 4)]
            win_m15 = df_m15.iloc[:i]

            bias = htf_bias(win_h4)
            if bias == "NEUTRAL":
                continue
            direction = "LONG" if bias == "BULLISH" else "SHORT"

            sigs = detect_signals_on_window(sym, win_h4, win_m15, direction, min_score)

            for sig in sigs:
                entry, sl, tp1, tp2, tp3, rr = sig["levels"]
                key = f"{sym}:{sig['setup']}:{direction}:{round(entry,4)}"
                if key in _sent_keys:
                    continue
                _sent_keys.add(key)

                # Simuler sur les 200 bougies suivantes
                df_fut = df_m15.iloc[i: i + 200]
                result, pnl, bars = simulate_trade(df_fut, direction, entry, sl, tp1, tp2)

                ts = df_m15.index[i]
                if hasattr(ts, "to_pydatetime"):
                    ts = ts.to_pydatetime()

                trade = BacktestTrade(
                    symbol=sym, market=mkt, setup=sig["setup"],
                    direction=direction,
                    entry=entry, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                    rr=rr, score=sig["score"], timestamp=ts,
                    result=result, pnl_usd=pnl, bars_held=bars,
                )
                all_trades.append(trade)
                trades_sym += 1

        print(f" ✅ {trades_sym} trades")
        time.sleep(0.5)   # rate-limit yfinance

    return all_trades


# ─────────────────────────────────────────────────────────────
#  CALCUL DES STATISTIQUES
# ─────────────────────────────────────────────────────────────

def compute_stats(trades: list[BacktestTrade]) -> dict:
    if not trades:
        return {}

    closed = [t for t in trades if t.result in ("TP1","TP2","SL")]
    n      = len(closed)
    if n == 0:
        return {"total": len(trades), "closed": 0}

    wins   = [t for t in closed if t.result in ("TP1","TP2")]
    losses = [t for t in closed if t.result == "SL"]
    open_  = [t for t in trades if t.result == "OPEN"]

    pnl_list = [t.pnl_usd for t in closed]
    cum_pnl  = np.cumsum(pnl_list)

    # Drawdown
    peak  = np.maximum.accumulate(cum_pnl)
    dd    = cum_pnl - peak
    max_dd = float(dd.min())

    # Sharpe simplifié (rendement journalier estimé)
    pnl_arr = np.array(pnl_list)
    sharpe  = 0.0
    if pnl_arr.std() > 0:
        sharpe = round(pnl_arr.mean() / pnl_arr.std() * np.sqrt(252), 2)

    # Par setup
    setups: dict[str, dict] = {}
    for t in closed:
        s = t.setup
        if s not in setups:
            setups[s] = {"total": 0, "wins": 0, "pnl": 0.0}
        setups[s]["total"] += 1
        if t.result in ("TP1","TP2"):
            setups[s]["wins"] += 1
        setups[s]["pnl"] += t.pnl_usd

    # Par marché
    markets: dict[str, dict] = {}
    for t in closed:
        m = t.market
        if m not in markets:
            markets[m] = {"total": 0, "wins": 0, "pnl": 0.0}
        markets[m]["total"] += 1
        if t.result in ("TP1","TP2"):
            markets[m]["wins"] += 1
        markets[m]["pnl"] += t.pnl_usd

    # TP breakdown
    tp1_n = sum(1 for t in closed if t.result == "TP1")
    tp2_n = sum(1 for t in closed if t.result == "TP2")
    sl_n  = len(losses)

    return {
        "total":       len(trades),
        "closed":      n,
        "open":        len(open_),
        "wins":        len(wins),
        "losses":      sl_n,
        "tp1":         tp1_n,
        "tp2":         tp2_n,
        "winrate":     round(len(wins) / n * 100, 1),
        "total_pnl":   round(float(np.sum(pnl_list)), 2),
        "avg_win":     round(float(np.mean([t.pnl_usd for t in wins])), 2) if wins else 0,
        "avg_loss":    round(float(np.mean([t.pnl_usd for t in losses])), 2) if losses else 0,
        "best_trade":  round(float(max(pnl_list)), 2),
        "worst_trade": round(float(min(pnl_list)), 2),
        "max_dd":      round(max_dd, 2),
        "sharpe":      sharpe,
        "profit_factor": round(
            sum(t.pnl_usd for t in wins) / max(abs(sum(t.pnl_usd for t in losses)), 0.01), 2
        ),
        "setups":  setups,
        "markets": markets,
    }


# ─────────────────────────────────────────────────────────────
#  FORMATEUR DU RAPPORT
# ─────────────────────────────────────────────────────────────

def format_report(stats: dict, months: int, cat: str) -> str:
    s = stats
    period_end   = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=months * 31)

    lines = [
        "╔══════════════════════════════════════════╗",
        "║  📊  BACKTEST SMC Signal Engine v7       ║",
        "╚══════════════════════════════════════════╝",
        "",
        f"📅 Période  : {period_start.strftime('%d/%m/%Y')} → {period_end.strftime('%d/%m/%Y')} ({months} mois)",
        f"🌍 Marchés  : {cat.upper()}",
        f"⚙️  Score min : {SCORE_THRESHOLD}  |  RR min : {MIN_RR}  |  Risque/trade : ${RISK_USD:.0f}",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "  📈  RÉSULTATS GLOBAUX",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"  Trades totaux    : {s.get('total', 0)}",
        f"  Trades clôturés  : {s.get('closed', 0)}",
        f"  Trades ouverts   : {s.get('open', 0)}",
        f"  ✅ Gagnants       : {s.get('wins', 0)}",
        f"  ❌ Perdants       : {s.get('losses', 0)}",
        f"  🎯 TP1 atteint    : {s.get('tp1', 0)}",
        f"  🎯 TP2 atteint    : {s.get('tp2', 0)}",
        "",
        f"  🏆 Win Rate       : {s.get('winrate', 0)}%",
        f"  💰 PnL total      : ${s.get('total_pnl', 0):+.2f}",
        f"  📊 Profit Factor  : {s.get('profit_factor', 0)}",
        f"  📉 Max Drawdown   : ${s.get('max_dd', 0):.2f}",
        f"  ⚡ Sharpe (≈)     : {s.get('sharpe', 0)}",
        f"  🟢 Gain moyen     : ${s.get('avg_win', 0):+.2f}",
        f"  🔴 Perte moyenne  : ${s.get('avg_loss', 0):+.2f}",
        f"  🌟 Meilleur trade : ${s.get('best_trade', 0):+.2f}",
        f"  💀 Pire trade     : ${s.get('worst_trade', 0):+.2f}",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "  🔧  PAR SETUP",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    for setup, d in sorted(s.get("setups", {}).items()):
        wr = round(d["wins"] / d["total"] * 100, 1) if d["total"] else 0
        lines.append(
            f"  {setup:<10} {d['total']:>4} trades  "
            f"WR {wr:>5.1f}%  PnL ${d['pnl']:>+8.2f}"
        )

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "  🌍  TOP 5 MARCHÉS (PnL)",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    top5 = sorted(s.get("markets", {}).items(), key=lambda x: x[1]["pnl"], reverse=True)[:5]
    for mkt, d in top5:
        wr = round(d["wins"] / d["total"] * 100, 1) if d["total"] else 0
        lines.append(
            f"  {mkt:<16} {d['total']:>3} trades  "
            f"WR {wr:>5.1f}%  PnL ${d['pnl']:>+8.2f}"
        )

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"  ⏰ Généré le {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M UTC')}",
        "  🤖 SMC Signal Engine v7 — Backtester",
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
#  ENVOI TELEGRAM
# ─────────────────────────────────────────────────────────────

def tg_send_report(text: str, token: str, chat_id: str) -> bool:
    """Envoie le rapport en plusieurs messages si trop long (limite 4096 chars)."""
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    MAX  = 4000
    chunks = [text[i:i+MAX] for i in range(0, len(text), MAX)]
    ok = True
    for chunk in chunks:
        try:
            r = requests.post(url, json={
                "chat_id": chat_id, "text": f"<pre>{chunk}</pre>",
                "parse_mode": "HTML", "disable_web_page_preview": True,
            }, timeout=15)
            if r.status_code != 200:
                print(f"  [TG] HTTP {r.status_code} : {r.text[:200]}")
                ok = False
        except Exception as e:
            print(f"  [TG] Erreur : {e}")
            ok = False
        time.sleep(0.3)
    return ok


# ─────────────────────────────────────────────────────────────
#  EXPORT CSV
# ─────────────────────────────────────────────────────────────

def export_csv(trades: list[BacktestTrade], path: str = "backtest_trades.csv") -> None:
    rows = []
    for t in trades:
        rows.append({
            "timestamp": str(t.timestamp),
            "symbol":    t.symbol,
            "market":    t.market,
            "setup":     t.setup,
            "direction": t.direction,
            "entry":     t.entry,
            "sl":        t.sl,
            "tp1":       t.tp1,
            "tp2":       t.tp2,
            "rr":        t.rr,
            "score":     t.score,
            "result":    t.result,
            "pnl_usd":   t.pnl_usd,
            "bars_held": t.bars_held,
        })
    pd.DataFrame(rows).to_csv(path, index=False)
    print(f"  💾 CSV exporté : {path}")


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Backtest historique SMC v7 — 3 mois",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--months",    type=int,   default=3,
                        help="Nombre de mois à tester (défaut: 3)")
    parser.add_argument("--cat",       default="all",
                        choices=["priority","forex","all"],
                        help="Catégorie de marchés")
    parser.add_argument("--min-score", type=int,   default=SCORE_THRESHOLD,
                        help=f"Score minimum (défaut: {SCORE_THRESHOLD})")
    parser.add_argument("--step",      type=int,   default=4,
                        help="Pas en bougies M15 entre chaque évaluation (défaut: 4 = ~1h)")
    parser.add_argument("--csv",       action="store_true",
                        help="Exporter les trades en CSV")
    parser.add_argument("--no-tg",     action="store_true",
                        help="Ne pas envoyer sur Telegram (affiche uniquement en console)")
    args = parser.parse_args()

    tg_token   = os.environ.get("TG_TOKEN", "")
    tg_chat_id = os.environ.get("TG_CHAT_ID", "")

    if not tg_token and not args.no_tg:
        print("\n  ⚠  TG_TOKEN absent — ajoute: export TG_TOKEN='ton_token'")
        print("      Ou lance avec --no-tg pour afficher uniquement en console.\n")

    symbols = MARKET_LISTS.get(args.cat, MARKET_LISTS["all"])

    print("\n  ══════════════════════════════════════════════")
    print("  📊  SMC Signal Engine v7 — Backtest historique")
    print("  ══════════════════════════════════════════════")

    trades = run_backtest(
        symbols   = symbols,
        months    = args.months,
        min_score = args.min_score,
        step_bars = args.step,
    )

    stats  = compute_stats(trades)
    report = format_report(stats, args.months, args.cat)

    print("\n" + report)

    if args.csv:
        export_csv(trades, f"backtest_{args.cat}_{args.months}mois.csv")

    if not args.no_tg and tg_token and tg_chat_id:
        print(f"\n  📤 Envoi du rapport sur Telegram (chat_id: {tg_chat_id}) …")
        ok = tg_send_report(report, tg_token, tg_chat_id)
        print(f"  {'✅ Rapport envoyé' if ok else '❌ Échec envoi Telegram'}")
    elif not args.no_tg and tg_token and not tg_chat_id:
        print("\n  ⚠  TG_CHAT_ID absent — ajoute: export TG_CHAT_ID='-100xxxxxxxx'")


if __name__ == "__main__":
    main()
