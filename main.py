

"""
╔══════════════════════════════════════════════════════════════════════════╗
║       ALPHABOT — SMC/ICT SIGNAL ENGINE  v13 — STRATÉGIE UNIQUE          ║
║                                                                          ║
║  STRATÉGIE (unique, LONG et SHORT) :                                    ║
║  ✦ Sweep de liquidité + Cassure de structure (BOS + CHoCH) M15,        ║
║    ancrée sur une zone H4 stratégique (OB H4 / Breaker H4 / Balance)   ║
║  ✦ Entrée directe (Sweep + CHoCH confirmés)      → signal  ⭐⭐          ║
║  ✦ Entrée confirmée (+ retest OB/FVG avec bougie                       ║
║    de rejet dans les 6 dernières bougies M5)      → signal  ⭐⭐⭐        ║
║  ✦ RR ≥ 3.0 imposé, score minimum SCORE_THRESHOLD (78/100)             ║
║                                                                          ║
║  MARCHÉS SCANNÉS (4, aucun autre) :                                    ║
║  ✦ Gold (XAUUSD) et BTC-USD   → session New York UNIQUEMENT            ║
║    (13h00–22h00 UTC)                                                    ║
║  ✦ Volatility 75 Index (R_75) et Volatility 25 Index (R_25)            ║
║    → 24h/24 (indices synthétiques Deriv)                                ║
║                                                                          ║
║  SORTIE / TP :                                                          ║
║  ✦ TP1 = RR3 minimum · TP2 = RR5 · TP3 = RR6+                          ║
║  ✦ À TP1 (RR3) : clôture totale possible, ou option de laisser courir  ║
║    vers TP2/TP3 avec SL remonté à l'entrée (break-even)                ║
║                                                                          ║
║  GESTION DU RISQUE :                                                    ║
║  ✦ RISQUE EN % DU CAPITAL : ACCOUNT_BALANCE_USD + RISK_PERCENT_PER_TRADE║
║    configurables via variables d'env (0.65% par défaut → $65 sur $10k) ║
║    RISK_USD calculé dynamiquement, lot calculé automatiquement          ║
║  ✦ Ajustement dynamique du risque selon la série de résultats récents  ║
║                                                                          ║
║  TELEGRAM :                                                             ║
║  ✦ 2 groupes séparés : Gold+BTC (TG_GROUP_GOLD_BTC_ID) et              ║
║    Volatility 75/25 (TG_GROUP_DERIV_ID)                                 ║
║  ✦ Chaque signal et chaque suivi de position (TP/SL) est envoyé à la   ║
║    fois dans le groupe concerné ET en privé à l'admin (TG_LEADER_ID)   ║
║                                                                          ║
║  INFRASTRUCTURE (héritée, inchangée) :                                  ║
║  ✦ FETCH DUAL-MODE : yfinance (défaut) + _yahoo_chart_json() sans       ║
║    dépendance (fallback requests pur — compatible Pydroid/Android)      ║
║  ✦ FLASK OPTIONNEL : import protégé par try/except (pas de crash si    ║
║    flask absent sur Pydroid)                                            ║
║  ✦ Corrélation guard, filtre news, filtre volatilité, limites          ║
║    anti-sur-trading (par symbole / globale / session / Deriv)          ║
╚══════════════════════════════════════════════════════════════════════════╝

Installation :
    pip install yfinance pandas numpy colorama flask requests websocket-client
    (yfinance et flask sont optionnels — fallback requests pur / sans dashboard web)

Usage :
    python main.py                    # scan complet live (Gold + BTC + V75 + V25)
    python main.py --cat gold         # Gold seulement
    python main.py --cat btc          # BTC seulement
    python main.py --cat deriv        # Volatility 75 + 25 seulement
    python main.py --symbol BTC-USD   # symbole unique
    python main.py --scan             # scan unique (test)
    python main.py --min-score 80     # filtre score
"""

import argparse
import threading
import time
import os
import json
import sqlite3
import uuid
import requests
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

import logging
import sys

import numpy as np
import pandas as pd

# ── .env — charge les variables d'environnement depuis un fichier .env situé
# à côté de ce script, si présent (sinon les variables d'env système/du
# shell/de la plateforme de déploiement s'appliquent normalement). Optionnel :
# si python-dotenv n'est pas installé, on continue sans planter.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

# ── yfinance — optionnel (fallback _yahoo_chart_json si absent) ───────────
try:
    import yfinance as yf
    _HAS_YFINANCE = True
except ImportError:
    yf = None
    _HAS_YFINANCE = False

# ── Flask — serveur HTTP pour Render (optionnel — pas nécessaire sur Pydroid) ──
try:
    from flask import Flask, jsonify
    flask_app = Flask(__name__)
    _HAS_FLASK = True
except ImportError:
    Flask = None
    jsonify = None
    flask_app = None
    _HAS_FLASK = False

_STATUS: dict = {
    "started_at"   : None,
    "last_scan"    : None,
    "cycle"        : 0,
    "symbols_count": 0,
    "last_signals" : [],
    "scan_running" : False,
}
_STATUS_LOCK = threading.Lock()


# Guard : si Flask absent, les décorateurs de route sont des no-ops
_route = flask_app.route if _HAS_FLASK and flask_app else (lambda *a, **k: (lambda f: f))

@_route("/")
def index():
    with _STATUS_LOCK:
        st = dict(_STATUS)

    signals_html = ""
    for s in reversed(st["last_signals"][-15:]):
        color    = "#e74c3c" if s["direction"] == "SHORT" else "#2ecc71"
        mode_col = {"AMD": "#9b59b6", "PRE-BOS": "#f39c12",
                    "SMC": "#58a6ff", "SWEEP_SHIFT": "#e67e22",
                    "CHOCH_LIQ": "#1abc9c", "BREAKER_HTF": "#e74c3c",
                    "OFS": "#27ae60", "SMC_TRADER": "#f1c40f",
                    "ETE_M15": "#ff4081", "BOS_RETEST": "#00bcd4",
                    "SWEEP_BOS_M15": "#c0392b",
                    "BREAKER": "#e74c3c", "SD": "#f39c12",
                    "OB": "#9b59b6", "BOS": "#00bcd4",
                    "MSS": "#1abc9c", "FVG": "#3498db",
                    }.get(s.get("mode", "SMC"), "#58a6ff")
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
  <title>SMC Signal Engine v10</title>
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
  <h1>⚡ SMC Signal Engine v10 — Breaker · S/D Zone · OB · BOS · MSS · FVG · AMD</h1>
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
  <h2>⚙️ Configuration v13 — Stratégie unique (Sweep + Cassure de structure)</h2>
  <table>
    <tr><th>Paramètre</th><th>Valeur</th></tr>
    <tr><td>Stratégie</td><td>Sweep de liquidité + Cassure de structure (BOS/CHoCH), LONG et SHORT</td></tr>
    <tr><td>Entrée directe</td><td>⭐⭐ Sweep + CHoCH confirmés → signal immédiat</td></tr>
    <tr><td>Entrée confirmée</td><td>⭐⭐⭐ + retour retester l'Order Block / FVG avec bougie de rejet</td></tr>
    <tr><td>Score minimum</td><td>{SCORE_THRESHOLD}/100</td></tr>
    <tr><td>RR minimum</td><td>1:{MIN_RR}</td></tr>
    <tr><td>Risque/trade</td><td>{RISK_PERCENT_PER_TRADE}% (${RISK_USD:.2f} sur ${ACCOUNT_BALANCE_USD:,.0f})</td></tr>
    <tr><td>Timeframes</td><td>H4 → M15 → M5</td></tr>
    <tr><td>Max signaux/jour</td><td>{MAX_SIGNALS_GLOBAL_PER_DAY} (reset 00h00 UTC)</td></tr>
    <tr><td>Gold + BTC</td><td>🇺🇸 Scan restreint à la session New York (13h00–22h00 UTC)</td></tr>
    <tr><td>Volatility 75 / 25</td><td>🌐 Scan 24h/24 (indices synthétiques)</td></tr>
    <tr><td>Groupes Telegram</td><td>Gold+BTC et Volatility 75/25 → 2 groupes séparés</td></tr>
    <tr><td>Intervalle scan</td><td>5 minutes</td></tr>
  </table>
</body>
</html>"""
    return html


@_route("/stats")
def stats_json():
    """Journal statistique — résumé JSON des performances."""
    from collections import defaultdict
    stats  = get_signal_stats(500)
    closed = [s for s in stats if s["result"] != "open"]
    open_t = [s for s in stats if s["result"] == "open"]

    by_setup: dict = defaultdict(list)
    by_sym:   dict = defaultdict(list)
    by_hour:  dict = defaultdict(list)

    for s in closed:
        by_setup[s["setup_type"]].append(s["pnl_r"])
        by_sym[s["symbol"]].append(s["pnl_r"])
        by_hour[s["hour_utc"]].append(s["pnl_r"])

    def _summary(d):
        return {
            k: {
                "trades": len(v),
                "winrate": round(sum(1 for r in v if r > 0) / len(v) * 100, 1) if v else 0,
                "avg_r":  round(sum(v) / len(v), 2) if v else 0,
                "total_r": round(sum(v), 2),
            }
            for k, v in sorted(d.items(), key=lambda x: -sum(x[1]))
        }

    return jsonify({
        "total_closed": len(closed),
        "total_open":   len(open_t),
        "winrate_pct":  round(sum(1 for s in closed if s["pnl_r"] > 0) / max(len(closed), 1) * 100, 1),
        "avg_r":        round(sum(s["pnl_r"] for s in closed) / max(len(closed), 1), 2),
        "total_r":      round(sum(s["pnl_r"] for s in closed), 2),
        "by_setup":     _summary(by_setup),
        "by_symbol":    _summary(by_sym),
        "by_hour_utc":  _summary(by_hour),
        "recent":       stats[:20],
    })


@_route("/status")
def status_json():
    with _STATUS_LOCK:
        return jsonify(_STATUS)


def start_flask(port: int = 10000) -> None:
    if not _HAS_FLASK or flask_app is None:
        print("  [FLASK] Flask non disponible — dashboard désactivé")
        return
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
# [v9.6 — quality tuning] Relevé de 74 → 82 à la demande de Noël :
# moins de signaux, mais on ne laisse plus passer les setups "moyens".
# Combiné à MIN_RR=3.0 et MAX_SIGNALS_GLOBAL_PER_DAY réduit, l'objectif
# est "peu de signaux, mais qui tiennent la route" plutôt que la quantité.
SCORE_THRESHOLD = 78
MIN_RR          = 3.0

# ── [Cahier des charges] SHORT UNIQUEMENT ──────────────────────
# True = le bot ignore tout signal LONG et ne scanne/n'envoie que des SHORT.
SHORT_ONLY = os.environ.get("SHORT_ONLY", "true").lower() != "false"

# ── [v15] TIMEFRAME D'EXÉCUTION ─────────────────────────────────
# "M5" = sweep, validation de corps de bougie et CHoCH/BOS évalués sur
# M5 uniquement (zone stratégique H4 conservée comme ancrage). "M15"
# = ancien comportement.
EXEC_TIMEFRAME = os.environ.get("EXEC_TIMEFRAME", "M5").upper()

# ── [v8.5/v10] GESTION DU RISQUE EN % DU CAPITAL ──────────────────────────
# Risque par trade = 0.65% de la taille du compte (configurable via env).
# ACCOUNT_BALANCE_USD peut être mis à jour sans toucher au code (dépôt/retrait).
# RISK_USD est recalculé dynamiquement et utilisé partout en aval.
ACCOUNT_BALANCE_USD     = float(os.environ.get("ACCOUNT_BALANCE_USD", "10000"))
RISK_PERCENT_PER_TRADE  = 0.65   # 0.65% du capital par trade
RISK_USD = round(ACCOUNT_BALANCE_USD * RISK_PERCENT_PER_TRADE / 100.0, 2)

# ── [v11] RISQUE PROGRESSIF (réduction après perte / remontée après gain) ──
# Après un Stop Loss touché : le risque du prochain trade est divisé par 2.
# En cas de pertes consécutives, la réduction continue (÷2, ÷4, ÷8...) avec
# un plancher pour ne jamais couper le risque à zéro.
# Après un gain validé (TP touché) : le risque remonte progressivement
# (×1.5) vers le risque plein (100%), sans jamais dépasser RISK_USD.
RISK_MIN_MULTIPLIER   = 0.125   # plancher : jamais moins de 12.5% du risque de base
RISK_RECOVERY_FACTOR  = 1.5     # facteur de remontée après un gain validé
RISK_LOOKBACK_TRADES  = 30      # profondeur d'historique rejouée


def get_dynamic_risk_multiplier(lookback: int = RISK_LOOKBACK_TRADES) -> float:
    """
    Calcule le multiplicateur de risque courant en rejouant l'historique
    des derniers trades clôturés (table signal_stats) :
      • Perte (pnl_r ≤ 0)  → multiplicateur ÷ 2   (plancher RISK_MIN_MULTIPLIER)
      • Gain  (pnl_r > 0)  → multiplicateur × 1.5 (plafond 1.0 = risque plein)

    Sans historique (premier lancement, DB absente) → 1.0 (risque plein).
    Ne lève jamais d'exception : retourne 1.0 en cas d'erreur DB.
    """
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.row_factory = sqlite3.Row
        rows = con.execute("""
            SELECT pnl_r FROM signal_stats
            WHERE result != 'open'
            ORDER BY timestamp ASC
        """).fetchall()
        con.close()
    except Exception as e:
        print(f"  [RISK] Erreur lecture historique — risque plein par défaut : {e}")
        return 1.0

    if not rows:
        return 1.0

    recent = rows[-lookback:] if len(rows) > lookback else rows

    mult = 1.0
    for r in recent:
        if r["pnl_r"] <= 0:
            mult = max(mult * 0.5, RISK_MIN_MULTIPLIER)
        else:
            mult = min(mult * RISK_RECOVERY_FACTOR, 1.0)
    return round(mult, 4)


def get_current_risk_usd() -> float:
    """RISK_USD ajusté dynamiquement selon la série de résultats récents (v11),
    sauf si le Recovery a été désactivé via le menu Telegram (v14)."""
    base = get_base_risk_usd()
    if not is_recovery_enabled():
        return base
    mult = get_dynamic_risk_multiplier()
    adjusted = round(base * mult, 2)
    if mult < 1.0:
        print(c(f"  [RISK] Risque réduit à {int(mult*100)}% suite aux pertes récentes "
                f"→ ${adjusted} (au lieu de ${base})", "yellow"))
    return adjusted


# ═════════════════════════════════════════════════════════════
#  [v14] PARAMÈTRES CONFIGURABLES VIA TELEGRAM
#  Solde, risque ($ ou %), levier, objectif de profit, recovery
#  → stockés en SQLite (persistent, survit aux redéploiements),
#  avec les constantes ci-dessus comme valeurs par défaut.
# ═════════════════════════════════════════════════════════════

_SETTINGS_DEFAULTS = {
    "account_balance":  str(ACCOUNT_BALANCE_USD),
    "risk_mode":        "percent",              # "percent" ou "fixed"
    "risk_value":       str(RISK_PERCENT_PER_TRADE),  # % si mode percent, $ si mode fixed
    "leverage":         "100",                  # levier ex: 1:100
    "profit_target":    "0",                    # objectif de profit $ (0 = désactivé)
    "recovery_enabled": "1",                    # "1" = recovery actif (défaut), "0" = désactivé
    "max_volume":       "0",                    # lot max autorisé par trade (0 = pas de plafond)
    "timeframe":        EXEC_TIMEFRAME,          # "M5" ou "M15"
    "session_mode":     "NY",                   # "NY" (13h-22h UTC) ou "24H"
    "active_markets":   "all",                  # "all" | "gold_btc" | "deriv"
    "profile_label":    "",                     # nom/étiquette libre du profil
    "prop_firm_mode":   "0",                    # "1" = règles Prop Firm actives
    "atr_sl_mult":      "0.6",                  # multiplicateur ATR pour le Stop Loss
}


def _settings_init() -> None:
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        con.commit()
        con.close()
    except Exception as e:
        print(f"  [SETTINGS] Init erreur : {e}")


def get_setting(key: str, default: Optional[str] = None) -> str:
    fallback = default if default is not None else _SETTINGS_DEFAULTS.get(key, "")
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)).fetchone()
        con.close()
        return row["value"] if row else fallback
    except Exception:
        return fallback


def set_setting(key: str, value: str) -> None:
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.execute(
            "INSERT INTO bot_settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )
        con.commit()
        con.close()
    except Exception as e:
        print(f"  [SETTINGS] Erreur écriture {key} : {e}")


def get_account_balance() -> float:
    try:
        return float(get_setting("account_balance"))
    except Exception:
        return ACCOUNT_BALANCE_USD


def get_risk_config() -> tuple:
    """Retourne (mode, value) — mode = 'percent' ou 'fixed'."""
    mode  = get_setting("risk_mode")
    value = get_setting("risk_value")
    try:
        return mode, float(value)
    except Exception:
        return "percent", RISK_PERCENT_PER_TRADE


def get_base_risk_usd() -> float:
    """Risque $ de base par trade, calculé depuis les paramètres configurés
    (solde + mode de risque), avant ajustement recovery."""
    balance = get_account_balance()
    mode, value = get_risk_config()
    if mode == "fixed":
        return round(value, 2)
    return round(balance * value / 100.0, 2)


def get_leverage() -> int:
    try:
        return int(float(get_setting("leverage")))
    except Exception:
        return 100


def get_profit_target() -> float:
    try:
        return float(get_setting("profit_target"))
    except Exception:
        return 0.0


def is_recovery_enabled() -> bool:
    return get_setting("recovery_enabled") == "1"


def get_max_volume() -> float:
    try:
        return float(get_setting("max_volume"))
    except Exception:
        return 0.0


def get_timeframe_setting() -> str:
    return get_setting("timeframe") or EXEC_TIMEFRAME


def get_session_mode() -> str:
    return get_setting("session_mode") or "NY"


def get_active_markets_setting() -> str:
    return get_setting("active_markets") or "all"


def get_profile_label() -> str:
    return get_setting("profile_label") or "—"


def is_prop_firm_mode() -> bool:
    return get_setting("prop_firm_mode") == "1"


def get_atr_sl_mult() -> float:
    """Multiplicateur ATR pour le Stop Loss (défaut 0.6, configurable via menu)."""
    try:
        return float(get_setting("atr_sl_mult"))
    except Exception:
        return 0.6


# ── Septuple Traction : N bougies consécutives minimum ───────
SEPTUPLE_MIN_CANDLES = 5   # 5 suffisent en practice (7 = très rare)

# ── AMD Phase Detection ────────────────────────────────────
AMD_LOOKBACK = 30   # FIX v3.1 : 30 bougies H4 suffisent (≈5 jours), 50 causait "unknown" sur instruments peu fournis

# ── Supply & Demand Zones ─────────────────────────────────
SD_MIN_IMPULSE_RATIO = 1.5  # corps bougie ≥ 1.5× ATR pour qualifier une zone S/D
SD_ZONE_BUFFER       = 0.15  # tolérance 15% de l'ATR pour "dans la zone"

# ─────────────────────────────────────────────────────────────
#  KILL ZONES SMC — ICT (UTC)
#
#  Seules ces fenêtres horaires sont autorisées pour l'envoi
#  de signaux. En dehors → scan ignoré, aucun signal envoyé.
#
#  v8.2 : recalées sur l'analyse volatilité réelle (GMT+0) :
#  ┌─────────────────────────────────────────────────────────┐
#  │  08h00–11h00 UTC  — London Open  ⭐ volatilité forte     │
#  │  13h00–22h00 UTC  — NY Open / overlap London ⭐⭐         │
#  │  Gold et BTC → réservés à la fenêtre NY Open uniquement  │
#  │  Tout le reste (22h-08h notamment) = faible volatilité,  │
#  │  bloqué pour éviter les SL inutiles                      │
#  └─────────────────────────────────────────────────────────┘
# ─────────────────────────────────────────────────────────────

# Kill zones exprimées en MINUTES depuis minuit UTC (permet les demi-heures, ex: 13h30)
LONDON_KZ_MIN: tuple[int, int] = (8 * 60,       11 * 60)        # 08h00–11h00 UTC
NY_KZ_MIN:     tuple[int, int] = (13 * 60,      22 * 60)        # 13h00–22h00 UTC [v15]
ASIAN_KZ_MIN:  tuple[int, int] = (0,            3 * 60)         # 00h00–03h00 UTC

# Kill zones principales (toutes paires, sauf indices US — voir is_kill_zone_active)
KILL_ZONES_UTC: list[tuple[int, int]] = [LONDON_KZ_MIN, NY_KZ_MIN]

# Conservé pour compatibilité nominale (anciennement en heures pleines)
ASIAN_KILL_ZONE_UTC: tuple[int, int] = ASIAN_KZ_MIN

# Compatibilité : SESSION_WINDOWS_UTC conservé pour les autres checks (en minutes désormais)
# [v11] Restreint à Londres + New York UNIQUEMENT (demande utilisateur) —
# la fenêtre asiatique est retirée du gate global, y compris pour JPY/AUD/NZD.
SESSION_WINDOWS_UTC: list[tuple[int, int]] = KILL_ZONES_UTC


US_INDEX_SYMBOLS: set[str] = set()   # indices US supprimés — plus aucun marché indice

# ═════════════════════════════════════════════════════════════
#  FILTRE NEWS ÉCONOMIQUES ⭐⭐⭐⭐⭐
#
#  Bloque les signaux 30 min AVANT et 30 min APRÈS une news
#  à impact FORT sur les devises majeures (USD, EUR, GBP, JPY).
#
#  Source : ForexFactory JSON feed (public, sans clé API).
#  Fallback silencieux si réseau indisponible (pas de blocage).
#
#  Currencies surveillées :
#    USD, EUR, GBP, JPY — les plus impactantes en SMC
#
#  Impact bloqué : "High" uniquement (rouge sur ForexFactory).
#  "Medium" et "Low" sont autorisés.
# ═════════════════════════════════════════════════════════════

NEWS_CURRENCIES_BLOCKED = {"USD", "EUR", "GBP", "JPY"}
# [v9 MOD-4] Fenêtres news asymétriques :
#   → AVANT la news : 30 min de blocage (inchangé — trop dangereux de trader juste avant)
#   → APRÈS la news : 10 min seulement (était 30 min)
#     Raison : les sweeps de liquidité et structures SMC valides se forment
#     dès les premières minutes post-news. Bloquer 30 min faisait rater ces setups.
NEWS_WINDOW_BEFORE      = 30      # blocage 30 min AVANT la news
NEWS_WINDOW_AFTER       = 10      # [v9 MOD-4] blocage 10 min seulement APRÈS la news (était 30)
NEWS_WINDOW_MINUTES     = 30      # conservé pour compatibilité interne (utilisé dans l'ancienne logique)
_news_cache: dict       = {}      # {date_str: [list of news dicts]}
_news_cache_ts: float   = 0.0
NEWS_CACHE_TTL          = 3600    # rafraîchissement toutes les heures

# Mapping symbole → devises concernées (seuls les 4 marchés actifs)
_SYMBOL_CURRENCIES: dict[str, set] = {
    "GC=F":     {"USD"},
    "BTC-USD":  {"USD"},
}


def _fetch_forex_news() -> list[dict]:
    """
    Télécharge les news du jour depuis ForexFactory (JSON public).
    Retourne une liste de dicts {time_utc: datetime, currency: str, impact: str}.
    Silencieux en cas d'erreur réseau.
    """
    global _news_cache, _news_cache_ts
    now_ts = time.time()

    if now_ts - _news_cache_ts < NEWS_CACHE_TTL and _news_cache:
        return list(_news_cache.get("events", []))

    try:
        today = datetime.now(timezone.utc).strftime("%b%d.%Y").lower()
        url   = f"https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        r     = requests.get(url, timeout=8)
        if r.status_code != 200:
            return []

        raw    = r.json()
        events = []
        for ev in raw:
            impact   = ev.get("impact", "").strip().lower()
            currency = ev.get("country", "").strip().upper()
            if impact not in ("high",):   # on ne bloque que les rouges
                continue
            if currency not in NEWS_CURRENCIES_BLOCKED:
                continue
            date_str = ev.get("date", "")
            time_str = ev.get("time", "")
            try:
                dt_naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %I:%M%p")
                dt_utc   = dt_naive.replace(tzinfo=timezone.utc)
                events.append({"time_utc": dt_utc, "currency": currency, "title": ev.get("title", "")})
            except Exception:
                continue

        _news_cache    = {"events": events}
        _news_cache_ts = now_ts
        return events

    except Exception as e:
        log.debug(f"  [NEWS] Fetch échoué : {e}")
        return []


def is_news_blackout(symbol: str) -> tuple[bool, str]:
    """
    Retourne (True, raison) si le symbole est dans une fenêtre de news forte.
    Retourne (False, "") si le marché est libre.
    """
    currencies = _SYMBOL_CURRENCIES.get(symbol, set())
    if not currencies:
        return False, ""

    relevant_currencies = currencies & NEWS_CURRENCIES_BLOCKED
    if not relevant_currencies:
        return False, ""

    now_utc = datetime.now(timezone.utc)
    events  = _fetch_forex_news()

    for ev in events:
        if ev["currency"] not in relevant_currencies:
            continue
        delta = (ev["time_utc"] - now_utc).total_seconds() / 60.0
        # [v9 MOD-4] Fenêtres asymétriques :
        #   delta > 0  → news dans le futur  → on bloque NEWS_WINDOW_BEFORE min avant
        #   delta < 0  → news passée         → on bloque NEWS_WINDOW_AFTER  min après
        window_blocked = (-NEWS_WINDOW_AFTER <= delta <= NEWS_WINDOW_BEFORE)
        if window_blocked:
            sign  = "dans" if delta >= 0 else "il y a"
            mins  = abs(int(delta))
            return True, (
                f"🚫 News {ev['currency']} impact FORT : \"{ev['title']}\" "
                f"({sign} {mins} min)"
            )

    return False, ""

def is_us_market_open() -> bool:
    """
    Vérifie si le marché US est ouvert (NYSE/NASDAQ).
    Heures : 13h30–20h00 UTC (9h30–16h00 ET), lundi–vendredi.
    Hors ces heures → pas de données 15m disponibles sur Yahoo Finance.
    """
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:   # weekend
        return False
    return 13 <= now.hour < 20


def is_session_active() -> bool:
    """Compatibilité — retourne True si au moins une kill zone est active."""
    now_min = datetime.now(timezone.utc).hour * 60 + datetime.now(timezone.utc).minute
    return any(start <= now_min < end for start, end in SESSION_WINDOWS_UTC)


def is_ny_session_active() -> bool:
    """Session NY Open uniquement (13h00-22h00 UTC).
    Utilisée pour restreindre Gold et BTC à la seule fenêtre New York
    (demande utilisateur) — Londres n'est plus autorisée pour ces 2 actifs."""
    now_min = datetime.now(timezone.utc).hour * 60 + datetime.now(timezone.utc).minute
    ny_start, ny_end = NY_KZ_MIN
    return ny_start <= now_min < ny_end


def is_kill_zone_active(symbol: str = "") -> tuple[bool, str]:
    """
    Vérifie si l'heure actuelle est dans une Kill Zone autorisée pour ce symbole.

    Règles [v12 — 4 marchés uniquement] :
      • Indices Deriv (Volatility 75 / Volatility 25) → 24h/24, AUCUN filtre
        de session (flux synthétique continu, pas de carnet d'ordres réel).
      • Gold (XAUUSD) et BTC → UNIQUEMENT la fenêtre NY Open
        (13h00-22h00 UTC). Londres n'est plus autorisée pour ces 2 actifs.
      • Tout autre symbole (legacy) → Londres (08h-11h) + NY (13h30-16h)
        UTC, conservé pour compatibilité mais non utilisé (plus aucun
        Forex/indice n'est scanné).

    Retourne (True, nom_session) ou (False, raison_blocage).
    """
    now      = datetime.now(timezone.utc)
    now_min  = now.hour * 60 + now.minute
    hh_mm    = now.strftime("%Hh%M")

    london_start, london_end = LONDON_KZ_MIN
    ny_start, ny_end         = NY_KZ_MIN

    # Indices Deriv (Volatility 75/25) — 24h/24, aucun filtre de session
    if is_deriv_symbol(symbol):
        return True, "🌐 Deriv — 24h/24 (aucun filtre de session)"

    # [v15] Mode session configurable via le menu Telegram (🌎 Session) :
    # "24H" désactive la fenêtre NY pour Gold/BTC aussi (scan continu).
    if get_session_mode() == "24H":
        return True, "🌐 Mode 24H (session filter désactivé via menu)"

    # Gold + BTC — réservés à la session New York uniquement
    if symbol in GOLD_SYMBOLS or is_crypto_symbol(symbol):
        if ny_start <= now_min < ny_end:
            return True, "🇺🇸 NY Open KZ (13h00-22h00 UTC)"
        return False, f"⛔ Gold/BTC — hors fenêtre NY Open (13h00-22h00 UTC), actuellement {hh_mm}"

    # Indices US — alignés UNIQUEMENT sur l'ouverture NY (pas de fenêtre Londres)
    if symbol in US_INDEX_SYMBOLS:
        if ny_start <= now_min < ny_end:
            return True, "🇺🇸 NY Open KZ (13h00-22h00 UTC)"
        return False, f"⛔ Indice US — hors fenêtre NY Open (13h00-22h00 UTC), actuellement {hh_mm}"

    # Legacy (Forex — non utilisé, plus aucune paire scannée)
    if london_start <= now_min < london_end:
        return True, "🇬🇧 London Open KZ (08h00-11h00 UTC)"
    if ny_start <= now_min < ny_end:
        return True, "🇺🇸 NY Open KZ (13h00-22h00 UTC)"

    return False, f"⛔ {hh_mm} UTC — hors Londres/New York, aucun signal envoyé"


def is_weekend() -> bool:
    """Retourne True si on est samedi ou dimanche (UTC)."""
    return datetime.now(timezone.utc).weekday() >= 5   # 5=Sat, 6=Sun


def is_crypto_symbol(symbol: str) -> bool:
    """BTC trade 24/7, y compris le weekend."""
    return symbol == "BTC-USD"

# v8 : BTC — on bloque les signaux SELL/SHORT sur BTC (tendance haussière forte)
BTC_SELL_BLOCKED = False

GOLD_SYMBOLS = {"GC=F"}

# [v11] Gold + BTC uniquement : pipeline Supply/Demand dédié M15(zone) → M1(entrée)
GOLD_BTC_M1_SYMBOLS = {"GC=F", "BTC-USD"}

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
    "GC=F"    : 1.20,    # Gold
    "BTC-USD" : 150.0,   # Bitcoin
}
ATR_MIN_DEFAULT = 0.00050
MAX_SPREAD_ATR_RATIO = 0.50   # élargi à 50% pour réduire les faux rejets de spread


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
        elif is_deriv_symbol(symbol) and close > 0:
            # Pas de référence ATR_MIN fixe pertinente (échelle de prix
            # variable selon l'indice) → seuil relatif au prix courant.
            atr_min = close * 0.0008
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

    # ── 2. FILTRE VOLUME — [v9 MOD-3] ────────────────────────
    # Forex & Gold : filtre volume DÉSACTIVÉ.
    # Les données de volume yfinance sont fragmentées sur le Forex (tick volume
    # partiel, souvent nul ou incohérent). Ce filtre rejetait d'excellents setups
    # SMC valides. Sur Forex/Gold, on se fie uniquement à l'ATR et la structure.
    #
    # Crypto (BTC, ETH) : filtre conservé mais abaissé à 0.50 (50% de la moyenne)
    # car les volumes crypto sont réels et disponibles en continu.
    if "volume" in df_ltf.columns and len(df_ltf) >= 21 and is_crypto_symbol(symbol):
        vol_now  = df_ltf["volume"].iloc[-1]
        vol_mean = df_ltf["volume"].rolling(20).mean().iloc[-1]
        if not pd.isna(vol_now) and not pd.isna(vol_mean) and vol_mean > 0:
            vol_ratio = vol_now / vol_mean
            # [v9 MOD-3] Seuil abaissé à 0.50 pour crypto (était 0.80 pour tout)
            if vol_ratio < 0.50:
                return False, f"volume crypto faible ({round(vol_ratio*100,0)}% de la moyenne 20)"

    # BTC — restreint à la seule session New York (demande utilisateur,
    # Londres n'est plus autorisée pour BTC désormais).
    if is_crypto_symbol(symbol):
        if not is_ny_session_active():
            return False, "hors session NY Open (13h00-22h00 UTC) — BTC réservé à NY"
        return True, ""
    # Indices Deriv (Volatility) : flux synthétique 24/7, pas de session ni
    # de kill zone réelle (pas de carnet d'ordres interbancaire derrière).
    if is_deriv_symbol(symbol):
        return True, ""
    # Gold : ouvert dim 23h-ven, mais restreint à la session NY uniquement
    # (demande utilisateur — Londres n'est plus autorisée pour Gold).
    if symbol in GOLD_SYMBOLS:
        if not is_gold_session_active():
            return False, "weekend — Gold fermé (sam + dim avant 23h UTC)"
        if not is_ny_session_active():
            return False, "hors session NY Open (13h00-22h00 UTC) — Gold réservé à NY"
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
    "GC=F"    : 0.30,    # Gold
    "BTC-USD" : 15.0,    # Bitcoin
}


def get_spread(symbol: str) -> float:
    return SPREAD_TABLE.get(symbol, 0.00015)


# ─────────────────────────────────────────────────────────────
#  CORRÉLATION GUARD
# ─────────────────────────────────────────────────────────────
_CORR_GROUPS: dict[str, str] = {
    "GC=F"    : "GOLD",
    "BTC-USD" : "BTC",
}

_active_corr_groups: dict[str, float] = {}
CORR_TTL = 600


def correlation_guard_reset() -> None:
    """Conservé pour compatibilité — ne fait plus rien (v9.6).
    Le guard interroge désormais active_trades en direct, donc il n'y a
    plus d'état en mémoire à réinitialiser entre les cycles."""
    pass


def correlation_guard(symbol: str, direction: str) -> tuple[bool, str]:
    """v9.6 — FIX : le guard interroge la table active_trades (trades
    réellement ouverts en DB) au lieu d'un TTL mémoire de 10 min.

    Bug v9.5 : correlation_guard_reset() était appelé à CHAQUE cycle de
    scan (toutes les 5 min), ce qui vidait le verrou avant même que le
    CORR_TTL (600s) n'expire. Résultat : aucune protection réelle au-delà
    d'un seul cycle — plusieurs paires corrélées (JPY, USD, GBP...)
    pouvaient être ouvertes en même temps et sauter ensemble sur leur SL.

    Désormais : un nouveau signal est bloqué si un trade encore OUVERT
    (closed=0) existe sur le même groupe de corrélation, dans la même
    direction — peu importe depuis combien de temps il est ouvert.
    """
    group = _CORR_GROUPS.get(symbol)
    if group is None:
        return True, ""

    same_group_symbols = [s for s, g in _CORR_GROUPS.items() if g == group]
    if not same_group_symbols:
        return True, ""

    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        placeholders = ",".join("?" for _ in same_group_symbols)
        row = con.execute(
            f"""SELECT trade_id, symbol FROM active_trades
                WHERE closed = 0 AND direction = ?
                AND symbol IN ({placeholders})
                LIMIT 1""",
            (direction, *same_group_symbols),
        ).fetchone()
        con.close()
    except Exception as e:
        # En cas d'erreur DB, on bloque par sécurité plutôt que de laisser
        # passer un signal potentiellement corrélé.
        return False, f"corrélation {group} — DB indisponible ({e})"

    if row:
        return False, f"corrélation {group} {direction} active (trade ouvert sur {row[1]})"
    return True, ""


# ─────────────────────────────────────────────────────────────
#  TELEGRAM
# ─────────────────────────────────────────────────────────────
# SÉCURITÉ : aucune valeur par défaut codée en dur. Si TG_TOKEN n'est pas
# défini dans l'environnement (.env), Telegram reste désactivé — le moteur
# ne doit JAMAIS retomber sur un token ou un chat/group ID appartenant à
# quelqu'un d'autre.
_TG_TOKEN_ENV = os.environ.get("TG_TOKEN", "8632763755:AAECibBa64ftswCGuDMF3DPFR5cuWKpQ_ZI")
_TG_ENABLED   = bool(_TG_TOKEN_ENV) if os.environ.get("TG_ENABLED", "") == "" else \
                os.environ.get("TG_ENABLED", "false").lower() == "true"

if not _TG_TOKEN_ENV:
    print("  [TG] ⚠  TG_TOKEN absent — envoi Telegram désactivé")

TELEGRAM_TOKEN     = _TG_TOKEN_ENV
TELEGRAM_CHAT_ID   = None
TELEGRAM_GROUP_ID  = os.environ.get("TG_GROUP_ID", "-1002335466840")
TELEGRAM_LEADER_ID = os.environ.get("TG_LEADER_ID", "6982051442")

# ── Groupes dédiés par marché ──────────────────────────────────
# Deriv (Volatility 75 / Volatility 25) → groupe Deriv
# Gold (XAUUSD) + BTC → groupe Gold/BTC
# Surchargeables via variables d'env (TG_GROUP_DERIV_ID / TG_GROUP_GOLD_BTC_ID)
TELEGRAM_GROUP_DERIV_ID    = os.environ.get("TG_GROUP_DERIV_ID", "-1002335466840")
TELEGRAM_GROUP_GOLD_BTC_ID = os.environ.get("TG_GROUP_GOLD_BTC_ID", "-5281258868")


def get_group_id_for_symbol(symbol: str) -> str:
    """Retourne le groupe Telegram approprié selon le marché du signal."""
    if is_deriv_symbol(symbol):
        return TELEGRAM_GROUP_DERIV_ID
    return TELEGRAM_GROUP_GOLD_BTC_ID

SIGNAL_COOLDOWN = 1800   # v8 : 30 min minimum entre 2 signaux sur la même paire (était 600)
_signal_cache: dict[str, float] = {}
_setup_sent: dict[str, bool] = {}
# Cache pour le cooldown par niveau de prix : {symbol -> (direction, entry_price, timestamp)}
_price_level_cache: dict[str, tuple[str, float, float]] = {}
PRICE_LEVEL_COOLDOWN = 1800   # secondes — cohérent avec SIGNAL_COOLDOWN
PRICE_LEVEL_TOLERANCE = 0.0003  # 0.03% — ne pas renvoyer si entry quasi-identique

# ── Trade Management — base de données persistante ────────────
# Astuce Render : définir TRADE_DB_PATH=/opt/render/project/src/trades.db
# dans les variables d'environnement pour persistance entre redémarrages.
TRADE_DB                = os.environ.get(
    "TRADE_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.db"),
)
TRADE_MONITOR_INTERVAL  = 60   # secondes entre chaque vérification des prix


def _setup_key(symbol: str, direction: str, score: int) -> str:
    bucket = (score // 5) * 5
    return f"{symbol}:{direction}:{bucket}"


def is_setup_already_sent(symbol: str, direction: str, score: int) -> bool:
    return _setup_sent.get(_setup_key(symbol, direction, score), False)


def is_price_level_duplicate(symbol: str, direction: str, entry_price: float) -> bool:
    """Retourne True si un signal récent sur la même paire a une entrée quasi-identique."""
    cached = _price_level_cache.get(symbol)
    if cached is None:
        return False
    cached_dir, cached_entry, cached_ts = cached
    if time.time() - cached_ts > PRICE_LEVEL_COOLDOWN:
        del _price_level_cache[symbol]
        return False
    if cached_dir != direction:
        return False
    if cached_entry <= 0:
        return False
    pct_diff = abs(entry_price - cached_entry) / cached_entry
    return pct_diff < PRICE_LEVEL_TOLERANCE


def record_price_level(symbol: str, direction: str, entry_price: float) -> None:
    _price_level_cache[symbol] = (direction, entry_price, time.time())


def mark_setup_sent(symbol: str, direction: str, score: int) -> None:
    _setup_sent[_setup_key(symbol, direction, score)] = True


def reset_setup(symbol: str) -> None:
    keys_to_del = [k for k in _setup_sent if k.startswith(f"{symbol}:")]
    for k in keys_to_del:
        del _setup_sent[k]
    _price_level_cache.pop(symbol, None)


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
_SIGNAL_COUNTER_FILE = os.path.join(os.path.dirname(TRADE_DB), "smc_signal_count.txt")

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


# ═════════════════════════════════════════════════════════════
#  TRADE MANAGEMENT — Suivi en temps réel après envoi du signal
#
#  Fonctionnement :
#    1. Chaque signal envoyé est enregistré dans SQLite
#    2. Un thread de fond vérifie le prix toutes les 60s
#    3. Alertes Telegram automatiques : TP1 / TP2 / TP3 / SL
#    4. Rappel "déplace SL en Break Even" 5 min après TP1
#
#  Alerte TP1 : "🎯 TP1 TOUCHÉ — ferme 30%, SL → Break Even"
#  Alerte TP2 : "🚀 TP2 TOUCHÉ — ferme 50% du restant"
#  Alerte TP3 : "💎 TP3 TOUCHÉ — ferme tout !"
#  Alerte SL  : "❌ STOP LOSS — ferme la position maintenant"
#
#  CONSEIL RENDER :
#    Définir TRADE_DB_PATH=/opt/render/project/src/trades.db
#    dans les variables d'environnement → persistance entre redémarrages
# ═════════════════════════════════════════════════════════════

def _init_trade_db() -> None:
    """Crée la table active_trades et signal_stats si elles n'existent pas encore."""
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS active_trades (
                trade_id   TEXT PRIMARY KEY,
                symbol     TEXT NOT NULL,
                direction  TEXT NOT NULL,
                entry      REAL NOT NULL,
                sl         REAL NOT NULL,
                tp1        REAL NOT NULL,
                tp2        REAL DEFAULT 0,
                tp3        REAL DEFAULT 0,
                lot        REAL DEFAULT 0,
                signal_num INTEGER DEFAULT 0,
                setup_type TEXT DEFAULT '',
                timestamp  TEXT NOT NULL,
                tp1_hit    INTEGER DEFAULT 0,
                tp2_hit    INTEGER DEFAULT 0,
                tp3_hit    INTEGER DEFAULT 0,
                sl_hit     INTEGER DEFAULT 0,
                be_set     INTEGER DEFAULT 0,
                closed     INTEGER DEFAULT 0
            )
        """)
        # ── Journal statistique ──────────────────────────────
        # Enregistre le résultat de chaque trade pour analyse a posteriori.
        # Permet de savoir : quel setup gagne le plus, quelle paire, quelle heure.
        con.execute("""
            CREATE TABLE IF NOT EXISTS signal_stats (
                stat_id    TEXT PRIMARY KEY,
                trade_id   TEXT NOT NULL,
                symbol     TEXT NOT NULL,
                direction  TEXT NOT NULL,
                setup_type TEXT NOT NULL,
                score      INTEGER DEFAULT 0,
                entry      REAL NOT NULL,
                sl         REAL NOT NULL,
                tp1        REAL NOT NULL,
                lot        REAL DEFAULT 0,
                signal_num INTEGER DEFAULT 0,
                timestamp  TEXT NOT NULL,
                hour_utc   INTEGER DEFAULT 0,
                weekday    INTEGER DEFAULT 0,
                result     TEXT DEFAULT 'open',
                exit_price REAL DEFAULT 0,
                pnl_r      REAL DEFAULT 0,
                duration_min INTEGER DEFAULT 0
            )
        """)
        con.commit()
        con.close()
    except Exception as e:
        print(f"  [TRADE_DB] Init erreur : {e}")

    # ── [v14] Migration : colonnes RR1 (BE) / RR2 (clôture partielle) ──
    # Ajoutées à part des TP1/TP2/TP3 existants (non modifiés) — servent
    # à des rappels intermédiaires plus précoces, conformes au cahier
    # des charges (RR1=sécuriser, RR2=clôture partielle, RR3=laisser
    # courir, RR6=objectif final — RR3/RR6 = TP1/TP3 déjà existants).
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        for col in ("rr1 REAL DEFAULT 0", "rr2 REAL DEFAULT 0",
                    "rr1_hit INTEGER DEFAULT 0", "rr2_hit INTEGER DEFAULT 0"):
            try:
                con.execute(f"ALTER TABLE active_trades ADD COLUMN {col}")
            except Exception:
                pass  # colonne déjà présente
        con.commit()
        con.close()
    except Exception as e:
        print(f"  [TRADE_DB] Migration RR1/RR2 erreur : {e}")


def register_trade(sig: "Signal", signal_num: int, setup_type: str = "SMC") -> str:
    """
    Enregistre un trade actif dans la base SQLite.
    Retourne l'ID unique du trade (8 chars).
    """
    _init_trade_db()
    trade_id = str(uuid.uuid4())[:8].upper()
    tp2 = getattr(sig, "tp2", 0.0) or 0.0
    tp3 = getattr(sig, "tp3", 0.0) or 0.0

    # ── [v14] RR1 (sécuriser/BE) et RR2 (clôture partielle) ────
    # Calculés indépendamment des TP1/TP2/TP3 existants (non touchés).
    risk = abs(sig.entry - sig.sl)
    if sig.direction == "LONG":
        rr1 = round(sig.entry + 1 * risk, 6)
        rr2 = round(sig.entry + 2 * risk, 6)
    else:
        rr1 = round(sig.entry - 1 * risk, 6)
        rr2 = round(sig.entry - 2 * risk, 6)

    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            INSERT OR IGNORE INTO active_trades
            (trade_id, symbol, direction, entry, sl, tp1, tp2, tp3,
             lot, signal_num, setup_type, timestamp, rr1, rr2)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            trade_id, sig.symbol, sig.direction,
            sig.entry, sig.sl, sig.tp, tp2, tp3,
            sig.lot, signal_num, setup_type,
            datetime.now(timezone.utc).isoformat(), rr1, rr2
        ))
        con.commit()
        con.close()
        print(c(f"  [TRADE] ✓ Trade #{trade_id} enregistré — {sig.symbol} {sig.direction}", "cyan"))
    except Exception as e:
        print(f"  [TRADE_DB] Erreur enregistrement : {e}")

    # ── Enregistrement dans le journal statistique ────────────
    _register_stat(trade_id, sig, signal_num, setup_type)
    return trade_id


def _register_stat(trade_id: str, sig: "Signal", signal_num: int, setup_type: str) -> None:
    """Insère une entrée dans signal_stats au moment de l'envoi du signal."""
    try:
        now     = datetime.now(timezone.utc)
        stat_id = str(uuid.uuid4())[:8].upper()
        score   = getattr(sig, "score", 0)
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            INSERT OR IGNORE INTO signal_stats
            (stat_id, trade_id, symbol, direction, setup_type, score,
             entry, sl, tp1, lot, signal_num, timestamp, hour_utc, weekday,
             result, exit_price, pnl_r, duration_min)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            stat_id, trade_id, sig.symbol, sig.direction, setup_type, score,
            sig.entry, sig.sl, sig.tp, sig.lot, signal_num,
            now.isoformat(), now.hour, now.weekday(),
            "open", 0.0, 0.0, 0
        ))
        con.commit()
        con.close()
    except Exception as e:
        print(f"  [STATS] Erreur insertion stat : {e}")


def _update_stat_result(trade_id: str, result: str, exit_price: float) -> None:
    """
    Met à jour le résultat d'un trade dans signal_stats.
    result = "tp1" | "tp2" | "tp3" | "sl"
    Calcule automatiquement pnl_r (en unités de R) et duration_min.
    """
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT * FROM signal_stats WHERE trade_id=?", (trade_id,)
        ).fetchone()
        if row is None:
            con.close()
            return

        entry     = row["entry"]
        sl        = row["sl"]
        direction = row["direction"]
        ts_open   = datetime.fromisoformat(row["timestamp"])
        risk      = abs(entry - sl)
        now       = datetime.now(timezone.utc)
        duration  = int((now - ts_open).total_seconds() / 60)

        if risk > 0:
            if direction == "LONG":
                pnl_r = (exit_price - entry) / risk
            else:
                pnl_r = (entry - exit_price) / risk
        else:
            pnl_r = 0.0

        con.execute("""
            UPDATE signal_stats
            SET result=?, exit_price=?, pnl_r=?, duration_min=?
            WHERE trade_id=?
        """, (result, exit_price, round(pnl_r, 2), duration, trade_id))
        con.commit()
        con.close()
        print(c(f"  [STATS] ✓ {trade_id} → {result}  pnl={round(pnl_r,2)}R  durée={duration}min", "cyan"))
    except Exception as e:
        print(f"  [STATS] Erreur update résultat : {e}")


def get_signal_stats(limit: int = 100) -> list[dict]:
    """
    Retourne les dernières entrées du journal statistique.
    Utile pour analyser les performances par setup, paire, heure.
    """
    try:
        _init_trade_db()
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT * FROM signal_stats ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"  [STATS] Erreur lecture : {e}")
        return []


def print_stats_summary() -> None:
    """Affiche un résumé des statistiques dans la console."""
    stats = get_signal_stats(500)
    closed = [s for s in stats if s["result"] != "open"]
    if not closed:
        print(c("  [STATS] Aucun trade clôturé dans le journal.", "yellow"))
        return

    wins  = [s for s in closed if s["pnl_r"] > 0]
    losses= [s for s in closed if s["pnl_r"] <= 0]
    wr    = round(len(wins) / len(closed) * 100, 1)
    avg_r = round(sum(s["pnl_r"] for s in closed) / len(closed), 2)

    print(c(f"\n  ╔══ 📊 JOURNAL STATISTIQUE ({'='*40})", "cyan"))
    print(c(f"  ║  Trades analysés : {len(closed)}  |  Winrate : {wr}%  |  R moyen : {avg_r}R", "cyan"))

    # Par setup
    from collections import defaultdict
    by_setup: dict = defaultdict(list)
    for s in closed:
        by_setup[s["setup_type"]].append(s["pnl_r"])
    print(c("  ║  Par setup :", "cyan"))
    for stype, rs in sorted(by_setup.items(), key=lambda x: -sum(x[1])):
        w = sum(1 for r in rs if r > 0)
        print(f"  ║    {stype:<12} : {len(rs)} trades  WR={round(w/len(rs)*100,0)}%  avg={round(sum(rs)/len(rs),2)}R")

    # Par paire
    by_sym: dict = defaultdict(list)
    for s in closed:
        by_sym[s["symbol"]].append(s["pnl_r"])
    print(c("  ║  Top paires :", "cyan"))
    top5 = sorted(by_sym.items(), key=lambda x: -sum(x[1]))[:5]
    for sym, rs in top5:
        w = sum(1 for r in rs if r > 0)
        print(f"  ║    {sym:<14} : {len(rs)} trades  WR={round(w/len(rs)*100,0)}%  total={round(sum(rs),2)}R")

    print(c("  ╚" + "═"*52, "cyan"))


def get_active_trades() -> list[dict]:
    """Retourne tous les trades non clôturés."""
    try:
        _init_trade_db()
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT * FROM active_trades WHERE closed=0 ORDER BY timestamp DESC"
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"  [TRADE_DB] Erreur lecture : {e}")
        return []


# Whitelist of allowed column names for update_trade_field (prevents SQL injection)
_ALLOWED_TRADE_FIELDS = frozenset({
    "tp1_hit", "tp2_hit", "tp3_hit", "sl_hit", "closed", "be_set",
    "close_price", "close_time", "pnl_pips", "rr1_hit", "rr2_hit",
})


def update_trade_field(trade_id: str, field: str, value) -> None:
    """Met à jour un champ d'un trade existant."""
    if field not in _ALLOWED_TRADE_FIELDS:
        print(f"  [TRADE_DB] ⛔ Champ non autorisé : {field}")
        return
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        con.execute(
            f"UPDATE active_trades SET {field}=? WHERE trade_id=?",
            (value, trade_id)
        )
        con.commit()
        con.close()
    except Exception as e:
        print(f"  [TRADE_DB] Erreur update {field} : {e}")


def update_trade_field_guarded(trade_id: str, field: str, value) -> int:
    """
    Met à jour un champ uniquement si sa valeur actuelle est 0/False (garde atomique).
    Retourne le nombre de lignes modifiées (1 si succès, 0 si déjà mis à jour).
    Utilisé pour éviter la race condition sur be_set.
    """
    if field not in _ALLOWED_TRADE_FIELDS:
        print(f"  [TRADE_DB] ⛔ Champ non autorisé : {field}")
        return 0
    try:
        con = sqlite3.connect(TRADE_DB, check_same_thread=False)
        cur = con.execute(
            f"UPDATE active_trades SET {field}=? WHERE trade_id=? AND ({field}=0 OR {field} IS NULL)",
            (value, trade_id)
        )
        con.commit()
        rows = cur.rowcount
        con.close()
        return rows
    except Exception as e:
        print(f"  [TRADE_DB] Erreur update_guarded {field} : {e}")
        return 0


def get_current_price_live(symbol: str) -> Optional[float]:
    """
    [v10] Récupère le prix actuel via yfinance OU requests direct (5m, fallback 1m).
    Utilisé uniquement par le trade monitor.
    """
    for interval in ("5m", "1m"):
        try:
            if _HAS_YFINANCE:
                df = yf.download(symbol, period="1d", interval=interval,
                                 auto_adjust=True, progress=False, threads=False)
                if df.empty:
                    continue
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0).str.lower()
                else:
                    df.columns = df.columns.str.lower()
            else:
                df = _yahoo_chart_json(symbol, interval, "1d")
                if df.empty:
                    continue
            df.dropna(subset=["close"], inplace=True)
            if not df.empty:
                return float(df["close"].iloc[-1])
        except Exception:
            continue
    return None


# ── Formatage des alertes trade ───────────────────────────────

def _fmt_trade_alert(trade: dict, event: str, price: float) -> str:
    """Formate le message Telegram d'alerte de trade."""
    dec = 2 if trade["entry"] > 100 else 5
    sym_map = {
        "GC=F"   : "XAUUSD / GOLD",
        "BTC-USD": "BTCUSD / Bitcoin",
    }
    sym_display = sym_map.get(
        trade["symbol"],
        trade["symbol"].replace("=X","").replace("-USD","").replace("^","")
    )
    dir_emoji = "🟢" if trade["direction"] == "LONG" else "🔴"
    dir_label = "BUY / LONG" if trade["direction"] == "LONG" else "SELL / SHORT"
    num_str   = f"#{trade['signal_num']}" if trade.get("signal_num") else ""

    entry = round(trade["entry"], dec)
    sl    = round(trade["sl"],    dec)
    tp1   = round(trade["tp1"],   dec)
    tp2   = round(trade["tp2"],   dec) if trade.get("tp2") and trade["tp2"] > 0 else None
    tp3   = round(trade["tp3"],   dec) if trade.get("tp3") and trade["tp3"] > 0 else None
    tp2_s = f"<code>{tp2}</code>" if tp2 else "—"
    tp3_s = f"<code>{tp3}</code>" if tp3 else "—"
    p_now = round(price, dec)

    SEP = "─" * 28

    if event == "RR1":
        return (
            f"🔒 <b>RR1 ATTEINT — Sécurise si tu veux</b>  {num_str}\n"
            f"{SEP}\n"
            f"{dir_emoji} <b>{sym_display}</b>  {dir_label}\n"
            f"💰 Entrée : <code>{entry}</code>\n"
            f"📍 Prix actuel : <code>{p_now}</code>\n"
            f"{SEP}\n"
            f"⚡ <b>Possibilité :</b> déplacer le SL sur <code>{entry}</code> "
            f"(Break Even) pour sécuriser — au choix\n"
            f"{SEP}\n"
            f"<i>@smcsignalspro</i>"
        )
    elif event == "RR2":
        return (
            f"💰 <b>RR2 ATTEINT — Clôture partielle possible</b>  {num_str}\n"
            f"{SEP}\n"
            f"{dir_emoji} <b>{sym_display}</b>  {dir_label}\n"
            f"📍 Prix actuel : <code>{p_now}</code>\n"
            f"{SEP}\n"
            f"⚡ <b>Possibilité :</b> clôturer une partie de la position ici, "
            f"pour ceux qui veulent sécuriser des gains — au choix\n"
            f"{SEP}\n"
            f"<i>@smcsignalspro</i>"
        )
    elif event == "TP1":
        return (
            f"🎯 <b>TP1 TOUCHÉ (RR3) !</b>  {num_str}\n"
            f"{SEP}\n"
            f"{dir_emoji} <b>{sym_display}</b>  {dir_label}\n"
            f"💰 Entrée : <code>{entry}</code>\n"
            f"✅ TP1 atteint : <code>{tp1}</code>  (RR ≥ 3)\n"
            f"📍 Prix actuel : <code>{p_now}</code>\n"
            f"{SEP}\n"
            f"⚡ <b>2 OPTIONS AU CHOIX :</b>\n"
            f"  1️⃣ <b>Clôture totale ici</b> — sécurise le RR3 en entier\n"
            f"  2️⃣ <b>Laisser courir</b> vers TP2 ({tp2_s}) / TP3 ({tp3_s})\n"
            f"      → 🔒 Déplace alors le SL sur <code>{entry}</code> (Break Even)\n"
            f"{SEP}\n"
            f"💡 Les deux options sont valables — à toi de choisir selon ta gestion\n"
            f"<i>@smcsignalspro</i>"
        )
    elif event == "TP2":
        return (
            f"🚀 <b>TP2 TOUCHÉ !</b>  {num_str}\n"
            f"{SEP}\n"
            f"{dir_emoji} <b>{sym_display}</b>  {dir_label}\n"
            f"✅ TP1 : <code>{tp1}</code>  ✓\n"
            f"🚀 TP2 atteint : <code>{tp2}</code>\n"
            f"📍 Prix actuel : <code>{p_now}</code>\n"
            f"💵 Gain cumulé estimé : <b>+$300</b>\n"
            f"{SEP}\n"
            f"⚡ <b>ACTIONS IMMÉDIATES :</b>\n"
            f"  • Ferme <b>50–70%</b> du restant\n"
            f"  • 🔒 Déplace SL → <code>{tp1}</code> (sécurise TP1)\n"
            f"  • Laisse le runner courir vers TP3 : {tp3_s}\n"
            f"{SEP}\n"
            f"💎 Excellent — gère ton runner !\n"
            f"<i>@smcsignalspro</i>"
        )
    elif event == "TP3":
        return (
            f"💎 <b>TP3 TOUCHÉ — TRADE COMPLET !</b>  {num_str}\n"
            f"{SEP}\n"
            f"{dir_emoji} <b>{sym_display}</b>  {dir_label}\n"
            f"✅ TP1 : <code>{tp1}</code>  ✓\n"
            f"✅ TP2 : {tp2_s}  ✓\n"
            f"💎 TP3 atteint : <code>{tp3}</code>\n"
            f"📍 Prix actuel : <code>{p_now}</code>\n"
            f"💵 Gain estimé : <b>+$600</b> 🏆\n"
            f"{SEP}\n"
            f"⚡ <b>ACTION :</b>\n"
            f"  • <b>Ferme TOUTE la position maintenant</b>\n"
            f"  • Objectif maximum atteint — trade parfait 🎯\n"
            f"{SEP}\n"
            f"🧠 Patience • Discipline • Résultat\n"
            f"<i>@smcsignalspro</i>"
        )
    elif event == "SL":
        return (
            f"❌ <b>STOP LOSS TOUCHÉ</b>  {num_str}\n"
            f"{SEP}\n"
            f"{dir_emoji} <b>{sym_display}</b>  {dir_label}\n"
            f"🔴 SL atteint : <code>{sl}</code>\n"
            f"📍 Prix actuel : <code>{p_now}</code>\n"
            f"💸 Perte : <b>-$100</b>  (risque contrôlé ✅)\n"
            f"{SEP}\n"
            f"⚡ <b>ACTION IMMÉDIATE :</b>\n"
            f"  • <b>Ferme la position maintenant</b>\n"
            f"  • ⛔ Ne pas moyenner à la baisse\n"
            f"  • Prochain setup sera meilleur 💪\n"
            f"{SEP}\n"
            f"📊 Le setup a été invalidé — risque maîtrisé\n"
            f"<i>@smcsignalspro</i>"
        )
    elif event == "BE_REMINDER":
        return (
            f"🔔 <b>RAPPEL : SL en BREAK EVEN</b>  {num_str}\n"
            f"{SEP}\n"
            f"{dir_emoji} <b>{sym_display}</b>  {dir_label}\n"
            f"✅ TP1 touché — protège ton trade maintenant\n"
            f"📍 Prix actuel : <code>{p_now}</code>\n"
            f"{SEP}\n"
            f"⚡ <b>Si ce n'est pas encore fait :</b>\n"
            f"  🔒 Déplace SL → <code>{entry}</code> <b>(Break Even)</b>\n"
            f"  • Position <b>risque zéro</b> — laisse le marché travailler\n"
            f"  • TP2 : {tp2_s}  |  TP3 : {tp3_s}\n"
            f"{SEP}\n"
            f"<i>@smcsignalspro</i>"
        )
    return f"⚡ Événement {event} — {sym_display} @ {p_now}"


def _send_trade_alert(trade: dict, event: str, price: float) -> None:
    """Envoie l'alerte trade par Telegram (leader + groupe)."""
    if not _TG_ENABLED:
        print(c(f"  [TRADE] {event} {trade['symbol']} @ {price} (TG désactivé)", "yellow"))
        return
    msg  = _fmt_trade_alert(trade, event, price)
    sent = False
    if TELEGRAM_LEADER_ID:
        ok = tg_send(msg, TELEGRAM_LEADER_ID)
        if ok:
            sent = True
    target_group = get_group_id_for_symbol(trade.get("symbol", ""))
    if target_group:
        tg_send(msg, target_group)
        sent = True
    icon = "✓" if sent else "✗"
    col  = "cyan" if sent else "red"
    print(c(f"  [TRADE] {icon} Alerte {event} — {trade['symbol']} @ {round(price, 5)}", col))


# ── Boucle de surveillance des trades actifs ──────────────────

def _monitor_trades_loop() -> None:
    """
    Thread de fond — vérifie toutes les TRADE_MONITOR_INTERVAL secondes
    si un TP ou SL a été touché pour chaque trade actif.

    Logique de surveillance :
      SL  → ferme et archive le trade
      TP1 → alerte + rappel BE 5 min après
      TP2 → alerte
      TP3 → alerte + ferme le trade
    """
    time.sleep(30)   # laisse le bot démarrer proprement
    print(c(f"  ✓ Trade Monitor actif — intervalle {TRADE_MONITOR_INTERVAL}s", "cyan"))

    while True:
        try:
            trades = get_active_trades()
            for trade in trades:
                try:
                    sym       = trade["symbol"]
                    direction = trade["direction"]
                    price     = get_current_price_live(sym)
                    if price is None:
                        continue

                    # ── SL touché ─────────────────────────────
                    if not trade["sl_hit"] and not trade["closed"]:
                        sl_hit = (
                            (direction == "LONG"  and price <= trade["sl"]) or
                            (direction == "SHORT" and price >= trade["sl"])
                        )
                        if sl_hit:
                            update_trade_field(trade["trade_id"], "sl_hit", 1)
                            update_trade_field(trade["trade_id"], "closed",  1)
                            _send_trade_alert(trade, "SL", price)
                            _update_stat_result(trade["trade_id"], "sl", price)
                            trade["sl_hit"] = 1
                            trade["closed"] = 1
                            continue   # trade clôturé

                    # ── [v14] RR1 touché (sécuriser / BE possible) ─
                    if not trade.get("rr1_hit", 0) and trade.get("rr1", 0):
                        rr1_hit = (
                            (direction == "LONG"  and price >= trade["rr1"]) or
                            (direction == "SHORT" and price <= trade["rr1"])
                        )
                        if rr1_hit:
                            update_trade_field(trade["trade_id"], "rr1_hit", 1)
                            _send_trade_alert(trade, "RR1", price)
                            trade["rr1_hit"] = 1

                    # ── [v14] RR2 touché (clôture partielle possible) ─
                    if not trade.get("rr2_hit", 0) and trade.get("rr2", 0):
                        rr2_hit = (
                            (direction == "LONG"  and price >= trade["rr2"]) or
                            (direction == "SHORT" and price <= trade["rr2"])
                        )
                        if rr2_hit:
                            update_trade_field(trade["trade_id"], "rr2_hit", 1)
                            _send_trade_alert(trade, "RR2", price)
                            trade["rr2_hit"] = 1

                    # ── TP1 touché ────────────────────────────
                    if not trade["tp1_hit"] and trade["tp1"] > 0:
                        tp1_hit = (
                            (direction == "LONG"  and price >= trade["tp1"]) or
                            (direction == "SHORT" and price <= trade["tp1"])
                        )
                        if tp1_hit:
                            update_trade_field(trade["trade_id"], "tp1_hit", 1)
                            _send_trade_alert(trade, "TP1", price)
                            _update_stat_result(trade["trade_id"], "tp1", price)
                            trade["tp1_hit"] = 1

                    # ── Rappel Break Even (5 min après TP1) ───
                    if (trade["tp1_hit"] and not trade.get("be_set", 0)
                            and not trade["tp2_hit"] and not trade["closed"]):
                        rows_updated = update_trade_field_guarded(trade["trade_id"], "be_set", 1)
                        if rows_updated:
                            # Rappel décalé dans un thread séparé pour ne pas bloquer
                            def _delayed_be(t=trade, p=price):
                                time.sleep(300)
                                # Re-vérifie que le trade est encore ouvert
                                refreshed = get_active_trades()
                                t_ref = next(
                                    (x for x in refreshed if x["trade_id"] == t["trade_id"]),
                                    None
                                )
                                if t_ref and not t_ref["tp2_hit"] and not t_ref["closed"]:
                                    new_price = get_current_price_live(t["symbol"]) or p
                                    _send_trade_alert(t, "BE_REMINDER", new_price)
                            threading.Thread(target=_delayed_be, daemon=True).start()

                    # ── TP2 touché ────────────────────────────
                    if (trade["tp1_hit"] and not trade["tp2_hit"]
                            and trade.get("tp2", 0) > 0):
                        tp2_hit = (
                            (direction == "LONG"  and price >= trade["tp2"]) or
                            (direction == "SHORT" and price <= trade["tp2"])
                        )
                        if tp2_hit:
                            update_trade_field(trade["trade_id"], "tp2_hit", 1)
                            _send_trade_alert(trade, "TP2", price)
                            _update_stat_result(trade["trade_id"], "tp2", price)
                            trade["tp2_hit"] = 1

                    # ── TP3 touché → clôture complète ─────────
                    if (trade["tp2_hit"] and not trade["tp3_hit"]
                            and trade.get("tp3", 0) > 0):
                        tp3_hit = (
                            (direction == "LONG"  and price >= trade["tp3"]) or
                            (direction == "SHORT" and price <= trade["tp3"])
                        )
                        if tp3_hit:
                            update_trade_field(trade["trade_id"], "tp3_hit", 1)
                            update_trade_field(trade["trade_id"], "closed",  1)
                            _send_trade_alert(trade, "TP3", price)
                            _update_stat_result(trade["trade_id"], "tp3", price)

                    time.sleep(2)   # pause entre chaque symbole

                except Exception as e_trade:
                    print(f"  [TRADE] Erreur {trade.get('symbol','?')} : {e_trade}")

        except Exception as e_loop:
            print(f"  [TRADE] Erreur boucle monitor : {e_loop}")

        time.sleep(TRADE_MONITOR_INTERVAL)


def _monitor_trades_thread() -> None:
    """Lance le thread de surveillance des trades."""
    t = threading.Thread(
        target=_monitor_trades_loop, daemon=True, name="trade-monitor"
    )
    t.start()
    return t


# ─────────────────────────────────────────────────────────────
#  RAPPORT JOURNALIER — envoi Telegram à 21h00 UTC ⭐⭐⭐⭐⭐
#
#  Calcule et envoie chaque jour à 21h00 UTC le winrate
#  de la journée + résumé cumulé de la semaine.
#  Format identique aux alertes du bot (lisible sur mobile).
# ─────────────────────────────────────────────────────────────

def _build_daily_report(date_str: str | None = None) -> str:
    """
    Construit le message du rapport journalier.
    date_str : format 'YYYY-MM-DD'. Si None → aujourd'hui UTC.
    """
    from collections import defaultdict

    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    stats  = get_signal_stats(500)
    today  = [s for s in stats if s["timestamp"].startswith(date_str) and s["result"] != "open"]
    open_t = [s for s in stats if s["result"] == "open"]

    # Calculs du jour
    wins_d  = [s for s in today if s["pnl_r"] > 0]
    loss_d  = [s for s in today if s["pnl_r"] <= 0]
    wr_d    = round(len(wins_d) / max(len(today), 1) * 100, 0)
    total_r = round(sum(s["pnl_r"] for s in today), 2)
    avg_r   = round(sum(s["pnl_r"] for s in today) / max(len(today), 1), 2)

    # Calculs de la semaine (7 derniers jours)
    from datetime import timedelta
    week_ago  = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    week_cl   = [s for s in stats if s["result"] != "open" and s["timestamp"] >= week_ago]
    wins_w    = [s for s in week_cl if s["pnl_r"] > 0]
    wr_w      = round(len(wins_w) / max(len(week_cl), 1) * 100, 0)
    total_r_w = round(sum(s["pnl_r"] for s in week_cl), 2)

    # Emoji résultat
    if not today:
        bilan_emoji = "😶"
        bilan_txt   = "Aucun trade clôturé aujourd'hui"
    elif wr_d >= 70:
        bilan_emoji = "🔥"
        bilan_txt   = "Excellente journée !"
    elif wr_d >= 50:
        bilan_emoji = "✅"
        bilan_txt   = "Journée positive"
    else:
        bilan_emoji = "⚠️"
        bilan_txt   = "Journée difficile — analyser les setups"

    # Détail par setup
    by_setup: dict = defaultdict(list)
    for s in today:
        by_setup[s["setup_type"]].append(s["pnl_r"])

    setup_lines = ""
    for stype, rs in sorted(by_setup.items(), key=lambda x: -sum(x[1])):
        w  = sum(1 for r in rs if r > 0)
        wr = int(w / len(rs) * 100)
        emo = "✅" if wr >= 50 else "❌"
        setup_lines += f"  {emo} {stype:<12} {len(rs)}t  WR {wr}%  {round(sum(rs),2):+.1f}R\n"

    if not setup_lines:
        setup_lines = "  — aucun trade\n"

    # Construction du message
    msg = (
        f"📊 <b>RAPPORT JOURNALIER SMC</b>\n"
        f"📅 {date_str}  —  21h00 UTC\n"
        f"{'─'*32}\n"
        f"{bilan_emoji}  <b>{bilan_txt}</b>\n\n"
        f"<b>Aujourd'hui :</b>\n"
        f"  📈 Trades clôturés : <b>{len(today)}</b>  "
        f"({len(wins_d)} ✅  {len(loss_d)} ❌)\n"
        f"  🎯 Winrate : <b>{int(wr_d)}%</b>\n"
        f"  💰 Total R : <b>{total_r:+.2f}R</b>  (moy {avg_r:+.2f}R)\n"
        f"  ⏳ Ouverts : {len(open_t)} signal(s) en cours\n\n"
        f"<b>7 derniers jours :</b>\n"
        f"  🗓️ {len(week_cl)} trades  WR <b>{int(wr_w)}%</b>  "
        f"Total <b>{total_r_w:+.2f}R</b>\n\n"
        f"<b>Par setup (aujourd'hui) :</b>\n"
        f"{setup_lines}"
        f"{'─'*32}\n"
        f"🧠 Patience • Discipline • Résultat\n"
        f"@smcsignalspro"
    )
    return msg


def _category_symbols(category: str) -> set:
    """Renvoie l'ensemble des symboles ('GC=F','BTC-USD',...) d'une catégorie."""
    if category == "gold_btc":
        return {s for s, _ in TIER_1_PRIORITY}
    return {s for s, _ in DERIV_SYMBOLS}


def _build_perf_report(category: str, period: str = "daily") -> str:
    """
    Construit le rapport de performance texte pour une catégorie
    ('gold_btc' ou 'deriv') sur une période ('daily' ou 'weekly').
    Détail complet : winrate, R total, par instrument (Gold vs BTC /
    V75 vs V25) et par setup.
    """
    from collections import defaultdict
    from datetime import timedelta

    cat_symbols = _category_symbols(category)
    sym_labels  = dict(TIER_1_PRIORITY + DERIV_SYMBOLS)

    stats  = [s for s in get_signal_stats(2000) if s["symbol"] in cat_symbols]
    now    = datetime.now(timezone.utc)

    if period == "daily":
        date_str      = now.strftime("%Y-%m-%d")
        period_trades = [s for s in stats if s["result"] != "open"
                          and s["timestamp"].startswith(date_str)]
        period_label  = f"📅 {date_str}  —  21h00 UTC"
        period_word   = "Aujourd'hui"
        compare_word   = "7 derniers jours"
        compare_days   = 7
    else:
        week_start    = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        period_trades = [s for s in stats if s["result"] != "open"
                          and s["timestamp"] >= week_start]
        period_label  = f"📅 Semaine du {week_start} au {now.strftime('%Y-%m-%d')}"
        period_word   = "Cette semaine"
        compare_word   = "28 derniers jours"
        compare_days   = 28

    open_t = [s for s in stats if s["result"] == "open"]

    wins   = [s for s in period_trades if s["pnl_r"] > 0]
    losses = [s for s in period_trades if s["pnl_r"] <= 0]
    wr     = round(len(wins) / max(len(period_trades), 1) * 100)
    total_r= round(sum(s["pnl_r"] for s in period_trades), 2)
    avg_r  = round(sum(s["pnl_r"] for s in period_trades) / max(len(period_trades), 1), 2)

    ref_start   = (now - timedelta(days=compare_days)).strftime("%Y-%m-%d")
    ref_trades  = [s for s in stats if s["result"] != "open" and s["timestamp"] >= ref_start]
    ref_wins    = [s for s in ref_trades if s["pnl_r"] > 0]
    ref_wr      = round(len(ref_wins) / max(len(ref_trades), 1) * 100)
    ref_total_r = round(sum(s["pnl_r"] for s in ref_trades), 2)

    if not period_trades:
        emoji, bilan = "😶", "Aucun trade clôturé"
    elif wr >= 70:
        emoji, bilan = "🔥", "Excellente performance !"
    elif wr >= 50:
        emoji, bilan = "✅", "Performance positive"
    else:
        emoji, bilan = "⚠️", "Période difficile — à analyser"

    # ── Détail par instrument (Gold vs BTC / V75 vs V25) ──────
    by_symbol: dict = defaultdict(list)
    for s in period_trades:
        by_symbol[s["symbol"]].append(s["pnl_r"])
    symbol_lines = ""
    for sym, rs in sorted(by_symbol.items(), key=lambda x: -sum(x[1])):
        w      = sum(1 for r in rs if r > 0)
        w_rate = int(w / len(rs) * 100)
        emo    = "✅" if w_rate >= 50 else "❌"
        label  = sym_labels.get(sym, sym)
        symbol_lines += f"  {emo} {label:<20} {len(rs)}t  WR {w_rate}%  {round(sum(rs),2):+.1f}R\n"
    if not symbol_lines:
        symbol_lines = "  — aucun trade\n"

    # ── Détail par setup ───────────────────────────────────────
    by_setup: dict = defaultdict(list)
    for s in period_trades:
        by_setup[s["setup_type"]].append(s["pnl_r"])
    setup_lines = ""
    for stype, rs in sorted(by_setup.items(), key=lambda x: -sum(x[1])):
        w      = sum(1 for r in rs if r > 0)
        w_rate = int(w / len(rs) * 100)
        emo    = "✅" if w_rate >= 50 else "❌"
        setup_lines += f"  {emo} {stype:<12} {len(rs)}t  WR {w_rate}%  {round(sum(rs),2):+.1f}R\n"
    if not setup_lines:
        setup_lines = "  — aucun trade\n"

    title = ("🥇 <b>RAPPORT PERFORMANCE — GOLD + BTC</b>" if category == "gold_btc"
              else "📡 <b>RAPPORT PERFORMANCE — DERIV (V75 / V25)</b>")

    msg = (
        f"{title}\n"
        f"{period_label}\n"
        f"{'─'*32}\n"
        f"{emoji}  <b>{bilan}</b>\n\n"
        f"<b>{period_word} :</b>\n"
        f"  📈 Trades clôturés : <b>{len(period_trades)}</b>  "
        f"({len(wins)} ✅  {len(losses)} ❌)\n"
        f"  🎯 Winrate : <b>{wr}%</b>\n"
        f"  💰 Total R : <b>{total_r:+.2f}R</b>  (moy {avg_r:+.2f}R)\n"
        f"  ⏳ Ouverts : {len(open_t)} signal(s) en cours\n\n"
        f"<b>{compare_word} :</b>\n"
        f"  🗓️ {len(ref_trades)} trades  WR <b>{ref_wr}%</b>  "
        f"Total <b>{ref_total_r:+.2f}R</b>\n\n"
        f"<b>Par instrument :</b>\n{symbol_lines}\n"
        f"<b>Par setup :</b>\n{setup_lines}"
        f"{'─'*32}\n"
        f"🧠 Patience • Discipline • Résultat\n"
        f"@smcsignalspro"
    )
    return msg


def generate_perf_chart(category: str, period: str = "daily") -> Optional[str]:
    """
    Génère une image claire (barres R, thème sombre) résumant la performance
    d'une catégorie sur les 14 derniers jours (daily) ou 8 dernières semaines
    (weekly), et retourne le chemin /tmp/*.png (ou None si échec/pas de data).
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from collections import defaultdict
        from datetime import timedelta

        cat_symbols = _category_symbols(category)
        stats = [s for s in get_signal_stats(3000)
                 if s["symbol"] in cat_symbols and s["result"] != "open"]

        now = datetime.now(timezone.utc)
        buckets: dict = defaultdict(float)

        if period == "daily":
            for i in range(13, -1, -1):
                d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
                buckets[d] = 0.0
            for s in stats:
                d = s["timestamp"][:10]
                if d in buckets:
                    buckets[d] += s["pnl_r"]
            labels   = list(buckets.keys())
            x_labels = [d[5:] for d in labels]
            chart_title = "Performance quotidienne (14 derniers jours)"
        else:
            for i in range(7, -1, -1):
                wk = now - timedelta(weeks=i)
                key = wk.strftime("%Y-S%W")
                buckets[key] = 0.0
            for s in stats:
                ts  = datetime.fromisoformat(s["timestamp"])
                key = ts.strftime("%Y-S%W")
                if key in buckets:
                    buckets[key] += s["pnl_r"]
            labels   = list(buckets.keys())
            x_labels = [l[-3:] for l in labels]
            chart_title = "Performance hebdomadaire (8 dernières semaines)"

        values = [buckets[k] for k in labels]
        if not any(values):
            pass  # on affiche quand même le graphique vide plutôt que rien

        BG, BG2  = "#0a0c10", "#0d1117"
        GREEN, RED, GRAY, FG = "#22c55e", "#ef4444", "#94a3b8", "#e2e8f0"

        fig, ax = plt.subplots(figsize=(10, 5.2), dpi=110, facecolor=BG)
        ax.set_facecolor(BG2)
        colors = [GREEN if v >= 0 else RED for v in values]
        ax.bar(range(len(values)), values, color=colors, width=0.6, zorder=3)
        ax.axhline(0, color="#334155", lw=0.8, zorder=2)
        ax.set_xticks(range(len(x_labels)))
        ax.set_xticklabels(x_labels, color=GRAY, fontsize=9, rotation=45, ha="right")
        ax.tick_params(colors=GRAY, labelsize=9)
        for spine in ax.spines.values():
            spine.set_color("#1e293b")
        cumul = round(sum(values), 2)
        cat_name = "Gold + BTC" if category == "gold_btc" else "Deriv — V75 / V25"
        ax.set_title(f"{cat_name}\n{chart_title}  •  Cumul {cumul:+.2f}R",
                     color=FG, fontsize=13, fontweight="bold")
        ax.set_ylabel("R", color=GRAY)
        ax.grid(axis="y", color="#1e293b", lw=0.5, ls="--", alpha=0.6, zorder=0)
        plt.tight_layout()

        path = f"/tmp/perf_{category}_{period}_{int(time.time())}.png"
        fig.savefig(path, dpi=110, facecolor=BG, bbox_inches="tight")
        plt.close(fig)
        import gc as _gc; _gc.collect()
        return path

    except Exception as e:
        print(f"  [CHART] Erreur génération graphique perf : {e}")
        return None


def _send_perf_report(category: str, period: str = "daily") -> None:
    """
    Construit et envoie le rapport de performance complet (image claire +
    texte détaillé) pour une catégorie, au groupe Telegram dédié et en privé
    à l'admin.
    """
    if not _TG_ENABLED:
        log.info(f"  [RAPPORT] {category}/{period} ignoré — TG_ENABLED=false")
        return

    group_id = TELEGRAM_GROUP_DERIV_ID if category == "deriv" else TELEGRAM_GROUP_GOLD_BTC_ID
    full_msg = _build_perf_report(category, period)
    chart_path = generate_perf_chart(category, period)

    cat_name    = "Gold + BTC" if category == "gold_btc" else "Deriv V75/V25"
    period_name = "Quotidien" if period == "daily" else "Hebdomadaire"
    caption = (f"📊 <b>Rapport {period_name} — {cat_name}</b>\n"
               f"📈 Graphique de performance ci-dessus — détail complet ⬇️")

    targets = [t for t in (group_id, TELEGRAM_LEADER_ID) if t]
    for chat_id in targets:
        if chart_path:
            ok = tg_send_photo(chart_path, caption, chat_id)
            if not ok:
                tg_send(full_msg, chat_id)
                continue
        tg_send(full_msg, chat_id)

    if chart_path:
        try:
            os.remove(chart_path)
        except OSError:
            pass

    log.info(f"  [RAPPORT] ✓ {period}/{category} envoyé ({len(targets)} destinataire(s))")


def _daily_report_loop() -> None:
    """
    Thread qui attend chaque jour 21h00 UTC et envoie 2 rapports séparés
    (Gold+BTC puis Deriv V75/V25), chacun avec un graphique clair + le
    détail texte complet. Utilise un sleep adaptatif pour viser 21:00:00 UTC.
    """
    print(c("  ✓ Rapport journalier activé — envoi chaque jour à 21h00 UTC "
            "(Gold+BTC et Deriv séparément)", "cyan"))
    while True:
        try:
            now   = datetime.now(timezone.utc)
            # Prochain 21h UTC
            next_21 = now.replace(hour=21, minute=0, second=0, microsecond=0)
            if now >= next_21:
                from datetime import timedelta
                next_21 += timedelta(days=1)
            wait_sec = (next_21 - now).total_seconds()
            log.info(f"  [RAPPORT] Prochain rapport dans {int(wait_sec//3600)}h{int((wait_sec%3600)//60)}m")
            time.sleep(wait_sec)

            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            _send_perf_report("gold_btc", "daily")
            _send_perf_report("deriv", "daily")

            # Affichage console
            print(c(f"\n  📊 RAPPORT JOURNALIER {date_str} ─────────────────", "cyan"))
            print_stats_summary()
            time.sleep(60)   # évite double envoi en cas de drift

        except Exception as e:
            log.error(f"  [RAPPORT] Erreur : {e}")
            time.sleep(300)


def _daily_report_thread() -> None:
    """Lance le thread du rapport journalier."""
    t = threading.Thread(target=_daily_report_loop, daemon=True, name="daily-report")
    t.start()
    return t


def _weekly_report_loop() -> None:
    """
    Thread qui attend chaque dimanche 21h05 UTC (juste après le rapport
    quotidien) et envoie 2 rapports hebdomadaires séparés (Gold+BTC puis
    Deriv V75/V25), chacun avec graphique + détail texte complet.
    """
    print(c("  ✓ Rapport hebdomadaire activé — envoi chaque dimanche à 21h05 UTC "
            "(Gold+BTC et Deriv séparément)", "cyan"))
    while True:
        try:
            from datetime import timedelta
            now = datetime.now(timezone.utc)
            days_until_sunday = (6 - now.weekday()) % 7  # weekday(): lundi=0 ... dimanche=6
            next_run = (now + timedelta(days=days_until_sunday)).replace(
                hour=21, minute=5, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=7)
            wait_sec = (next_run - now).total_seconds()
            log.info(f"  [RAPPORT HEBDO] Prochain rapport dans "
                     f"{int(wait_sec//86400)}j {int((wait_sec % 86400)//3600)}h")
            time.sleep(wait_sec)

            _send_perf_report("gold_btc", "weekly")
            _send_perf_report("deriv", "weekly")

            print(c(f"\n  📊 RAPPORT HEBDOMADAIRE ─────────────────", "cyan"))
            time.sleep(60)

        except Exception as e:
            log.error(f"  [RAPPORT HEBDO] Erreur : {e}")
            time.sleep(300)


def _weekly_report_thread() -> None:
    """Lance le thread du rapport hebdomadaire."""
    t = threading.Thread(target=_weekly_report_loop, daemon=True, name="weekly-report")
    t.start()
    return t


# ─────────────────────────────────────────────────────────────
#  COMMANDE ADMIN  /rapport  — 2 rapports indépendants
#  Deriv (V75/V25) → TELEGRAM_GROUP_DERIV_ID
#  Gold + BTC      → TELEGRAM_GROUP_GOLD_BTC_ID
# ─────────────────────────────────────────────────────────────

def _build_category_report(category: str) -> str:
    """Construit le rapport /rapport pour une catégorie ('deriv' ou 'gold_btc') :
    catégorie → état du marché → signaux disponibles → trades actifs →
    derniers signaux."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if category == "deriv":
        title       = "📡 <b>RAPPORT DERIV — Volatility 75 / Volatility 25</b>"
        cat_symbols = DERIV_SYMBOLS
        cat_labels  = {"Volatility 75 Index", "Volatility 25 Index"}
    else:
        title       = "🥇 <b>RAPPORT GOLD + BTC</b>"
        cat_symbols = TIER_1_PRIORITY
        cat_labels  = {"Gold", "Bitcoin"}

    sep = "─" * 30
    lines = [title, f"🕐 <code>{ts}</code>", sep, "<b>📊 État du marché :</b>"]

    for sym, label in cat_symbols:
        ok, reason = is_kill_zone_active(sym)
        icon = "🟢" if ok else "🔴"
        lines.append(f"{icon} {label} ({sym}) — {reason}")

    lines.append(sep)
    lines.append("<b>⚡ Signaux disponibles aujourd'hui :</b>")
    for sym, label in cat_symbols:
        used = _daily_counts.get(sym, 0)
        lines.append(f"• {label} : {used}/{MAX_SIGNALS_PER_DAY} signaux/jour")
    if category == "deriv":
        lines.append(f"• Cumul Deriv (V75+V25) : {_daily_deriv_count}/{MAX_SIGNALS_DERIV_PER_DAY}")
    lines.append(f"• Cumul global (4 marchés) : {_daily_global_count}/{MAX_SIGNALS_GLOBAL_PER_DAY}")

    lines.append(sep)
    lines.append("<b>📈 Trades actifs :</b>")
    cat_sym_set = {s for s, _ in cat_symbols}
    try:
        active = [t for t in get_active_trades() if t.get("symbol") in cat_sym_set]
    except Exception as e:
        active = []
        lines.append(f"⚠ Erreur lecture trades actifs : {e}")
    if active:
        for t in active:
            dir_emoji = "🟢" if t["direction"] in ("LONG", "BUY") else "🔴"
            lines.append(
                f"{dir_emoji} {t['symbol']} {t['direction']} — Entry <code>{t['entry']}</code> "
                f"| SL <code>{t['sl']}</code> | TP1 <code>{t['tp1']}</code>"
            )
    else:
        lines.append("Aucun trade actif")

    lines.append(sep)
    lines.append("<b>🕓 Derniers signaux envoyés :</b>")
    with _STATUS_LOCK:
        recent = [s for s in reversed(_STATUS["last_signals"]) if s.get("market") in cat_labels][:5]
    if recent:
        for s in recent:
            lines.append(
                f"• {s['ts']} — {s['market']} {s['direction']} "
                f"[{s.get('mode','')}] score {s['score']}/100 RR 1:{s['rr']}"
            )
    else:
        lines.append("Aucun signal récent")

    return "\n".join(lines)


def send_admin_report() -> None:
    """Génère et envoie les 2 rapports /rapport, chacun dans son groupe dédié."""
    if not _TG_ENABLED:
        log.info("  [CMD] /rapport ignoré — TG_ENABLED=false")
        return
    try:
        deriv_msg = _build_category_report("deriv")
        if TELEGRAM_GROUP_DERIV_ID:
            ok1 = tg_send(deriv_msg, TELEGRAM_GROUP_DERIV_ID)
            log.info(f"  [CMD] {'✓' if ok1 else '✗'} Rapport Deriv → {TELEGRAM_GROUP_DERIV_ID}")
    except Exception as e:
        log.error(f"  [CMD] Erreur rapport Deriv : {e}")

    try:
        gb_msg = _build_category_report("gold_btc")
        if TELEGRAM_GROUP_GOLD_BTC_ID:
            ok2 = tg_send(gb_msg, TELEGRAM_GROUP_GOLD_BTC_ID)
            log.info(f"  [CMD] {'✓' if ok2 else '✗'} Rapport Gold+BTC → {TELEGRAM_GROUP_GOLD_BTC_ID}")
    except Exception as e:
        log.error(f"  [CMD] Erreur rapport Gold+BTC : {e}")


_TG_UPDATE_OFFSET = 0

# ── [v14] Menu Paramètres — état "en attente de saisie" par admin ─────────
# Quand l'admin tape un bouton comme "💰 Solde", on attend son prochain
# message texte comme étant la nouvelle valeur pour ce paramètre.
_AWAITING_INPUT: dict = {}   # {from_id: "account_balance" | "risk_value" | "leverage" | "profit_target"}


def _is_admin(from_id: str) -> bool:
    return bool(TELEGRAM_LEADER_ID) and str(from_id) == str(TELEGRAM_LEADER_ID)


def _settings_menu_text() -> str:
    balance   = get_account_balance()
    mode, val = get_risk_config()
    risk_lbl  = f"{val}%" if mode == "percent" else f"${val}"
    risk_usd  = get_base_risk_usd()
    leverage  = get_leverage()
    target    = get_profit_target()
    recovery  = "✅ Activé" if is_recovery_enabled() else "❌ Désactivé"
    max_vol   = get_max_volume()
    tf        = get_timeframe_setting()
    session   = get_session_mode()
    markets_lbl = {"all": "Tous (Gold+BTC+V75+V25)", "gold_btc": "Gold + BTC",
                   "deriv": "Volatility 75 + 25"}.get(get_active_markets_setting(), "Tous")
    profile   = get_profile_label()
    prop_firm = "✅ Actif" if is_prop_firm_mode() else "❌ Inactif"
    atr_mult  = get_atr_sl_mult()
    return (
        "⚙️ <b>MENU PARAMÈTRES — AlphaBot</b>\n"
        "─────────────────────────\n"
        f"💰 Solde du compte : <code>${balance:,.2f}</code>\n"
        f"🎯 Risque par trade : <code>{risk_lbl}</code> (≈ ${risk_usd:.2f})\n"
        f"⚖️ Levier : <code>1:{leverage}</code>\n"
        f"🏆 Objectif de profit : <code>{'$'+format(target, ',.2f') if target > 0 else 'non défini'}</code>\n"
        f"🔁 Recovery : {recovery}\n"
        f"📊 Volume max : <code>{max_vol if max_vol > 0 else 'illimité'}</code>\n"
        f"⏱ Timeframe : <code>{tf}</code>\n"
        f"🌎 Session : <code>{session}</code>\n"
        f"📈 Marchés actifs : <code>{markets_lbl}</code>\n"
        f"👤 Profil : <code>{profile}</code>\n"
        f"🏢 Mode Prop Firm : {prop_firm}\n"
        f"📐 ATR SL (×) : <code>{atr_mult}</code>\n"
        "─────────────────────────\n"
        "Touche un bouton pour modifier un paramètre."
    )


def _settings_menu_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "💰 Solde", "callback_data": "set:account_balance"},
             {"text": "🎯 Risque %", "callback_data": "set:risk_value"}],
            [{"text": "⚖️ Levier", "callback_data": "set:leverage"},
             {"text": "🏆 Objectif profit", "callback_data": "set:profit_target"}],
            [{"text": "🔁 Recovery ON/OFF", "callback_data": "toggle:recovery"},
             {"text": "📊 Volume max", "callback_data": "set:max_volume"}],
            [{"text": "⏱ Timeframe (M5/M15)", "callback_data": "toggle:timeframe"},
             {"text": "🌎 Session (NY/24H)", "callback_data": "toggle:session"}],
            [{"text": "📈 Marchés actifs", "callback_data": "toggle:markets"},
             {"text": "👤 Profil", "callback_data": "set:profile_label"}],
            [{"text": "🏢 Mode Prop Firm ON/OFF", "callback_data": "toggle:propfirm"},
             {"text": "📐 ATR SL (×)", "callback_data": "set:atr_sl_mult"}],
            [{"text": "🔄 Actualiser", "callback_data": "refresh"},
             {"text": "❌ Fermer", "callback_data": "close"}],
        ]
    }


def tg_send_menu(chat_id: str) -> None:
    try:
        requests.post(
            _tg_url("sendMessage"),
            json={
                "chat_id": chat_id, "text": _settings_menu_text(),
                "parse_mode": "HTML", "reply_markup": _settings_menu_keyboard(),
            },
            timeout=10,
        )
    except Exception as e:
        print(c(f"  [TG] Erreur menu : {e}", "red"))


_SETTING_PROMPTS = {
    "account_balance": "💰 Envoie le nouveau <b>solde du compte</b> (en $), ex: <code>10000</code>",
    "risk_value":       "🎯 Envoie le nouveau <b>risque par trade</b> — un %, ex: <code>0.65%</code>, "
                        "ou un montant fixe, ex: <code>$50</code>",
    "leverage":         "⚖️ Envoie le nouveau <b>levier</b>, ex: <code>100</code> pour 1:100",
    "profit_target":    "🏆 Envoie le nouvel <b>objectif de profit</b> en $, ex: <code>500</code> "
                        "(<code>0</code> pour désactiver)",
    "max_volume":       "📊 Envoie le nouveau <b>volume (lot) max</b> par trade, ex: <code>0.5</code> "
                        "(<code>0</code> pour illimité)",
    "profile_label":    "👤 Envoie le nouveau <b>nom de profil</b> (libre), ex: <code>Compte Prop 10k</code>",
    "atr_sl_mult":      "📐 Envoie le nouveau <b>multiplicateur ATR pour le SL</b>, ex: <code>0.6</code>",
}


def _handle_callback_query(cq: dict) -> None:
    cq_id   = cq.get("id", "")
    data    = cq.get("data", "")
    from_id = str(cq.get("from", {}).get("id", ""))
    chat_id = str(cq.get("message", {}).get("chat", {}).get("id", ""))

    try:
        requests.post(_tg_url("answerCallbackQuery"), json={"callback_query_id": cq_id}, timeout=10)
    except Exception:
        pass

    if not _is_admin(from_id):
        return

    if data == "close":
        return
    if data == "refresh":
        tg_send_menu(chat_id)
        return
    if data == "toggle:recovery":
        new_val = "0" if is_recovery_enabled() else "1"
        set_setting("recovery_enabled", new_val)
        log.info(f"  [MENU] Recovery → {'ON' if new_val=='1' else 'OFF'}")
        tg_send_menu(chat_id)
        return
    if data == "toggle:timeframe":
        new_val = "M15" if get_timeframe_setting() == "M5" else "M5"
        set_setting("timeframe", new_val)
        log.info(f"  [MENU] Timeframe → {new_val}")
        tg_send_menu(chat_id)
        return
    if data == "toggle:session":
        new_val = "24H" if get_session_mode() == "NY" else "NY"
        set_setting("session_mode", new_val)
        log.info(f"  [MENU] Session → {new_val}")
        tg_send_menu(chat_id)
        return
    if data == "toggle:markets":
        order = ["all", "gold_btc", "deriv"]
        cur   = get_active_markets_setting()
        new_val = order[(order.index(cur) + 1) % len(order)] if cur in order else "all"
        set_setting("active_markets", new_val)
        log.info(f"  [MENU] Marchés actifs → {new_val}")
        tg_send_menu(chat_id)
        return
    if data == "toggle:propfirm":
        new_val = "0" if is_prop_firm_mode() else "1"
        set_setting("prop_firm_mode", new_val)
        log.info(f"  [MENU] Mode Prop Firm → {'ON' if new_val=='1' else 'OFF'}")
        tg_send_menu(chat_id)
        return
    if data.startswith("set:"):
        key = data.split(":", 1)[1]
        _AWAITING_INPUT[from_id] = key
        tg_send(_SETTING_PROMPTS.get(key, "Envoie la nouvelle valeur :"), chat_id)
        return


def _parse_risk_value_input(raw: str) -> Optional[tuple]:
    """Parse '0.65%' → ('percent', 0.65) ; '$50' ou '50' → ('fixed', 50.0)."""
    raw = raw.strip()
    try:
        if raw.endswith("%"):
            return "percent", float(raw[:-1].strip())
        if raw.startswith("$"):
            return "fixed", float(raw[1:].strip())
        # pas de symbole : on garde le mode actuel
        mode, _ = get_risk_config()
        return mode, float(raw)
    except Exception:
        return None


def _handle_awaiting_input(text: str, from_id: str, chat_id: str) -> bool:
    """Si l'admin a un paramètre en attente de saisie, traite `text` comme
    la nouvelle valeur. Retourne True si consommé."""
    key = _AWAITING_INPUT.get(from_id)
    if not key:
        return False
    del _AWAITING_INPUT[from_id]

    try:
        if key == "account_balance":
            set_setting("account_balance", str(float(text.strip().replace("$", "").replace(",", ""))))
            tg_send("✅ Solde du compte mis à jour.", chat_id)
        elif key == "risk_value":
            parsed = _parse_risk_value_input(text)
            if parsed is None:
                tg_send("⚠️ Valeur invalide. Réessaie, ex: <code>0.65%</code> ou <code>$50</code>", chat_id)
                return True
            mode, value = parsed
            set_setting("risk_mode", mode)
            set_setting("risk_value", str(value))
            tg_send("✅ Risque par trade mis à jour.", chat_id)
        elif key == "leverage":
            set_setting("leverage", str(int(float(text.strip()))))
            tg_send("✅ Levier mis à jour.", chat_id)
        elif key == "profit_target":
            set_setting("profit_target", str(float(text.strip().replace("$", "").replace(",", ""))))
            tg_send("✅ Objectif de profit mis à jour.", chat_id)
        elif key == "max_volume":
            set_setting("max_volume", str(float(text.strip().replace(",", "."))))
            tg_send("✅ Volume max mis à jour.", chat_id)
        elif key == "profile_label":
            set_setting("profile_label", text.strip()[:64])
            tg_send("✅ Profil mis à jour.", chat_id)
        elif key == "atr_sl_mult":
            val = float(text.strip().replace(",", "."))
            if val <= 0:
                tg_send("⚠️ Valeur invalide, doit être > 0.", chat_id)
                return True
            set_setting("atr_sl_mult", str(val))
            tg_send("✅ Multiplicateur ATR SL mis à jour.", chat_id)
    except Exception:
        tg_send("⚠️ Valeur invalide, paramètre inchangé.", chat_id)
        return True

    tg_send_menu(chat_id)
    return True


def _handle_incoming_message(text: str, from_id: str, chat_id: str = "") -> None:
    """Route les commandes admin reçues par Telegram."""
    chat_id = chat_id or from_id

    # Seul l'admin (TG_LEADER_ID) peut piloter le bot par message privé.
    if not _is_admin(from_id):
        return

    # Si un paramètre est en attente de saisie, ce message est sa valeur.
    if _handle_awaiting_input(text, from_id, chat_id):
        return

    cmd = (text or "").strip().split()[0].lower() if text else ""
    if cmd == "/rapport":
        log.info("  [CMD] ✓ /rapport reçu — génération des 2 rapports")
        send_admin_report()
    elif cmd in ("/parametres", "/menu", "/settings", "/start"):
        log.info("  [CMD] ✓ /parametres reçu — envoi du menu")
        tg_send_menu(chat_id)


def _telegram_command_listener() -> None:
    """Long-polling Telegram getUpdates() pour écouter /rapport et le menu
    /parametres (boutons inline). Aucune URL publique/webhook requise →
    compatible Render Background Worker (tourne dans un thread daemon,
    comme le Trade Monitor et le rapport journalier)."""
    global _TG_UPDATE_OFFSET
    if not _TG_ENABLED:
        log.info("  [CMD] Écoute commandes désactivée (TG_ENABLED=false)")
        return
    log.info("  ✓ Commandes admin /rapport + /parametres activées (long-polling)")
    while True:
        try:
            r = requests.get(
                _tg_url("getUpdates"),
                params={"offset": _TG_UPDATE_OFFSET + 1, "timeout": 25},
                timeout=30,
            )
            for upd in r.json().get("result", []):
                _TG_UPDATE_OFFSET = max(_TG_UPDATE_OFFSET, upd.get("update_id", 0))

                if "callback_query" in upd:
                    _handle_callback_query(upd["callback_query"])
                    continue

                msg     = upd.get("message") or upd.get("channel_post") or {}
                text    = msg.get("text", "")
                from_id = str(msg.get("from", {}).get("id", ""))
                chat_id = str(msg.get("chat", {}).get("id", from_id))
                if text:
                    _handle_incoming_message(text, from_id, chat_id)
        except Exception as e:
            log.error(f"  [CMD] Erreur listener commandes : {e}")
            time.sleep(5)
        time.sleep(1)


def _telegram_command_thread() -> None:
    """Lance le thread d'écoute des commandes admin (/rapport, /parametres)."""
    t = threading.Thread(target=_telegram_command_listener, daemon=True, name="tg-commands")
    t.start()
    return t


# ─────────────────────────────────────────────────────────────

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
    sym_map = {"GC=F": "XAUUSD / GOLD", "BTC-USD": "BTCUSD / Bitcoin"}
    sym_display = sym_map.get(sig.symbol,
        sig.symbol.replace("=X", "").replace("-USD", "").replace("^", ""))

    dir_arrow = "🟢 BUY / LONG" if sig.direction == "LONG" else "🔴 SELL / SHORT"
    num_str   = f"#{signal_num}" if signal_num else ""

    # ── Description setup + confirmations utilisées ──────────
    # v13 — stratégie unique : Sweep de liquidité + Cassure de structure
    SETUP_DESCRIPTIONS = {
        "MSS": ("SWEEP + CASSURE DE STRUCTURE",
                "Zone H4 (OB jaune/Breaker/Balance) + Sweep + BOS+CHoCH M15 + RR≥3",
                "✔ Zone H4 : OB jaune, Breaker Block ou Balance\n✔ BOS M15 dans le sens attendu\n✔ CHoCH M15 (cassure de structure)\n✔ Sweep de liquidité (bonus de score)\n✔ RR minimum 1:3"),
    }
    s_title, s_logic, s_conf = SETUP_DESCRIPTIONS.get(
        mode, (mode, "SMC confluence", "✔ Critères SMC validés"))

    msg = (
        f"<b>⭐ SMC SIGNALS PRO</b>\n"
        f"🟢 <b>NOUVEAU SIGNAL {num_str}</b>\n"
        f"<b>{sym_display}</b>\n"
        f"{'─'*30}\n"
        f"💎  <b>SETUP :</b> {s_title}\n"
        f"📐  <b>LOGIQUE :</b> <i>{s_logic}</i>\n"
        f"{'⭐'*max(getattr(sig,'stars',3),1)}  <b>QUALITÉ ENTRÉE :</b> "
        f"{'Confirmée (retest + rejet)' if getattr(sig,'stars',3) >= 3 else 'Directe (sweep + CHoCH)'}\n"
        f"🎯  <b>DIRECTION :</b> <b>{dir_arrow}</b>\n"
        f"💰  <b>ENTRY M15 :</b> <code>{sig.entry}</code>\n"
        f"📦  <b>LOT :</b> <code>{base_lot}</code>  <i>(risque $100)</i>\n"
        f"{'─'*30}\n"
        f"🎯  <b>TP1 :</b> <code>{sig.tp}</code>  {_rr(sig.tp)}  {_pct(sig.tp)}  <b>{_gain(sig.tp)}</b>\n"
        f"🚀  <b>TP2 :</b> <code>{tp2}</code>  {_rr(tp2)}  {_pct(tp2)}  <b>{_gain(tp2)}</b>\n"
        f"💎  <b>TP3 :</b> <code>{tp3}</code>  {_rr(tp3)}  {_pct(tp3)}  <b>{_gain(tp3)}</b>\n"
        f"🔴  <b>SL :</b> <code>{sig.sl}</code>  {_sl_pct()}  <b>-$100</b>\n"
        f"{'─'*30}\n"
        f"✅ <b>CONFIRMATIONS :</b>\n{s_conf}\n"
        f"{'─'*30}\n"
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
        sym_display = ({"GC=F": "XAUUSD", "BTC-USD": "BTCUSD"}
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
    elif is_price_level_duplicate(sig.symbol, sig.direction, sig.entry):
        print(c(f"  [TG] ⏭ Groupe — niveau de prix quasi-identique ({sig.symbol} {sig.entry})", "yellow"))
    else:
        mark_setup_sent(sig.symbol, sig.direction, sig.score)
        target_group = get_group_id_for_symbol(sig.symbol)
        if target_group:
            if chart_path:
                ok_grp = tg_send_photo(chart_path, msg, target_group)
                print(c(f"  [TG] {'✓ Groupe (photo)' if ok_grp else '✗ Groupe photo échoué'}", "green" if ok_grp else "red"))
            else:
                ok_grp = tg_send(msg, target_group)
                print(c(f"  [TG] {'✓ Groupe (texte)' if ok_grp else '✗ Groupe texte échoué'}", "green" if ok_grp else "red"))
            if ok_grp:
                record_price_level(sig.symbol, sig.direction, sig.entry)
        else:
            print(c("  [TG] ⚠ Aucun groupe cible défini pour ce marché", "red"))

    # Nettoyage fichier temporaire
    if chart_path:
        try:
            import os as _os
            _os.remove(chart_path)
        except Exception:
            pass

    # ── Enregistrement du trade pour suivi automatique ────────
    # Le monitor vérifiera TP1/TP2/TP3/SL et enverra les alertes
    try:
        setup_t = getattr(sig, "mode", "SMC")
        register_trade(sig, num, setup_type=setup_t)
    except Exception as _reg_e:
        print(c(f"  [TRADE] ⚠ Enregistrement échoué : {_reg_e}", "yellow"))


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
    stars:      int    = 3     # 2⭐ = entrée directe (sweep+CHoCH) | 3⭐ = entrée confirmée (retest+rejet)


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
    """Calcule la taille de lot pour les 4 marchés actifs : Gold, BTC, Deriv (R_75/R_25)."""
    sl_distance = abs(entry - sl)
    if sl_distance == 0:
        return 0.0

    if symbol == "GC=F":
        lot = risk_usd / (sl_distance * 100.0)
        return max(0.01, round(lot, 2))
    if is_deriv_symbol(symbol):
        # ⚠️ Approximatif : je n'ai pas de spec de contrat Deriv vérifiée
        # pour ces indices (taille de lot / valeur du point varie selon
        # le broker/plateforme). Formule générique $ risqué ÷ distance SL,
        # comme pour BTC. À CALIBRER avec la fiche "spécification du
        # symbole" de ton MT5 Deriv avant tout trade réel.
        return round(risk_usd / sl_distance, 4)
    if symbol == "BTC-USD":
        return round(risk_usd / sl_distance, 6)

    # Fallback (ne devrait jamais être atteint avec les 4 marchés actifs)
    sl_pips = sl_distance / 0.0001
    return max(0.01, round(risk_usd / (sl_pips * 10.0), 2))


# ─────────────────────────────────────────────────────────────
#  DERIV — INDICES SYNTHÉTIQUES (Volatility 100/50/25, 24/7)
# ─────────────────────────────────────────────────────────────
# Données de marché publiques : aucun token API requis (juste un app_id).
# ─────────────────────────────────────────────────────────────
#  FETCH YAHOO FINANCE — API JSON brut (sans yfinance) [v8.6/v10]
#
#  Permet de faire tourner le bot sur Pydroid/Android sans yfinance
#  (lxml/curl_cffi/peewee échouent souvent à se compiler sur Android).
#  Même source de données que yfinance — exactement la même API Yahoo.
#
#  Si yfinance est installé (_HAS_YFINANCE=True), fetch() l'utilise.
#  Sinon, fetch() bascule automatiquement sur _yahoo_chart_json().
# ─────────────────────────────────────────────────────────────

def _period_to_seconds(period: str) -> int:
    """Convertit '5d' / '1mo' / '30d' / '1y' en secondes. Tolérant, jamais d'exception."""
    p = (period or "5d").strip().lower()
    try:
        if p.endswith("mo"):
            return int(p[:-2]) * 30 * 86400
        if p.endswith("y"):
            return int(p[:-1]) * 365 * 86400
        if p.endswith("d"):
            return int(p[:-1]) * 86400
        return int(p) * 86400
    except ValueError:
        return 5 * 86400


_YAHOO_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
}


def _yahoo_chart_json(symbol: str, interval: str, period: str) -> pd.DataFrame:
    """
    Télécharge des bougies via l'API JSON publique de Yahoo Finance en
    HTTP brut (requests uniquement, sans yfinance). Compatible Pydroid/Android.
    C'est la même source de données que yfinance — mêmes résultats.
    """
    period2 = int(time.time())
    period1 = period2 - _period_to_seconds(period)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "interval": interval,
        "period1": period1,
        "period2": period2,
        "includePrePost": "false",
        "events": "div,splits",
    }
    try:
        r = requests.get(url, params=params, headers=_YAHOO_HEADERS, timeout=15)
        data = r.json()
    except Exception:
        return pd.DataFrame()

    try:
        result = data["chart"]["result"][0]
        ts     = result["timestamp"]
        quote  = result["indicators"]["quote"][0]
    except (KeyError, IndexError, TypeError):
        return pd.DataFrame()

    df = pd.DataFrame({
        "open":   quote.get("open"),
        "high":   quote.get("high"),
        "low":    quote.get("low"),
        "close":  quote.get("close"),
        "volume": quote.get("volume"),
    }, index=pd.to_datetime(ts, unit="s", utc=True))

    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["close", "high", "low"])
    return df


# app_id 1089 = app_id public/démo fourni par Deriv pour les requêtes en
# lecture (candles/ticks). Pour de la prod plus stable, créer son propre
# app_id gratuit sur https://developers.deriv.com (Manage Applications)
# et le mettre dans la variable d'env DERIV_APP_ID.
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"

DERIV_SYMBOLS: list[tuple[str, str]] = [
    ("R_75", "Volatility 75 Index"),
    ("R_25", "Volatility 25 Index"),
]

# Filtre haute-TF utilisé comme "biais" pour les indices Deriv (équivalent
# du H4 utilisé en Forex). Mettre "30m" si tu préfères M30 plutôt que H1.
DERIV_BIAS_TF = os.environ.get("DERIV_BIAS_TF", "15m")   # [v11] M15 pour tous les indices Volatility (V100/V50/V25)

_DERIV_GRANULARITY: dict[str, int] = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1d": 86400,
}


def is_deriv_symbol(symbol: str) -> bool:
    """Indices synthétiques Deriv (Volatility, et variantes 1s type 1HZ100V)."""
    return symbol.startswith("R_") or symbol.startswith("1HZ")


def fetch_deriv(symbol: str, interval: str, count: int = 300,
               retries: int = 3, retry_delay: int = 5) -> pd.DataFrame:
    """
    Récupère des bougies OHLC depuis l'API publique Deriv (WebSocket).
    Pas d'auth nécessaire — lecture de marché uniquement.
    Nécessite : pip install websocket-client
    """
    try:
        import websocket  # websocket-client
    except ImportError:
        log.warning("  ⚠️ Module 'websocket-client' manquant — pip install websocket-client")
        return pd.DataFrame()

    granularity = _DERIV_GRANULARITY.get(interval, 900)
    req = {
        "ticks_history": symbol,
        "style": "candles",
        "granularity": granularity,
        "count": count,
        "end": "latest",
        "adjust_start_time": 1,
    }

    for attempt in range(1, retries + 1):
        ws = None
        try:
            ws = websocket.create_connection(DERIV_WS_URL, timeout=15)
            ws.send(json.dumps(req))
            raw = ws.recv()
            ws.close()
            data = json.loads(raw)

            if "error" in data:
                raise RuntimeError(data["error"].get("message", "Erreur API Deriv"))

            candles = data.get("candles", [])
            if not candles:
                raise RuntimeError("réponse Deriv vide (pas de candles)")

            df = pd.DataFrame(candles)
            df.rename(columns={"epoch": "time"}, inplace=True)
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
            df.set_index("time", inplace=True)
            for col in ("open", "high", "low", "close"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["volume"] = 0.0  # pas de volume réel sur un indice synthétique
            df.dropna(subset=["close", "high", "low"], inplace=True)
            if not df.empty:
                return df
            raise RuntimeError("DataFrame vide après nettoyage")
        except Exception as e:
            try:
                if ws:
                    ws.close()
            except Exception:
                pass
            if attempt < retries:
                time.sleep(retry_delay * attempt)
            else:
                log.warning(f"  ⚠️ Deriv fetch échec {symbol} ({interval}) : {e}")
    return pd.DataFrame()


def fetch(symbol: str, interval: str, period: str = "5d",
          retries: int = 3, retry_delay: int = 15) -> pd.DataFrame:
    """
    [v10] Fetch dual-mode :
      • Mode A (par défaut)  : yfinance si disponible (_HAS_YFINANCE=True)
      • Mode B (fallback)    : _yahoo_chart_json() via requests pur
        → compatible Pydroid/Android (pas de lxml/curl_cffi/peewee requis)
    Les deux modes retournent le même format de DataFrame.
    """
    # Indices synthétiques Deriv → API dédiée (pas de Yahoo Finance)
    if is_deriv_symbol(symbol):
        return fetch_deriv(symbol, interval)

    periods_fallback = list(dict.fromkeys([period, "5d", "10d", "1mo"]))

    for p in periods_fallback:
        for attempt in range(1, retries + 1):
            try:
                # ── Mode A : yfinance ──────────────────────────────────
                if _HAS_YFINANCE:
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
                # ── Mode B : requests direct (sans yfinance) ───────────
                else:
                    df = _yahoo_chart_json(symbol, interval, p)

                if not df.empty:
                    # Normalise les colonnes minimales requises
                    for col in ("open", "high", "low", "close", "volume"):
                        if col not in df.columns:
                            df[col] = float("nan")
                    for _col in ("open", "high", "low", "close", "volume"):
                        if _col in df.columns:
                            df[_col] = pd.to_numeric(df[_col], errors="coerce")
                    df.dropna(subset=["close", "high", "low"], inplace=True)
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
        try:
            h_prev = df["high"].iloc[i-1]
            h_curr = df["high"].iloc[i]
            h_next = df["high"].iloc[i+1]
            if pd.isna(h_prev) or pd.isna(h_curr) or pd.isna(h_next):
                continue
            if h_curr > h_prev and h_curr > h_next:
                result.append((i, float(h_curr)))
        except (TypeError, ValueError):
            continue
    return result


def swing_lows(df: pd.DataFrame) -> list[tuple[int, float]]:
    """Retourne les swing lows (index, valeur)."""
    result = []
    for i in range(1, len(df) - 1):
        try:
            l_prev = df["low"].iloc[i-1]
            l_curr = df["low"].iloc[i]
            l_next = df["low"].iloc[i+1]
            if pd.isna(l_prev) or pd.isna(l_curr) or pd.isna(l_next):
                continue
            if l_curr < l_prev and l_curr < l_next:
                result.append((i, float(l_curr)))
        except (TypeError, ValueError):
            continue
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

    # Fenêtre d'analyse : AMD_LOOKBACK dernières bougies H4
    window    = df_h4.iloc[-AMD_LOOKBACK:]
    atr_full  = (df_h4["high"] - df_h4["low"]).rolling(14).mean()
    atr_now   = atr_full.iloc[-1]

    # ── 1. RANGE (Accumulation zone) ──────────────────────────
    # FIX v3.1 : fenêtres proportionnelles à AMD_LOOKBACK
    split         = AMD_LOOKBACK * 2 // 3   # = 20 pour lookback=30
    range_window  = window.iloc[:split]      # historique du range
    recent_window = window.iloc[split:]      # manipulation + distribution récentes

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


def detect_h4_balance_zone(df_h4: pd.DataFrame, direction: str,
                            lookback: int = 12) -> dict:
    """
    Détecte une zone de BALANCE / ÉQUILIBRE H4 (range de consolidation
    institutionnel) — alternative au Breaker Block H4 et à l'OB H4.

    Une "balance" = série de bougies H4 dont les hauts/bas restent
    compressés dans une bande étroite (≤ 1.6×ATR H4), signe d'accumulation
    ou de distribution avant un mouvement directionnel (AMD).

    Score bonus :
      +15 si le prix est actuellement DANS la balance (retest de l'équilibre)
      +10 si le prix vient tout juste de sortir de la balance dans le
          sens attendu (cassure directionnelle récente)

    Retourne dict {detected, top, bottom, mid, score_bonus, reason}
    """
    empty = {"detected": False, "top": None, "bottom": None, "mid": None,
              "score_bonus": 0, "reason": ""}

    if len(df_h4) < lookback + 5:
        return empty

    atr_h4 = (df_h4["high"] - df_h4["low"]).rolling(14).mean().iloc[-1]
    if pd.isna(atr_h4) or atr_h4 <= 0:
        return empty

    # Fenêtre de balance : on exclut les 3 dernières bougies (mouvement
    # récent éventuel de sortie de range)
    window = df_h4.iloc[-(lookback + 3):-3]
    if len(window) < lookback:
        return empty

    zone_top    = window["high"].max()
    zone_bottom = window["low"].min()
    zone_height = zone_top - zone_bottom

    # Une vraie balance = range compressé (pas une tendance étendue)
    if zone_height <= 0 or zone_height > atr_h4 * 1.6:
        return empty

    price_now = df_h4["close"].iloc[-1]
    zone_mid  = (zone_top + zone_bottom) / 2
    tol       = atr_h4 * 0.25

    in_zone = (zone_bottom - tol) <= price_now <= (zone_top + tol)

    # Cassure directionnelle récente (3 dernières bougies) hors de la balance
    recent = df_h4.iloc[-3:]
    if direction == "LONG":
        broke_out = recent["close"].iloc[-1] > zone_top
    else:
        broke_out = recent["close"].iloc[-1] < zone_bottom

    if in_zone:
        reason = (f"⚖️ Balance H4 [{round(zone_bottom, 5)}–{round(zone_top, 5)}] "
                   f"— prix en zone d'équilibre  (+15)")
        return {"detected": True, "top": zone_top, "bottom": zone_bottom,
                 "mid": zone_mid, "score_bonus": 15, "reason": reason}
    elif broke_out:
        reason = (f"⚖️ Balance H4 [{round(zone_bottom, 5)}–{round(zone_top, 5)}] "
                   f"— cassure directionnelle {direction}  (+10)")
        return {"detected": True, "top": zone_top, "bottom": zone_bottom,
                 "mid": zone_mid, "score_bonus": 10, "reason": reason}

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
    """
    Épaule-Tête-Épaule M15 — VERSION AMÉLIORÉE (priorité maximale).

    Critères stricts :
      SHORT : 3 swing highs (L épaule < Tête > R épaule), épaules symétriques ±30%,
              neckline cassée (close < neckline), retest neckline en cours = entrée.
      LONG  : Inverse H&S — 3 swing lows symétriques, neckline cassée à la hausse.

    Amélioration vs version originale :
      • Fenêtre élargie à 80 bougies M15 (= 20h, capture patterns complets)
      • Symétrie épaules validée (hauteurs ± 30%)
      • Retest neckline détecté (= entrée optimale = prix revient sur neckline après cassure)
      • Score : 30 pts (standard) → 38 pts (avec retest neckline) → 42 pts (symétrie parfaite)
      • Neckline inclinée acceptée (max 15° de pente)
    """
    empty = PatternResult(False, "", direction, 0, "")
    if len(df) < 40:
        return empty

    window = df.iloc[-80:]   # 80 bougies M15 = ~20h
    atr    = (window["high"] - window["low"]).rolling(14).mean().iloc[-1]
    if pd.isna(atr) or atr == 0:
        return empty

    price_now = window["close"].iloc[-1]

    if direction == "SHORT":
        pts = _swing_points(window, col_high=True)
        if len(pts) < 3:
            return empty
        for i in range(len(pts) - 1, 1, -1):
            idx_r, h_r = pts[i]
            idx_h, h_h = pts[i - 1]
            idx_l, h_l = pts[i - 2]

            # Structure ETE : tête plus haute que les deux épaules
            if not (h_h > h_r and h_h > h_l):
                continue
            # Espacement minimal entre les points (au moins 4 bougies chacun)
            if not ((idx_h - idx_l) >= 4 and (idx_r - idx_h) >= 4):
                continue
            # Symétrie épaules : écart max 30% de la hauteur de tête
            shoulder_sym = abs(h_r - h_l) / (h_h - min(h_l, h_r) + 1e-10)
            if shoulder_sym > 0.30:
                continue

            # Neckline : ligne reliant les creux entre épaules et tête
            neckline_left  = window["low"].iloc[idx_l:idx_h].min()
            neckline_right = window["low"].iloc[idx_h:idx_r].min()
            neckline       = (neckline_left + neckline_right) / 2

            # Cassure de la neckline (close en dessous)
            neckline_broken = price_now < neckline + atr * 0.3

            if not neckline_broken:
                continue

            # Retest neckline = entrée optimale (prix revient sur neckline après cassure)
            retest = (neckline - atr * 0.5) <= price_now <= (neckline + atr * 0.8)

            # Score selon qualité
            if retest and shoulder_sym < 0.15:
                score = 42   # Symétrie parfaite + retest = setup premium
                desc  = f"⭐ ETE M15 PREMIUM — retest neckline @ {round(neckline,5)}"
            elif retest:
                score = 38   # Retest neckline = entrée optimale
                desc  = f"🎯 ETE M15 — retest neckline @ {round(neckline,5)}"
            else:
                score = 30   # Cassure sans retest
                desc  = f"📐 ETE M15 — neckline cassée @ {round(neckline,5)}"

            desc += f" | tête={round(h_h,5)} épaules≈{round((h_l+h_r)/2,5)}"
            return PatternResult(True, "Épaule-Tête-Épaule 🏔️", "SHORT", score, desc)

    else:  # LONG — ETE inversé
        pts = _swing_points(window, col_high=False)
        if len(pts) < 3:
            return empty
        for i in range(len(pts) - 1, 1, -1):
            idx_r, l_r = pts[i]
            idx_h, l_h = pts[i - 1]
            idx_l, l_l = pts[i - 2]

            if not (l_h < l_r and l_h < l_l):
                continue
            if not ((idx_h - idx_l) >= 4 and (idx_r - idx_h) >= 4):
                continue

            shoulder_sym = abs(l_r - l_l) / (min(l_l, l_r) - l_h + 1e-10)
            if shoulder_sym > 0.30:
                continue

            neckline_left  = window["high"].iloc[idx_l:idx_h].max()
            neckline_right = window["high"].iloc[idx_h:idx_r].max()
            neckline       = (neckline_left + neckline_right) / 2

            neckline_broken = price_now > neckline - atr * 0.3
            if not neckline_broken:
                continue

            retest = (neckline - atr * 0.8) <= price_now <= (neckline + atr * 0.5)

            if retest and shoulder_sym < 0.15:
                score = 42
                desc  = f"⭐ ETE Inversé M15 PREMIUM — retest neckline @ {round(neckline,5)}"
            elif retest:
                score = 38
                desc  = f"🎯 ETE Inversé M15 — retest neckline @ {round(neckline,5)}"
            else:
                score = 30
                desc  = f"📐 ETE Inversé M15 — neckline cassée @ {round(neckline,5)}"

            desc += f" | tête={round(l_h,5)} épaules≈{round((l_l+l_r)/2,5)}"
            return PatternResult(True, "ETE Inversé 🏔️", "LONG", score, desc)

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
    """
    Lance tous les détecteurs sur H4 + M15.

    PRIORITÉ DES SETUPS (ordre de score décroissant) :
      1. ETE M15 premium (retest neckline)          → 42 pts
      2. ETE M15 standard (neckline cassée)          → 30–38 pts
      3. Double/Triple Top/Bottom                    → 20–22 pts
      4. Wedge, Flag, Triangle                       → 12–16 pts

    M15 est scanné EN PREMIER — priorité sur H4.
    """
    results = []

    # ── M15 EN PRIORITÉ (ton timeframe d'entrée) ──────────────
    results.append(detect_head_shoulders(df_ltf, direction))   # ETE M15 — priorité max
    results.append(detect_double_top_bottom(df_ltf, direction))
    results.append(detect_triple_top_bottom(df_ltf, direction))
    results.append(detect_wedge(df_ltf, direction))
    results.append(detect_triangle(df_ltf, direction))
    results.append(detect_flag(df_ltf, direction))

    # ── H4 EN SUPPORT (confirmation HTF) ──────────────────────
    results.append(detect_head_shoulders(df_h4, direction))
    results.append(detect_double_top_bottom(df_h4, direction))
    results.append(detect_triple_top_bottom(df_h4, direction))
    results.append(detect_wedge(df_h4, direction))
    results.append(detect_triangle(df_h4, direction))
    results.append(detect_flag(df_h4, direction))

    detected = [p for p in results if p.detected]

    # Bonus si ETE M15 + autre confluence H4
    ete_m15   = any("Épaule" in p.pattern_name or "ETE" in p.pattern_name for p in detected[:6])
    h4_confirm = any(p for p in detected[6:])
    if ete_m15 and h4_confirm:
        # Boost le score ETE de 5 pts supplémentaires (confluence HTF)
        for p in detected:
            if "Épaule" in p.pattern_name or "ETE" in p.pattern_name:
                object.__setattr__(p, 'score_bonus', min(p.score_bonus + 5, 50))
                object.__setattr__(p, 'description', p.description + " + confluence H4 ✓")
                break

    return detected


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

def _sl_in_liquidity_zone(
    sl:        float,
    direction: str,
    atr:       float,
    df_m5:     pd.DataFrame,
    liq_map:   Optional[LiquidityMap],
    ob:        Optional[OrderBlock],
    tol:       float = 0.15,
) -> bool:
    """
    Vérifie si le SL calculé tombe dans une zone de liquidité évidente :
      1. Juste derrière un plus haut/bas structurel connu (BSL/SSL/EQH/EQL H4)
      2. Accumulation de mèches M5 récentes autour du niveau du SL
      3. À l'intérieur de l'Order Block

    tol = tolérance en multiples d'ATR pour considérer un niveau "proche" du SL.
    """
    band = atr * tol

    # ── 1. Proche d'un plus haut/bas structurel évident ────────
    if liq_map is not None:
        levels = []
        if direction == "LONG":
            levels = list(liq_map.ssl_levels) + list(liq_map.eql_levels)
            if liq_map.nearest_ssl is not None:
                levels.append(liq_map.nearest_ssl)
            if liq_map.pdl is not None:
                levels.append(liq_map.pdl)
        else:
            levels = list(liq_map.bsl_levels) + list(liq_map.eqh_levels)
            if liq_map.nearest_bsl is not None:
                levels.append(liq_map.nearest_bsl)
            if liq_map.pdh is not None:
                levels.append(liq_map.pdh)
        if any(abs(sl - lvl) <= band for lvl in levels if lvl is not None):
            return True

    # ── 2. Accumulation de mèches M5 autour du SL ───────────────
    if len(df_m5) >= 10:
        recent = df_m5.iloc[-15:]
        wick_hits = 0
        for i in range(len(recent)):
            h, l = recent["high"].iloc[i], recent["low"].iloc[i]
            level = l if direction == "LONG" else h   # mèche du côté du SL
            if abs(level - sl) <= band:
                wick_hits += 1
        if wick_hits >= 3:   # ≥3 mèches proches = accumulation
            return True

    # ── 3. SL à l'intérieur de l'Order Block ────────────────────
    if ob is not None:
        lo, hi = min(ob.bottom, ob.top), max(ob.bottom, ob.top)
        if lo <= sl <= hi:
            return True

    return False


def compute_sl_tp_v3(
    df_m5:     pd.DataFrame,
    df_m15:    pd.DataFrame,
    direction: str,
    ob:        Optional[OrderBlock],
    fvg:       Optional[FVG],
    sd_zone:   Optional[SupplyDemandZone],
    liq_map:   Optional[LiquidityMap],
    symbol:    str = "",
    score:     int = 0,
) -> tuple[float, float, float, float, float, float]:
    """
    Entry / SL / TP1 / TP2 / TP3 v3+ — cibles structurelles réelles.

    ENTRÉE (priorité décroissante) :
      1. Milieu de la zone Supply/Demand
      2. Milieu du FVG M5
      3. Close M5 courant

    STOP LOSS (palier selon la qualité du setup — `score`) :
      score ≥ 90     : derrière l'OB + 1.0×ATR
      score 80–89    : derrière l'OB + 0.6×ATR
      score 70–79    : 0.6×ATR fixe depuis l'entrée (pas de référence OB)
      (score < 70 : aucun signal — filtré en amont)
      Puis règle anti-liquidité : si le SL tombe derrière un plus haut/bas
      évident, dans une accumulation de mèches, ou à l'intérieur de l'OB,
      il est repoussé de +0.10 à 0.20×ATR au-delà de cette zone.

    TAKE PROFIT (3 niveaux structurels) :
      TP1 : RR3 min — BSL/SSL nearest ou PDH/PDL ou swing M15
      TP2 : RR5-6  — swing H/L M15 suivant au-delà de TP1
      TP3 : RR8-10 — liquidité majeure H4 / swing H4 / extension max
    """
    atr    = (df_m5["high"] - df_m5["low"]).rolling(14).mean().iloc[-1]
    close  = df_m5["close"].iloc[-1]
    spread = get_spread(symbol) if symbol else 0.0
    dec    = 2 if close > 100 else 5

    # ── 1. ENTRÉE ─────────────────────────────────────────────
    if sd_zone is not None:
        entry = round((sd_zone.top + sd_zone.bottom) / 2, dec)
    elif fvg is not None:
        entry = round((max(fvg.top, fvg.bottom) + min(fvg.top, fvg.bottom)) / 2, dec)
    else:
        entry = round(close, dec)

    # ── 2. STOP LOSS — palier de base selon le score ────────────
    # Setups haute qualité : SL ancré derrière l'OB (probabilité élevée
    # que le prix ne revienne pas dedans). Setups standards : SL fixe
    # depuis l'entrée, comme avant.
    atr_sl_mult = get_atr_sl_mult()
    if score >= 90 and ob is not None:
        anchor, sl_distance = (ob.bottom if direction == "LONG" else ob.top), atr * 1.0
    elif score >= 80 and ob is not None:
        anchor, sl_distance = (ob.bottom if direction == "LONG" else ob.top), atr * atr_sl_mult
    else:
        anchor, sl_distance = entry, atr * atr_sl_mult

    if direction == "LONG":
        sl = round(anchor - sl_distance, dec)
    else:  # SHORT
        sl = round(anchor + sl_distance, dec)

    # ── 2b. Règle anti-liquidité ─────────────────────────────
    # Si le SL tombe dans une zone de liquidité évidente (plus haut/bas
    # structurel, accumulation de mèches, ou intérieur de l'OB), on le
    # repousse de 0.10 à 0.20×ATR au-delà de cette zone plutôt que de le
    # laisser exposé à un simple retest.
    if _sl_in_liquidity_zone(sl, direction, atr, df_m5, liq_map, ob):
        extra = atr * 0.15   # milieu de la fourchette 0.10–0.20×ATR
        sl = round(sl - extra, dec) if direction == "LONG" else round(sl + extra, dec)

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
    # TP1 = RR3 minimum, TP2 = RR5, TP3 = RR6+
    rr3_min = entry + risk * 3.0 if direction == "LONG" else entry - risk * 3.0
    rr5_min = entry + risk * 5.0 if direction == "LONG" else entry - risk * 5.0
    rr6_min = entry + risk * 6.0 if direction == "LONG" else entry - risk * 6.0

    if targets:
        # TP1 : première cible structurelle >= RR3
        tp1_cands = [t for t in targets if (t >= rr3_min if direction == "LONG" else t <= rr3_min)]
        tp1 = round(tp1_cands[0], dec) if tp1_cands else round(rr3_min, dec)
    else:
        tp1 = round(rr3_min, dec)

    # TP2 : cible structurelle suivante >= RR5, sinon RR5 mathematique
    if targets:
        tp2_cands = [t for t in targets if (t >= rr5_min if direction == "LONG" else t <= rr5_min)
                     and t != tp1]
        tp2 = round(tp2_cands[0], dec) if tp2_cands else round(rr5_min, dec)
    else:
        tp2 = round(rr5_min, dec)

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

    # ── 5. GARANTIR L'ORDRE TP1 < TP2 < TP3 (LONG) / TP1 > TP2 > TP3 (SHORT) ──
    if direction == "LONG":
        tp1, tp2, tp3 = tuple(sorted([tp1, tp2, tp3]))
    else:
        tp1, tp2, tp3 = tuple(sorted([tp1, tp2, tp3], reverse=True))

    # ── 6. RR net sur TP1 ─────────────────────────────────────
    if direction == "LONG":
        gain_net = (tp1 - entry) - spread
    else:
        gain_net = (entry - tp1) - spread

    rr_net = round(gain_net / risk, 2) if gain_net > 0 and risk > 0 else 0.0
    return entry, sl, tp1, rr_net, tp2, tp3


# ═════════════════════════════════════════════════════════════
#  SETUP BOS RETEST — BOS → X (sweep) → OB/FVG → Continuation
#  + toutes confluences : Order Flow, Breaker, BSL/SSL, OB H4
# ═════════════════════════════════════════════════════════════

def detect_bos_retest_setup(
    df_m15:       pd.DataFrame,
    df_m5:        pd.DataFrame,
    direction:    str,
    bos_list:     list,
    obs_m15:      list,
    fvg_active,
    liq_taken:    bool,
    price_now:    float,
    atr:          float,
    ofs_detected: bool = False,
    ofs_bonus:    int  = 0,
    breaker_ok:   bool = False,
    bsl_ssl_swept:bool = False,
    ob_htf_active:bool = False,
) -> dict:
    """BOS → X (stop hunt) → retracement OB/FVG → continuation."""
    result = {"detected": False, "score_bonus": 0, "reasons": [], "retest_zone": None}
    if not bos_list:
        return result

    last_bos = bos_list[-1]
    expected = "bearish" if direction == "SHORT" else "bullish"
    if last_bos.get("type", "") != expected:
        return result

    bos_level = last_bos.get("level", 0.0)
    score, reasons = 0, []
    reasons.append(f"✅ BOS M15 {direction} @ {round(bos_level,5)}  (+15)")
    score += 15

    ob_zone = next((o for o in reversed(obs_m15) if o.direction == expected), None)
    in_ob   = False
    if ob_zone:
        ob_lo = min(ob_zone.top, ob_zone.bottom)
        ob_hi = max(ob_zone.top, ob_zone.bottom)
        if (ob_lo - atr*0.5) <= price_now <= (ob_hi + atr*0.5):
            in_ob = True
            reasons.append(f"🏛️ Older Block M15 [{round(ob_lo,5)}–{round(ob_hi,5)}]  (+12)")
            score += 12
            result["retest_zone"] = ob_zone

    in_fvg = False
    if fvg_active is not None:
        flo = min(fvg_active.top, fvg_active.bottom)
        fhi = max(fvg_active.top, fvg_active.bottom)
        if (flo - atr*0.3) <= price_now <= (fhi + atr*0.3):
            in_fvg = True
            reasons.append(f"📍 Valid FVG M15 [{round(flo,5)}–{round(fhi,5)}]  (+10)")
            score += 10

    if liq_taken:
        reasons.append("💧 Liquidité prise — point X (stop hunt)  (+10)")
        score += 10
    if in_ob and in_fvg:
        reasons.append("⚡ Confluence OB + FVG = zone institutionnelle  (+8)")
        score += 8
    if ofs_detected and ofs_bonus > 0:
        pts = min(ofs_bonus, 12)
        reasons.append(f"📈 Order Flow Shift M15 aligné  (+{pts})")
        score += pts
    if breaker_ok:
        reasons.append("🔥 Breaker Block M15  (+7)")
        score += 7
    if bsl_ssl_swept:
        reasons.append("💧 BSL/SSL sweepée — hunt institutionnel  (+8)")
        score += 8
    if ob_htf_active:
        reasons.append("🏛️ Older Block H4 actif — confluence HTF  (+5)")
        score += 5

    if not (in_ob or in_fvg):
        return result

    result.update({"detected": True, "score_bonus": score, "reasons": reasons})
    return result


# ═════════════════════════════════════════════════════════════
#  SETUP PURE M15 : SWEEP SSL/BSL → BOS M15 → PULLBACK BREAKER
#
#  Séquence obligatoire (celle visible dans toutes tes images) :
#    ① Le prix sweep un SSL (LONG) ou BSL (SHORT) sur M15
#       → chasse les stops institutionnels
#    ② Juste après le sweep : BOS M15 dans la direction opposée
#       → premier signe de changement de structure
#    ③ Le prix pullback dans la zone naturelle (Breaker naturel =
#       dernière bougie qui a causé le BOS, ou FVG créé lors du BOS)
#       → zone d'entrée institutionnelle (point X)
#    ④ Entrée sur bougie de confirmation dans la zone pullback
#
#  Ce setup est différent de :
#    - detect_smc_trader   : nécessite H4 → M15 → M5 et MSS
#    - detect_bos_retest   : pas de sweep SSL/BSL obligatoire
#
#  Score bonus max : 30 pts
#    +12 sweep SSL/BSL confirmé
#    +10 BOS M15 post-sweep validé
#    +8  prix dans zone pullback (OB naturel / FVG post-BOS)
# ═════════════════════════════════════════════════════════════

def detect_sweep_bos_m15_setup(
    df_m15:    pd.DataFrame,
    direction: str,
    liq_map:   "LiquidityMap",
    atr:       float,
) -> dict:
    """
    Détecte la séquence pure M15 :
      ① Sweep SSL (LONG) ou BSL (SHORT) sur M15
      ② BOS M15 post-sweep dans la direction du trade
      ③ Pullback dans la zone naturelle (Breaker / FVG post-BOS)

    Paramètres :
      df_m15    : données M15 (au moins 40 bougies)
      direction : "LONG" | "SHORT"
      liq_map   : LiquidityMap (pour vérifier swept_ssl/swept_bsl)
      atr       : ATR M15 courant

    Retourne dict :
      detected      : bool
      sweep_level   : float | None  — niveau SSL/BSL sweepé
      bos_level     : float | None  — niveau BOS post-sweep
      pullback_zone : tuple[float,float] | None  — zone naturelle (low, high)
      score_bonus   : int
      reasons       : list[str]
    """
    empty = {
        "detected"     : False,
        "sweep_level"  : None,
        "bos_level"    : None,
        "pullback_zone": None,
        "score_bonus"  : 0,
        "reasons"      : [],
    }

    if len(df_m15) < 40 or atr <= 0:
        return empty

    window    = df_m15.iloc[-60:].reset_index(drop=True)
    n         = len(window)
    price_now = window["close"].iloc[-1]
    score     = 0
    reasons   = []

    # ── ① SWEEP SSL (LONG) ou BSL (SHORT) ────────────────────────
    # On cherche une bougie récente (dans les 20 dernières) qui a
    # percé un swing low/high puis clôturé au-dessus/dessous.
    sweep_idx   = None
    sweep_level = None

    if direction == "LONG":
        # Chercher un SSL sweep : low perce un swing low récent puis close >  swing low
        for i in range(n - 20, n - 1):
            if i < 5:
                continue
            swing_lo = window["low"].iloc[max(0, i-10):i].min()
            bar_lo   = window["low"].iloc[i]
            bar_cl   = window["close"].iloc[i]
            # Bougie a percé sous le swing low puis refermé au-dessus → sweep
            if bar_lo < swing_lo - atr * 0.05 and bar_cl > swing_lo - atr * 0.10:
                sweep_idx   = i
                sweep_level = swing_lo
                break   # on prend le plus récent

    else:  # SHORT
        # Chercher un BSL sweep : high perce un swing high récent puis close < swing high
        for i in range(n - 20, n - 1):
            if i < 5:
                continue
            swing_hi = window["high"].iloc[max(0, i-10):i].max()
            bar_hi   = window["high"].iloc[i]
            bar_cl   = window["close"].iloc[i]
            if bar_hi > swing_hi + atr * 0.05 and bar_cl < swing_hi + atr * 0.10:
                sweep_idx   = i
                sweep_level = swing_hi
                break

    if sweep_idx is None or sweep_level is None:
        return empty

    score += 12
    sweep_dir = "SSL" if direction == "LONG" else "BSL"
    reasons.append(
        f"💧 Sweep {sweep_dir} M15 @ {round(sweep_level, 5)}  (+12)"
    )

    # ── ② BOS M15 POST-SWEEP ──────────────────────────────────────
    # On cherche un BOS dans la bonne direction APRÈS la bougie sweep
    bos_idx   = None
    bos_level = None
    search_from = sweep_idx + 1

    if direction == "LONG":
        # BOS haussier : close > swing high des N bougies avant
        for i in range(search_from, n):
            lookback_start = max(sweep_idx - 5, 0)
            swing_hi_ref   = window["high"].iloc[lookback_start:i].max()
            if window["close"].iloc[i] > swing_hi_ref:
                bos_idx   = i
                bos_level = swing_hi_ref
                break
    else:  # SHORT
        # BOS baissier : close < swing low des N bougies avant
        for i in range(search_from, n):
            lookback_start = max(sweep_idx - 5, 0)
            swing_lo_ref   = window["low"].iloc[lookback_start:i].min()
            if window["close"].iloc[i] < swing_lo_ref:
                bos_idx   = i
                bos_level = swing_lo_ref
                break

    if bos_idx is None or bos_level is None:
        return empty   # Pas de BOS post-sweep → séquence invalide

    score += 10
    bos_dir_lbl = "haussier" if direction == "LONG" else "baissier"
    reasons.append(
        f"📐 BOS M15 {bos_dir_lbl} post-sweep @ {round(bos_level, 5)}  (+10)"
    )

    # ── ③ ZONE PULLBACK = BREAKER NATUREL ────────────────────────
    # La zone naturelle est la dernière bougie impulse qui a causé le BOS
    # (ou le FVG créé lors de l'impulsion post-sweep).
    # On cherche la bougie de BOS elle-même et la bougie juste avant.
    pullback_zone = None

    if bos_idx >= 1:
        # Bougie qui a cassé la structure
        bos_candle_hi = window["high"].iloc[bos_idx]
        bos_candle_lo = window["low"].iloc[bos_idx]
        # Bougie précédente (OB naturel)
        prev_hi = window["high"].iloc[bos_idx - 1]
        prev_lo = window["low"].iloc[bos_idx - 1]

        if direction == "LONG":
            # Zone pullback = corps de la bougie OB naturel (dernière bougie baissière avant le BOS haussier)
            ob_hi = max(bos_candle_hi, prev_hi)
            ob_lo = min(bos_candle_lo, prev_lo)
            pullback_zone = (ob_lo, ob_hi)
        else:
            ob_hi = max(bos_candle_hi, prev_hi)
            ob_lo = min(bos_candle_lo, prev_lo)
            pullback_zone = (ob_lo, ob_hi)

    # Vérifier si le prix courant est dans la zone pullback (ou proche)
    in_pullback = False
    if pullback_zone is not None:
        pb_lo, pb_hi = pullback_zone
        tol = atr * 0.5
        in_pullback = (pb_lo - tol) <= price_now <= (pb_hi + tol)

    if in_pullback and pullback_zone is not None:
        score += 8
        reasons.append(
            f"🏛️ Prix dans zone pullback / Breaker naturel "
            f"[{round(pullback_zone[0], 5)}–{round(pullback_zone[1], 5)}]  (+8)"
        )
    else:
        # Pas dans la zone → séquence détectée mais entrée non encore optimale
        # On retourne quand même "detected" pour le mode, mais score réduit
        reasons.append(
            f"⏳ Attente pullback dans Breaker naturel "
            f"[{round(pullback_zone[0] if pullback_zone else 0, 5)}–"
            f"{round(pullback_zone[1] if pullback_zone else 0, 5)}]"
        )
        # Séquence ①② valide mais ③ pas encore → score sans bonus pullback
        # On retourne quand même si score ≥ 22 (sweep + BOS)
        if score < 20:
            return empty

    return {
        "detected"     : True,
        "sweep_level"  : sweep_level,
        "bos_level"    : bos_level,
        "pullback_zone": pullback_zone,
        "in_pullback"  : in_pullback,
        "score_bonus"  : min(score, 30),
        "reasons"      : reasons,
    }


# ═════════════════════════════════════════════════════════════
#  MOTEUR PRINCIPAL v3 — H4 → M15 → M5
# ═════════════════════════════════════════════════════════════


# ═════════════════════════════════════════════════════════════
#   SMC SIGNAL ENGINE  v4  — MULTI-SETUP INDÉPENDANT
#   ─────────────────────────────────────────────────────────
#   Architecture :
#     Chaque setup est autonome — aucun ne dépend des autres.
#     Un signal BREAKER peut partir sans AMD.
#     Un signal AMD peut partir sans Breaker.
#
#   Priorités (T = Tier) :
#     T1 🥇  BREAKER     Sweep BSL/SSL + Breaker + Retest + Bougie
#     T2 🥈  ORDER BLOCK OB H4/M15 + BOS + FVG
#     T3 🥉  BOS_RETEST  BOS M15 + Retest + Confirmation
#     T4     AMD         Accumulation + Manipulation + Distribution
#     T5     FVG         FVG non mitiqué + Prix dans zone
#     T6     MSS/CHoCH   CHoCH M15 + Equal Liq sweep
# ═════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────
#  DATACLASS SIGNAL v4 — commun à tous les setups
# ─────────────────────────────────────────────────────────────

@dataclass
class SetupSignal:
    """Signal produit par un module indépendant."""
    symbol:     str
    setup_type: str      # "BREAKER" | "OB" | "BOS" | "AMD" | "FVG" | "MSS"
    tier:       int      # 1 (meilleur) → 6
    direction:  str      # "LONG" | "SHORT"
    entry:      float
    sl:         float
    tp:         float
    tp2:        float
    tp3:        float
    rr:         float
    score:      int      # 0–100
    reasons:    list     = field(default_factory=list)
    timestamp:  object   = field(default_factory=lambda: datetime.now(timezone.utc))
    htf_bias:   str      = "NEUTRAL"
    lot:        float    = 0.0
    df_chart:   object   = field(default=None, repr=False)
    fvg_ref:    object   = field(default=None, repr=False)
    ob_ref:     object   = field(default=None, repr=False)
    stars:      int      = 3   # 2⭐ = entrée directe (sweep+CHoCH) | 3⭐ = entrée confirmée (retest+rejet)

    def to_signal(self) -> "Signal":
        """Convertit en Signal v3 pour réutiliser tg_notify et generate_chart_image."""
        return Signal(
            symbol    = self.symbol,
            direction = self.direction,
            entry     = self.entry,
            sl        = self.sl,
            tp        = self.tp,
            rr        = self.rr,
            score     = self.score,
            timestamp = self.timestamp,
            htf_bias  = self.htf_bias,
            lot       = self.lot,
            mode      = self.setup_type,
            reasons   = self.reasons,
            df_chart  = self.df_chart,
            fvg_chart = self.fvg_ref,
            ob_chart  = self.ob_ref,
            tp2       = self.tp2,
            tp3       = self.tp3,
            stars     = self.stars,
        )


# ─────────────────────────────────────────────────────────────
#  HELPERS PARTAGÉS PAR LES 6 MODULES
# ─────────────────────────────────────────────────────────────

def _fetch_data(symbol: str) -> tuple:
    """
    Télécharge H4 / H1 / M15 / M5 (+ M1 pour Gold/BTC) une seule fois.
    Retourne (df_h4, df_h1, df_m15, df_m5, df_m1).
    df_m1 est vide (DataFrame) pour tous les symboles SAUF Gold et BTC.
    H1 est utilisé par le module Supply/Demand (T2) pour détecter les zones institutionnelles.

    Indices Deriv (Volatility) : pas de session réelle (24/7), H4 est moins
    pertinent comme filtre que sur du Forex classique. On utilise H1 (ou M30
    via DERIV_BIAS_TF) comme biais — placé à la fois dans le slot H4 et H1
    pour ne rien casser dans les modules qui lisent l'un ou l'autre.

    [v11] Gold (GC=F) et BTC (BTC-USD) : pipeline dédié M15→M1.
    Sur ces 2 actifs uniquement, le module Supply/Demand (T2) utilise le
    M15 comme timeframe de référence de zone/tendance (à la place du H1)
    et le M1 comme timeframe de confirmation d'entrée (à la place du M5),
    pour une réactivité accrue sur ces marchés très volatils.
    """
    if is_deriv_symbol(symbol):
        df_bias = fetch(symbol, DERIV_BIAS_TF, period="n/a")
        df_m15  = fetch(symbol, "15m", period="n/a")
        df_m5   = fetch(symbol, "5m",  period="n/a")
        if df_bias.empty or df_m15.empty or df_m5.empty:
            return None, None, None, None, None
        return df_bias, df_bias, df_m15, df_m5, pd.DataFrame()

    ltf_p   = "2d"
    mtf_p   = "5d"
    h1_p    = "15d"

    df_h4  = fetch(symbol, "4h",  period="30d")
    df_h1  = fetch(symbol, "1h",  period=h1_p)
    df_m15 = fetch(symbol, "15m", period=mtf_p)
    df_m5  = fetch(symbol, "5m",  period=ltf_p)

    if df_h4.empty or df_m15.empty or df_m5.empty:
        return None, None, None, None, None
    # df_h1 peut être vide sur certains instruments — on retourne un df vide plutôt que None
    if df_h1.empty:
        df_h1 = pd.DataFrame()

    # ── [v13] M1 non utilisé par la stratégie unique (Sweep+Cassure) ──
    # Conservé vide pour ne pas casser la signature de retour à 5 éléments.
    df_m1 = pd.DataFrame()

    return df_h4, df_h1, df_m15, df_m5, df_m1


def _direction_from_bias(bias: str) -> Optional[str]:
    if bias == "BULLISH": return "LONG"
    if bias == "BEARISH": return "SHORT"
    return None


def _compute_levels(
    symbol: str, direction: str,
    df_m5: pd.DataFrame, df_m15: pd.DataFrame,
    ob: Optional[OrderBlock] = None,
    fvg: Optional[FVG] = None,
    sd_zone: Optional[SupplyDemandZone] = None,
    liq_map: Optional[LiquidityMap] = None,
    score: int = 0,
) -> tuple:
    """Wrapper compute_sl_tp_v3 → (entry, sl, tp1, rr, tp2, tp3)"""
    return compute_sl_tp_v3(
        df_m5=df_m5, df_m15=df_m15, direction=direction,
        ob=ob, fvg=fvg, sd_zone=sd_zone, liq_map=liq_map, symbol=symbol,
        score=score,
    )


def _m15_candle_confirmed(df_m15: pd.DataFrame, direction: str) -> bool:
    """Vérifie que la dernière bougie M15 est clôturée dans la bonne direction (corps ≥ 40%)."""
    if len(df_m15) < 2:
        return False
    o  = df_m15["open"].iloc[-1]
    cl = df_m15["close"].iloc[-1]
    rng = df_m15["high"].iloc[-1] - df_m15["low"].iloc[-1]
    body_ratio = abs(cl - o) / rng if rng > 0 else 0
    if direction == "LONG"  and cl > o and body_ratio >= 0.40: return True
    if direction == "SHORT" and cl < o and body_ratio >= 0.40: return True
    return False


# ─────────────────────────────────────────────────────────────
#  v7 — VALIDATEUR CENTRAL D'ENTRÉE STRATÉGIQUE M15
#  Appliqué sur TOUS les 7 modules avant d'envoyer un signal
# ─────────────────────────────────────────────────────────────

def _validate_strategic_entry_m15(
    symbol: str,
    direction: str,
    df_m15: pd.DataFrame,
    df_m5: pd.DataFrame,
    df_h4: pd.DataFrame,
    df_h1: Optional[pd.DataFrame] = None,
) -> tuple[bool, list[str], int]:
    """
    Filtre d'entrée stratégique M15 — v7.

    Règles OBLIGATOIRES (si l'une échoue → retourne False) :
      ① Prix dans OB M15/H4  OU  Zone S/D H1/H4
      ② Bougie M15 : Englobante OU Mèche de rejet (wick ≥ 2× corps)
      ③ BOS M15 présent dans les 5 dernières bougies (avant ou après entrée)

    Bonus (ne bloque pas) :
      ④ Liquidité sweepée avant l'entrée → +15 pts

    Retourne : (valide: bool, raisons: list[str], bonus_score: int)
    """
    reasons: list[str] = []
    bonus   = 0
    expected = "bullish" if direction == "LONG" else "bearish"

    if len(df_m15) < 5 or len(df_m5) < 5 or len(df_h4) < 10:
        return False, ["⛔ Données insuffisantes pour validation entrée"], 0

    price_now = df_m5["close"].iloc[-1]
    atr_m5    = (df_m5["high"] - df_m5["low"]).rolling(14).mean().iloc[-1]
    atr_m15   = (df_m15["high"] - df_m15["low"]).rolling(14).mean().iloc[-1]
    if pd.isna(atr_m5) or atr_m5 == 0:
        return False, ["⛔ ATR M5 invalide"], 0

    # ── ① Zone stratégique : OB ou S/D ───────────────────────
    in_strategic_zone = False
    zone_label        = ""

    # Test OB M15
    bos_m15_list = detect_bos(df_m15)
    obs_m15      = detect_order_blocks(df_m15, bos_m15_list)
    ob_m15_match = next(
        (o for o in reversed(obs_m15)
         if o.direction == expected and
            (min(o.top, o.bottom) - atr_m5 * 0.4) <= price_now <= (max(o.top, o.bottom) + atr_m5 * 0.4)),
        None
    )
    if ob_m15_match:
        in_strategic_zone = True
        zone_label = f"OB M15 [{round(ob_m15_match.bottom, 5)}–{round(ob_m15_match.top, 5)}]"

    # Test OB H4 si OB M15 absent
    if not in_strategic_zone:
        bos_h4  = detect_bos(df_h4)
        obs_h4  = detect_order_blocks(df_h4, bos_h4)
        ob_h4_match = next(
            (o for o in reversed(obs_h4)
             if o.direction == expected and
                min(o.top, o.bottom) <= price_now <= max(o.top, o.bottom)),
            None
        )
        if ob_h4_match:
            in_strategic_zone = True
            zone_label = f"OB H4 [{round(ob_h4_match.bottom, 5)}–{round(ob_h4_match.top, 5)}]"

    # Test Zone S/D H1 si disponible
    if not in_strategic_zone and df_h1 is not None and not df_h1.empty and len(df_h1) >= 10:
        atr_h1 = (df_h1["high"] - df_h1["low"]).rolling(14).mean().iloc[-1]
        if not pd.isna(atr_h1) and atr_h1 > 0:
            sd_zone_type = "demand" if direction == "LONG" else "supply"
            zones_h1 = detect_supply_demand_zones(df_h1, sd_zone_type)
            active_z = price_in_sd_zone(price_now, zones_h1, atr_h1)
            if active_z is None:
                tol = atr_h1 * 0.5
                for z in zones_h1[:3]:
                    if (z.bottom - tol) <= price_now <= (z.top + tol):
                        active_z = z
                        break
            if active_z:
                in_strategic_zone = True
                zone_label = f"Zone S/D H1 [{round(active_z.bottom, 5)}–{round(active_z.top, 5)}]"

    # Test Zone S/D H4 en dernier recours
    if not in_strategic_zone:
        atr_h4 = (df_h4["high"] - df_h4["low"]).rolling(14).mean().iloc[-1]
        if not pd.isna(atr_h4) and atr_h4 > 0:
            sd_zone_type = "demand" if direction == "LONG" else "supply"
            zones_h4 = detect_supply_demand_zones(df_h4, sd_zone_type)
            active_z = price_in_sd_zone(price_now, zones_h4, atr_h4)
            if active_z:
                in_strategic_zone = True
                zone_label = f"Zone S/D H4 [{round(active_z.bottom, 5)}–{round(active_z.top, 5)}]"

    if not in_strategic_zone:
        return False, ["⛔ REJETÉ v7 : prix hors zone stratégique (OB/S&D requis)"], 0

    reasons.append(f"📍 Zone stratégique : {zone_label}  ✅")

    # ── ② Bougie M15 : Englobante OU Mèche de rejet ──────────
    o   = df_m15["open"].iloc[-1]
    h   = df_m15["high"].iloc[-1]
    l   = df_m15["low"].iloc[-1]
    cl  = df_m15["close"].iloc[-1]
    rng = h - l

    body       = abs(cl - o)
    lower_wick = min(o, cl) - l
    upper_wick = h - max(o, cl)
    is_bull    = cl > o

    # Bougie précédente pour englobante
    p_o  = df_m15["open"].iloc[-2]
    p_cl = df_m15["close"].iloc[-2]
    p_h  = df_m15["high"].iloc[-2]
    p_l  = df_m15["low"].iloc[-2]

    candle_valid = False
    candle_label = ""

    if direction == "LONG":
        # Englobante haussière
        if is_bull and cl > p_h and o <= p_cl and (body / rng if rng > 0 else 0) >= 0.45:
            candle_valid = True
            candle_label = "Bullish Engulfing M15"
        # Marteau / Mèche de rejet basse ≥ 2× corps
        elif lower_wick >= body * 2.0 and upper_wick <= body * 0.7 and body > 0:
            candle_valid = True
            candle_label = "Hammer / Wick Rejet M15"
        # Hammer inversé haussier (mèche haute + clôture positive)
        elif is_bull and upper_wick >= body * 1.5 and lower_wick <= body * 0.5 and body > 0:
            candle_valid = True
            candle_label = "Inverted Hammer haussier M15"

    elif direction == "SHORT":
        # Englobante baissière
        if not is_bull and cl < p_l and o >= p_cl and (body / rng if rng > 0 else 0) >= 0.45:
            candle_valid = True
            candle_label = "Bearish Engulfing M15"
        # Shooting Star / Mèche de rejet haute ≥ 2× corps
        elif upper_wick >= body * 2.0 and lower_wick <= body * 0.7 and body > 0:
            candle_valid = True
            candle_label = "Shooting Star / Wick Rejet M15"
        # Pin Bar baissière
        elif not is_bull and lower_wick >= body * 1.5 and upper_wick <= body * 0.5 and body > 0:
            candle_valid = True
            candle_label = "Pin Bar baissière M15"

    if not candle_valid:
        return False, [
            f"📍 Zone stratégique : {zone_label}  ✅",
            "⛔ REJETÉ v7 : bougie M15 non confirmée (Englobante ou Mèche rejet requise)"
        ], 0

    reasons.append(f"🕯️ Bougie M15 : {candle_label}  ✅")

    # ── ③ BOS M15 dans les 5 dernières bougies ───────────────
    bos_recent = [b for b in bos_m15_list[-5:] if b["type"] == expected]
    # Accepter aussi les 5 dernières bougies pour le BOS "post-entrée"
    if not bos_recent:
        bos_recent = [b for b in bos_m15_list[-8:] if b["type"] == expected]

    if not bos_recent:
        return False, reasons + [
            "⛔ REJETÉ v7 : pas de BOS M15 dans les 5 dernières bougies"
        ], 0

    bos_level = bos_recent[-1]["level"]
    reasons.append(f"✅ BOS M15 {expected} @ {round(bos_level, 5)}  ✅")

    # ── ④ Bonus liquidité sweepée ─────────────────────────────
    liq = detect_liquidity_sweep(df_m15)
    liq_swept = liq["bullish_sweep"] if direction == "LONG" else liq["bearish_sweep"]
    if liq_swept:
        bonus += 15
        reasons.append("💧 Liquidité sweepée avant entrée  (+15 bonus)")

    return True, reasons, bonus


def _rr_ok(entry: float, sl: float, tp: float, direction: str, min_rr: float) -> bool:
    risk = abs(entry - sl)
    if risk <= 0:
        return False
    gain = (tp - entry) if direction == "LONG" else (entry - tp)
    return (gain / risk) >= min_rr


def _rr_ok_flexible(
    entry: float, sl: float,
    tp1: float, tp2: float, tp3: float,
    direction: str,
    min_rr: float = MIN_RR,
) -> bool:
    """
    [v9 MOD-5b] Validation RR flexible sur 3 cibles.

    Règle standard : TP1 doit offrir RR ≥ min_rr (ex: 2.5).

    Règle assouplie : si TP1 offre un RR ≥ 1.8 (minimum viable),
    on accepte le signal à condition que TP3 offre un RR ≥ 3.0.
    Cela permet de valider des setups où l'entrée est légèrement avancée
    mais où le potentiel global (TP2/TP3) est largement positif.

    Paramètres :
      tp2, tp3 : 0.0 si non disponibles → on utilise uniquement la règle standard.
    """
    risk = abs(entry - sl)
    if risk <= 0:
        return False

    def _rr_val(tp: float) -> float:
        gain = (tp - entry) if direction == "LONG" else (entry - tp)
        return gain / risk

    rr_tp1 = _rr_val(tp1)

    # Règle standard : TP1 ≥ min_rr
    if rr_tp1 >= min_rr:
        return True

    # Règle assouplie : TP1 entre 1.8 et min_rr, ET TP3 ≥ 3.0
    if rr_tp1 >= 1.8 and tp3 > 0:
        rr_tp3 = _rr_val(tp3)
        if rr_tp3 >= 3.0:
            return True

    return False


def detect_sd_entry_candle_m15(df_m15: pd.DataFrame, direction: str) -> tuple:
    """
    Détecte les bougies d'entrée M15 spécifiques au scanner Supply/Demand.

    BUY  : Bullish Engulfing · Hammer · Morning Star
    SELL : Bearish Engulfing · Shooting Star · Evening Star

    Retourne (True, "nom_pattern") ou (False, "").
    """
    if len(df_m15) < 4:
        return False, ""

    o   = df_m15["open"].iloc[-1]
    h   = df_m15["high"].iloc[-1]
    l   = df_m15["low"].iloc[-1]
    cl  = df_m15["close"].iloc[-1]
    rng = h - l

    body        = abs(cl - o)
    lower_wick  = min(o, cl) - l
    upper_wick  = h - max(o, cl)
    is_bull     = cl > o
    body_ratio  = body / rng if rng > 0 else 0

    p_o  = df_m15["open"].iloc[-2]
    p_h  = df_m15["high"].iloc[-2]
    p_l  = df_m15["low"].iloc[-2]
    p_cl = df_m15["close"].iloc[-2]

    if direction == "LONG":

        # ── Bullish Engulfing ─────────────────────────────────
        if is_bull and cl > p_h and o <= p_cl and body_ratio >= 0.5:
            return True, "Bullish Engulfing"

        # ── Hammer (marteau) ──────────────────────────────────
        # Corps en haut de la bougie, mèche basse ≥ 2× corps
        if lower_wick >= body * 2.0 and upper_wick <= body * 0.6 and body_ratio >= 0.15:
            return True, "Hammer"

        # ── Morning Star (3 bougies) ──────────────────────────
        if len(df_m15) >= 5:
            pp_o  = df_m15["open"].iloc[-3]
            pp_cl = df_m15["close"].iloc[-3]
            pp_is_bear   = pp_cl < pp_o
            mid_is_small = abs(p_cl - p_o) < abs(pp_cl - pp_o) * 0.5
            confirms_up  = is_bull and cl > (pp_o + pp_cl) / 2
            if pp_is_bear and mid_is_small and confirms_up:
                return True, "Morning Star"

    elif direction == "SHORT":

        # ── Bearish Engulfing ─────────────────────────────────
        if not is_bull and cl < p_l and o >= p_cl and body_ratio >= 0.5:
            return True, "Bearish Engulfing"

        # ── Shooting Star ─────────────────────────────────────
        # Corps en bas de la bougie, mèche haute ≥ 2× corps
        if upper_wick >= body * 2.0 and lower_wick <= body * 0.6 and body_ratio >= 0.15:
            return True, "Shooting Star"

        # ── Evening Star (3 bougies) ──────────────────────────
        if len(df_m15) >= 5:
            pp_o  = df_m15["open"].iloc[-3]
            pp_cl = df_m15["close"].iloc[-3]
            pp_is_bull   = pp_cl > pp_o
            mid_is_small = abs(p_cl - p_o) < abs(pp_cl - pp_o) * 0.5
            confirms_dn  = not is_bull and cl < (pp_o + pp_cl) / 2
            if pp_is_bull and mid_is_small and confirms_dn:
                return True, "Evening Star"

    return False, ""


# ─────────────────────────────────────────────────────────────
#  ENTRÉE CONFIRMÉE — retest OB/FVG + bougie de rejet (3⭐)
#
#  Séquence complète : Sweep → CHoCH → retour dans l'OB/FVG →
#  bougie de rejet claire → entrée.
#  Sans cette confirmation, l'entrée reste "directe" (2⭐) :
#  Sweep + CHoCH suffisent, sans attendre le retour en zone.
# ─────────────────────────────────────────────────────────────

def detect_ob_retest_rejection(
    df_m5:     pd.DataFrame,
    ob:        Optional["OrderBlock"],
    fvg:       Optional["FVG"],
    direction: str,
    lookback:  int = 6,
) -> dict:
    """
    Vérifie si, après le CHoCH, le prix est revenu retester l'OB/FVG
    ET qu'une bougie de rejet claire (mèche + clôture dans le sens du
    trade, corps ≥ 35% du range) s'est formée dans les `lookback`
    dernières bougies M5.

    Retourne {"confirmed": bool, "reason": str | None}.
    """
    zone = None
    if ob is not None:
        zone = (min(ob.bottom, ob.top), max(ob.bottom, ob.top))
    elif fvg is not None:
        zone = (min(fvg.bottom, fvg.top), max(fvg.bottom, fvg.top))

    if zone is None or len(df_m5) < lookback + 1:
        return {"confirmed": False, "reason": None}

    lo, hi = zone
    recent = df_m5.iloc[-lookback:]

    for i in range(len(recent)):
        o, h, l, cl = (recent["open"].iloc[i], recent["high"].iloc[i],
                        recent["low"].iloc[i], recent["close"].iloc[i])
        rng = h - l
        if rng <= 0:
            continue
        body = abs(cl - o)
        touched_zone = (l <= hi) and (h >= lo)   # bougie a touché la zone OB/FVG
        if not touched_zone:
            continue
        body_ratio = body / rng
        if direction == "LONG":
            # rejet haussier : mèche basse dans la zone, clôture au-dessus, corps significatif
            rejection = (cl > o) and (body_ratio >= 0.35) and (cl > lo)
        else:
            # rejet baissier : mèche haute dans la zone, clôture en dessous, corps significatif
            rejection = (cl < o) and (body_ratio >= 0.35) and (cl < hi)
        if rejection:
            return {
                "confirmed": True,
                "reason": f"🔁 Retest OB/FVG + bougie de rejet confirmée (corps {round(body_ratio*100)}%)  → entrée 3⭐",
            }

    return {"confirmed": False, "reason": None}


# ─────────────────────────────────────────────────────────────
#  T6 — MSS/CHoCH  Market Structure Shift + Equal Liquidity
# ─────────────────────────────────────────────────────────────

def check_mss_setup(
    symbol: str, df_m5: pd.DataFrame, direction: str,
    liq_map: Optional[LiquidityMap] = None,
    min_rr: float = MIN_RR,
) -> Optional[SetupSignal]:
    """
    Stratégie unique — 100% M5, AUCUN ancrage H4, AUCUN biais de tendance (v16)

    Critères OBLIGATOIRES (si l'un échoue → setup invalide) :
      ① BOS M5 dans le sens attendu (cassure de structure, corps de bougie
         — pas la mèche)                                    → +20 pts
      ② CHoCH M5 (changement de structure), exigé EN PLUS du BOS
                                                              → +25 à +35 pts
      RR ≥ 3.0 imposé (min_rr forcé à 3.0 minimum pour ce setup)

    Bonus (ne bloquent pas, s'ajoutent au score) :
      ③ Sweep de liquidité (Equal High/Low ou BSL/SSL, calculés sur M5)
      ④ OB ou FVG M5 post-CHoCH                             → +15 à +20 pts
      ⑤ Bougie M5 clôturée (corps ≥ 40% du range)            → +15 pts

    Seuil : score ≥ 55/100.
    Entrée directe (sweep + CHoCH confirmés, sans retest)     = 2⭐
    Entrée confirmée (+ retest OB/FVG avec bougie de rejet)   = 3⭐
    """
    score, reasons = 0, []
    min_rr = max(min_rr, 3.0)   # RR3 minimum imposé sur ce setup

    if len(df_m5) < 20:
        return None

    if liq_map is None:
        liq_map = build_liquidity_map(df_m5, df_m5)

    price_now = df_m5["close"].iloc[-1]
    atr_m5    = (df_m5["high"] - df_m5["low"]).rolling(14).mean().iloc[-1]
    expected  = "bullish" if direction == "LONG" else "bearish"

    # ── ① BOS M5 dans le sens attendu (obligatoire) ───────────
    bos_m5     = detect_bos(df_m5)
    bos_recent = [b for b in bos_m5[-8:] if b["type"] == expected]
    if not bos_recent:
        return None   # Pas de BOS M5 → confirmation d'entrée incomplète
    score += 20
    reasons.append(f"📐 BOS M5 {expected} @ {round(bos_recent[-1]['level'], 5)}  (+20)")

    # ── ② CHoCH M5 (obligatoire — exigé EN PLUS du BOS) ───────
    choch_res = detect_choch_eql_setup(df_m5, df_m5, liq_map, direction)
    if choch_res["detected"] and choch_res["score_bonus"] >= 15:
        choch_pts = min(choch_res["score_bonus"], 35)
        score += choch_pts
        reasons += choch_res.get("reasons", [])
        reasons.append(f"🔄 CHoCH M5 détecté  (+{choch_pts})")
    else:
        # Fallback strict : BOS contraire suivi d'un BOS dans notre sens
        # (= CHoCH structurel équivalent)
        opp = "bearish" if direction == "LONG" else "bullish"
        recents  = bos_m5[-6:]
        has_opp  = any(b["type"] == opp      for b in recents)
        has_same = any(b["type"] == expected  for b in recents[-3:])
        if has_opp and has_same:
            score += 25
            reasons.append(f"🔄 CHoCH M5 : retournement {opp}→{expected}  (+25)")
        else:
            return None   # BOS + CHoCH requis ENSEMBLE → sans CHoCH, invalide

    # ── ③ Sweep de liquidité — OPTIONNEL (bonus non bloquant) ─
    eqh_ok = bool(liq_map.eqh_levels) and direction == "SHORT"
    eql_ok = bool(liq_map.eql_levels) and direction == "LONG"
    bsl_ssl_swept = (liq_map.swept_bsl and direction == "SHORT") or \
                    (liq_map.swept_ssl and direction == "LONG")

    if eqh_ok or eql_ok:
        score += 30
        lbl = "EQH sweepé" if eqh_ok else "EQL sweepé"
        reasons.append(f"💧 {lbl} — pool de liquidité institutionnel  (+30)")
    elif bsl_ssl_swept:
        score += 20
        reasons.append(f"💧 BSL/SSL sweepée  (+20)")

    # ── ④ OB ou FVG post-CHoCH ────────────────────────────────
    fvgs_m5    = detect_fvg(df_m5)
    fvg_active = active_fvg(df_m5, fvgs_m5, expected)
    obs_m5     = detect_order_blocks(df_m5, bos_m5)
    ob_match   = next(
        (o for o in reversed(obs_m5)
         if o.direction == expected and
            (min(o.top,o.bottom)-atr_m5*0.3) <= price_now <= (max(o.top,o.bottom)+atr_m5*0.3)),
        None
    )

    if fvg_active:
        score += 20
        reasons.append(f"📍 FVG M5 post-CHoCH  (+20)")
    elif ob_match:
        score += 15
        reasons.append(f"🏛️ OB M5 post-CHoCH  (+15)")

    # ── ⑤ Bougie M5 clôturée ──────────────────────────────────
    if _m15_candle_confirmed(df_m5, direction):
        score += 15
        reasons.append(f"🕯️ Bougie M5 clôturée  (+15)")

    if score < 55:
        return None

    # ── ⑥ Entrée directe (2⭐) vs entrée confirmée par retest (3⭐) ──
    # Les 2 systèmes tournent en parallèle : Sweep+CHoCH suffit pour
    # tirer un signal (2⭐) ; si en plus le prix est revenu retester
    # l'OB/FVG avec une bougie de rejet, le signal passe en 3⭐.
    retest = detect_ob_retest_rejection(df_m5, ob_match, fvg_active, direction)
    stars  = 3 if retest["confirmed"] else 2
    if retest["confirmed"]:
        reasons.append(retest["reason"])
    else:
        reasons.append("⚡ Entrée directe — Sweep+CHoCH sans retest OB/FVG  → 2⭐")

    entry, sl, tp1, rr, tp2, tp3 = _compute_levels(
        symbol, direction, df_m5, df_m5, ob_match, fvg_active, None, liq_map, score=score
    )

    if not _rr_ok(entry, sl, tp1, direction, min_rr):
        return None

    lot  = compute_lot(symbol, entry, sl, risk_usd=get_current_risk_usd())
    max_vol = get_max_volume()
    if max_vol > 0 and lot > max_vol:
        lot = max_vol
    bias = "BULLISH" if direction == "LONG" else "BEARISH"

    return SetupSignal(
        symbol=symbol, setup_type="MSS", tier=5,
        direction=direction, entry=entry, sl=sl, tp=tp1, tp2=tp2, tp3=tp3,
        rr=rr, score=score, reasons=reasons,
        htf_bias=bias, lot=lot,
        df_chart=df_m5, fvg_ref=fvg_active, ob_ref=ob_match,
        stars=stars,
    )


# ─────────────────────────────────────────────────────────────
#  ORCHESTRATEUR — scan_symbol()
#  Stratégie unique : Sweep de liquidité + Cassure de structure
#  (BOS/CHoCH), LONG et SHORT. Entrée directe = 2⭐, entrée sur
#  retest OB/FVG confirmé par bougie de rejet = 3⭐.
# ─────────────────────────────────────────────────────────────

SETUP_LABELS = {
    "MSS": "SWEEP + CASSURE DE STRUCTURE",
}


def scan_symbol(symbol: str, mkt: str, min_rr: float = MIN_RR) -> list[SetupSignal]:
    """
    v16 — Stratégie unique, 100% M5, sans ancrage H4, sans biais de tendance.

    Fonctionnement :
      1. Téléchargement des données M5 (une seule fois)
      2. Filtres communs (volatilité, news)
      3. Carte de liquidité construite uniquement sur M5
      4. Test des deux directions (LONG et SHORT) : la direction est
         déterminée UNIQUEMENT par le sweep + la cassure de structure
         (BOS + CHoCH, corps de bougie) sur M5 — aucun filtre de biais
         H4/H1, aucune notion de "tendance de fond"
           → entrée directe (2⭐) ou entrée confirmée par retest
             OB/FVG + bougie de rejet (3⭐)
      5. Retourne le(s) signal(aux) qui passent le score et le RR minimum
    """
    # ── 1. Téléchargement des données (une seule fois) ────────
    df_h4, df_h1, df_m15, df_m5, df_m1 = _fetch_data(symbol)
    if df_m5 is None or df_m5.empty:
        return []

    # ── 2. Filtre volatilité + volume ─────────────────────────
    vol_ok, _ = check_volatility(symbol, df_m5, df_m15)
    if not vol_ok:
        return []

    # ── 2b. FILTRE NEWS ÉCONOMIQUES ───────────────────────────
    news_blocked, news_reason = is_news_blackout(symbol)
    if news_blocked:
        log.info(f"  ⛔ {symbol} — {news_reason}")
        return []

    # ── 3. Carte de liquidité — 100% M5, aucun ancrage H4 ─────
    liq_map = build_liquidity_map(df_m5, df_m5)

    # ── 4. Test des deux directions — Sweep + Cassure de structure M5 ─
    signals: list[SetupSignal] = []
    for direction in ("LONG", "SHORT"):
        # ── [Cahier des charges] SHORT UNIQUEMENT ──────────────
        if SHORT_ONLY and direction != "SHORT":
            continue
        try:
            sig = check_mss_setup(symbol, df_m5, direction, liq_map, min_rr)
            if sig is not None:
                stars = getattr(sig, "stars", 2)
                log.info(
                    f"  ✅ Signal validé {symbol} [SWEEP+CASSURE] "
                    f"{'⭐' * stars} {direction} score={sig.score} RR={sig.rr}"
                )
                signals.append(sig)
        except Exception as e:
            log.debug(f"  {symbol} [SWEEP+CASSURE] {direction} erreur : {e}")

    return signals





# ─────────────────────────────────────────────────────────────
#  WATCHLIST (réutilisée depuis v3)
# ─────────────────────────────────────────────────────────────

# ── v8 : Paires MAJEURES uniquement — suppression des exotiques ──
# Résultat analyse : AUDCHF/AUDNZD/CADCHF/USDNOK/USDZAR/USDMXN = trop de SL
# On garde : 7 forex majeurs + Gold + BTC + 4 crosses liquides

TIER_1_PRIORITY: list[tuple[str, str]] = [
    ("GC=F",    "Gold"),
    ("BTC-USD", "Bitcoin"),
]

# SUPPRIMÉS (demande utilisateur — plus aucun Forex / indice US) :
# EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, NZDUSD, USDCAD (TIER_2_FOREX)
# EURGBP, EURJPY, GBPJPY, GBPAUD, ^GSPC, ^NDX (TIER_3_EXTRA)
# R_100, R_50 (Deriv — remplacés par R_75)
#
# Seuls 4 marchés restent actifs : XAUUSD (Gold), BTC, Volatility 75 Index,
# Volatility 25 Index.


def get_symbols(cat: str = "all") -> list[tuple[str, str]]:
    if cat == "gold":     return [("GC=F", "Gold")]
    if cat == "btc":      return [("BTC-USD", "Bitcoin")]
    if cat == "deriv":    return DERIV_SYMBOLS
    if cat == "priority": return TIER_1_PRIORITY
    # "all" = respecte le réglage 📈 Marchés actifs du menu Telegram
    active = get_active_markets_setting()
    if active == "gold_btc":
        return TIER_1_PRIORITY
    if active == "deriv":
        return DERIV_SYMBOLS
    # "all" (défaut) = les 4 marchés conservés : Gold + BTC + Volatility 75/25
    return TIER_1_PRIORITY + DERIV_SYMBOLS


# ─────────────────────────────────────────────────────────────
#  LOGGING + COMPTEURS JOURNALIERS
# ─────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("smc_v4")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s  %(message)s", datefmt="%H:%M:%S"))
        logger.addHandler(h)
    return logger


log = setup_logging()

MAX_SIGNALS_PER_DAY        = 3   # [v11] légèrement relevé (2→3) pour accompagner le passage à 8 signaux/jour globaux
MAX_SIGNALS_GLOBAL_PER_DAY = 8   # [v11] relevé de 2 → 8 signaux/jour GLOBAL (demande utilisateur, tous marchés confondus)
                                  # Règle absolue : jamais plus de 4 alertes par journée UTC.
MAX_SIGNALS_PER_SESSION    = 4   # [v11] max 4 signaux par session (Londres et New York comptés séparément)
MAX_SIGNALS_DERIV_PER_DAY  = 5   # [v11] indices synthétiques Deriv (Volatility) : max 5 signaux/jour, tous ensemble

_daily_counts:        dict[str, int] = {}
_daily_global_count:  int            = 0   # compteur global journalier
_daily_deriv_count:   int            = 0   # [v11] compteur dédié indices Volatility (V100/V50/V25)
_session_counts:      dict[str, int] = {}  # [v11] {"LONDON": n, "NY": n}
_last_session_label:  str            = ""
_daily_date:          str            = ""

def _current_session_label() -> str:
    """[v11] Retourne 'LONDON', 'NY' ou '' si on est hors des deux kill zones."""
    now_min = datetime.now(timezone.utc).hour * 60 + datetime.now(timezone.utc).minute
    if LONDON_KZ_MIN[0] <= now_min < LONDON_KZ_MIN[1]:
        return "LONDON"
    if NY_KZ_MIN[0] <= now_min < NY_KZ_MIN[1]:
        return "NY"
    return ""

def _reset_session_if_needed() -> None:
    """[v11] Remet à zéro le compteur d'une session dès l'entrée dans une nouvelle fenêtre."""
    global _last_session_label
    label = _current_session_label()
    if label and label != _last_session_label:
        _session_counts[label] = 0
        _last_session_label = label
    elif not label:
        _last_session_label = ""

def _reset_daily_if_needed() -> None:
    """
    [v9 MOD-1] Remet à zéro tous les compteurs si le jour UTC a changé.
    Reset automatique à 00h00 UTC — aucune action manuelle requise.
    """
    global _daily_date, _daily_global_count, _daily_deriv_count
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != _daily_date:
        _daily_counts.clear()
        _session_counts.clear()
        _daily_global_count = 0
        _daily_deriv_count  = 0
        _daily_date = today
        log.info(f"  🔄 [v9] Compteurs journaliers remis à zéro — nouveau jour UTC : {today}")

def check_daily_limit(symbol: str) -> bool:
    """
    [v11] Retourne True si on peut encore envoyer un signal.
    Vérifie QUATRE limites :
      • Limite par symbole  : max MAX_SIGNALS_PER_DAY signaux/jour/paire
      • Limite globale      : max MAX_SIGNALS_GLOBAL_PER_DAY signaux/jour (toutes paires)
      • Limite par session  : max MAX_SIGNALS_PER_SESSION signaux (Londres et NY séparément)
      • Limite indices Deriv: max MAX_SIGNALS_DERIV_PER_DAY signaux/jour (V100/V50/V25 confondus)
    Si l'une ou l'autre est dépassée → retourne False, signal bloqué.
    """
    _reset_daily_if_needed()
    _reset_session_if_needed()

    per_symbol_ok = _daily_counts.get(symbol, 0) < MAX_SIGNALS_PER_DAY
    global_ok     = _daily_global_count < MAX_SIGNALS_GLOBAL_PER_DAY

    session_label = _current_session_label()
    session_ok    = (_session_counts.get(session_label, 0) < MAX_SIGNALS_PER_SESSION) if session_label else True

    deriv_ok = (_daily_deriv_count < MAX_SIGNALS_DERIV_PER_DAY) if is_deriv_symbol(symbol) else True

    if not global_ok:
        log.info(
            f"  ⏹ [v9] Limite globale atteinte : {_daily_global_count}/{MAX_SIGNALS_GLOBAL_PER_DAY} "
            f"signaux envoyés aujourd'hui — aucun signal supplémentaire jusqu'à 00h00 UTC."
        )
    if session_label and not session_ok:
        log.info(
            f"  ⏹ [v11] Limite session {session_label} atteinte : "
            f"{_session_counts.get(session_label, 0)}/{MAX_SIGNALS_PER_SESSION}"
        )
    if is_deriv_symbol(symbol) and not deriv_ok:
        log.info(
            f"  ⏹ [v11] Limite indices synthétiques atteinte : "
            f"{_daily_deriv_count}/{MAX_SIGNALS_DERIV_PER_DAY} — plus de signal Volatility aujourd'hui."
        )

    return per_symbol_ok and global_ok and session_ok and deriv_ok

def increment_daily_count(symbol: str) -> None:
    """[v11] Incrémente les compteurs : symbole, global, session, et Deriv si applicable."""
    global _daily_global_count, _daily_deriv_count
    _reset_daily_if_needed()
    _reset_session_if_needed()
    _daily_counts[symbol]  = _daily_counts.get(symbol, 0) + 1
    _daily_global_count   += 1

    session_label = _current_session_label()
    if session_label:
        _session_counts[session_label] = _session_counts.get(session_label, 0) + 1

    if is_deriv_symbol(symbol):
        _daily_deriv_count += 1

    log.info(
        f"  📊 [v11] Compteur : {symbol} → {_daily_counts[symbol]}/{MAX_SIGNALS_PER_DAY}  |  "
        f"Global : {_daily_global_count}/{MAX_SIGNALS_GLOBAL_PER_DAY}  |  "
        f"Session {session_label or '—'} : {_session_counts.get(session_label, 0)}/{MAX_SIGNALS_PER_SESSION}"
        + (f"  |  Deriv : {_daily_deriv_count}/{MAX_SIGNALS_DERIV_PER_DAY}" if is_deriv_symbol(symbol) else "")
    )


# ─────────────────────────────────────────────────────────────
#  BOUCLE LIVE v4 — MULTI-SETUP
# ─────────────────────────────────────────────────────────────

_last_bias_v4: dict[str, str] = {}

def run_live_v4(cat: str = "all", min_rr: float = MIN_RR, interval: int = 300) -> None:
    """
    Boucle principale v4 — multi-setup indépendant.

    Chaque cycle (5 min par défaut) :
      1. Récupère les données H4/M15/M5 par actif
      2. Lance les 6 modules en parallèle
      3. Collecte tous les signaux valides
      4. Envoie Telegram par ordre de priorité (T1 avant T6)
      5. Limite 4 signaux/jour/symbole (limite globale : 3 signaux/cycle)
    """
    symbols = get_symbols(cat)

    # Message de démarrage Telegram
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    startup_msg = (
        f"⚡ <b>SMC Signal Engine v10 — FUSION COMPLÈTE (v8.6 + v9.6 + v4)</b>\n"
        f"{'─'*30}\n"
        f"🕐 <code>{ts}</code>\n"
        f"📊 <b>Marchés ({len(symbols)}) :</b> XAUUSD (Gold) · BTC · "
        f"Volatility 75 Index · Volatility 25 Index\n"
        f"🥇 T1 Breaker · 🥈 T2 S/D Zone · 🥉 T3 OB\n"
        f"T4 BOS · T5 MSS · T6 FVG · T7 AMD\n"
        f"⚡ <b>Max 3 signaux/cycle</b> — 1 signal/paire/cycle\n"
        f"📆 <b>Max {MAX_SIGNALS_GLOBAL_PER_DAY} signaux/jour GLOBAL</b> — qualité > quantité\n"
        f"🚫 <b>BTC SELL bloqué</b> — tendance haussière\n"
        f"📈 <b>Score min :</b> {SCORE_THRESHOLD}/100\n"
        f"⏱ <b>Cooldown :</b> 30min entre 2 signaux/paire\n"
        f"⚖️ RR min : 1:{min_rr}  |  Risque : $100/trade\n"
        f"{'─'*30}\n"
        f"✅ Bot v8 démarré — scan toutes les {interval//60} minutes"
    )
    try:
        if TELEGRAM_LEADER_ID:
            requests.post(_tg_url("sendMessage"), json={
                "chat_id": TELEGRAM_LEADER_ID, "text": startup_msg, "parse_mode": "HTML",
            }, timeout=10)
    except Exception:
        pass

    # ── Confirmation de démarrage dans les DEUX groupes ────────────────
    for grp_id, grp_label in (
        (TELEGRAM_GROUP_DERIV_ID,    "Deriv (Volatility 75/25)"),
        (TELEGRAM_GROUP_GOLD_BTC_ID, "Gold + BTC"),
    ):
        if not grp_id:
            continue
        try:
            r = requests.post(_tg_url("sendMessage"), json={
                "chat_id": grp_id, "text": startup_msg, "parse_mode": "HTML",
            }, timeout=10)
            ok = r.status_code == 200
        except Exception:
            ok = False
        log.info(f"  [TG] {'✓' if ok else '✗'} Message de démarrage → groupe {grp_label} ({grp_id})")

    with _STATUS_LOCK:
        _STATUS["started_at"]    = ts
        _STATUS["symbols_count"] = len(symbols)
        _STATUS["scan_running"]  = True

    cycle_n = 0
    consecutive_errors = 0

    while True:
        try:
            cycle_n += 1
            now_utc = datetime.now(timezone.utc)
            now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

            # Filtrage weekend
            if is_weekend():
                symbols_to_scan = [
                    (s, m) for s, m in symbols
                    if is_crypto_symbol(s) or is_deriv_symbol(s)
                    or (s in GOLD_SYMBOLS and is_gold_session_active())
                ]
                if not symbols_to_scan:
                    if cycle_n % 10 == 1:
                        log.info(f"  💤 [{cycle_n}] {now_utc.strftime('%H:%M UTC')} — Weekend — attente")
                    with _STATUS_LOCK:
                        _STATUS["cycle"] = cycle_n
                        _STATUS["scan_running"] = False
                    time.sleep(interval)
                    continue
            elif not is_session_active():
                symbols_to_scan = [(s, m) for s, m in symbols if is_crypto_symbol(s) or is_deriv_symbol(s)]
                if not symbols_to_scan:
                    if cycle_n % 10 == 1:
                        log.info(f"  💤 [{cycle_n}] — Hors session")
                    time.sleep(interval)
                    continue
            else:
                # Filtre indices US hors heures NYSE/NASDAQ (pas de données 15m disponibles)
                if not is_us_market_open():
                    symbols_to_scan = [(s, m) for s, m in symbols if s not in US_INDEX_SYMBOLS]
                    if cycle_n % 20 == 1:
                        log.info("  ℹ️  Indices US exclus (hors session NYSE 13h30-20h00 UTC)")
                else:
                    symbols_to_scan = symbols

            with _STATUS_LOCK:
                _STATUS["scan_running"] = True
                _STATUS["cycle"]        = cycle_n
                _STATUS["last_scan"]    = now_str

            log.info(f"  🔍 [{cycle_n}] {now_utc.strftime('%H:%M UTC')} — {len(symbols_to_scan)} marchés")
            correlation_guard_reset()

            # ── Tableau d'en-tête du cycle ────────────────────
            W = 100
            print(f"\n{'╔'+'═'*W+'╗'}")
            print(f"║  🔍  CYCLE v8 #{cycle_n}  [{now_str}]  {len(symbols_to_scan)} marchés"
                  + " " * max(0, W - 3 - len(now_str) - len(str(len(symbols_to_scan))) - 22) + "║")
            print(f"║  T1=BREAKER · T2=S/D · T3=OB · T4=BOS · T5=MSS · T6=FVG · T7=AMD  [v8-MAJEURS]"
                  + " " * max(0, W - 67) + "║")
            print(f"{'╠'+'═'*W+'╣'}")
            print(f"  {'N°':<4} {'Marché':<14} {'Sym':<12}  {'Biais':>6}  "
                  f"{'Setups détectés':<40}  {'Statut'}")
            print(f"  {'─'*W}")

            cycle_signals: list[tuple[str, str, SetupSignal]] = []

            for i, (sym, mkt) in enumerate(symbols_to_scan, 1):
                prefix = f"  {i:<4} {mkt:<14} {c(sym, 'cyan'):<12}"
                print(prefix + "  … ", end="", flush=True)

                try:
                    # ── Kill Zone check — filtre horaire par symbole ──
                    # v8.1 : appliqué à TOUS les marchés (Forex + Gold + Crypto)
                    # pour bannir les heures de nuit/faible liquidité partout
                    kz_ok, kz_reason = is_kill_zone_active(sym)
                    if not kz_ok:
                        print(f"\r{prefix}  {'—':>6}  {'—':<40}  {kz_reason}")
                        continue

                    # Diagnostic rapide (affichage console uniquement —
                    # ne filtre plus rien : plus de biais de tendance H4,
                    # la direction est décidée uniquement par le sweep +
                    # la cassure de structure M5 dans scan_symbol())
                    df_peek = fetch(sym, "4h", period="5d")
                    if not df_peek.empty:
                        bias_str = htf_bias(df_peek)
                    else:
                        bias_str = "NEUTRAL"

                    bias_col = "green" if bias_str == "BULLISH" else (
                               "red" if bias_str == "BEARISH" else "yellow")

                    # ── Lancement du module unique (Sweep + Cassure M5) ─
                    sigs = scan_symbol(sym, mkt, min_rr=min_rr)

                    if not sigs:
                        print(f"\r{prefix}  {c(bias_str[:4], bias_col):>6}  {'Aucun setup validé':<40}  🔵 Attente")
                        time.sleep(0.5)
                        continue

                    # Affichage des setups trouvés
                    setup_names = " | ".join(
                    c(f"T{s.tier}:{s.setup_type}({s.score})", "yellow" if s.score >= 75 else "cyan")
                        for s in sigs
                    )
                    print(f"\r{prefix}  {c(bias_str[:4], bias_col):>6}  {setup_names:<40}  "
                          + c(f"⚡ {len(sigs)} signal(s)", "yellow"))

                    # Mise à jour biais
                    if _last_bias_v4.get(sym) and _last_bias_v4[sym] != bias_str:
                        reset_setup(sym)
                    _last_bias_v4[sym] = bias_str

                    for sig in sigs:
                        cycle_signals.append((mkt, sym, sig))

                except Exception as e:
                    print(f"\r{prefix}  {'—':>6}  {'—':<40}  "
                          + c(f"⚠ {str(e)[:40]}", "red"))

                time.sleep(1)

            # ── Envoi des meilleurs signaux du cycle ──────────
            print(f"  {'─'*W}")

            # ── v8 : max 3 signaux par cycle — 1 seul module par paire ──
            # Priorité : T1 > T2 > T3. On ne double plus les signaux OB+BOS+FVG sur la même paire
            seen_pairs_cycle = set()
            deduped_signals = []
            cycle_signals.sort(key=lambda x: (x[2].tier, -x[2].score))
            for item in cycle_signals:
                pair_key = item[1]  # symbol
                if pair_key not in seen_pairs_cycle:
                    seen_pairs_cycle.add(pair_key)
                    deduped_signals.append(item)
            cycle_signals = deduped_signals[:3]  # max 3 par cycle

            if cycle_signals:
                print(c(f"\n  ⚡ {len(cycle_signals)} SIGNAL(S) — Envoi Telegram…", "yellow"))

            for mkt, sym, setup_sig in cycle_signals:
                if not check_daily_limit(sym):
                    # Distinguer : limite symbole ou limite globale
                    _reset_daily_if_needed()
                    if _daily_global_count >= MAX_SIGNALS_GLOBAL_PER_DAY:
                        log.info(f"  ⏹ Limite globale atteinte ({MAX_SIGNALS_GLOBAL_PER_DAY} signaux/jour) — plus aucun signal aujourd'hui")
                        break   # inutile de continuer la boucle
                    else:
                        log.info(f"  ⏭ {sym} — limite {MAX_SIGNALS_PER_DAY} signaux/jour/paire atteinte")
                    continue

                # v8 : bloquer les SELL sur BTC (tendance macro haussière)
                if BTC_SELL_BLOCKED and sym == "BTC-USD" and setup_sig.direction in ("SHORT", "SELL"):
                    log.info(f"  🚫 BTC SELL bloqué (v8 — biais haussier macro)")
                    continue

                corr_ok, corr_reason = correlation_guard(sym, setup_sig.direction)
                if not corr_ok:
                    log.info(f"  🟠 {sym} — corrélation bloquée ({corr_reason})")
                    continue

                increment_daily_count(sym)
                tier_lbl = SETUP_LABELS.get(setup_sig.setup_type, setup_sig.setup_type)

                # Conversion en Signal v3 pour tg_notify
                sig_v3 = setup_sig.to_signal()
                log.info(f"  ⚡ {setup_sig.direction} {mkt} [{setup_sig.setup_type}]"
                         f"  score={setup_sig.score}  RR=1:{setup_sig.rr}  lot={setup_sig.lot}")
                tg_notify(sig_v3, tier=tier_lbl, mode=setup_sig.setup_type)

                with _STATUS_LOCK:
                    _STATUS["last_signals"].append({
                        "ts"       : now_utc.strftime("%d/%m %H:%M"),
                        "market"   : mkt,
                        "direction": setup_sig.direction,
                        "entry"    : setup_sig.entry,
                        "sl"       : setup_sig.sl,
                        "tp"       : setup_sig.tp,
                        "rr"       : setup_sig.rr,
                        "score"    : setup_sig.score,
                        "lot"      : setup_sig.lot,
                        "mode"     : setup_sig.setup_type,
                    })
                    _STATUS["last_signals"] = _STATUS["last_signals"][-20:]

            if not cycle_signals:
                print(c(f"  ℹ️  Aucun signal validé ce cycle", "white"))

            print(f"{'╚'+'═'*W+'╝'}")
            consecutive_errors = 0
            log.info(f"  ⏳ Prochain scan dans {interval}s\n")
            time.sleep(interval)

        except KeyboardInterrupt:
            log.info("\n  Session live v4 terminée.")
            break
        except Exception as e:
            consecutive_errors += 1
            log.error(f"  ✗ Erreur critique : {e}")
            wait = min(60 * consecutive_errors, 300)
            time.sleep(wait)


# ═════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SMC Signal Engine v10 — FUSION COMPLÈTE (v8.6 + v9.6 + v4)",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--symbol", default=None,
                        help="Symbole unique (ex: GC=F, BTC-USD, R_75, R_25)")
    parser.add_argument("--cat",    default="all",
                        choices=["gold", "priority", "btc", "deriv", "all"])
    parser.add_argument("--scan",   action="store_true",
                        help="Scan unique (test local, sans Telegram)")
    parser.add_argument("--min-rr",  type=float, default=MIN_RR)
    parser.add_argument("--interval",type=int,   default=300,
                        help="Intervalle scan secondes (défaut: 300 = 5 min)")
    args = parser.parse_args()

    # Flask dashboard
    flask_port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=start_flask, args=(flask_port,),
                     daemon=True, name="flask").start()
    time.sleep(3)
    log.info(f"  ✓ Flask dashboard port {flask_port}")
    start_self_ping(flask_port)

    # ── Trade Monitor — alertes TP/SL automatiques ───────────
    _init_trade_db()
    _settings_init()
    _monitor_trades_thread()
    log.info("  ✓ Trade Monitor actif — alertes TP1/TP2/TP3/SL automatiques")
    _daily_report_thread()    # Rapport Telegram à 21h00 UTC (Gold+BTC + Deriv séparés)
    _weekly_report_thread()   # Rapport Telegram hebdomadaire — dimanche 21h05 UTC
    _telegram_command_thread()  # Commande admin /rapport (long-polling)
    print_stats_summary()     # Résumé stats au démarrage

    if args.symbol:
        # Test d'un seul symbole
        sigs = scan_symbol(args.symbol, args.symbol, min_rr=args.min_rr)
        if sigs:
            for s in sigs:
                print(c(f"\n⚡ {s.setup_type} T{s.tier}  {s.direction}  "
                        f"score={s.score}  RR=1:{s.rr}", "yellow"))
                for r in s.reasons:
                    print(f"   • {r}")
                tg_notify(s.to_signal(), tier=SETUP_LABELS[s.setup_type], mode=s.setup_type)
        else:
            print(c(f"  Aucun setup validé pour {args.symbol}", "white"))

    elif args.scan:
        symbols = get_symbols(args.cat)
        all_sigs: list[tuple[str, SetupSignal]] = []
        print(f"\n  SMC v4 — Scan {len(symbols)} symboles…\n")
        for sym, mkt in symbols:
            sigs = scan_symbol(sym, mkt, min_rr=args.min_rr)
            for s in sigs:
                all_sigs.append((mkt, s))
                d = "green" if s.direction == "LONG" else "red"
                print(f"  {mkt:<16} {c(s.direction, d)}  [{s.setup_type}]"
                      f"  score={s.score}  RR=1:{s.rr}")
        if not all_sigs:
            print(c("  Aucun signal.", "yellow"))

    else:
        run_live_v4(cat=args.cat, min_rr=args.min_rr, interval=args.interval)



