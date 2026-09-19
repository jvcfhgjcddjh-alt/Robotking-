#!/usr/bin/env python3
"""AlphaBot BOS + CHoCH — Gold & BTC — version en un seul fichier.

Signaux Telegram sur XAUUSD et BTCUSD, timeframe modifiable à tout moment via /timeframe (M1 par défaut, réglable via DEFAULT_TIMEFRAME),
entrée directe sur CHoCH.
Prix : Gold via l'API publique Deriv (WebSocket), BTC via l'API publique Binance. Aucune clé de prix.

Lancer :
    pip install requests python-dotenv matplotlib websocket-client flask
    python main.py

Déploiement Render (Web Service) : le process écoute sur $PORT et expose GET /health,
pendant que la boucle de trading et le polling Telegram tournent en arrière-plan.
Variables utiles : TELEGRAM_TOKEN, CHAT_ID_GROUPE, CHAT_ID_ADMIN, TIMEFRAME,
DEFAULT_RISK_USD, DEFAULT_LEVERAGE, PORT.

Le bot tourne en permanence (pas de /start /stop) : scan continu, une analyse à chaque
nouvelle bougie clôturée du timeframe choisi.

Risque : le lot est calculé à partir du risque $ choisi + SL, SANS solde de compte.
Le levier ne sert qu'à afficher une marge indicative (jamais utilisé pour le lot).
Grille de sortie par défaut : paliers RR1/RR2/RR3 notifiés au groupe, BE (SL -> entrée) à
RR2, TP finale à RR4 (réglable via BE_RR / TP_RR / RR_LEVELS). Aucun PnL $ n'est affiché,
ni au groupe ni en privé Leader — uniquement des R et des taux de réussite par RR.

Réglages : section 1 (CONFIGURATION) ou fichier .env (voir README) — ex. TIMEFRAME=M15.
Le timeframe se change aussi à chaud depuis Telegram (/timeframe) : le dernier choix est mémorisé, sauf si la
variable TIMEFRAME est modifiée sur Render (elle reprend alors la main au démarrage suivant).
Stickers / images / GIF du groupe (TP, SL, BE, motivation) : envoyer le média au bot en privé, puis choisir la
catégorie (gestion via /medias). Optionnel : variables MEDIA_TP, MEDIA_SL, MEDIA_BE, MEDIA_MOTIV.
Sommaire : 1 Configuration · 2 Base de données · 3 Données de prix · 4 Signaux
           5 Risque · 6 Suivi des positions · 7 Graphique · 8 Telegram · 9 Boucle principale
           10 Serveur web (Render)
"""
import json
import math
import os
import random
import re
import sqlite3
import sys
import threading
import time
import traceback
from datetime import datetime, timezone

import requests

try:  # logs Render en temps réel : pas de buffering des print()
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv optionnel
    pass

try:
    import websocket  # websocket-client : WebSocket public Deriv (prix du Gold)
except ImportError:
    websocket = None

try:
    from flask import Flask  # serveur /health pour Render (Web Service)
except ImportError:
    Flask = None


# ============================================================================
# 1. CONFIGURATION
#
# Configuration : tout se règle ici ou dans le fichier .env
# ============================================================================

def _env_float(name, default):
    return float(os.getenv(name, default))


def _env_int(name, default):
    return int(os.getenv(name, default))


def _env_bool(name, default):
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")


# --- Telegram -------------------------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID_GROUPE = os.getenv("CHAT_ID_GROUPE", "")   # signaux + suivi (sans lot)
CHAT_ID_ADMIN = os.getenv("CHAT_ID_ADMIN", "")     # privé : lot, PnL $, commandes

# --- Données de prix : exactement deux sources publiques (aucune clé, aucune authentification) ---
#   Gold (XAUUSD) : API publique Deriv, WebSocket (ticks_history), symbole "frxXAUUSD"
#   BTC  (BTCUSD) : API publique Binance, REST klines, symbole "BTCUSDT"
DERIV_WS_URL = os.getenv("DERIV_WS_URL", "wss://api.derivws.com/trading/v1/options/ws/public")

# --- Marchés (Gold + BTC uniquement) ---------------------------------------
# Timeframe commun aux deux actifs : M1, M3, M5, M15, M30 ou H1 (aussi accepté : 1m, 5m, 15m, 1h...).
# Valeur de départ : variable TIMEFRAME (Render / .env), ex. TIMEFRAME=M15 ; ensuite /timeframe sur Telegram.
TF_LABELS = {1: "M1", 3: "M3", 5: "M5", 15: "M15", 30: "M30", 60: "H1"}


def _parse_timeframe(txt):
    """'M5' / '5m' / '5' -> 5 ; 'H1' / '1h' -> 60 ; sinon None."""
    s = str(txt).strip().upper()
    m = re.fullmatch(r"M?(\d+)M?", s)
    if m:
        return int(m.group(1))
    m = re.fullmatch(r"H(\d+)|(\d+)H", s)
    return int(m.group(1) or m.group(2)) * 60 if m else None


# Timeframe courant : DEFAULT_TIMEFRAME au départ, puis recalculé par load_timeframe() (section 2) et modifiable à chaud
# via /timeframe. TIMEFRAME_MIN / TF_LABEL / TF_SEC ne sont donc pas des constantes.
DEFAULT_TIMEFRAME = "M1"   # 1er démarrage : ni variable TIMEFRAME sur Render, ni choix Telegram enregistré
TIMEFRAME_MIN = _parse_timeframe(DEFAULT_TIMEFRAME)
TF_LABEL = TF_LABELS[TIMEFRAME_MIN]
TF_SEC = TIMEFRAME_MIN * 60

SYMBOLS = {
    "XAUUSD": {
        "source": "deriv", "deriv_symbol": "frxXAUUSD",
        "value_per_point": 100.0,   # $ par point (1.00 de prix) pour 1 lot standard
        "min_lot": 0.01, "lot_step": 0.01, "decimals": 2,
    },
    "BTCUSD": {
        "source": "binance", "binance_symbol": "BTCUSDT",
        "value_per_point": 1.0,     # $ par point pour 1 lot (1 BTC) - à ajuster selon le broker
        "min_lot": 0.001, "lot_step": 0.001, "decimals": 2,
    },
}

# --- Stratégie BOS + CHoCH ---------------------------------------------------
SWING_DEPTH = _env_int("SWING_DEPTH", 3)
ATR_PERIOD = 14
ENTRY_CHOCH1 = _env_bool("ENTRY_CHOCH1", True)    # CHoCH qui suit un BOS (retournement classique)
ENTRY_CHOCH2 = _env_bool("ENTRY_CHOCH2", True)    # CHoCH + CHoCH (CHoCH qui suit directement un CHoCH)
SL_BUFFER_ATR = _env_float("SL_BUFFER_ATR", 0.1)   # buffer au-delà de la ligne du BOS
MIN_SL_ATR = _env_float("MIN_SL_ATR", 0.3)         # SL minimum (en ATR)
MAX_SL_ATR = _env_float("MAX_SL_ATR", 6.0)         # au-delà : signal ignoré (BOS trop ancien)
BE_RR = _env_float("BE_RR", 2.0)          # RR auquel le SL est déplacé à l'entrée (BE)
TP_RR = _env_float("TP_RR", 4.0)          # RR de la TP finale (clôture complète, pas de TP1/TP2)
RR_LEVELS = (1.0, 2.0, 3.0)                # paliers intermédiaires notifiés au groupe (hors TP finale)
MAX_POSITIONS = _env_int("MAX_POSITIONS", 3)     # positions max en cours par actif

# --- Risque & levier --------------------------------------------------------
# Le lot est calculé uniquement à partir du risque $ et de la distance du SL (aucun solde requis).
DEFAULT_RISK_USD = _env_float("DEFAULT_RISK_USD", 10.0)  # modifiable à tout moment via /risque
MAX_RISK_USD = 100000.0
# Le levier ne sert qu'à afficher une marge indicative en privé (n'influence jamais le lot).
DEFAULT_LEVERAGE = _env_float("DEFAULT_LEVERAGE", 200.0)  # modifiable via /levier
LEVERAGE_PRESETS = (50, 100, 200, 500)

# --- Système ---------------------------------------------------------------
_SCAN_INTERVAL_ENV = _env_int("SCAN_INTERVAL", 0)    # secondes entre deux vérifications ; 0 = automatique


def scan_interval():
    """Secondes entre deux vérifications : SCAN_INTERVAL si défini, sinon 5 s en M1 et 20 s au-delà."""
    return _SCAN_INTERVAL_ENV or (5 if TIMEFRAME_MIN == 1 else 20)
CANDLES_LIMIT = 300
DB_PATH = os.getenv("DB_PATH", "alphabot.db")
DAILY_REPORT_HOUR_UTC = _env_int("DAILY_REPORT_HOUR_UTC", 21)


# ============================================================================
# 2. BASE DE DONNÉES (SQLite)
#
# SQLite : paramètres persistants, trades, méta.
# ============================================================================

_lock = threading.RLock()
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.row_factory = sqlite3.Row
_conn.execute("PRAGMA journal_mode=WAL")
_conn.executescript("""
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_key TEXT UNIQUE, symbol TEXT, side TEXT, kind TEXT,
    entry REAL, sl REAL, sl_initial REAL, tp REAL,
    risk_usd REAL, lot REAL, leverage REAL,
    status TEXT DEFAULT 'OPEN', be_hit INTEGER DEFAULT 0,
    opened_ts INTEGER, last_ts INTEGER, closed_ts INTEGER,
    outcome TEXT, result_r REAL, pnl_usd REAL
);
""")
_conn.commit()


def _q(sql, args=(), commit=False):
    with _lock:
        cur = _conn.execute(sql, args)
        if commit:
            _conn.commit()
        return cur


def _ensure_column(table, col, decl):
    """Migration légère : ajoute une colonne si une base existante (ancienne version) ne l'a pas encore."""
    cols = [r["name"] for r in _q(f"PRAGMA table_info({table})").fetchall()]
    if col not in cols:
        _q(f"ALTER TABLE {table} ADD COLUMN {col} {decl}", commit=True)


# Bases créées par une version antérieure (colonnes tp1/tp2/rr1_hit/tp1_hit) : on ajoute
# les nouvelles colonnes sans toucher aux anciennes ni perdre l'historique déjà enregistré.
_ensure_column("trades", "tp", "REAL")
_ensure_column("trades", "be_hit", "INTEGER DEFAULT 0")
_ensure_column("trades", "leverage", "REAL")
_ensure_column("trades", "rr1_hit", "INTEGER DEFAULT 0")
_ensure_column("trades", "rr2_hit", "INTEGER DEFAULT 0")
_ensure_column("trades", "rr3_hit", "INTEGER DEFAULT 0")
_ensure_column("trades", "timeframe", "TEXT")
# Suivi de l'envoi du signal d'ouverture (1 = envoyé). Les anciens trades restent à 1 : pas de renvoi.
_ensure_column("trades", "sent_admin", "INTEGER DEFAULT 1")
_ensure_column("trades", "sent_group", "INTEGER DEFAULT 1")


# --- paramètres / méta -------------------------------------------------------
def get_setting(key, default=None):
    r = _q("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def set_setting(key, value):
    _q("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
       (key, str(value)), commit=True)


def get_meta(key, default=None):
    r = _q("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def set_meta(key, value):
    _q("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
       (key, str(value)), commit=True)


def get_risk():
    return float(get_setting("risk_usd", DEFAULT_RISK_USD))


def set_risk(v):
    set_setting("risk_usd", round(float(v), 2))


def get_leverage():
    return float(get_setting("leverage", DEFAULT_LEVERAGE))


def set_leverage(v):
    set_setting("leverage", round(float(v), 2))


# --- timeframe (modifiable à chaud) --------------------------------------------------------
_tf_lock = threading.RLock()   # un passage de scan et un changement de timeframe ne se chevauchent jamais


def _apply_timeframe(minutes):
    global TIMEFRAME_MIN, TF_LABEL, TF_SEC
    TIMEFRAME_MIN, TF_LABEL, TF_SEC = minutes, TF_LABELS[minutes], minutes * 60


def load_timeframe():
    """Timeframe au démarrage : la variable TIMEFRAME (Render / .env) reprend la main dès qu'elle change ;
    sinon on reprend le dernier choix fait via Telegram ; à défaut DEFAULT_TIMEFRAME. Retourne la source."""
    env_raw = os.getenv("TIMEFRAME", "").strip().upper()
    env_tf = _parse_timeframe(env_raw) if env_raw else None
    if env_raw and env_tf not in TF_LABELS:
        print(f"⚠️  TIMEFRAME={env_raw!r} non supporté (choix : {', '.join(TF_LABELS.values())}) — ignoré.")
        env_tf = None
    saved = _parse_timeframe(get_setting("timeframe", ""))
    if env_tf and env_raw != get_setting("timeframe_env"):
        set_setting("timeframe_env", env_raw)
        set_setting("timeframe", env_tf)
        chosen, source = env_tf, "variable TIMEFRAME de Render"
    elif saved in TF_LABELS:
        chosen, source = saved, "dernier choix fait sur Telegram"
    elif env_tf:
        chosen, source = env_tf, "variable TIMEFRAME de Render"
    else:
        chosen, source = TIMEFRAME_MIN, "défaut du code"
    _apply_timeframe(chosen)
    return source


_TF_SOURCE = load_timeframe()


# --- trades ------------------------------------------------------------------
def signal_exists(key):
    return _q("SELECT 1 FROM trades WHERE signal_key=?", (key,)).fetchone() is not None


def count_open(symbol):
    return _q("SELECT COUNT(*) n FROM trades WHERE symbol=? AND status='OPEN'", (symbol,)).fetchone()["n"]


def add_trade(**t):
    cols = ",".join(t)
    marks = ",".join("?" for _ in t)
    cur = _q(f"INSERT INTO trades({cols}) VALUES({marks})", tuple(t.values()), commit=True)
    return cur.lastrowid


def update_trade(trade_id, **fields):
    sets = ",".join(f"{k}=?" for k in fields)
    _q(f"UPDATE trades SET {sets} WHERE id=?", (*fields.values(), trade_id), commit=True)


def open_trades(symbol=None):
    if symbol:
        rows = _q("SELECT * FROM trades WHERE status='OPEN' AND symbol=? ORDER BY id", (symbol,))
    else:
        rows = _q("SELECT * FROM trades WHERE status='OPEN' ORDER BY id")
    return [dict(r) for r in rows.fetchall()]


def _max_loss_streak(closed_sorted):
    """Plus longue série de SL consécutifs (un TP ou un BE interrompt la série)."""
    streak = best = 0
    for r in closed_sorted:
        if (r["result_r"] or 0) < 0:
            streak += 1
            best = max(best, streak)
        else:
            streak = 0
    return best


def query_stats(since_ts=None, symbol=None, timeframe=None):
    """Statistiques filtrables (période / symbole / timeframe) : winrate, taux par RR, série de pertes max.

    Pas de $ : uniquement des compteurs, des R et des taux.
    """
    sql, args = "SELECT * FROM trades WHERE 1=1", []
    if since_ts is not None:
        sql += " AND opened_ts>=?"; args.append(since_ts)
    if symbol:
        sql += " AND symbol=?"; args.append(symbol)
    if timeframe:
        sql += " AND timeframe=?"; args.append(timeframe)
    rows = [dict(r) for r in _q(sql, tuple(args)).fetchall()]
    closed = [r for r in rows if r["status"] == "CLOSED"]
    wins = [r for r in closed if (r["result_r"] or 0) > 0]
    losses = [r for r in closed if (r["result_r"] or 0) < 0]
    be = [r for r in closed if (r["result_r"] or 0) == 0]
    decided = len(wins) + len(losses)
    n_closed = len(closed)

    def _rate(field):
        hit = sum(1 for r in closed if r.get(field))
        return (100.0 * hit / n_closed) if n_closed else 0.0

    rr_rates = {int(lvl): _rate(f"rr{int(lvl)}_hit") for lvl in RR_LEVELS}
    tp_hit = sum(1 for r in closed if r["outcome"] == "TP")
    rr_rates["TP"] = (100.0 * tp_hit / n_closed) if n_closed else 0.0
    closed_sorted = sorted(closed, key=lambda r: r["closed_ts"] or 0)

    return {
        "signals": len(rows), "open": len(rows) - n_closed, "closed": n_closed,
        "wins": len(wins), "losses": len(losses), "be": len(be),
        "winrate": (100.0 * len(wins) / decided) if decided else 0.0,
        "total_r": sum((r["result_r"] or 0) for r in closed),
        "rr_rates": rr_rates,
        "max_loss_streak": _max_loss_streak(closed_sorted),
    }


def now_ts():
    return int(time.time())


# ============================================================================
# 3. DONNÉES DE PRIX
#
# Bougies CLÔTURÉES uniquement, au timeframe TIMEFRAME_MIN. Exactement deux sources publiques :
#   Gold (XAUUSD) -> Deriv : WebSocket public, ticks_history (style candles), sans authentification
#   BTC (BTCUSD)  -> Binance : REST public /api/v3/klines, sans clé
# Format d'une bougie : {"t": epoch_ouverture_sec, "o":, "h":, "l":, "c":}
# ============================================================================

CLOSE_GRACE_SEC = 2   # marge après la clôture : décalage d'horloge / finalisation de la bougie chez la source
BINANCE_BASES = ["https://api.binance.com", "https://data-api.binance.vision"]  # 2e = miroir public Binance
BINANCE_INTERVALS = {1: "1m", 3: "3m", 5: "5m", 15: "15m", 30: "30m", 60: "1h"}


def _only_closed(candles):
    now = time.time()
    return sorted((c for c in candles if c["t"] + TF_SEC + CLOSE_GRACE_SEC <= now),
                  key=lambda c: c["t"])


# --- BTC : API publique Binance (klines), sans clé --------------------------------------
def _binance(symbol):
    last_err = None
    for base in BINANCE_BASES:
        try:
            r = requests.get(
                base + "/api/v3/klines",
                params={"symbol": symbol, "interval": BINANCE_INTERVALS[TIMEFRAME_MIN],
                        "limit": CANDLES_LIMIT},
                timeout=15)
            r.raise_for_status()
            return [{"t": int(k[0] // 1000), "o": float(k[1]), "h": float(k[2]),
                     "l": float(k[3]), "c": float(k[4])} for k in r.json()]
        except Exception as e:  # essaie l'URL suivante
            last_err = e
    raise RuntimeError(f"Binance indisponible : {last_err}")


# --- Gold : API publique Deriv (WebSocket ticks_history), sans clé ni authentification --------
class DerivError(RuntimeError):
    """Erreur renvoyée par l'API Deriv (différent d'un problème de connexion)."""


_deriv_ws = None      # connexion WebSocket publique, réutilisée d'un appel à l'autre
_deriv_req_id = 0


def _deriv_request(payload):
    """Envoie une requête sur le WebSocket public Deriv et retourne la réponse (bougies).

    La connexion est réutilisée ; si elle est tombée (inactivité, réseau), une reconnexion
    est tentée une fois avant d'abandonner.
    """
    global _deriv_ws, _deriv_req_id
    if websocket is None:
        raise RuntimeError("module manquant : pip install websocket-client")
    last_err = None
    for _ in range(2):
        try:
            if _deriv_ws is None:
                _deriv_ws = websocket.create_connection(DERIV_WS_URL, timeout=15)
            _deriv_req_id += 1
            rid = _deriv_req_id
            _deriv_ws.send(json.dumps({**payload, "req_id": rid}))
            while True:
                msg = json.loads(_deriv_ws.recv())
                if msg.get("req_id", rid) != rid:
                    continue  # réponse tardive d'une requête précédente
                if "error" in msg:
                    err = msg["error"]
                    raise DerivError(err.get("message", err) if isinstance(err, dict) else err)
                if "candles" in msg:
                    return msg
        except DerivError:
            raise
        except Exception as e:  # connexion morte / timeout : on repart sur une connexion neuve
            last_err = e
            try:
                if _deriv_ws is not None:
                    _deriv_ws.close()
            except Exception:
                pass
            _deriv_ws = None
    raise RuntimeError(f"Deriv indisponible : {last_err}")


def _deriv(deriv_symbol):
    """Bougies du timeframe demandé, construites par Deriv (ticks_history, style candles)."""
    msg = _deriv_request({
        "ticks_history": deriv_symbol, "style": "candles", "granularity": TF_SEC,
        "count": CANDLES_LIMIT, "end": "latest",
        "adjust_start_time": 1,   # marché fermé (week-end) : recule jusqu'aux dernières bougies dispo
    })
    return [{"t": int(k["epoch"]), "o": float(k["open"]), "h": float(k["high"]),
             "l": float(k["low"]), "c": float(k["close"])} for k in msg["candles"]]


def get_candles(symbol):
    """Retourne la liste des bougies clôturées (plus ancienne -> plus récente)."""
    cfg = SYMBOLS[symbol]
    if cfg["source"] == "deriv":
        raw = _deriv(cfg["deriv_symbol"])
    elif cfg["source"] == "binance":
        raw = _binance(cfg["binance_symbol"])
    else:
        raise ValueError(f"Source de prix inconnue pour {symbol} : {cfg['source']}")
    return _only_closed(raw)


# ============================================================================
# 4. SIGNAUX BOS / CHoCH
#
# Détection BOS / CHoCH et construction du signal (entrée directe).
#
# Règles :
# - Swing high/low confirmés (profondeur SWING_DEPTH), cassure validée par la CLÔTURE.
# - BOS  = cassure dans le sens de la tendance en cours.
# - CHoCH = cassure dans le sens opposé à la tendance en cours.
# - Entrée directe à la clôture de la bougie qui casse (CHoCH).
#     CHOCH1 : CHoCH qui suit un BOS (retournement classique)
#     CHOCH2 : CHoCH qui suit directement un autre CHoCH (CHoCH + CHoCH)
# - SL : sous la ligne du dernier BOS opposé (achat) / au-dessus (vente), + petit buffer ATR.
# ============================================================================

def atr_series(c, period=ATR_PERIOD):
    out, trs = [], []
    for i, x in enumerate(c):
        if i == 0:
            tr = x["h"] - x["l"]
        else:
            pc = c[i - 1]["c"]
            tr = max(x["h"] - x["l"], abs(x["h"] - pc), abs(x["l"] - pc))
        trs.append(tr)
        out.append(sum(trs) / len(trs) if i < period else (out[-1] * (period - 1) + tr) / period)
    return out


def analyze(c, depth=SWING_DEPTH):
    """Parcourt les bougies et retourne la liste des événements de structure."""
    n = len(c)
    atr = atr_series(c)
    swing_h = swing_l = None          # swings non cassés : (prix, index)
    last_h = last_l = None            # dernier swing confirmé (cassé ou non)
    trend, prev = 0, None
    bos_lvl = {1: None, -1: None}     # dernier niveau de BOS par direction
    events = []

    for i in range(n):
        p = i - depth
        if p >= depth:
            win_h = [x["h"] for x in c[p - depth:p + depth + 1]]
            win_l = [x["l"] for x in c[p - depth:p + depth + 1]]
            if c[p]["h"] == max(win_h) and all(c[p]["h"] > c[k]["h"] for k in range(p - depth, p)):
                swing_h = last_h = (c[p]["h"], p)
            if c[p]["l"] == min(win_l) and all(c[p]["l"] < c[k]["l"] for k in range(p - depth, p)):
                swing_l = last_l = (c[p]["l"], p)

        close = c[i]["c"]
        d = None
        if swing_h and close > swing_h[0]:
            d, level, src = 1, swing_h[0], swing_h[1]
            swing_h = None
        elif swing_l and close < swing_l[0]:
            d, level, src = -1, swing_l[0], swing_l[1]
            swing_l = None
        if d is None:
            continue

        if trend == 0:
            kind = "INIT"      # première cassure : sert juste à définir la tendance
        elif trend == d:
            kind = "BOS"
        else:
            kind = "CHOCH"

        ev = {"i": i, "t": c[i]["t"], "dir": d, "kind": kind, "level": level,
              "src": src, "atr": atr[i], "type": None, "sl_level": None}

        if kind in ("BOS", "INIT"):
            bos_lvl[d] = level
        else:  # CHOCH
            ev["type"] = "CHOCH1" if prev in ("BOS", "INIT") else "CHOCH2"
            lvl = bos_lvl[-d]                      # ligne du dernier BOS opposé
            if lvl is None or (d == 1 and lvl >= close) or (d == -1 and lvl <= close):
                fb = last_l if d == 1 else last_h  # repli : dernier swing opposé
                lvl = fb[0] if fb else None
            ev["sl_level"] = lvl
        trend, prev = d, kind
        events.append(ev)
    return events


def build_signal(c, ev):
    """Transforme un CHoCH en signal (entrée, SL, TP). None si invalide."""
    if ev["kind"] != "CHOCH" or ev["sl_level"] is None:
        return None
    if ev["type"] == "CHOCH1" and not ENTRY_CHOCH1:
        return None
    if ev["type"] == "CHOCH2" and not ENTRY_CHOCH2:
        return None
    d, a = ev["dir"], ev["atr"]
    entry = c[ev["i"]]["c"]
    sl = ev["sl_level"] - d * SL_BUFFER_ATR * a
    if (d == 1 and sl >= entry) or (d == -1 and sl <= entry):
        return None
    risk = abs(entry - sl)
    if risk < MIN_SL_ATR * a:
        risk = MIN_SL_ATR * a
        sl = entry - d * risk
    if risk > MAX_SL_ATR * a:
        return None
    return {
        "dir": d, "side": "BUY" if d == 1 else "SELL", "type": ev["type"],
        "entry": entry, "sl": sl, "risk": risk,
        "tp": entry + d * risk * TP_RR,
        "t": ev["t"], "bos_level": ev["sl_level"],
    }


# ============================================================================
# 5. RISQUE (calcul du lot)
#
# Calcul du lot à partir du montant en $ à risquer.
# ============================================================================

def calc_lot(symbol, risk_usd, sl_distance):
    """Lot = montant risqué / (distance SL en points x valeur du point par lot).

    Arrondi vers le bas au pas du broker ; si le résultat est sous le lot minimum,
    retourne le lot minimum (le risque réel sera alors plus élevé : voir 'real_risk').
    """
    cfg = SYMBOLS[symbol]
    vpp = cfg["value_per_point"]
    if sl_distance <= 0 or vpp <= 0 or risk_usd <= 0:
        return {"lot": 0.0, "real_risk": 0.0, "raised_to_min": False}
    raw = risk_usd / (sl_distance * vpp)
    step, min_lot = cfg["lot_step"], cfg["min_lot"]
    lot = math.floor(raw / step + 1e-9) * step
    raised = False
    if lot < min_lot:
        lot, raised = min_lot, True
    lot = round(lot, 6)
    return {"lot": lot, "real_risk": lot * sl_distance * vpp, "raised_to_min": raised}


def calc_margin(symbol, lot, price, leverage):
    """Marge indicative pour ce lot à ce prix/levier — n'influence jamais le calcul du lot ci-dessus.

    Notionnel = lot x value_per_point x prix (value_per_point ~ taille du contrat pour ces 2 actifs).
    À titre indicatif seulement : la marge réelle dépend du broker.
    """
    if leverage <= 0:
        return 0.0
    notional = lot * SYMBOLS[symbol]["value_per_point"] * price
    return notional / leverage


# ============================================================================
# 6. SUIVI DES POSITIONS
#
# Suivi des positions sur les bougies clôturées.
#
# Grille : RR{BE_RR} -> SL à l'entrée (BE) | RR{TP_RR} -> clôture finale (TP), pas de clôture partielle.
# Si SL et objectif sont touchés dans la même bougie, le SL est compté en premier (prudent).
# ============================================================================

def track_trade(trade, candles):
    """Fait avancer un trade avec les nouvelles bougies. Retourne la liste des événements.

    Grille simple : RR{BE_RR} -> SL déplacé à l'entrée (BE) ; RR{TP_RR} -> clôture finale (TP).
    Pas de clôture partielle (pas de TP1/TP2).
    Événements : {"name": "BE_MOVED"|"TP"|"SL"|"BE", "trade": trade_dict}
      - "BE_MOVED" : SL déplacé à l'entrée, position toujours ouverte.
      - "BE"       : position refermée à l'entrée (0 R), après un déplacement du SL.
      - "TP"       : take-profit finale touchée (+TP_RR R).
      - "SL"       : stop initial touché (-1 R).
    Si SL et TP sont touchés dans la même bougie, le SL est compté en premier (prudent).
    """
    events = []
    side = 1 if trade["side"] == "BUY" else -1
    entry, risk = trade["entry"], abs(trade["entry"] - trade["sl_initial"])
    last_ts = trade["last_ts"]

    for c in candles:
        if c["t"] <= last_ts:
            continue
        last_ts = c["t"]
        adverse = c["l"] if side == 1 else c["h"]
        favorable = c["h"] if side == 1 else c["l"]

        # 1) stop touché (initial ou déplacé à l'entrée) ?
        if (side == 1 and adverse <= trade["sl"]) or (side == -1 and adverse >= trade["sl"]):
            at_be = bool(trade["be_hit"]) and abs(trade["sl"] - entry) < 1e-9
            r = 0.0 if at_be else -1.0
            trade.update(status="CLOSED", closed_ts=c["t"], result_r=r,
                         pnl_usd=r * trade["risk_usd"], outcome="BE" if at_be else "SL")
            events.append({"name": "BE" if at_be else "SL", "trade": dict(trade)})
            break

        # 2) progression : paliers RR1/RR2/RR3 (notification groupe, BE combiné au palier BE_RR)
        r_fav = (favorable - entry) * side / risk
        for lvl in RR_LEVELS:
            field = f"rr{int(lvl)}_hit"
            if not trade[field] and r_fav >= lvl:
                trade[field] = 1
                be_now = not trade["be_hit"] and lvl >= BE_RR
                if be_now:
                    trade["be_hit"], trade["sl"] = 1, entry
                events.append({"name": f"RR{int(lvl)}", "be_moved": be_now, "trade": dict(trade)})
        # BE_RR peut ne pas être un palier RR1/2/3 rond (ex. BE_RR=2.5) : on le couvre séparément
        if not trade["be_hit"] and BE_RR not in RR_LEVELS and r_fav >= BE_RR:
            trade["be_hit"], trade["sl"] = 1, entry
            events.append({"name": "BE_MOVED", "trade": dict(trade)})
        if r_fav >= TP_RR:
            trade.update(status="CLOSED", closed_ts=c["t"], result_r=TP_RR,
                         pnl_usd=TP_RR * trade["risk_usd"], outcome="TP")
            events.append({"name": "TP", "trade": dict(trade)})
            break

    trade["last_ts"] = last_ts
    fields = {k: trade[k] for k in ("sl", "be_hit", "status", "last_ts", "closed_ts",
                                    "result_r", "pnl_usd", "outcome",
                                    "rr1_hit", "rr2_hit", "rr3_hit")}
    update_trade(trade["id"], **fields)
    return events


# ============================================================================
# 7. GRAPHIQUE
#
# Image du graphique envoyée avec chaque signal (matplotlib, optionnel).
# ============================================================================

CHART_DIR = "charts"


def make_chart(symbol, candles, events, sig, decimals=2, n_show=70, extend=25):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except ImportError:
        return None

    os.makedirs(CHART_DIR, exist_ok=True)
    # nettoyage : garde les 40 images les plus récentes
    files = sorted((os.path.join(CHART_DIR, f) for f in os.listdir(CHART_DIR)), key=os.path.getmtime)
    for f in files[:-40]:
        try:
            os.remove(f)
        except OSError:
            pass

    end = next((k for k, c in enumerate(candles) if c["t"] == sig["t"]), len(candles) - 1)
    start = max(0, end - n_show + 1)
    view = candles[start:end + 1]
    fig, ax = plt.subplots(figsize=(9, 5), dpi=110)
    fig.patch.set_facecolor("#0e1117")
    ax.set_facecolor("#0e1117")

    for k, c in enumerate(view):
        up = c["c"] >= c["o"]
        col = "#26a69a" if up else "#ef5350"
        ax.plot([k, k], [c["l"], c["h"]], color=col, lw=0.8)
        ax.add_patch(Rectangle((k - 0.3, min(c["o"], c["c"])), 0.6,
                               max(abs(c["c"] - c["o"]), 1e-9), color=col))

    for ev in events:  # lignes BOS / CHoCH visibles
        if start <= ev["i"] <= end and ev["src"] >= start and ev["kind"] != "INIT":
            col = "#26a69a" if ev["dir"] == 1 else "#ef5350"
            ax.plot([ev["src"] - start, ev["i"] - start], [ev["level"]] * 2,
                    color=col, lw=0.9, ls="--")
            ax.text((ev["src"] + ev["i"]) / 2 - start, ev["level"], ev["kind"].replace("CHOCH", "CHoCH"),
                    color=col, fontsize=7, ha="center", va="bottom")

    x0, x1 = len(view) - 1, len(view) - 1 + extend
    e, sl, tp = sig["entry"], sig["sl"], sig["tp"]
    ax.add_patch(Rectangle((x0, min(e, tp)), x1 - x0, abs(tp - e), color="#26a69a", alpha=0.25))
    ax.add_patch(Rectangle((x0, min(e, sl)), x1 - x0, abs(e - sl), color="#ef5350", alpha=0.25))
    for lvl, lab, col in ((e, "Entrée", "#ffffff"), (sl, "SL", "#ef5350"),
                          (tp, "TP", "#26a69a")):
        ax.axhline(lvl, color=col, lw=0.6, ls=":")
        ax.text(x1 + 0.5, lvl, f"{lab} {lvl:.{decimals}f}", color=col, fontsize=8, va="center")

    ax.set_xlim(-1, x1 + 14)
    lo = min(min(c["l"] for c in view), sl, tp)
    hi = max(max(c["h"] for c in view), sl, tp)
    pad = (hi - lo) * 0.05
    ax.set_ylim(lo - pad, hi + pad)
    ax.set_title(f"{symbol} {TF_LABEL} - {sig['side']} ({'CHoCH + CHoCH' if sig['type'] == 'CHOCH2' else 'CHoCH'})",
                 color="white", fontsize=11)
    ax.tick_params(colors="#888888", labelsize=7)
    ax.set_xticks([])
    for s in ax.spines.values():
        s.set_color("#333333")
    path = os.path.join(CHART_DIR, f"{symbol}_{sig['t']}.png")
    fig.savefig(path, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return path


# ============================================================================
# 8. TELEGRAM
#
# Telegram : messages groupe (signaux + suivi, SANS lot), messages privés (lot, PnL $),
# et commandes réservées à l'admin (/risque, /stats, /trades, /start, /stop).
# ============================================================================

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


# --- envoi ------------------------------------------------------------------------
def _post(method, data=None, files=None, _retry=True):
    if not TELEGRAM_TOKEN:
        print(f"[telegram OFF] {method}: {(data or {}).get('text', '')[:80]}")
        return None
    try:
        r = requests.post(f"{TELEGRAM_API}/{method}", data=data, files=files, timeout=30)
        res = r.json()
    except Exception as e:
        print(f"[telegram] erreur {method}: {e}")
        return None
    if not res.get("ok") and "message is not modified" in str(res.get("description", "")):
        return res   # bouton pressé sans changement de contenu : sans importance, on ne pollue pas les logs
    if not res.get("ok"):
        print(f"[telegram] {method} REFUSÉ vers {(data or {}).get('chat_id')} : "
              f"{res.get('error_code')} {res.get('description')}")
        wait = (res.get("parameters") or {}).get("retry_after")
        if wait and _retry and not files and wait <= 30:   # flood control : on patiente puis on retente
            time.sleep(wait + 1)
            return _post(method, data, files, _retry=False)
    return res


def send(chat_id, text, photo=None, reply_markup=None):
    if not chat_id:
        return None
    data = {"chat_id": chat_id, "parse_mode": "HTML"}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    if photo:
        try:
            with open(photo, "rb") as f:
                data["caption"] = text[:1000]
                res = _post("sendPhoto", data, files={"photo": f})
            if res and res.get("ok"):
                return res
        except OSError:
            pass
    data["text"] = text
    data.pop("caption", None)
    res = _post("sendMessage", data)
    if res and not res.get("ok") and "parse entities" in str(res.get("description", "")):
        data.pop("parse_mode", None)          # balise HTML invalide : on renvoie en texte brut
        data["text"] = re.sub(r"<[^>]+>", "", text)
        res = _post("sendMessage", data)
    return res


def _delivered(chat_id, res):
    """True si le message est bien parti (ou s'il n'y avait rien à envoyer : pas de chat / mode console)."""
    if not chat_id or not TELEGRAM_TOKEN:
        return True
    return bool(res and res.get("ok"))


def to_group(text, photo=None):
    return send(CHAT_ID_GROUPE, text, photo)


def to_admin(text, reply_markup=None):
    return send(CHAT_ID_ADMIN, text, reply_markup=reply_markup)


# --- stickers / images / GIF du groupe (TP, SL, BE, motivation) --------------------------------
# Un média est mémorisé sous la forme "type:file_id" (type : sticker | photo | animation).
# L'admin en ajoute en envoyant simplement un sticker, une image ou un GIF au bot EN PRIVÉ, puis en choisissant
# la catégorie (voir _handle_update). Stockage : base SQLite + variables d'env optionnelles MEDIA_TP, MEDIA_SL,
# MEDIA_BE, MEDIA_MOTIV (liste séparée par des virgules, cf. /medias export) qui survivent à un redéploiement
# Render sans disque persistant. Plusieurs médias par catégorie : un est tiré au hasard à chaque événement.
MEDIA_CATS = {"TP": "🎯 TP", "SL": "🔴 SL", "BE": "➖ BE", "MOTIV": "💪 Motivation"}
_MEDIA_SEND = {"sticker": "sendSticker", "photo": "sendPhoto", "animation": "sendAnimation"}
_pending_media = {}   # (chat_id, message_id) -> "type:file_id" en attente du choix de catégorie


def get_media(cat):
    stored = json.loads(get_setting(f"media:{cat}", "[]"))
    env = [m.strip() for m in os.getenv(f"MEDIA_{cat}", "").split(",") if m.strip()]
    return list(dict.fromkeys(stored + env))


def add_media(cat, item):
    stored = json.loads(get_setting(f"media:{cat}", "[]"))
    if item not in stored:
        set_setting(f"media:{cat}", json.dumps(stored + [item]))
    return len(get_media(cat))


def clear_media(cat):
    set_setting(f"media:{cat}", "[]")


def to_group_media(cat):
    """Envoie au groupe un média tiré au hasard dans la catégorie (silencieux si aucun n'est configuré)."""
    items = get_media(cat)
    if not items or not CHAT_ID_GROUPE:
        return
    kind, _, file_id = random.choice(items).partition(":")
    if kind not in _MEDIA_SEND:
        return
    res = _post(_MEDIA_SEND[kind], {"chat_id": CHAT_ID_GROUPE, kind: file_id})
    if res is not None and not res.get("ok"):
        print(f"[telegram] média {cat} refusé : {res.get('description')}")


# --- formats ---------------------------------------------------------------------
def _fmt(v, dec):
    return f"{v:,.{dec}f}".replace(",", " ")


def _kind_label(kind):
    return "CHoCH (après BOS)" if kind == "CHOCH1" else "CHoCH + CHoCH"


def _rr_price(sig, level):
    side = 1 if sig["side"] == "BUY" else -1
    return sig["entry"] + side * level * sig["risk"]


def group_signal(symbol, sig, position_n, dec, tf=None):
    icon = "🟢" if sig["side"] == "BUY" else "🔴"
    rr_lines = "\n".join(
        f"RR{int(lvl)} : {_fmt(_rr_price(sig, lvl), dec)}" for lvl in RR_LEVELS)
    return (
        f"Nouveau signal\n"
        f"{icon} <b>{sig['side']} {symbol}</b> · {tf or TF_LABEL}\n"
        f"Type : {_kind_label(sig['type'])}\n\n"
        f"Entrée : <b>{_fmt(sig['entry'], dec)}</b>\n"
        f"SL : {_fmt(sig['sl'], dec)}\n"
        f"{rr_lines}\n"
        f"TP : {_fmt(sig['tp'], dec)}\n\n"
        f"BE → RR{BE_RR:g}\n"
        f"TP final → RR{TP_RR:g}"
        + (f"\n\nPosition {position_n}/{MAX_POSITIONS} sur {symbol}" if position_n else "")
    )


def admin_signal(symbol, sig, risk_usd, leverage, lot_info, margin, dec):
    """Message complet envoyé uniquement en privé/admin : signal + risque + levier + lot."""
    txt = (
        f"💰 <b>{symbol} {sig['side']}</b> (privé)\n"
        f"Entrée : {_fmt(sig['entry'], dec)}\n"
        f"SL : {_fmt(sig['sl'], dec)} (distance {_fmt(sig['risk'], dec)} pts)\n"
        f"TP : {_fmt(sig['tp'], dec)} (RR{TP_RR:g})\n\n"
        f"Risque : {risk_usd:g} $\n"
        f"Levier : {leverage:g}x\n"
        f"👉 <b>Lot calculé : {lot_info['lot']:g}</b>\n"
        f"Marge indicative ≈ {margin:.2f} $"
    )
    if lot_info["raised_to_min"]:
        txt += f"\n⚠️ Lot minimum appliqué : risque réel ≈ {lot_info['real_risk']:.2f} $"
    return txt


MOTIVATION_LINES = (
    "💪 Un SL fait partie du jeu : capital protégé, on passe au prochain setup.",
    "🧠 Discipline avant émotion : l'avantage se joue sur la série, pas sur un trade.",
    "🔥 Une perte maîtrisée prépare le prochain gain. On reste concentrés.",
    "📈 Même les meilleurs traders perdent — la différence, c'est le risque maîtrisé.",
)
_EVENT_MEDIA = {"TP": ("TP",), "SL": ("SL", "MOTIV"), "BE": ("BE",)}   # événement -> catégories envoyées


def group_event_media(name):
    """Sticker / image / GIF envoyé au groupe après une clôture (TP, SL, BE)."""
    for cat in _EVENT_MEDIA.get(name, ()):
        to_group_media(cat)


def group_event(ev, dec):
    t, name = ev["trade"], ev["name"]
    head = f"{t['symbol']} {t['side']} @ {_fmt(t['entry'], dec)}"
    if name.startswith("RR"):
        lvl = int(name[2:])
        txt = f"🟡 <b>{name} atteint ✅</b> — {head}"
        if ev.get("be_moved"):
            txt += "\nSL déplacé à l'entrée (BE) 🔒"
        if lvl >= 2:
            txt += "\n💡 Clôture partielle possible ici pour ceux qui le souhaitent."
        return txt
    if name == "BE_MOVED":
        return f"🔒 <b>RR{BE_RR:g} atteint</b> — {head}\nSL déplacé à l'entrée (BE)."
    if name == "TP":
        return f"🎯 <b>RR{TP_RR:g} atteint — TP ✅</b> — {head}\nRésultat : <b>WIN ({t['result_r']:+.2f} R)</b>"
    if name == "BE":
        return f"➖ <b>Clôture à l'entrée (BE)</b> — {head}\nRésultat : <b>{t['result_r']:+.2f} R</b>"
    return (f"🔴 <b>SL touché ❌</b> — {head}\nRésultat : <b>LOSS ({t['result_r']:+.2f} R)</b>\n\n"
            f"<i>{random.choice(MOTIVATION_LINES)}</i>")


def admin_event(ev):
    t = ev["trade"]
    if ev["name"] in ("TP", "SL", "BE"):
        return f"📒 {t['symbol']} {t['side']} clôturé ({ev['name']}) : <b>{t['result_r']:+.2f} R</b>"
    return None


def _stats_line(icon, title, s):
    rr = s["rr_rates"]
    return (f"{icon} <b>{title}</b> — {s['closed']} trades · TP {s['wins']} · SL {s['losses']} "
            f"· BE {s['be']} · Winrate {s['winrate']:.1f}%\n"
            f"RR1 {rr[1]:.0f}% · RR2 {rr[2]:.0f}% · RR3 {rr[3]:.0f}% · RR4/TP {rr['TP']:.0f}% "
            f"· Pertes max {s['max_loss_streak']}")


def _period_starts():
    now = datetime.now(timezone.utc)
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week = day.timestamp() - day.weekday() * 86400
    month = day.replace(day=1)
    return int(day.timestamp()), int(week), int(month.timestamp())


def daily_report():
    day, _, _ = _period_starts()
    return _stats_line("📊", "Rapport du jour", query_stats(since_ts=day))


# --- commandes (admin uniquement) ----------------------------------------------------
def _risk_keyboard():
    return {"inline_keyboard": [[
        {"text": "−5", "callback_data": "risk:-5"}, {"text": "−1", "callback_data": "risk:-1"},
        {"text": "+1", "callback_data": "risk:+1"}, {"text": "+5", "callback_data": "risk:+5"}]]}


def _risk_text():
    return (f"💰 Montant à risquer par trade : <b>{get_risk():g} $</b>\n"
            f"Modifie avec les boutons ou tape /risque 25")


def _leverage_keyboard():
    return {"inline_keyboard": [
        [{"text": f"{v}x", "callback_data": f"lev:{v}"} for v in LEVERAGE_PRESETS]
    ]}


def _leverage_text():
    return (f"⚙️ Levier (indicatif — sert uniquement à estimer la marge) : <b>{get_leverage():g}x</b>\n"
            f"Choisis un préréglage ou tape /levier 100")


def _with_back(kb):
    kb["inline_keyboard"].append([{"text": "🔙 Menu", "callback_data": "menu:home"}])
    return kb


def _tf_keyboard():
    btns = [{"text": ("✅ " if m == TIMEFRAME_MIN else "") + lbl, "callback_data": f"tf:{m}"}
            for m, lbl in TF_LABELS.items()]
    return _with_back({"inline_keyboard": [btns[:3], btns[3:]]})


def _tf_text():
    n = len(open_trades())
    note = f"\n⚠️ {n} position(s) ouverte(s) : suivies avec les bougies du nouveau timeframe." if n else ""
    return (f"⏱ Timeframe : <b>{TF_LABEL}</b> (Gold + BTC)\n"
            f"Choisis avec les boutons ou tape /timeframe M15. Effet immédiat : le bot se recale sur la dernière "
            f"bougie clôturée, sans rejouer l'historique.{note}")


def _media_keyboard(prefix, suffix="", verb=""):
    """Boutons des catégories, 2 par ligne ; callback_data = prefix:CAT[suffix]."""
    cats = list(MEDIA_CATS.items())
    return {"inline_keyboard": [
        [{"text": verb + lbl, "callback_data": f"{prefix}:{cat}{suffix}"} for cat, lbl in cats[i:i + 2]]
        for i in range(0, len(cats), 2)]}


def _media_text():
    counts = "\n".join(f"{lbl} : {len(get_media(cat))}" for cat, lbl in MEDIA_CATS.items())
    return (f"🎭 <b>Stickers / images / GIF du groupe</b>\n{counts}\n\n"
            "➕ Ajouter : envoie-moi ici (en privé) un sticker, une image ou un GIF, puis choisis la catégorie.\n"
            "🗑 Vider : boutons ci-dessous (les MEDIA_* définis sur Render restent).\n"
            "🧪 Tester : /testmedia TP (envoi au groupe)\n"
            "💾 /medias export : lignes à coller sur Render pour survivre aux redéploiements.")


def _media_from_message(msg):
    """'type:file_id' si le message contient un sticker / GIF / image, sinon None."""
    if "sticker" in msg:
        return f"sticker:{msg['sticker']['file_id']}"
    if "animation" in msg:
        return f"animation:{msg['animation']['file_id']}"
    if "photo" in msg:
        return f"photo:{msg['photo'][-1]['file_id']}"
    return None


def _menu_keyboard():
    return {"inline_keyboard": [
        [{"text": "💰 Risque", "callback_data": "menu:risque"}, {"text": "⚙️ Levier", "callback_data": "menu:levier"}],
        [{"text": "⏱ Timeframe", "callback_data": "menu:timeframe"}, {"text": "🎭 Médias", "callback_data": "menu:medias"}],
        [{"text": "📊 Stats", "callback_data": "menu:stats"}, {"text": "📈 Positions", "callback_data": "menu:trades"}],
        [{"text": "🔄 Actualiser", "callback_data": "menu:home"}],
    ]}


def _back_keyboard():
    return {"inline_keyboard": [[{"text": "🔙 Menu", "callback_data": "menu:home"}]]}


def _home_text():
    return (f"🤖 <b>ALPHABOT</b>\n\n"
            f"💰 Risque : {get_risk():g} $\n"
            f"⚙️ Levier : {get_leverage():g}x\n"
            f"⏱ Timeframe : {TF_LABEL}")


def _is_admin(user_id):
    return str(user_id) == str(CHAT_ID_ADMIN)


def handle_command(text):
    """Retourne (texte, clavier|None) pour une commande admin. Le bot tourne en permanence :
    pas de /start ni /stop — le scan et le suivi des positions ne s'arrêtent jamais."""
    parts = text.strip().split()
    cmd = parts[0].split("@")[0].lower()
    if cmd in ("/aide", "/help"):
        txt = (_home_text() + "\n\n"
               "/risque [montant] — montant à risquer par trade ($)\n"
               "/levier [valeur] — levier (indicatif, calcul de marge)\n"
               "/stats — statistiques (jour / semaine + taux par RR)\n"
               "/trades — positions en cours (avec RR actuel)\n"
               "/timeframe [M1|M3|M5|M15|M30|H1] — change le timeframe à chaud\n"
               "/medias — stickers / images / GIF du groupe (TP, SL, BE, motivation)\n"
               "/menu — menu à boutons")
        return (txt, _menu_keyboard())
    if cmd == "/menu":
        return (_home_text(), _menu_keyboard())
    if cmd == "/risque":
        if len(parts) > 1:
            try:
                v = float(parts[1].replace(",", "."))
                if not 0 < v <= MAX_RISK_USD:
                    raise ValueError
                set_risk(v)
            except ValueError:
                return ("Montant invalide. Exemple : /risque 10", None)
        return (_risk_text(), _risk_keyboard())
    if cmd == "/levier":
        if len(parts) > 1:
            try:
                v = float(parts[1].replace(",", "."))
                if v <= 0:
                    raise ValueError
                set_leverage(v)
            except ValueError:
                return ("Levier invalide. Exemple : /levier 100", None)
        return (_leverage_text(), _leverage_keyboard())
    if cmd in ("/timeframe", "/tf"):
        if len(parts) > 1:
            tf = _parse_timeframe(parts[1])
            if tf not in TF_LABELS:
                return ("Timeframe invalide. Choix : " + ", ".join(TF_LABELS.values())
                        + "\nExemple : /timeframe M15", None)
            set_timeframe(tf)
        return (_tf_text(), _tf_keyboard())
    if cmd == "/medias":
        if len(parts) > 1 and parts[1].lower() == "export":
            lines = [f"<code>MEDIA_{c}={','.join(get_media(c))}</code>" for c in MEDIA_CATS if get_media(c)]
            return ("\n".join(lines) or "Aucun média enregistré.", None)
        return (_media_text(), _with_back(_media_keyboard("mclr", verb="🗑 ")))
    if cmd == "/testmedia":
        cat = parts[1].upper() if len(parts) > 1 else ""
        cat = "MOTIV" if cat.startswith("MOTIV") else cat
        if cat not in MEDIA_CATS:
            return ("Exemple : /testmedia TP — catégories : TP, SL, BE, MOTIV (envoi au groupe).", None)
        if not get_media(cat):
            return (f"Aucun média enregistré pour {MEDIA_CATS[cat]}.", None)
        to_group_media(cat)
        return (f"✅ Envoyé au groupe ({MEDIA_CATS[cat]}).", None)
    if cmd == "/stats":
        d, w, mo = _period_starts()
        lines = [
            _stats_line("📊", "Aujourd'hui", query_stats(since_ts=d)),
            _stats_line("📅", "Cette semaine", query_stats(since_ts=w)),
            _stats_line("📆", "Ce mois", query_stats(since_ts=mo)),
            _stats_line("🌐", "Global", query_stats()),
            _stats_line("🥇", "XAUUSD", query_stats(symbol="XAUUSD")),
            _stats_line("₿", "BTCUSD", query_stats(symbol="BTCUSD")),
        ]
        tfs = [r["timeframe"] for r in
               _q("SELECT DISTINCT timeframe FROM trades WHERE timeframe IS NOT NULL ORDER BY timeframe").fetchall()]
        for tf in tfs:
            lines.append(_stats_line("⏱", tf, query_stats(timeframe=tf)))
        txt = (f"📈 <b>STATISTIQUES</b>\n\nPositions en cours : {len(open_trades())}\n\n"
               + "\n\n".join(lines))
        return (txt, None)
    if cmd == "/trades":
        rows = open_trades()
        if not rows:
            return ("📈 <b>POSITIONS EN COURS</b>\n\nAucune position en cours.", None)
        lines = []
        for i, t in enumerate(rows, 1):
            dec = SYMBOLS[t["symbol"]]["decimals"]
            st = " (BE)" if t["be_hit"] else ""
            tp_str = _fmt(t["tp"], dec) if t["tp"] is not None else "—"
            rr_now = current_rr(t)
            rr_str = f"{rr_now:+.1f}" if rr_now is not None else "—"
            lines.append(f"{i}. <b>{t['symbol']} {t['side']}</b>{st}\n"
                         f"Entry : {_fmt(t['entry'], dec)}\n"
                         f"SL : {_fmt(t['sl'], dec)}\n"
                         f"TP : {tp_str}\n"
                         f"RR actuel : {rr_str}\n"
                         f"Lot : {t['lot']:g}")
        return ("📈 <b>POSITIONS EN COURS</b>\n\n" + "\n\n".join(lines), None)
    return ("Commande inconnue. /aide", None)


_MENU_ACTIONS = {
    "risque": "/risque", "levier": "/levier", "stats": "/stats", "trades": "/trades",
    "timeframe": "/timeframe", "medias": "/medias",
}


def _edit(cq, text, kb):
    """Édite le message du bouton pressé (texte + clavier)."""
    data = {"chat_id": cq["message"]["chat"]["id"], "message_id": cq["message"]["message_id"],
            "text": text, "parse_mode": "HTML"}
    if kb:
        data["reply_markup"] = json.dumps(kb)
    _post("editMessageText", data)


def _handle_update(u):
    if "callback_query" in u:
        cq = u["callback_query"]
        if not _is_admin(cq["from"]["id"]):
            return
        data = cq.get("data", "")
        ack = ""
        if data.startswith("risk:"):
            set_risk(max(1.0, get_risk() + float(data[5:])))
            _edit(cq, _risk_text(), _risk_keyboard())
            ack = f"{get_risk():g} $"
        elif data.startswith("lev:"):
            set_leverage(float(data[4:]))
            _edit(cq, _leverage_text(), _leverage_keyboard())
            ack = f"{get_leverage():g}x"
        elif data.startswith("tf:"):
            tf = int(data[3:])
            if tf in TF_LABELS:
                set_timeframe(tf)
                _edit(cq, _tf_text(), _tf_keyboard())
                ack = TF_LABEL
        elif data.startswith("mset:"):
            _, cat, mid = data.split(":")
            item = _pending_media.pop((cq["message"]["chat"]["id"], int(mid)), None)
            if item and cat in MEDIA_CATS:
                _edit(cq, f"✅ Ajouté à {MEDIA_CATS[cat]} — {add_media(cat, item)} au total.", None)
                ack = "Ajouté"
            else:
                _edit(cq, "⌛ Média plus en attente (bot redémarré ?) — renvoie-le.", None)
        elif data.startswith("mclr:"):
            if data[5:] in MEDIA_CATS:
                clear_media(data[5:])
                _edit(cq, _media_text(), _with_back(_media_keyboard("mclr", verb="🗑 ")))
                ack = "Vidé"
        elif data == "menu:home":
            _edit(cq, _home_text(), _menu_keyboard())
        elif data.startswith("menu:") and data[5:] in _MENU_ACTIONS:
            reply, kb = handle_command(_MENU_ACTIONS[data[5:]])
            _edit(cq, reply, kb or _back_keyboard())
        _post("answerCallbackQuery", {"callback_query_id": cq["id"], "text": ack})
        return
    msg = u.get("message")
    if not msg or msg["chat"].get("type") != "private" or not _is_admin(msg["from"]["id"]):
        return  # les membres du groupe ne peuvent rien modifier
    item = _media_from_message(msg)
    if item:  # sticker / image / GIF reçu en privé : on propose de l'assigner à une catégorie
        _pending_media[(msg["chat"]["id"], msg["message_id"])] = item
        send(msg["chat"]["id"], "🎭 Pour quel événement ce média ?",
             reply_markup=_media_keyboard("mset", f":{msg['message_id']}"))
        return
    text = msg.get("text", "")
    if text.startswith("/"):
        reply, kb = handle_command(text)
        send(msg["chat"]["id"], reply, reply_markup=kb)


def poll_loop():
    offset = 0
    while True:
        try:
            r = requests.get(f"{TELEGRAM_API}/getUpdates", params={
                "offset": offset, "timeout": 25,
                "allowed_updates": '["message","callback_query"]'}, timeout=40).json()
            for u in r.get("result", []):
                offset = u["update_id"] + 1
                try:
                    _handle_update(u)
                except Exception as e:
                    print(f"[telegram] update error: {e}")
        except Exception as e:
            print(f"[telegram] polling: {e}")
            time.sleep(5)


def start_polling():
    if not TELEGRAM_TOKEN:
        print("[telegram] TELEGRAM_TOKEN absent : commandes désactivées")
        return
    threading.Thread(target=poll_loop, daemon=True).start()


# ============================================================================
# 9. BOUCLE PRINCIPALE
#
# Orchestration : à chaque nouvelle bougie clôturée -> suivi des positions,
# détection des CHoCH, envoi Telegram. Rapport quotidien à DAILY_REPORT_HOUR_UTC.
# ============================================================================

# Un CHoCH plus vieux que ce nombre de bougies n'est plus signalé
# (ex. redémarrage après une longue pause : on ne renvoie pas de signal périmé).
SIGNAL_MAX_AGE = 2

_fetch_state = {}  # symbole -> {"slot": ouverture de la bougie attendue, "tries": nb d'essais}
_last_price = {}   # symbole -> dernier prix de clôture connu (pour le RR actuel affiché dans /trades)


def set_timeframe(minutes):
    """Change le timeframe à chaud (Telegram) et le mémorise pour les prochains démarrages.

    Les repères « dernière bougie vue » du nouveau timeframe sont effacés : au prochain passage le bot se
    recale silencieusement sur la dernière bougie clôturée (pas de rejeu de l'historique). Les positions
    ouvertes restent suivies, avec les bougies du nouveau timeframe."""
    with _tf_lock:
        if minutes == TIMEFRAME_MIN:
            return
        set_setting("timeframe", minutes)
        _apply_timeframe(minutes)
        _q("DELETE FROM meta WHERE key LIKE ?", (f"last_t:%:{TF_LABEL}",), commit=True)
        _fetch_state.clear()
    print(f"[config] timeframe -> {TF_LABEL}")


def current_rr(trade):
    """RR actuel d'une position ouverte, à partir du dernier prix connu (None si prix indisponible)."""
    price = _last_price.get(trade["symbol"])
    if price is None:
        return None
    side = 1 if trade["side"] == "BUY" else -1
    risk = abs(trade["entry"] - trade["sl_initial"])
    if risk <= 0:
        return None
    return (price - trade["entry"]) * side / risk


def _ts_str(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _should_fetch(symbol, last_seen):
    """N'interroge l'API que lorsqu'une nouvelle bougie est attendue (limite les appels inutiles).

    Jusqu'à 6 essais espacés de SCAN_INTERVAL par bougie ; un seul essai par bougie si aucune
    nouvelle bougie n'est arrivée depuis un moment (marché fermé, week-end...).
    """
    if last_seen is None:
        return True
    # ouverture de la dernière bougie censée être clôturée (même marge que _only_closed)
    expected = int((time.time() - CLOSE_GRACE_SEC) // TF_SEC) * TF_SEC - TF_SEC
    if expected <= last_seen:
        return False
    st = _fetch_state.setdefault(symbol, {"slot": None, "tries": 0})
    if st["slot"] != expected:
        st["slot"], st["tries"] = expected, 0
    stale = expected - last_seen > 3 * TF_SEC
    if st["tries"] >= (1 if stale else 6):
        return False
    st["tries"] += 1
    return True


def publish_signal(symbol, candles, events, sig):
    """Enregistre le trade puis envoie : signal (+ image) au groupe, lot en privé à l'admin."""
    dec = SYMBOLS[symbol]["decimals"]
    key = f"{symbol}:{TF_LABEL}:{sig['t']}"
    if signal_exists(key):
        return
    n_open = count_open(symbol)
    if n_open >= MAX_POSITIONS:
        print(f"[{symbol}] {sig['side']} {sig['type']} ignoré ({n_open}/{MAX_POSITIONS} positions ouvertes)")
        return

    risk_usd = get_risk()
    leverage = get_leverage()
    lot_info = calc_lot(symbol, risk_usd, sig["risk"])
    if lot_info["lot"] <= 0:
        print(f"[{symbol}] signal ignoré : lot invalide")
        return
    margin = calc_margin(symbol, lot_info["lot"], sig["entry"], leverage)

    trade_id = add_trade(
        sent_admin=0, sent_group=0,   # passent à 1 seulement quand Telegram a confirmé l'envoi
        signal_key=key, symbol=symbol, side=sig["side"], kind=sig["type"], timeframe=TF_LABEL,
        entry=sig["entry"], sl=sig["sl"], sl_initial=sig["sl"], tp=sig["tp"],
        risk_usd=lot_info["real_risk"],  # risque réel du lot pris (= risque demandé sauf lot minimum)
        lot=lot_info["lot"], leverage=leverage, opened_ts=now_ts(), last_ts=sig["t"])
    print(f"[{symbol}] SIGNAL {sig['side']} {sig['type']} @ {sig['entry']:.{dec}f} "
          f"(bougie {_ts_str(sig['t'])})")

    try:
        chart = make_chart(symbol, candles, events, sig, dec)
    except Exception as e:   # un souci de graphique ne doit JAMAIS empêcher l'envoi du signal
        print(f"[{symbol}] make_chart a échoué ({type(e).__name__}) : signal envoyé sans image")
        if isinstance(e, RecursionError):
            print("[chart] matplotlib incompatible avec cette version de Python -> "
                  "fixer PYTHON_VERSION=3.13.x sur Render (ou mettre matplotlib à jour)")
        traceback.print_exc(limit=-3)   # 3 dernières lignes seulement (évite les logs de 1000 lignes)
        chart = None

    _deliver_signal(trade_id, symbol,
                    admin_txt=admin_signal(symbol, sig, risk_usd, leverage, lot_info, margin, dec),
                    group_txt=group_signal(symbol, sig, n_open + 1, dec),
                    chart=chart)


def _deliver_signal(trade_id, symbol, admin_txt, group_txt, chart=None, only_missing=None):
    """Envoie le signal d'ouverture (admin d'abord, puis groupe) et note en base ce qui est bien parti."""
    row = _q("SELECT sent_admin, sent_group FROM trades WHERE id=?", (trade_id,)).fetchone()
    if row is None:
        return
    for who, chat_id, sender, txt, photo in (
            ("admin", CHAT_ID_ADMIN, to_admin, admin_txt, None),
            ("group", CHAT_ID_GROUPE, to_group, group_txt, chart)):
        if row[f"sent_{who}"]:
            continue
        try:
            res = sender(txt, photo=photo) if who == "group" else sender(txt)
            if _delivered(chat_id, res):
                update_trade(trade_id, **{f"sent_{who}": 1})
            else:
                print(f"[{symbol}] signal #{trade_id} NON envoyé ({who}) — nouvel essai automatique")
        except Exception:
            print(f"[{symbol}] signal #{trade_id} : erreur d'envoi ({who}) :")
            traceback.print_exc()


_retry_state = {}   # trade_id -> [nb d'essais, dernier essai]


def retry_unsent_signals():
    """Renvoie les signaux d'ouverture jamais confirmés par Telegram (tant que le trade est ouvert)."""
    rows = _q("SELECT * FROM trades WHERE sent_admin=0 OR sent_group=0 ORDER BY id").fetchall()
    for r in rows:
        t = dict(r)
        tries, last = _retry_state.get(t["id"], [0, 0.0])
        if time.time() - last < 30:
            continue
        if t["status"] != "OPEN" or tries >= 8:
            print(f"[retry] signal #{t['id']} abandonné (trade {t['status']}, {tries} essais)")
            update_trade(t["id"], sent_admin=1, sent_group=1)
            continue
        _retry_state[t["id"]] = [tries + 1, time.time()]
        sym = t["symbol"]
        dec = SYMBOLS[sym]["decimals"]
        sig = {"side": t["side"], "type": t["kind"], "entry": t["entry"], "sl": t["sl_initial"],
               "tp": t["tp"], "risk": abs(t["entry"] - t["sl_initial"])}
        lot_info = {"lot": t["lot"], "real_risk": t["risk_usd"], "raised_to_min": False}
        lev = t["leverage"] or get_leverage()
        margin = calc_margin(sym, t["lot"], t["entry"], lev)
        late = "⏱ <i>Envoi différé</i>\n"
        _deliver_signal(t["id"], sym,
                        admin_txt=late + admin_signal(sym, sig, t["risk_usd"], lev, lot_info, margin, dec),
                        group_txt=late + group_signal(sym, sig, None, dec, tf=t["timeframe"]))


def process_symbol(symbol):
    """Un passage pour un actif : suivi des positions ouvertes, puis détection des nouveaux CHoCH."""
    dec = SYMBOLS[symbol]["decimals"]
    meta_key = f"last_t:{symbol}:{TF_LABEL}"  # par timeframe : en changer réinitialise proprement
    raw = get_meta(meta_key)
    last_seen = int(raw) if raw is not None else None

    if not _should_fetch(symbol, last_seen):
        return
    candles = get_candles(symbol)
    if not candles:
        return
    last_t = candles[-1]["t"]
    _last_price[symbol] = candles[-1]["c"]

    if last_seen is None:  # premier lancement : on s'initialise sur la dernière bougie
        set_meta(meta_key, last_t)
        print(f"[{symbol}] initialisé sur la bougie {_ts_str(last_t)} (l'historique n'est pas signalé)")
        return
    if last_t <= last_seen:
        return  # pas encore de nouvelle bougie clôturée

    # 0) signaux d'ouverture pas encore confirmés : ils partent avant tout message de suivi
    retry_unsent_signals()

    # 1) suivi des positions ouvertes (BE / TP / SL)
    for trade in open_trades(symbol):
        for ev in track_trade(trade, candles):
            to_group(group_event(ev, dec))
            group_event_media(ev["name"])
            txt = admin_event(ev)
            if txt:
                to_admin(txt)

    # 2) nouveaux signaux : CHoCH survenus sur les bougies clôturées depuis le dernier passage
    events = analyze(candles)
    n = len(candles)
    for ev in events:
        if ev["t"] <= last_seen or ev["i"] < n - SIGNAL_MAX_AGE:
            continue
        sig = build_signal(candles, ev)
        if sig:
            publish_signal(symbol, candles, events, sig)

    set_meta(meta_key, last_t)


def maybe_daily_report():
    """Envoie le rapport du jour au groupe, une fois par jour, à partir de DAILY_REPORT_HOUR_UTC."""
    now = datetime.now(timezone.utc)
    if now.hour < DAILY_REPORT_HOUR_UTC:
        return
    today = now.strftime("%Y-%m-%d")
    if get_meta("last_report") == today:
        return
    set_meta("last_report", today)
    to_group(daily_report())
    to_group_media("MOTIV")


def _trading_loop():
    """La boucle de scan (symboles + rapport quotidien), tourne en continu en arrière-plan."""
    try:
        while True:
            try:
                retry_unsent_signals()
            except Exception:
                traceback.print_exc()
            for symbol in SYMBOLS:
                try:
                    with _tf_lock:   # un changement de timeframe attend la fin du passage en cours
                        process_symbol(symbol)
                except Exception:
                    print(f"[{symbol}] erreur :")
                    traceback.print_exc()
            try:
                maybe_daily_report()
            except Exception:
                traceback.print_exc()
            time.sleep(scan_interval())
    except KeyboardInterrupt:
        print("\nArrêt demandé.")


# ============================================================================
# 10. SERVEUR WEB (Render)
#
# Render Web Service exige un port HTTP ouvert : /health répond pendant que la
# boucle de trading (ci-dessus) et le polling Telegram tournent en arrière-plan.
# ============================================================================

def _run(port):
    """Lance le serveur Flask (bloquant) — la boucle de trading tourne dans un thread séparé."""
    if Flask is None:
        print("⚠️  Flask absent (pip install flask) : pas de serveur /health, boucle en direct.")
        _trading_loop()
        return
    app = Flask(__name__)

    @app.get("/health")
    def _health():
        return {"status": "ok"}, 200

    @app.get("/")
    def _root():
        return "AlphaBot en ligne.", 200

    threading.Thread(target=_trading_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=port)


def main():
    print(f"AlphaBot BOS + CHoCH — Gold & BTC ({TF_LABEL})")
    print(f"Timeframe {TF_LABEL} — source : {_TF_SOURCE} | TIMEFRAME vu par le process : {os.getenv('TIMEFRAME')!r}")
    sources = " | ".join(f"{s} <- {c['source'].capitalize()} ({c.get('deriv_symbol') or c.get('binance_symbol')})"
                         for s, c in SYMBOLS.items())
    print(f"Prix : {sources} | scan toutes les {scan_interval()}s")
    if websocket is None:
        raise SystemExit("Module manquant pour les prix du Gold (Deriv) : pip install websocket-client")
    if not TELEGRAM_TOKEN:
        print("⚠️  TELEGRAM_TOKEN absent : les messages sont affichés dans la console uniquement.")
    else:
        if not CHAT_ID_GROUPE:
            print("⚠️  CHAT_ID_GROUPE absent : aucun signal ne sera envoyé au groupe.")
        if not CHAT_ID_ADMIN:
            print("⚠️  CHAT_ID_ADMIN absent : ni lot, ni commandes.")

    start_polling()
    to_admin(f"🤖 AlphaBot démarré — timeframe <b>{TF_LABEL}</b> ({_TF_SOURCE}). Change-le avec /timeframe.")
    _run(_env_int("PORT", 10000))


if __name__ == "__main__":
    main()

