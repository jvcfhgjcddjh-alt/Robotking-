
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
RR (RR1-RR4), timeframe d'entrée (M1/M5/M15) et filtre HTF (ON/OFF) se règlent aussi à chaud depuis le menu
Telegram « ⚙️ Paramètres signal » (/signal) : mémorisés en base, ils survivent aux redémarrages Render.
Stickers / images / GIF du groupe (TP, SL, BE, motivation) : envoyer le média au bot en privé, puis choisir la
catégorie (gestion via /medias). Optionnel : variables MEDIA_TP, MEDIA_SL, MEDIA_BE, MEDIA_MOTIV.
Auto-tests du moteur H1 -> M15 -> M5 -> M1 (sans réseau ni base de prod) :  python main.py --selftest
Sommaire : 1 Configuration · 2 Base de données · 3 Données de prix · 4 Signaux
           5 Risque · 6 Suivi des positions · 7 Graphique · 8 Telegram · 9 Boucle principale
           10 Serveur web (Render) · 11 Auto-tests
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
        "min_lot": 0.001, "lot_step": 0.001, "decimals": 0,
    },
}

# --- Stratégie BOS + CHoCH ---------------------------------------------------
SWING_DEPTH = _env_int("SWING_DEPTH", 3)
ATR_PERIOD = 14
ENTRY_CHOCH1 = _env_bool("ENTRY_CHOCH1", True)    # CHoCH qui suit un BOS (retournement classique)
ENTRY_CHOCH2 = _env_bool("ENTRY_CHOCH2", True)    # CHoCH + CHoCH (CHoCH qui suit directement un CHoCH)
DEBUG_CHOCH = _env_bool("DEBUG_CHOCH", True)      # logge chaque CHoCH ignoré (et pourquoi) au lieu de rien dire
HTF_FILTER = _env_bool("HTF_FILTER", True)   # ON : HTF = contexte, POI = zone de réaction, CHoCH = déclencheur (htf_poi_setup)
HTF_MINUTES = _env_int("HTF_MINUTES", 60)    # unité de temps de référence pour la tendance ("liquidité externe")
# Cascade du filtre HTF : H1 (biais + liquidité externe) -> M15 (confirmation du contexte) -> M5 (retracement, liquidité interne,
# fin du mouvement) -> UT d'entrée (trigger final uniquement). Seules les UT > UT d'entrée portent des POI.
HTF_CASCADE = tuple(sorted({HTF_MINUTES, 15, 5}, reverse=True))
POI_MAX_AGE = _env_int("POI_MAX_AGE", 60)                  # âge max d'un POI (OB / FVG), en bougies de sa propre UT
POI_MIN_ATR = _env_float("POI_MIN_ATR", 0.15)              # taille mini d'un POI, en ATR de son UT (écarte les micro-gaps)
SWEEP_LOOKBACK = _env_int("SWEEP_LOOKBACK", 10)            # bougies d'entrée comparées pour détecter un balayage de liquidité
SL_BUFFER_ATR = _env_float("SL_BUFFER_ATR", 0.1)   # buffer au-delà de la ligne du BOS
MIN_SL_ATR = _env_float("MIN_SL_ATR", 0.3)         # SL minimum (en ATR)
MAX_SL_ATR = _env_float("MAX_SL_ATR", 6.0)         # au-delà : signal ignoré (BOS trop ancien)
BE_RR = _env_float("BE_RR", 2.0)          # RR auquel le SL est déplacé à l'entrée (BE)
TP_RR = _env_float("TP_RR", 4.0)          # RR de la TP finale (clôture complète, pas de TP1/TP2)
RR_LEVELS = (1.0, 2.0, 3.0)                # paliers intermédiaires notifiés au groupe (hors TP finale)
# ⚙️ PARAMÈTRES SIGNAL (menu Telegram) : TP_RR, HTF_FILTER et le timeframe ci-dessus ne sont que les valeurs de
# DÉPART ; le choix fait sur Telegram est mémorisé en base (table settings) et prime après chaque redémarrage.
SIGNAL_RR_CHOICES = (1, 2, 3, 4)     # boutons « RR1 | RR2 | RR3 | RR4 »
SIGNAL_TF_CHOICES = (1, 5, 15)       # boutons « M1 | M5 | M15 » (timeframe du déclenchement final)
MAX_POSITIONS = _env_int("MAX_POSITIONS", 3)     # positions max en cours par actif

# --- Type d'entrée : DIRECT (au marché) ou LIMIT (on attend le retour du prix) ---------------
# ENTRY_MODE : MARKET = toujours direct (par défaut) | LIMIT = toujours limit | BOTH = le bot choisit et le précise.
# En BOTH : si la clôture du signal est loin du niveau cassé (> LIMIT_EXT_ATR x ATR), le prix est "étendu" ->
# ordre LIMIT sur le niveau cassé (retest) ; sinon -> entrée DIRECTE. Le message dit toujours lequel.
ENTRY_MODE = os.getenv("ENTRY_MODE", "MARKET").strip().upper()   # par défaut : rien que des entrées DIRECTES
if ENTRY_MODE not in ("MARKET", "LIMIT", "BOTH"):
    ENTRY_MODE = "MARKET"
LIMIT_EXT_ATR = _env_float("LIMIT_EXT_ATR", 0.6)
LIMIT_CANCEL_RR = _env_float("LIMIT_CANCEL_RR", 2.0)         # ordre annulé si le prix part de +X R sans toucher la limit
LIMIT_EXPIRY_CANDLES = _env_int("LIMIT_EXPIRY_CANDLES", 30)  # ordre annulé s'il n'est pas exécuté après N bougies

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
if "--selftest" in sys.argv:   # auto-tests (section 11) : base SQLite jetable, jamais celle de production
    import tempfile
    os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "selftest.db")
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
# Entrées LIMIT : type d'ordre, bougie du signal, bougie d'exécution, prix de marché au moment du signal.
_ensure_column("trades", "order_type", "TEXT DEFAULT 'MARKET'")
_ensure_column("trades", "placed_ts", "INTEGER")
_ensure_column("trades", "filled_ts", "INTEGER")
_ensure_column("trades", "ref_price", "REAL")


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


# --- paramètres signal : RR et filtre HTF (modifiables à chaud depuis Telegram) ---------------------
def get_tp_rr():
    """RR de la TP finale des NOUVEAUX signaux : dernier choix Telegram, sinon TP_RR (Render / défaut)."""
    try:
        v = float(get_setting("tp_rr", TP_RR))
    except (TypeError, ValueError):
        return TP_RR
    return v if v > 0 else TP_RR


def set_tp_rr(v):
    set_setting("tp_rr", float(v))


def get_htf_filter():
    """Filtre HTF actif ? Dernier choix Telegram, sinon HTF_FILTER (Render / défaut)."""
    v = get_setting("htf_filter")
    return HTF_FILTER if v is None else str(v) == "1"


def set_htf_filter(on):
    set_setting("htf_filter", "1" if on else "0")


def trade_tp_rr(trade):
    """RR de la TP d'UN trade, relu depuis son entrée / SL initial / TP enregistrés au moment du signal.
    Changer le RR sur Telegram n'affecte donc jamais un trade déjà ouvert. Repli : TP_RR (anciens trades sans TP)."""
    try:
        risk = abs(trade["entry"] - trade["sl_initial"])
        if trade.get("tp") is not None and risk > 0:
            rr = round(abs(trade["tp"] - trade["entry"]) / risk, 2)
            if rr > 0:
                return rr
    except (KeyError, TypeError):
        pass
    return TP_RR


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
    return _q("SELECT COUNT(*) n FROM trades WHERE symbol=? AND status IN ('OPEN','PENDING')", (symbol,)).fetchone()["n"]


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
        rows = _q("SELECT * FROM trades WHERE status IN ('OPEN','PENDING') AND symbol=? ORDER BY id", (symbol,))
    else:
        rows = _q("SELECT * FROM trades WHERE status IN ('OPEN','PENDING') ORDER BY id")
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
    rows = [r for r in rows if r["status"] != "CANCELLED"]   # ordres LIMIT jamais exécutés : hors statistiques
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


def _only_closed(candles, tf_sec=None):
    now = time.time()
    tf_sec = tf_sec or TF_SEC
    return sorted((c for c in candles if c["t"] + tf_sec + CLOSE_GRACE_SEC <= now),
                  key=lambda c: c["t"])


# --- BTC : API publique Binance (klines), sans clé --------------------------------------
def _binance(symbol, minutes=None):
    last_err = None
    interval = BINANCE_INTERVALS[minutes or TIMEFRAME_MIN]
    for base in BINANCE_BASES:
        try:
            r = requests.get(
                base + "/api/v3/klines",
                params={"symbol": symbol, "interval": interval,
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


def _deriv(deriv_symbol, minutes=None):
    """Bougies du timeframe demandé, construites par Deriv (ticks_history, style candles)."""
    sec = (minutes or TIMEFRAME_MIN) * 60
    msg = _deriv_request({
        "ticks_history": deriv_symbol, "style": "candles", "granularity": sec,
        "count": CANDLES_LIMIT, "end": "latest",
        "adjust_start_time": 1,   # marché fermé (week-end) : recule jusqu'aux dernières bougies dispo
    })
    return [{"t": int(k["epoch"]), "o": float(k["open"]), "h": float(k["high"]),
             "l": float(k["low"]), "c": float(k["close"])} for k in msg["candles"]]


def get_candles(symbol, minutes=None):
    """Retourne la liste des bougies clôturées (plus ancienne -> plus récente).

    minutes=None -> timeframe actuellement sélectionné (M1/M5/...) ; sinon un autre timeframe fixe
    (ex. HTF_MINUTES) sans toucher au suivi des positions ni aux signaux en cours."""
    cfg = SYMBOLS[symbol]
    if cfg["source"] == "deriv":
        raw = _deriv(cfg["deriv_symbol"], minutes)
    elif cfg["source"] == "binance":
        raw = _binance(cfg["binance_symbol"], minutes)
    else:
        raise ValueError(f"Source de prix inconnue pour {symbol} : {cfg['source']}")
    return _only_closed(raw, (minutes or TIMEFRAME_MIN) * 60)


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
# - Filtre HTF ON : cascade H1 -> M15 -> M5 -> entrée (M1 par défaut), chaque UT a UN rôle :
#     H1  = biais principal + liquidité externe · M15 = confirmation du contexte (H1 et M15 doivent être alignés)
#     M5  = retracement / liquidité interne / fin du mouvement (CHoCH M5 dans le sens du contexte)
#     M1  = trigger final UNIQUEMENT : il ne détermine jamais le biais et ne peut pas s'opposer au contexte.
#     Chaîne : contexte -> liquidité interne -> sweep / réaction -> fin du retracement -> CHoCH M5 -> POI (OB / FVG) mitigé -> CHoCH M1.
#     Un CHoCH M1 contre le contexte = retracement interne, jamais un signal. Le biais ne change que si la structure H1 ET M15
#     est réellement cassée (clôture au-delà d'un swing, règles d'`analyze`) : le contexte opposé devient alors le contexte valide.
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


_htf_cache = {}       # symbole -> (epoch du dernier calcul, direction : 1 haussier / -1 baissier / 0 inconnu)
HTF_CACHE_SEC = 300   # le H1 ne clôture qu'1x/heure : inutile de le retélécharger à chaque scan


def htf_bias(symbol):
    """Tendance actuelle sur l'unité de temps HTF_MINUTES (H1 par défaut) : 1 haussier, -1 baissier,
    0 si indéterminé (pas assez de bougies / erreur réseau -> le filtre HTF laisse alors passer).

    Sert à distinguer la liquidité externe (la direction HTF, celle qu'on a le droit de trader) de la
    liquidité interne (un retracement à contre-sens sur l'unité de temps courte, qu'on ne trade pas)."""
    now = time.time()
    cached = _htf_cache.get(symbol)
    if cached and now - cached[0] < HTF_CACHE_SEC:
        return cached[1]
    try:
        raw = get_candles(symbol, minutes=HTF_MINUTES)
    except Exception as e:
        print(f"[{symbol}] htf_bias : lecture H{HTF_MINUTES // 60 or 1} impossible ({e}) — filtre neutre ce passage")
        return cached[1] if cached else 0
    trend = _last_dir(raw)
    _htf_cache[symbol] = (now, trend)
    return trend


def _last_dir(raw):
    """Direction de la dernière cassure de structure d'`analyze` (1 / -1) ; 0 si pas assez de bougies ou aucune cassure."""
    if len(raw) <= 2 * SWING_DEPTH + 1:
        return 0
    evs = analyze(raw)
    return evs[-1]["dir"] if evs else 0


# --- Filtre HTF ON : HTF = CONTEXTE · POI = ZONE DE RÉACTION · CHoCH = DÉCLENCHEUR -------------------------
# Rien de neuf côté structure : les POI sont tirés des cassures que `analyze` détecte déjà, sur H1 / M15 / M5.
_ctx_cache = {}   # (symbole, UT en minutes) -> (epoch du calcul, bougies clôturées)


def _ctx_candles(symbol, tf):
    """Bougies clôturées d'une UT de contexte (H1 / M15 / M5), gardées en cache quelques secondes.
    Liste vide si la lecture échoue et qu'aucun cache n'existe."""
    now = time.time()
    hit = _ctx_cache.get((symbol, tf))
    if hit and now - hit[0] < min(HTF_CACHE_SEC, tf * 12):   # 1/5 de la durée d'une bougie, plafonné à HTF_CACHE_SEC
        return hit[1]
    try:
        raw = get_candles(symbol, minutes=tf)
    except Exception as e:
        print(f"[{symbol}] contexte {_tf_lbl(tf)} : lecture impossible ({e})")
        return hit[1] if hit else []
    _ctx_cache[(symbol, tf)] = (now, raw)
    return raw


def find_pois(raw, tf, d):
    """POI (OB / FVG) de sens d (1 = haussier / demande, -1 = baissier / offre) sur les bougies `raw` d'une UT.

    Un POI naît d'une cassure de structure récente de `analyze` (dans le sens d) :
      OB  = dernière bougie de couleur opposée à l'origine de l'impulsion (plus bas / plus haut de la jambe)
      FVG = déséquilibre à 3 bougies à l'intérieur de cette impulsion
    Valide tant qu'aucune clôture ultérieure ne le traverse. Plus récent d'abord."""
    n = len(raw)
    if n <= 2 * SWING_DEPTH + 1:
        return []
    atr, out = atr_series(raw), []

    def _add(kind, lo, hi, f):   # f = bougie qui valide la zone
        far = lo if d == 1 else hi   # bord lointain : une clôture au-delà = POI traversé, donc mort
        if hi - lo < POI_MIN_ATR * atr[f] or any((x["c"] - far) * d < 0 for x in raw[f + 1:]):
            return
        out.append({"kind": kind, "tf": tf, "d": d, "lo": lo, "hi": hi, "f": f,
                    "t_valid": raw[f]["t"] + tf * 60})   # utilisable dès la clôture de la bougie f (pas de lookahead)

    for ev in analyze(raw):
        i, s = ev["i"], ev["src"]
        if ev["dir"] != d or i < n - POI_MAX_AGE:
            continue
        rng = range(s, i + 1)
        o = min(rng, key=lambda k: raw[k]["l"]) if d == 1 else max(rng, key=lambda k: raw[k]["h"])   # origine
        ob = next((k for k in range(o, s - 1, -1) if (raw[k]["c"] - raw[k]["o"]) * d < 0), None)
        if ob is not None:
            _add("OB", raw[ob]["l"], raw[ob]["h"], i)
        for k in range(o + 1, i):   # bougies k-1, k, k+1 : le gap est entre la 1re et la 3e
            lo, hi = (raw[k - 1]["h"], raw[k + 1]["l"]) if d == 1 else (raw[k + 1]["h"], raw[k - 1]["l"])
            if hi > lo:
                _add("FVG", lo, hi, k + 1)
    uniq = {(p["kind"], p["lo"], p["hi"]): p for p in out}
    return sorted(uniq.values(), key=lambda p: -p["f"])


def poi_reaction(c, ev, poi):
    """Sur les bougies d'entrée `c` : le point de retournement (extrême de la jambe qui précède le CHoCH `ev`)
    a-t-il RÉELLEMENT mitigé le POI, puis réagi ? Retourne (mitigation: bool, réaction: str | None).

    Mitigation réelle : la mèche du point de retournement entre dans la zone (le POI existait déjà à ce moment
    et aucune clôture d'entrée ne l'a traversé). Réaction : bougie de rejet dans le sens du trade qui clôture
    au-delà du milieu du POI, OU balayage de liquidité (mèche au-delà du dernier extrême, clôture de retour)."""
    d, i = ev["dir"], ev["i"]
    e = _turn_index(c, ev)
    x, lo, hi = c[e], poi["lo"], poi["hi"]
    far = lo if d == 1 else hi
    if x["t"] < poi["t_valid"] or any((k["c"] - far) * d < 0 for k in c[:i + 1] if k["t"] >= poi["t_valid"]):
        return False, None   # POI pas encore formé au retournement, ou déjà traversé par une clôture
    if not (x["l"] <= hi and x["h"] >= lo):
        return False, None   # le retournement s'est fait hors de la zone : pas de mitigation réelle
    mid = (lo + hi) / 2
    if any((c[j]["c"] - c[j]["o"]) * d > 0 and (c[j]["c"] - mid) * d > 0 for j in range(e, max(i, e + 1))):
        return True, "rejet du POI"
    if _sweep_at(c, e, d)[1]:
        return True, "balayage de liquidité"
    return True, None


def _turn_index(c, ev):
    """Index du point de retournement : extrême de la jambe (du swing cassé `src` jusqu'au CHoCH `ev`) — plus bas si le CHoCH
    est haussier, plus haut s'il est baissier."""
    d, rng = ev["dir"], range(ev["src"], ev["i"] + 1)
    return min(rng, key=lambda k: c[k]["l"]) if d == 1 else max(rng, key=lambda k: c[k]["h"])


def _sweep_at(c, e, d):
    """Balayage de liquidité sur la bougie e : mèche au-delà du dernier extrême des SWEEP_LOOKBACK bougies précédentes, puis clôture
    de retour. Retourne (niveau balayé | None, balayé: bool) — d = 1 : liquidité sous les plus bas, d = -1 : au-dessus des plus hauts."""
    prior = c[max(0, e - SWEEP_LOOKBACK):e]
    if not prior:
        return None, False
    x = c[e]
    if d == 1:
        lvl = min(k["l"] for k in prior)
        return lvl, x["l"] < lvl < x["c"]
    lvl = max(k["h"] for k in prior)
    return lvl, x["h"] > lvl > x["c"]


def _poi_log(symbol, msg):
    if DEBUG_CHOCH:
        print(f"[{symbol}]   {msg}")


def _tf_lbl(tf):
    return TF_LABELS.get(tf, f"M{tf}")


def _poi_txt(p, dec):
    return f"{_tf_lbl(p['tf'])} {p['kind']} {'bullish' if p['d'] == 1 else 'bearish'} {p['lo']:.{dec}f}-{p['hi']:.{dec}f}"


def _dir_txt(d):
    return {1: "haussier", -1: "baissier", 0: "indéterminé"}[d]


def _ext_liquidity(symbol, d, dec):
    """H1 = liquidité externe (LOG UNIQUEMENT, ne filtre rien) : extrême de la jambe H1 en cours dans le sens du contexte d
    (plus haut si haussier, plus bas si baissier), c'est-à-dire la liquidité que le prix vise à l'extérieur du range interne."""
    h1 = _ctx_candles(symbol, HTF_MINUTES)
    evs = analyze(h1) if len(h1) > 2 * SWING_DEPTH + 1 else []
    if not evs or evs[-1]["dir"] != d:
        return "indéterminée"
    leg = h1[evs[-1]["src"]:]
    lvl = max(k["h"] for k in leg) if d == 1 else min(k["l"] for k in leg)
    return (f"H{HTF_MINUTES // 60 or 1} {'haut' if d == 1 else 'bas'} {lvl:.{dec}f} "
            f"(à {abs(lvl - h1[-1]['c']):.{dec}f} pts du prix)")


def m5_retracement(symbol, d, dec):
    """M5 = retracement, liquidité interne, fin du mouvement, pour un contexte H1/M15 de sens d (1 / -1).

    Ne refait aucune détection : dernière cassure de `analyze` sur M5, `_turn_index` / `_sweep_at` / `find_pois` / `poi_reaction`.
      EN COURS      : la structure M5 est encore à contre-sens du contexte -> retracement pas terminé
      AUCUN         : M5 déjà dans le sens du contexte via un BOS -> pas de retracement à attendre (continuation)
      TERMINÉ       : CHoCH M5 dans le sens du contexte, avec sweep de liquidité interne OU réaction sur un POI
      NON CONFIRMÉ  : CHoCH M5 dans le sens du contexte, mais ni sweep ni réaction -> fin du retracement non prouvée
      INDÉTERMINÉ   : pas assez de bougies M5 (ou lecture impossible)
    Retourne {"state", "liq" (liquidité interne), "choch" (CHoCH M5)} — textes pour les logs."""
    m5 = _ctx_candles(symbol, 5)
    evs = analyze(m5) if len(m5) > 2 * SWING_DEPTH + 1 else []
    if not evs:
        return {"state": "INDÉTERMINÉ", "liq": "—", "choch": "—"}
    last = evs[-1]
    word = "haut" if d == -1 else "bas"   # extrême de la jambe de retracement : bas si contexte haussier, haut sinon
    if last["dir"] != d:
        leg = m5[last["src"]:]
        lvl = min(k["l"] for k in leg) if d == 1 else max(k["h"] for k in leg)
        return {"state": "EN COURS", "liq": f"M5 {word} {lvl:.{dec}f} (extrême du retracement en cours, pas de CHoCH M5 {_dir_txt(d)})",
                "choch": f"non — dernière cassure M5 : {last['kind']} {_dir_txt(last['dir'])}"}
    if last["kind"] != "CHOCH":
        return {"state": "AUCUN", "liq": "— (M5 déjà dans le sens du contexte, pas de retracement)",
                "choch": f"non requis — M5 en {last['kind']} {_dir_txt(d)}"}

    op = ">" if d == 1 else "<"
    choch = f"{last['type']} {_dir_txt(d)} · clôture {m5[last['i']]['c']:.{dec}f} {op} {last['level']:.{dec}f}"
    e = _turn_index(m5, last)
    lvl, swept = _sweep_at(m5, e, d)
    proof = f"balayage de liquidité interne ({word} {lvl:.{dec}f})" if swept else None
    if not proof:   # à défaut de sweep : le point de retournement M5 a mitigé un POI H1 / M15 / M5 et y a réagi
        for tf in (t for t in HTF_CASCADE if t >= 5):
            for p in find_pois(_ctx_candles(symbol, tf), tf, d):
                mit, react = poi_reaction(m5, last, p)
                if mit and react:
                    proof = f"{react} sur {_poi_txt(p, dec)}"
                    break
            if proof:
                break
    liq = f"M5 {word} {lvl:.{dec}f} " + ("balayé" if swept else "non balayé") if lvl is not None else "M5 — (pas d'historique)"
    if not proof:
        return {"state": "NON CONFIRMÉ", "liq": liq, "choch": choch + " · aucun sweep ni réaction"}
    return {"state": "TERMINÉ", "liq": liq, "choch": f"{choch} · {proof}"}


def _mtf_block(symbol, tr, final):
    """Synthèse de l'analyse multi-UT d'un CHoCH d'entrée : HTF bias / liquidité externe / liquidité interne / état du
    retracement / CHoCH M5 / trigger / POI / état final. Toujours les mêmes lignes, « — » pour une étape non atteinte."""
    rows = (("HTF bias", "htf"), ("liquidité ext.", "ext"), ("liquidité int.", "int"), ("retracement", "retr"),
            ("CHoCH M5", "choch5"), (f"trigger {_tf_lbl(TIMEFRAME_MIN)}", "trigger"), ("POI", "poi"))
    print(f"[{symbol}] ▸ analyse multi-UT")
    for lbl, k in rows:
        print(f"[{symbol}]   {lbl:<15}: {tr.get(k, '—')}")
    print(f"[{symbol}]   {'état final':<15}: {final}")


def htf_poi_setup(symbol, c, ev, trace=None):
    """Filtre HTF ON. Cascade H1 -> M15 -> M5 -> entrée : chaque UT a un rôle, l'entrée (M1) n'est que le trigger.

      1) H1 = biais principal (+ liquidité externe) ; M15 = confirmation : H1 et M15 doivent être alignés -> contexte.
         Contexte indéterminé ou non confirmé -> aucun signal. Le M1 ne détermine jamais seul le biais.
      2) CHoCH d'entrée contre le contexte = retracement interne (liquidité interne) : jamais un signal. Le contexte ne change
         que si H1 ET M15 cassent réellement leur structure (clôture au-delà d'un swing, règles d'`analyze`).
      3) M5 : le retracement doit être terminé (`m5_retracement`) : CHoCH M5 dans le sens du contexte après sweep / réaction,
         ou M5 déjà dans le sens du contexte (continuation).
      4) POI (OB / FVG) valide, mitigé pour de vrai, avec réaction / liquidité — inchangé (`find_pois` / `poi_reaction`).
      5) Le CHoCH d'entrée (validé par `analyze` / `build_signal` : clôture, ATR, ENTRY_CHOCH1/2 — inchangé) est le trigger.
    Les UT >= UT d'entrée sont sautées (entrée M5 : pas d'étape M5 ; entrée M15 : ni M15 ni M5).
    Retourne (setup, poi, motif_de_refus) ; motif_de_refus vaut None quand le signal est accepté.
    `trace` (dict) est rempli au fil des étapes pour `_mtf_block`."""
    tr = trace if trace is not None else {}
    d, dec = ev["dir"], SYMBOLS[symbol]["decimals"]
    side, htf, tfl = ("BUY" if d == 1 else "SELL"), f"H{HTF_MINUTES // 60 or 1}", _tf_lbl(TIMEFRAME_MIN)
    setup = "CONTINUATION"   # seul setup possible : un contexte aligné, jamais un trade à contre-sens
    tr["trigger"] = (f"{ev['type']} {_dir_txt(d)} · clôture {c[ev['i']]['c']:.{dec}f} "
                     f"{'>' if d == 1 else '<'} {ev['level']:.{dec}f}")

    # 1) contexte : H1 (biais) confirmé par M15
    bias = htf_bias(symbol)
    use_m15 = TIMEFRAME_MIN < 15
    m15 = _last_dir(_ctx_candles(symbol, 15)) if use_m15 else bias
    ctx_txt = f"{htf} {_dir_txt(bias)}" + (f" · M15 {_dir_txt(m15)}" if use_m15 else "")
    if bias:
        tr["ext"] = _ext_liquidity(symbol, bias, dec)
    if bias == 0:
        tr["htf"] = f"{ctx_txt} → contexte INDÉTERMINÉ"
        return setup, None, f"biais {htf} indéterminé -- {tfl} ne peut pas déterminer le biais seul"
    if m15 != bias:
        tr["htf"] = f"{ctx_txt} → contexte NON CONFIRMÉ"
        return setup, None, (f"contexte non confirmé : {htf} {_dir_txt(bias)} / M15 {_dir_txt(m15)} "
                             f"-- pas de trade tant que M15 n'est pas aligné avec {htf}")
    tr["htf"] = f"{ctx_txt} → contexte {_dir_txt(bias).upper()}"

    # 2) un CHoCH d'entrée à contre-sens du contexte est un retracement interne, pas une entrée
    if d != bias:
        return setup, None, (f"{side} contre le contexte {_dir_txt(bias)} ({ctx_txt}) : retracement interne, pas une entrée "
                             f"-- le biais ne change que si {htf} et M15 cassent réellement leur structure")

    # 3) M5 : liquidité interne -> sweep / réaction -> fin du retracement -> CHoCH M5
    if TIMEFRAME_MIN < 5:
        r = m5_retracement(symbol, d, dec)
        tr.update({"int": r["liq"], "retr": r["state"], "choch5": r["choch"]})
        if r["state"] == "INDÉTERMINÉ":
            return setup, None, "structure M5 indéterminée -- retracement impossible à évaluer"
        if r["state"] == "EN COURS":
            return setup, None, (f"retracement M5 en cours (M5 encore {_dir_txt(-d)}) -- {tfl} seul ne suffit pas, "
                                 f"attendre un CHoCH M5 {_dir_txt(d)}")
        if r["state"] == "NON CONFIRMÉ":
            return setup, None, "CHoCH M5 sans balayage de liquidité interne ni réaction sur POI -- fin du retracement non confirmée"
    else:
        tr.update({"int": "—", "retr": f"non applicable (UT d'entrée {tfl})", "choch5": "—"})

    # 4) POI -> mitigation -> réaction (inchangé)
    tfs = [tf for tf in HTF_CASCADE if tf > TIMEFRAME_MIN]
    if not tfs:   # UT d'entrée >= H1 : aucune UT supérieure pour porter un POI -> le contexte aligné suffit (filtre d'origine)
        tr["poi"] = "non applicable (aucune UT supérieure à l'entrée)"
        return setup, None, None
    _poi_log(symbol, f"{side} {ev['type']} : {ctx_txt} -> {setup}")

    found = []   # H1 -> M15 -> M5 : la plus haute UT prime
    for tf in tfs:
        pois = find_pois(_ctx_candles(symbol, tf), tf, d)
        if pois:
            _poi_log(symbol, "POI détecté : " + " | ".join(_poi_txt(p, dec) for p in pois[:3])
                     + (f" (+{len(pois) - 3})" if len(pois) > 3 else ""))
        found += pois
    if not found:
        tr["poi"] = "aucun"
        return setup, None, (f"aucun POI {'bullish' if d == 1 else 'bearish'} valide sur "
                             f"{'/'.join(_tf_lbl(t) for t in tfs) or 'aucune UT'} -- un CHoCH seul ne suffit pas")

    touched = []   # (POI, réaction) pour chaque POI réellement mitigé
    for p in found:
        mit, react = poi_reaction(c, ev, p)
        if mit:
            touched.append((p, react))
    if not touched:
        tr["poi"] = f"{len(found)} POI détecté(s), aucun mitigé"
        return setup, None, "aucune mitigation réelle : le point de retournement n'a touché aucun POI"
    poi, react = next(((p, r) for p, r in touched if r), touched[0])
    _poi_log(symbol, f"mitigation {_poi_txt(poi, dec)} : le point de retournement est entré dans la zone")
    if not react:
        tr["poi"] = f"{_poi_txt(poi, dec)} mitigé, sans réaction"
        return setup, None, f"POI {_poi_txt(poi, dec)} mitigé mais ni réaction ni balayage de liquidité"
    tr["poi"] = f"{_poi_txt(poi, dec)} mitigé · {react}"
    _poi_log(symbol, f"réaction : {react}")
    _poi_log(symbol, f"CHoCH confirmé ({ev['type']}) : clôture {c[ev['i']]['c']:.{dec}f} au-delà de {ev['level']:.{dec}f}")
    return setup, poi, None


def build_signal(c, ev, symbol=None):
    """Transforme un CHoCH en signal (entrée, SL, TP). None si invalide.

    DEBUG_CHOCH=True (par défaut) : chaque CHoCH ignoré est loggé avec la raison, pour ne plus
    jamais se demander en silence pourquoi tel retournement n'a pas donné de signal.
    """
    trace = {}   # rempli par htf_poi_setup : HTF bias / liquidités / retracement / CHoCH M5 / trigger / POI

    def _rej(reason):
        if symbol and DEBUG_CHOCH:
            side = "BUY" if ev["dir"] == 1 else "SELL"
            print(f"[{symbol}] CHoCH {side} {ev.get('type') or ev['kind']} ignoré : {reason}")
            if trace:
                _mtf_block(symbol, trace, f"⛔ REJETÉ -- {reason}")
        return None

    if ev["kind"] != "CHOCH":
        return None
    if ev["sl_level"] is None:
        return _rej("pas de niveau de repli valide (sl_level manquant)")
    if ev["type"] == "CHOCH1" and not ENTRY_CHOCH1:
        return _rej("ENTRY_CHOCH1 désactivé")
    if ev["type"] == "CHOCH2" and not ENTRY_CHOCH2:
        return _rej("ENTRY_CHOCH2 désactivé")
    htf_on, tp_rr = get_htf_filter(), get_tp_rr()   # réglages Telegram, lus à chaque signal : effet immédiat
    setup = poi = None
    if htf_on and symbol:
        # HTF = contexte · POI = zone de réaction · CHoCH = déclencheur : plus de blocage automatique contre le biais H1
        setup, poi, why = htf_poi_setup(symbol, c, ev, trace)
        if why:
            return _rej(why)
    d, a = ev["dir"], ev["atr"]
    entry = c[ev["i"]]["c"]
    sl = ev["sl_level"] - d * SL_BUFFER_ATR * a
    if (d == 1 and sl >= entry) or (d == -1 and sl <= entry):
        return _rej(f"SL structurel du mauvais côté de l'entrée (sl={sl:.2f}, entrée={entry:.2f})")
    risk = abs(entry - sl)
    if risk < MIN_SL_ATR * a:
        risk = MIN_SL_ATR * a
        sl = entry - d * risk
    if risk > MAX_SL_ATR * a:
        return _rej(f"SL trop large : {risk:.2f} pts > {MAX_SL_ATR}xATR ({MAX_SL_ATR * a:.2f} pts) "
                    f"-- niveau de référence trop ancien/éloigné")
    sig = {
        "dir": d, "side": "BUY" if d == 1 else "SELL", "type": ev["type"],
        "order": "MARKET", "ref_price": entry,
        "entry": entry, "sl": sl, "risk": risk,
        "tp": entry + d * risk * tp_rr,
        "t": ev["t"], "bos_level": ev["sl_level"],
        "rr": tp_rr, "htf": htf_on, "tf": TF_LABEL,   # affichés sur le signal
        "setup": setup, "poi": poi,                   # CONTINUATION (None si filtre HTF OFF)
    }
    # Entrée LIMIT : retest du niveau cassé (la ligne du CHoCH), avec le même SL structurel.
    lvl = ev["level"]
    want_limit = ENTRY_MODE == "LIMIT" or (ENTRY_MODE == "BOTH" and abs(entry - lvl) > LIMIT_EXT_ATR * a)
    if want_limit and ((d == 1 and sl < lvl < entry) or (d == -1 and entry < lvl < sl)):
        risk_l = abs(lvl - sl)
        if risk_l < MIN_SL_ATR * a:
            risk_l = MIN_SL_ATR * a
            sl = lvl - d * risk_l
        # si le prix a déjà dépassé le seuil d'annulation, la limit n'a plus de sens : on reste en DIRECT
        if (entry - lvl) * d < LIMIT_CANCEL_RR * risk_l:
            sig.update(order="LIMIT", entry=lvl, sl=sl, risk=risk_l, tp=lvl + d * risk_l * tp_rr)
    if setup:   # dernier maillon de la chaîne POI -> mitigation -> réaction -> CHoCH : accepté (les refus passent par _rej)
        src = f" depuis {_poi_txt(poi, SYMBOLS[symbol]['decimals'])}" if poi else ""
        print(f"[{symbol}] signal ACCEPTÉ : {sig['side']} {sig['type']} -- {setup}{src}")
        if trace:
            _mtf_block(symbol, trace, f"✅ ACCEPTÉ -- {sig['side']} {sig['type']} · {setup}{src}")
    return sig


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
    tp_rr = trade_tp_rr(trade)   # RR de CE trade (fixé à l'ouverture), pas le réglage courant
    last_ts = trade["last_ts"]

    for c in candles:
        if c["t"] <= last_ts:
            continue
        last_ts = c["t"]
        adverse = c["l"] if side == 1 else c["h"]
        favorable = c["h"] if side == 1 else c["l"]
        just_filled = False

        # 0) ordre LIMIT en attente : exécuté, annulé (prix parti / expiré) ou toujours en attente
        if trade["status"] == "PENDING":
            touched = (adverse <= entry) if side == 1 else (adverse >= entry)
            if touched:
                trade.update(status="OPEN", filled_ts=c["t"])
                events.append({"name": "FILLED", "trade": dict(trade)})
                just_filled = True
            else:
                reason = None
                if (favorable - entry) * side >= LIMIT_CANCEL_RR * risk:
                    reason = "Le prix est reparti sans toucher ta limit : trop tard, on ne court pas après."
                elif c["t"] - (trade.get("placed_ts") or 0) >= LIMIT_EXPIRY_CANDLES * TF_SEC:
                    reason = f"Ordre expiré : pas exécuté après {LIMIT_EXPIRY_CANDLES} bougies."
                if reason:
                    trade.update(status="CANCELLED", closed_ts=c["t"], outcome="CANCELLED",
                                 result_r=None, pnl_usd=None)
                    events.append({"name": "CANCELLED", "reason": reason, "trade": dict(trade)})
                    break
                continue

        # 1) stop touché (initial ou déplacé à l'entrée) ?
        if (side == 1 and adverse <= trade["sl"]) or (side == -1 and adverse >= trade["sl"]):
            at_be = bool(trade["be_hit"]) and abs(trade["sl"] - entry) < 1e-9
            r = 0.0 if at_be else -1.0
            trade.update(status="CLOSED", closed_ts=c["t"], result_r=r,
                         pnl_usd=r * trade["risk_usd"], outcome="BE" if at_be else "SL")
            events.append({"name": "BE" if at_be else "SL", "trade": dict(trade)})
            break
        if just_filled:
            continue   # bougie d'exécution : pas de progression comptée (l'ordre des mèches est inconnu)

        # 2) progression : paliers RR1/RR2/RR3 (notification groupe, BE combiné au palier BE_RR)
        r_fav = (favorable - entry) * side / risk
        for lvl in RR_LEVELS:
            if lvl > tp_rr:
                continue   # palier au-delà de la TP de ce trade : jamais atteint (trade clôturé avant)
            field = f"rr{int(lvl)}_hit"
            if not trade[field] and r_fav >= lvl:
                trade[field] = 1
                if lvl >= tp_rr:
                    continue   # palier = TP finale : annoncé par l'événement TP, pas en double
                be_now = not trade["be_hit"] and lvl >= BE_RR
                if be_now:
                    trade["be_hit"], trade["sl"] = 1, entry
                events.append({"name": f"RR{int(lvl)}", "be_moved": be_now, "trade": dict(trade)})
        # BE_RR peut ne pas être un palier RR1/2/3 rond (ex. BE_RR=2.5) : on le couvre séparément
        if not trade["be_hit"] and BE_RR not in RR_LEVELS and BE_RR < tp_rr and r_fav >= BE_RR:
            trade["be_hit"], trade["sl"] = 1, entry
            events.append({"name": "BE_MOVED", "trade": dict(trade)})
        if r_fav >= tp_rr:
            trade.update(status="CLOSED", closed_ts=c["t"], result_r=tp_rr,
                         pnl_usd=tp_rr * trade["risk_usd"], outcome="TP")
            events.append({"name": "TP", "trade": dict(trade)})
            break

    trade["last_ts"] = last_ts
    fields = {k: trade[k] for k in ("sl", "be_hit", "status", "last_ts", "closed_ts", "filled_ts",
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
    ax.set_title(f"{symbol} {TF_LABEL} - {sig['side']}{' LIMIT' if sig.get('order') == 'LIMIT' else ''} ({'CHoCH + CHoCH' if sig['type'] == 'CHOCH2' else 'CHoCH'})",
                 color="white", fontsize=11)
    ax.tick_params(colors="#888888", labelsize=7)
    ax.set_xticks([])
    for s in ax.spines.values():
        s.set_color("#333333")
    path = os.path.join(CHART_DIR, f"{symbol}_{sig['t']}.png")
    fig.savefig(path, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return path


CHART_ON_EVENTS = {"SL", "BE", "TP", "RR1", "RR2", "RR3", "RR4"}  # événements accompagnés d'une image


def make_event_chart(symbol, candles, events, trade, dec):
    """Même rendu que le graphique d'entrée (bougies + niveaux), mais recentré sur la bougie la
    plus récente : sert à montrer où le SL / RR / TP vient d'être touché, comme pour un signal."""
    sig_like = {
        "entry": trade["entry"], "sl": trade["sl"], "tp": trade["tp"],
        "side": trade["side"], "type": trade["kind"], "order": trade.get("order_type", "MARKET"),
        "t": candles[-1]["t"],
    }
    try:
        return make_chart(symbol, candles, events, sig_like, dec)
    except Exception as e:
        print(f"[{symbol}] make_event_chart a échoué ({type(e).__name__}) : notification envoyée sans image")
        return None


# ============================================================================
# 7bis. ANALYSE MANUELLE (menu Telegram « 📊 Analyse »)
#
# Lecture seule, à la demande (admin uniquement) : réutilise le moteur existant (analyze,
# find_pois, htf_bias, _ext_liquidity, m5_retracement, _sweep_at) pour un instantané de la
# structure H1/M15/M5/M1, sans jamais toucher au moteur de signal ni aux positions.
# ============================================================================

def make_analysis_chart(symbol, tf, candles, events, pois, price, dec, n_show=90, extend=20):
    """Bougies réelles + structure BOS/CHoCH + zones OB/FVG proches du prix, sur l'UT `tf`."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except ImportError:
        return None

    os.makedirs(CHART_DIR, exist_ok=True)
    files = sorted((os.path.join(CHART_DIR, f) for f in os.listdir(CHART_DIR)), key=os.path.getmtime)
    for f in files[:-40]:
        try:
            os.remove(f)
        except OSError:
            pass

    view = candles[-n_show:]
    start = len(candles) - len(view)
    fig, ax = plt.subplots(figsize=(9, 5), dpi=110)
    fig.patch.set_facecolor("#0e1117")
    ax.set_facecolor("#0e1117")

    for k, c in enumerate(view):
        up = c["c"] >= c["o"]
        col = "#26a69a" if up else "#ef5350"
        ax.plot([k, k], [c["l"], c["h"]], color=col, lw=0.8)
        ax.add_patch(Rectangle((k - 0.3, min(c["o"], c["c"])), 0.6,
                               max(abs(c["c"] - c["o"]), 1e-9), color=col))

    for ev in events:
        if start <= ev["i"] <= len(candles) - 1 and ev["src"] >= start and ev["kind"] != "INIT":
            col = "#26a69a" if ev["dir"] == 1 else "#ef5350"
            ax.plot([ev["src"] - start, ev["i"] - start], [ev["level"]] * 2, color=col, lw=0.9, ls="--")
            ax.text((ev["src"] + ev["i"]) / 2 - start, ev["level"], ev["kind"].replace("CHOCH", "CHoCH"),
                    color=col, fontsize=7, ha="center", va="bottom")

    x0, x1 = len(view) - 1, len(view) - 1 + extend
    for p in pois:
        col = "#26a69a" if p["d"] == 1 else "#ef5350"
        ax.add_patch(Rectangle((x0, p["lo"]), x1 - x0, p["hi"] - p["lo"], color=col, alpha=0.22))
        ax.text(x1 + 0.5, (p["lo"] + p["hi"]) / 2, f"{_tf_lbl(p['tf'])} {p['kind']}", color=col, fontsize=7, va="center")

    ax.axhline(price, color="#ffffff", lw=0.7, ls=":")
    ax.text(x1 + 0.5, price, f"PRIX {price:.{dec}f}", color="#ffffff", fontsize=8, va="center")

    ax.set_xlim(-1, x1 + 20)
    lows = [c["l"] for c in view] + [p["lo"] for p in pois] + [price]
    highs = [c["h"] for c in view] + [p["hi"] for p in pois] + [price]
    lo, hi = min(lows), max(highs)
    pad = (hi - lo) * 0.05 or 1
    ax.set_ylim(lo - pad, hi + pad)
    ax.set_title(f"{symbol} {_tf_lbl(tf)} — ANALYSE", color="white", fontsize=11)
    ax.tick_params(colors="#888888", labelsize=7)
    ax.set_xticks([])
    for s in ax.spines.values():
        s.set_color("#333333")
    path = os.path.join(CHART_DIR, f"{symbol}_analyse_{int(time.time())}.png")
    fig.savefig(path, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return path


def build_technical_analysis(symbol):
    """Instantané technique H1/M15/M5/M1 : structure, liquidité, sweep, CHoCH, OB/FVG/POI, scénario.
    Retourne (texte, chemin_image|None). N'appelle jamais build_signal : lecture seule."""
    dec = SYMBOLS[symbol]["decimals"]
    htf_lbl = f"H{HTF_MINUTES // 60 or 1}"
    tfs = (HTF_MINUTES, 15, 5, 1)
    data = {}
    for tf in tfs:
        raw = _ctx_candles(symbol, tf)
        data[tf] = {"raw": raw, "events": analyze(raw)} if len(raw) > 2 * SWING_DEPTH + 1 else None

    missing = [_tf_lbl(tf) for tf in tfs if not data.get(tf)]
    if missing:
        return (f"⚠️ Données insuffisantes pour analyser {symbol} pour le moment "
                f"({', '.join(missing)} indisponible(s)). Réessaie dans quelques instants.", None)

    price = data[1]["raw"][-1]["c"]
    bias = htf_bias(symbol)

    struct_lines = []
    for tf in tfs:
        evs = data[tf]["events"]
        if not evs:
            struct_lines.append(f"{_tf_lbl(tf)} : structure indéterminée (historique insuffisant)")
            continue
        last = evs[-1]
        icon = "🟢" if last["dir"] == 1 else "🔴"
        struct_lines.append(f"{_tf_lbl(tf)} : {icon} {_dir_txt(last['dir'])} "
                             f"(dernier {last['kind'].replace('CHOCH', 'CHoCH')} à {last['level']:.{dec}f})")

    ext_txt = _ext_liquidity(symbol, bias, dec) if bias != 0 else "indéterminée (biais H1 non confirmé)"
    retr = m5_retracement(symbol, bias, dec) if bias != 0 else {"state": "INDÉTERMINÉ", "liq": "—", "choch": "—"}

    sweep_lines = []
    for tf in (15, 5, 1):
        raw = data[tf]["raw"]
        idx = len(raw) - 1
        lvl_bull, swept_bull = _sweep_at(raw, idx, 1)     # liquidité sous les plus bas, prise puis rejetée -> haussier
        lvl_bear, swept_bear = _sweep_at(raw, idx, -1)    # liquidité au-dessus des plus hauts, prise puis rejetée -> baissier
        if swept_bull:
            sweep_lines.append(f"{_tf_lbl(tf)} : balayage de liquidité côté bas ({lvl_bull:.{dec}f}) — signal haussier potentiel")
        if swept_bear:
            sweep_lines.append(f"{_tf_lbl(tf)} : balayage de liquidité côté haut ({lvl_bear:.{dec}f}) — signal baissier potentiel")
    if not sweep_lines:
        sweep_lines = ["Aucun balayage de liquidité sur la dernière bougie H15/M5/M1."]

    all_bull, all_bear = [], []
    for tf in (HTF_MINUTES, 15, 5):
        raw = data[tf]["raw"]
        all_bull += find_pois(raw, tf, 1)
        all_bear += find_pois(raw, tf, -1)
    key = lambda p: abs((p["lo"] + p["hi"]) / 2 - price)
    all_bull.sort(key=key)
    all_bear.sort(key=key)

    poi_lines = ["Zones haussières (demande) :"] + ([f"  • {_poi_txt(p, dec)}" for p in all_bull[:3]] or ["  • aucune détectée"])
    poi_lines += ["Zones baissières (offre) :"] + ([f"  • {_poi_txt(p, dec)}" for p in all_bear[:3]] or ["  • aucune détectée"])

    zones_reaction = []
    if all_bull:
        zones_reaction.append(f"Support / POI le plus proche sous le prix : {_poi_txt(all_bull[0], dec)}")
    if all_bear:
        zones_reaction.append(f"Résistance / POI le plus proche au-dessus du prix : {_poi_txt(all_bear[0], dec)}")
    zones_attente = [f"  • {_poi_txt(p, dec)} (haussière, plus loin)" for p in all_bull[1:3]]
    zones_attente += [f"  • {_poi_txt(p, dec)} (baissière, plus loin)" for p in all_bear[1:3]]

    if bias == 0:
        scenario = "Biais H1 indéterminé : structure encore peu claire pour un scénario directionnel fiable — attendre une cassure nette."
    else:
        state_txt = {
            "EN COURS": "le retracement M5 est toujours en cours (M5 encore à contre-sens du contexte) : pas d'entrée tant qu'un CHoCH M5 dans le sens du contexte n'apparaît pas",
            "AUCUN": "M5 est déjà aligné avec le contexte (pas de retracement à attendre) : continuation possible",
            "TERMINÉ": "le retracement M5 est terminé (CHoCH M5 confirmé par un balayage de liquidité ou une réaction sur POI) : le contexte peut reprendre",
            "NON CONFIRMÉ": "un CHoCH M5 est apparu mais sans confirmation (ni sweep ni réaction sur POI) : prudence",
            "INDÉTERMINÉ": "structure M5 insuffisante pour juger du retracement",
        }.get(retr["state"], retr["state"])
        scenario = (f"Contexte {htf_lbl} <b>{_dir_txt(bias).upper()}</b> (à confirmer par M15). {state_txt}. "
                    f"Liquidité externe visée : {ext_txt}.")

    text = (
        f"📈 <b>ANALYSE TECHNIQUE — {symbol}</b>\n"
        f"Prix actuel : <b>{price:.{dec}f}</b>\n\n"
        f"<b>Structure multi-UT</b>\n" + "\n".join(struct_lines) + "\n\n"
        f"<b>Liquidité</b>\n"
        f"Externe ({htf_lbl}) : {ext_txt}\n"
        f"Interne (M5) : {retr['liq']}\n\n"
        f"<b>Sweep récents</b>\n" + "\n".join(sweep_lines) + "\n\n"
        f"<b>CHoCH M5</b>\n{retr['choch']}\n\n"
        f"<b>Zones OB / FVG / POI ({htf_lbl}/M15/M5)</b>\n" + "\n".join(poi_lines) + "\n\n"
        + (f"<b>Zones stratégiques (réaction probable)</b>\n" + "\n".join(zones_reaction) + "\n\n" if zones_reaction else "")
        + (f"<b>Zones d'attente</b>\n" + "\n".join(zones_attente) + "\n\n" if zones_attente else "")
        + f"<b>Scénario technique actuel</b>\n{scenario}\n\n"
        f"⚠️ Analyse informative — n'affecte ni le moteur de signal ni les positions en cours."
    )

    chart_tf = 15
    chart_path = None
    try:
        chart_pois = (all_bull[:2] + all_bear[:2])
        chart_path = make_analysis_chart(symbol, chart_tf, data[chart_tf]["raw"], data[chart_tf]["events"],
                                          chart_pois, price, dec)
    except Exception as e:
        print(f"[{symbol}] make_analysis_chart a échoué ({type(e).__name__}) : analyse envoyée sans image")
    return text, chart_path


_ff_cache = {}   # calendrier ForexFactory (public, sans clé) : mis en cache 15 min


def _forexfactory_events():
    now = time.time()
    if _ff_cache.get("data") is not None and now - _ff_cache.get("ts", 0) < 900:
        return _ff_cache["data"]
    events = []
    for url in ("https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                "https://nfs.faireconomy.media/ff_calendar_nextweek.json"):
        try:
            r = requests.get(url, timeout=15)
            if r.ok:
                events += r.json()
        except Exception as e:
            print(f"[fondamental] lecture calendrier échouée ({url}) : {e}")
    _ff_cache["data"], _ff_cache["ts"] = events, now
    return events


def build_fundamental_analysis(symbol):
    """Résumé du calendrier économique public (ForexFactory, sans clé) pertinent pour l'actif.
    Aucune génération de texte libre : uniquement des événements réels du calendrier, sans invention."""
    try:
        raw_events = _forexfactory_events()
    except Exception as e:
        return f"⚠️ Calendrier économique indisponible pour le moment ({type(e).__name__})."
    if not raw_events:
        return "⚠️ Calendrier économique indisponible pour le moment — réessaie plus tard."

    relevant = {"USD"} | ({"CHF", "JPY"} if symbol == "XAUUSD" else set())
    now = datetime.now(timezone.utc)
    upcoming, recent = [], []
    for e in raw_events:
        cur = e.get("country") or e.get("currency")
        if cur not in relevant or str(e.get("impact", "")).strip() not in ("High", "Medium"):
            continue
        try:
            dt = datetime.fromisoformat(str(e.get("date", "")).replace("Z", "+00:00"))
        except ValueError:
            continue
        item = {"title": e.get("title", "?"), "cur": cur, "impact": e.get("impact", ""),
                "dt": dt, "forecast": e.get("forecast", "")}
        (upcoming if dt >= now else recent).append(item)
    upcoming.sort(key=lambda x: x["dt"])
    recent.sort(key=lambda x: x["dt"], reverse=True)
    recent = [x for x in recent if (now - x["dt"]).days <= 2]

    def _fmt_ev(x):
        icon = "🔴" if x["impact"] == "High" else "🟠"
        fc = f" (prév. {x['forecast']})" if x["forecast"] else ""
        return f"{icon} {x['dt'].strftime('%a %d/%m %H:%M UTC')} — {x['cur']} {x['title']}{fc}"

    lines_up = [_fmt_ev(x) for x in upcoming[:6]] or ["Aucun événement macro à fort impact recensé dans les prochains jours."]
    lines_re = [_fmt_ev(x) for x in recent[:4]] or ["Aucun événement macro récent à fort impact (48h)."]
    watch_note = (
        "Le Gold réagit surtout aux taux réels, au dollar et à l'aversion au risque "
        "(données Fed / inflation / emploi US, tensions géopolitiques, JPY/CHF en valeurs refuge)."
        if symbol == "XAUUSD" else
        "Le BTC réagit surtout à la liquidité globale et aux données macro USD (taux, inflation, emploi), "
        "ainsi qu'au sentiment risk-on / risk-off des marchés."
    )
    return (
        f"📰 <b>ANALYSE FONDAMENTALE — {symbol}</b>\n\n"
        f"<b>À surveiller (prochains jours)</b>\n" + "\n".join(lines_up) + "\n\n"
        f"<b>Événements récents (48h)</b>\n" + "\n".join(lines_re) + "\n\n"
        f"<b>Contexte</b>\n{watch_note}\n\n"
        f"⚠️ Résumé du calendrier économique public (ForexFactory) — analyse informative, "
        f"n'affecte ni le moteur de signal ni les positions."
    )


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


def check_group(send_test=False):
    """Vérifie que le bot peut écrire dans CHAT_ID_GROUPE. Retourne (ok, message lisible)."""
    if not TELEGRAM_TOKEN:
        return False, "TELEGRAM_TOKEN absent."
    if not CHAT_ID_GROUPE:
        return False, "CHAT_ID_GROUPE n'est pas défini sur Render : aucun signal n'ira au groupe."
    hint = ""
    if not str(CHAT_ID_GROUPE).startswith("-"):
        hint = ("\n👉 Un groupe/canal a un ID NÉGATIF (ex. -1001234567890). Un ID positif est celui d'une "
                "personne, qui doit d'abord avoir démarré le bot.")
    res = _post("sendMessage" if send_test else "getChat",
                {"chat_id": CHAT_ID_GROUPE, "text": "✅ Test de connexion AlphaBot"} if send_test
                else {"chat_id": CHAT_ID_GROUPE})
    if res and res.get("ok"):
        return True, f"Groupe joignable (CHAT_ID_GROUPE = {CHAT_ID_GROUPE})."
    why = (res or {}).get("description", "pas de réponse de Telegram")
    return False, (f"⚠️ Le bot ne peut pas écrire dans le groupe (CHAT_ID_GROUPE = {CHAT_ID_GROUPE}) : "
                   f"{why}{hint}\n👉 Vérifie l'ID, et que le bot a bien été ajouté au groupe "
                   f"(administrateur si c'est un canal).")


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


def _dist(sig, level, dec):
    """Distance signée (en points) entre l'entrée et un niveau : SL négatif, TP positif pour un BUY (inverse pour un SELL)."""
    side = 1 if sig["side"] == "BUY" else -1
    d = (level - sig["entry"]) * side
    return f"{d:+,.{dec}f}".replace(",", " ")


def _limit_cancel_level(sig):
    """Prix au-delà duquel l'ordre LIMIT est annulé (le prix est parti sans toucher la limit)."""
    side = 1 if sig["side"] == "BUY" else -1
    return sig["entry"] + side * LIMIT_CANCEL_RR * sig["risk"]


def _exec_block(sig, dec):
    """(texte d'exécution, libellé + valeur de l'entrée) — DIRECT ou LIMIT, toujours explicite."""
    if sig.get("order") == "LIMIT":
        ref = sig.get("ref_price")
        now = f" (prix actuel ≈ {_fmt(ref, dec)})" if ref else ""
        mins = LIMIT_EXPIRY_CANDLES * TF_SEC // 60
        txt = (f"⏳ <b>{sig['side']} LIMIT</b> — <b>N'ENTRE PAS MAINTENANT</b>\n"
               f"Place un ordre limit à {_fmt(sig['entry'], dec)}{now}, puis attends le retour du prix.\n"
               f"❌ Annulé si le prix atteint {_fmt(_limit_cancel_level(sig), dec)} sans toucher la limit, "
               f"ou après {mins} min : je te préviens.")
        return txt, f"Entrée (limit) : <b>{_fmt(sig['entry'], dec)}</b>"
    return (f"⚡ <b>{sig['side']} AU MARCHÉ</b> — entrée directe, pas d'ordre limit",
            f"Entrée : <b>≈ {_fmt(sig['entry'], dec)}</b>")


def _sig_rr(sig):
    """RR de la TP du signal (fixé à la création ; recalculé depuis entrée / SL / TP pour un renvoi différé)."""
    if sig.get("rr"):
        return sig["rr"]
    return round(abs(sig["tp"] - sig["entry"]) / sig["risk"], 2) if sig.get("risk") else TP_RR


def _params_lines(sig, tf=None):
    """Les 3 paramètres affichés sur un nouveau signal."""
    htf = sig.get("htf")
    if htf is None:
        htf = get_htf_filter()
    return (f"Timeframe : {tf or sig.get('tf') or TF_LABEL}\n"
            f"RR : {_sig_rr(sig):g}\n"
            f"Filtre HTF : {'ON' if htf else 'OFF'}")


def group_signal(symbol, sig, position_n, dec, tf=None):
    icon = "🟢" if sig["side"] == "BUY" else "🔴"
    rr_lines = "\n".join(
        f"RR{int(lvl)} : {_fmt(_rr_price(sig, lvl), dec)}" for lvl in RR_LEVELS)
    exec_txt, entry_line = _exec_block(sig, dec)
    label = f"{sig['side']} LIMIT" if sig.get("order") == "LIMIT" else sig["side"]
    return (
        f"Nouveau signal\n"
        f"{icon} <b>{label} {symbol}</b> · {tf or TF_LABEL}\n"
        f"Type : {_kind_label(sig['type'])}\n"
        f"Exécution : {exec_txt}\n\n"
        f"{entry_line}\n"
        f"SL : {_fmt(sig['sl'], dec)} ({_dist(sig, sig['sl'], dec)} pts)\n"
        f"{rr_lines}\n"
        f"TP : {_fmt(sig['tp'], dec)} ({_dist(sig, sig['tp'], dec)} pts)\n\n"
        f"BE → RR{BE_RR:g}\n"
        f"TP final → RR{_sig_rr(sig):g}\n\n"
        f"{_params_lines(sig, tf)}"
        + (f"\n\nPosition {position_n}/{MAX_POSITIONS} sur {symbol}" if position_n else "")
    )


def admin_signal(symbol, sig, risk_usd, leverage, lot_info, margin, dec):
    """Message complet envoyé uniquement en privé/admin : signal + risque + levier + lot."""
    exec_txt, entry_line = _exec_block(sig, dec)
    label = f"{sig['side']} LIMIT" if sig.get("order") == "LIMIT" else sig["side"]
    base = "TA limit" if sig.get("order") == "LIMIT" else "TON prix d'entrée réel"
    txt = (
        f"💰 <b>{symbol} {label}</b> (privé)\n"
        f"Exécution : {exec_txt}\n"
        f"{entry_line.replace('<b>', '').replace('</b>', '')}\n"
        f"SL : {_fmt(sig['sl'], dec)} (distance {_fmt(sig['risk'], dec)} pts)\n"
        f"TP : {_fmt(sig['tp'], dec)} (RR{_sig_rr(sig):g})\n"
        f"📐 Si ton prix broker diffère : SL {_dist(sig, sig['sl'], dec)} / TP {_dist(sig, sig['tp'], dec)} "
        f"pts depuis {base}.\n\n"
        f"Risque : {risk_usd:g} $\n"
        f"Levier : {leverage:g}x\n"
        f"👉 <b>Lot calculé : {lot_info['lot']:g}</b>\n"
        f"Marge indicative ≈ {margin:.2f} $\n\n"
        f"{_params_lines(sig)}"
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
    if name == "FILLED":
        return (f"✅ <b>{t['side']} LIMIT exécuté</b> — {t['symbol']} @ {_fmt(t['entry'], dec)}\n"
                f"Position ouverte · SL {_fmt(t['sl'], dec)} · TP {_fmt(t['tp'], dec)}")
    if name == "CANCELLED":
        return (f"❌ <b>Ordre LIMIT annulé</b> — {head}\n{ev['reason']}\n"
                f"Retire ton ordre limit s'il est encore en attente.")
    if name == "BE_MOVED":
        return f"🔒 <b>RR{BE_RR:g} atteint</b> — {head}\nSL déplacé à l'entrée (BE)."
    if name == "TP":
        return f"🎯 <b>RR{trade_tp_rr(t):g} atteint — TP ✅</b> — {head}\nRésultat : <b>WIN ({t['result_r']:+.2f} R)</b>"
    if name == "BE":
        return f"➖ <b>Clôture à l'entrée (BE)</b> — {head}\nRésultat : <b>{t['result_r']:+.2f} R</b>"
    return (f"🔴 <b>SL touché ❌</b> — {head}\nRésultat : <b>LOSS ({t['result_r']:+.2f} R)</b>\n\n"
            f"<i>{random.choice(MOTIVATION_LINES)}</i>")


def admin_event(ev):
    t = ev["trade"]
    if ev["name"] == "FILLED":
        return f"✅ {t['symbol']} {t['side']} LIMIT exécuté @ {t['entry']:g} — position ouverte."
    if ev["name"] == "CANCELLED":
        return f"❌ {t['symbol']} {t['side']} LIMIT annulé — {ev['reason']} Retire l'ordre."
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


# --- ⚙️ PARAMÈTRES SIGNAL : RR / timeframe d'entrée / filtre HTF ------------------------------------
def _signal_text():
    return (f"⚙️ <b>PARAMÈTRES SIGNAL</b>\n\n"
            f"🎯 RR actuel : RR{get_tp_rr():g}\n"
            f"⏱ Timeframe : {TF_LABEL}\n"
            f"🧠 Filtre HTF : {'🟢 ACTIVÉ' if get_htf_filter() else '🔴 DÉSACTIVÉ'}")


def _signal_keyboard():
    return {"inline_keyboard": [
        [{"text": "🎯 RR", "callback_data": "sig:rr"}, {"text": "⏱ TIMEFRAME", "callback_data": "sig:tf"},
         {"text": "🧠 FILTRE HTF", "callback_data": "sig:htf"}],
        [{"text": "🔙 Menu", "callback_data": "menu:home"}]]}


def _signal_back(rows):
    return {"inline_keyboard": rows + [[{"text": "🔙 Paramètres", "callback_data": "sig:home"}]]}


def _signal_rr_text():
    return (f"🎯 RR actuel : <b>RR{get_tp_rr():g}</b>\n"
            f"S'applique immédiatement aux NOUVEAUX signaux. Les trades déjà ouverts gardent leur TP / SL / RR.")


def _signal_rr_keyboard():
    cur = get_tp_rr()
    return _signal_back([[{"text": ("✅ " if float(v) == cur else "") + f"RR{v}", "callback_data": f"srr:{v}"}
                          for v in SIGNAL_RR_CHOICES]])


def _signal_tf_text():
    n = len(open_trades())
    note = f"\n⚠️ {n} position(s) ouverte(s) : suivies avec les bougies du nouveau timeframe." if n else ""
    return (f"⏱ Timeframe d'entrée : <b>{TF_LABEL}</b>\n"
            f"Timeframe du déclenchement final du signal. Effet immédiat : le bot se recale sur la dernière "
            f"bougie clôturée, sans rejouer l'historique.{note}")


def _signal_tf_keyboard():
    return _signal_back([[{"text": ("✅ " if m == TIMEFRAME_MIN else "") + TF_LABELS[m], "callback_data": f"stf:{m}"}
                          for m in SIGNAL_TF_CHOICES]])


def _signal_htf_text():
    return (f"🧠 Filtre HTF : {'🟢 ACTIVÉ' if get_htf_filter() else '🔴 DÉSACTIVÉ'}\n"
            f"ACTIVÉ : H{HTF_MINUTES // 60 or 1} = biais, M15 = confirmation du contexte, M5 = fin du retracement, "
            f"POI (OB / FVG) mitigé = zone de réaction, CHoCH d'entrée = trigger final. "
            f"Un CHoCH seul ne suffit pas et ne peut jamais aller contre le contexte H1 / M15 (c'est un retracement). "
            f"DÉSACTIVÉ : logique de signal d'origine, sans ce filtre. Effet immédiat.")


def _signal_htf_keyboard():
    on = get_htf_filter()
    return _signal_back([[{"text": ("✅ " if on else "") + "🟢 ACTIVÉ", "callback_data": "shtf:1"},
                          {"text": ("✅ " if not on else "") + "🔴 DÉSACTIVÉ", "callback_data": "shtf:0"}]])


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


_ANALYSE_SYMS = {"XAU": "XAUUSD", "BTC": "BTCUSD"}


def _analyse_text():
    return ("📊 <b>ANALYSE</b>\n\n"
            "Choisis l'actif. Lecture seule : n'affecte jamais le moteur de signal ni les positions.")


def _analyse_keyboard():
    return _with_back({"inline_keyboard": [
        [{"text": "🥇 XAUUSD", "callback_data": "ana:sym:XAU"}, {"text": "₿ BTCUSD", "callback_data": "ana:sym:BTC"}],
    ]})


def _analyse_symbol_text(symbol):
    return f"📊 <b>{symbol}</b>\n\nChoisis le type d'analyse :"


def _analyse_symbol_keyboard(symbol):
    code = next(k for k, v in _ANALYSE_SYMS.items() if v == symbol)
    return {"inline_keyboard": [
        [{"text": "📈 TECHNIQUE", "callback_data": f"ana:tech:{code}"},
         {"text": "📰 FONDAMENTAL", "callback_data": f"ana:fond:{code}"}],
        [{"text": "🔙 Analyse", "callback_data": "ana:home"}, {"text": "🔙 Menu", "callback_data": "menu:home"}],
    ]}


def _menu_keyboard():
    return {"inline_keyboard": [
        [{"text": "💰 Risque", "callback_data": "menu:risque"}, {"text": "⚙️ Levier", "callback_data": "menu:levier"}],
        [{"text": "⏱ Timeframe", "callback_data": "menu:timeframe"}, {"text": "🎭 Médias", "callback_data": "menu:medias"}],
        [{"text": "📊 Stats", "callback_data": "menu:stats"}, {"text": "📈 Positions", "callback_data": "menu:trades"}],
        [{"text": "📊 Analyse", "callback_data": "ana:home"}],
        [{"text": "⚙️ Paramètres signal", "callback_data": "menu:signal"}],
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
               "/statut — prix BTC / Gold et dernière bougie scannée\n"
               "/testgroupe — envoie un message test au groupe (vérifie CHAT_ID_GROUPE)\n"
               "/trades — positions en cours (avec RR actuel)\n"
               "/timeframe [M1|M3|M5|M15|M30|H1] — change le timeframe à chaud\n"
               "/signal — paramètres du signal (RR, timeframe, filtre HTF)\n"
               "/medias — stickers / images / GIF du groupe (TP, SL, BE, motivation)\n"
               "/analyse — analyse technique / fondamentale à la demande (BTCUSD, XAUUSD)\n"
               "/menu — menu à boutons")
        return (txt, _menu_keyboard())
    if cmd in ("/analyse", "/analyze"):
        return (_analyse_text(), _analyse_keyboard())
    if cmd == "/testgroupe":
        return (check_group(send_test=True)[1], None)
    if cmd in ("/statut", "/status", "/prix"):
        return ("📡 <b>Scan en cours</b> — " + TF_LABEL + "\n" + "\n".join(scan_status_lines()), None)
    if cmd == "/menu":
        return (_home_text(), _menu_keyboard())
    if cmd in ("/signal", "/parametres", "/paramètres"):
        return (_signal_text(), _signal_keyboard())
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
            if t["status"] == "PENDING":
                lines.append(f"{i}. ⏳ <b>{t['symbol']} {t['side']} LIMIT</b> — en attente\n"
                             f"Limit : {_fmt(t['entry'], dec)}\n"
                             f"SL : {_fmt(t['sl'], dec)}\n"
                             f"TP : {tp_str}\n"
                             f"Lot : {t['lot']:g}")
                continue
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
    "timeframe": "/timeframe", "medias": "/medias", "signal": "/signal",
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
        elif data.startswith("sig:"):
            page = data[4:]
            if page == "rr":
                _edit(cq, _signal_rr_text(), _signal_rr_keyboard())
            elif page == "tf":
                _edit(cq, _signal_tf_text(), _signal_tf_keyboard())
            elif page == "htf":
                _edit(cq, _signal_htf_text(), _signal_htf_keyboard())
            else:
                _edit(cq, _signal_text(), _signal_keyboard())
        elif data.startswith("srr:"):
            rr = int(data[4:])
            if rr in SIGNAL_RR_CHOICES:
                set_tp_rr(rr)
                _edit(cq, _signal_text(), _signal_keyboard())
                ack = f"RR{rr}"
        elif data.startswith("stf:"):
            tf = int(data[4:])
            if tf in SIGNAL_TF_CHOICES:
                set_timeframe(tf)
                _edit(cq, _signal_text(), _signal_keyboard())
                ack = TF_LABEL
        elif data.startswith("shtf:"):
            set_htf_filter(data[5:] == "1")
            _edit(cq, _signal_text(), _signal_keyboard())
            ack = "Filtre HTF : " + ("ON" if get_htf_filter() else "OFF")
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
        elif data == "ana:home":
            _edit(cq, _analyse_text(), _analyse_keyboard())
        elif data.startswith("ana:sym:"):
            sym = _ANALYSE_SYMS.get(data[8:])
            if sym:
                _edit(cq, _analyse_symbol_text(sym), _analyse_symbol_keyboard(sym))
        elif data.startswith("ana:tech:"):
            sym = _ANALYSE_SYMS.get(data[9:])
            if sym:
                _edit(cq, f"⏳ Analyse technique {sym} en cours…", None)
                chat_id = cq["message"]["chat"]["id"]
                txt, chart = build_technical_analysis(sym)
                if chart:
                    send(chat_id, f"📊 {sym} — zones actuelles (H{HTF_MINUTES // 60 or 1}/M15/M5/M1)", photo=chart)
                send(chat_id, txt, reply_markup=_analyse_symbol_keyboard(sym))
                ack = "Analyse envoyée"
        elif data.startswith("ana:fond:"):
            sym = _ANALYSE_SYMS.get(data[9:])
            if sym:
                _edit(cq, f"⏳ Analyse fondamentale {sym} en cours…", None)
                txt = build_fundamental_analysis(sym)
                send(cq["message"]["chat"]["id"], txt, reply_markup=_analyse_symbol_keyboard(sym))
                ack = "Analyse envoyée"
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
_last_candle = {}  # symbole -> ouverture (ts) de la dernière bougie clôturée reçue
_last_check = {}   # symbole -> heure (time.time) du dernier appel réussi à la source de prix


def scan_status_lines():
    """Une ligne par actif : dernier prix, dernière bougie, positions ouvertes (pour /statut et les logs)."""
    out = []
    for sym, cfg in SYMBOLS.items():
        p, t = _last_price.get(sym), _last_candle.get(sym)
        if p is None or t is None:
            out.append(f"⏳ {sym} : en attente des premières données")
            continue
        stale = time.time() - t > 3 * TF_SEC + 60
        note = " — marché fermé ? (pas de nouvelle bougie)" if stale else ""
        out.append(f"{'💤' if stale else '✅'} {sym} : {_fmt(p, cfg['decimals'])} · bougie {_ts_str(t)} · "
                   f"{count_open(sym)} position(s){note}")
    return out


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
        status="PENDING" if sig.get("order") == "LIMIT" else "OPEN",
        order_type=sig.get("order", "MARKET"), placed_ts=sig["t"], ref_price=sig.get("ref_price"),
        sent_admin=0, sent_group=0,   # passent à 1 seulement quand Telegram a confirmé l'envoi
        signal_key=key, symbol=symbol, side=sig["side"], kind=sig["type"], timeframe=TF_LABEL,
        entry=sig["entry"], sl=sig["sl"], sl_initial=sig["sl"], tp=sig["tp"],
        risk_usd=lot_info["real_risk"],  # risque réel du lot pris (= risque demandé sauf lot minimum)
        lot=lot_info["lot"], leverage=leverage, opened_ts=now_ts(), last_ts=sig["t"])
    print(f"[{symbol}] SIGNAL {sig['side']} {sig.get('order', 'MARKET')} {sig['type']} @ {sig['entry']:.{dec}f} "
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
        if t["status"] not in ("OPEN", "PENDING") or tries >= 8:
            print(f"[retry] signal #{t['id']} abandonné (trade {t['status']}, {tries} essais)")
            update_trade(t["id"], sent_admin=1, sent_group=1)
            continue
        _retry_state[t["id"]] = [tries + 1, time.time()]
        sym = t["symbol"]
        dec = SYMBOLS[sym]["decimals"]
        sig = {"side": t["side"], "type": t["kind"], "entry": t["entry"], "sl": t["sl_initial"],
               "tp": t["tp"], "risk": abs(t["entry"] - t["sl_initial"]),
               "order": t.get("order_type") or "MARKET", "ref_price": t.get("ref_price"), "tf": t["timeframe"]}
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
    _last_candle[symbol] = last_t
    _last_check[symbol] = time.time()

    if last_seen is None:  # premier lancement : on s'initialise sur la dernière bougie
        set_meta(meta_key, last_t)
        print(f"[{symbol}] initialisé sur la bougie {_ts_str(last_t)} (l'historique n'est pas signalé)")
        return
    if last_t <= last_seen:
        return  # pas encore de nouvelle bougie clôturée

    # 0) signaux d'ouverture pas encore confirmés : ils partent avant tout message de suivi
    retry_unsent_signals()

    # 1) suivi des positions ouvertes (BE / TP / SL)
    events = analyze(candles)
    for trade in open_trades(symbol):
        for ev in track_trade(trade, candles):
            chart = make_event_chart(symbol, candles, events, ev["trade"], dec) \
                if ev["name"] in CHART_ON_EVENTS else None
            to_group(group_event(ev, dec), photo=chart)
            group_event_media(ev["name"])
            txt = admin_event(ev)
            if txt:
                to_admin(txt)

    # 2) nouveaux signaux : CHoCH survenus sur les bougies clôturées depuis le dernier passage
    n = len(candles)
    for ev in events:
        if ev["t"] <= last_seen or ev["i"] < n - SIGNAL_MAX_AGE:
            continue
        sig = build_signal(candles, ev, symbol)
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
        last_hb = 0.0
        while True:
            if time.time() - last_hb >= 300:   # « je scanne bien » dans les logs, toutes les 5 min
                last_hb = time.time()
                try:
                    print("[scan] " + " | ".join(scan_status_lines()))
                except Exception:
                    traceback.print_exc()
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
    ok_grp, msg_grp = check_group()
    print(f"[groupe] {msg_grp}")
    if not ok_grp and TELEGRAM_TOKEN:
        to_admin(msg_grp)   # tu es prévenu dès le démarrage, pas au moment d'un signal perdu
    to_admin(f"🤖 AlphaBot démarré — timeframe <b>{TF_LABEL}</b> ({_TF_SOURCE}). Change-le avec /timeframe.")
    _run(_env_int("PORT", 10000))




# ============================================================================
# 11. AUTO-TESTS  (python main.py --selftest)
#
# Tests du moteur H1 -> M15 -> M5 -> M1 (filtre HTF ON). Aucun réseau, aucune écriture en base de prod : `--selftest` utilise une
# base SQLite jetable, et `get_candles` est remplacé par des bougies synthétiques. Un seul flux M1 est fabriqué par scénario
# (tendance + ondulations + bruit) ; M5 / M15 / H1 en sont dérivés par agrégation, donc les 4 UT sont cohérentes entre elles.
# Chaque CHoCH M1 est évalué avec `build_signal` comme `process_symbol` le fait en live, sur les bougies connues À CE MOMENT-LÀ
# (pas de lookahead). L'état H1 / M15 / M5 qui sert à classer les CHoCH est recalculé ici avec `analyze`, indépendamment du
# code de décision. Scénarios : retracement bullish sans SELL · retracement bearish sans BUY · continuation valide ·
# invalidation HTF · chop. Optionnel : OLD_ENGINE=<ancien main.py> ajoute la non-régression contre l'ancien moteur.
# ============================================================================

import contextlib
import importlib.util
import io
import unittest
from collections import Counter

_ST_OLD = os.environ.get("OLD_ENGINE")


class _Self:
    """Le moteur testé = ce fichier lui-même, quel que soit le mode de chargement (script ou import)."""
    def __getattr__(self, k):
        return globals()[k]

    def __setattr__(self, k, v):
        globals()[k] = v


_E = _Self()
_ST_SYM = "XAUUSD"
_ST_T0 = 1_700_000_000 - (1_700_000_000 % 3600)
_ST_WAVES = ((37, 1.6), (110, 3.0), (23, 0.8))   # (période en min, amplitude) : créent des swings M5 / M15


def _st_load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ----------------------------------------------------------------------------- données synthétiques
def _st_path(waypoints, sigma, seed, waves=_ST_WAVES):
    rng, out, prev = random.Random(seed), [], waypoints[0][1]
    ph = [rng.uniform(0, 6.28) for _ in waves]
    for (m0, p0), (m1, p1) in zip(waypoints, waypoints[1:]):
        for m in range(m0, m1):
            wave = sum(a * math.sin(2 * math.pi * m / per + f) for (per, a), f in zip(waves, ph))
            c = p0 + (p1 - p0) * (m - m0) / (m1 - m0) + wave + rng.gauss(0, sigma)
            h = max(prev, c) + abs(rng.gauss(0, sigma * .5))
            l = min(prev, c) - abs(rng.gauss(0, sigma * .5))
            out.append({"t": _ST_T0 + m * 60, "o": prev, "h": h, "l": l, "c": c})
            prev = c
    return out


def _st_agg(m1, tf, limit=300):
    """Bougies `tf` minutes COMPLÈTES construites depuis les M1 (comme get_candles : 300 dernières, clôturées)."""
    if tf == 1:
        return m1[-limit:]
    sec, end, groups = tf * 60, m1[-1]["t"] + 60, {}
    for x in m1:
        groups.setdefault(x["t"] // sec * sec, []).append(x)
    out = [{"t": g, "o": xs[0]["o"], "h": max(x["h"] for x in xs), "l": min(x["l"] for x in xs), "c": xs[-1]["c"]}
           for g, xs in sorted(groups.items()) if g + sec <= end]
    return out[-limit:]


def _st_zigzag(start, legs):
    wps, m, p = [(0, start)], 0, start
    for dur, dp in legs:
        m, p = m + dur, p + dp
        wps.append((m, p))
    return wps


def _st_trend_legs(seed, n, sign):
    """sign=+1 : jambes hautes longues + petits retracements (tendance haussière) ; sign=-1 : l'inverse."""
    rnd, legs = random.Random(seed), []
    for _ in range(n):
        legs.append((rnd.randint(420, 700), sign * rnd.uniform(28, 50)))
        legs.append((rnd.randint(200, 320), -sign * rnd.uniform(10, 20)))
    return legs


def _st_chop_legs(seed, n=40, amp=12):
    rnd, legs, sgn = random.Random(seed), [], 1
    for _ in range(n):
        legs.append((rnd.randint(150, 330), sgn * rnd.uniform(amp * .7, amp * 1.2)))
        sgn = -sgn
    return legs


# ----------------------------------------------------------------------------- évaluation
class _StFeed:
    """Branche le moteur sur un flux M1 tronqué à `n` bougies (n avance CHoCH par CHoCH)."""

    def __init__(self, mod, m1, only_m1=False):
        self.mod, self.m1, self.n, self.only_m1 = mod, m1, len(m1), only_m1
        mod.get_candles = self._get
        mod._apply_timeframe(1)

    def _get(self, symbol, minutes=None):
        tf = minutes or self.mod.TIMEFRAME_MIN
        if self.only_m1 and tf != 1:
            raise RuntimeError("UT supérieures indisponibles (test « M1 seul »)")
        return _st_agg(self.m1[:self.n], tf)


def _st_eval(mod, m1, only_m1=False, tolerate=False):
    """[(ev, signal | None, log)] pour chaque CHoCH M1, évalué sur les bougies connues à sa clôture.
    tolerate=True : une exception du moteur est notée dans le log au lieu d'interrompre (ancien moteur : IndexError si H1 sans cassure)."""
    feed, res = _StFeed(mod, m1, only_m1), []
    for ev in mod.analyze(m1):
        if ev["kind"] != "CHOCH":
            continue
        feed.n = ev["i"] + 1
        mod._htf_cache.clear()
        mod._ctx_cache.clear()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            try:
                sig = mod.build_signal(m1[:feed.n], ev, _ST_SYM)
            except Exception as exc:
                if not tolerate:
                    raise
                sig = None
                print(f"EXCEPTION {type(exc).__name__}")
        res.append((ev, sig, buf.getvalue()))
    return res


def _st_state(m1, n):
    """(H1, M15, M5 dir, M5 kind) à la clôture de la bougie n-1, via `analyze` direct (indépendant de la logique de décision)."""
    def last(tf):
        c = _st_agg(m1[:n], tf)
        evs = _E.analyze(c) if len(c) > 2 * _E.SWING_DEPTH + 1 else []
        return (evs[-1]["dir"], evs[-1]["kind"]) if evs else (0, None)
    (h1, _), (m15, _), (m5, k5) = last(60), last(15), last(5)
    return h1, m15, m5, k5


def _st_reason(log):
    m = re.search(r"ignoré : (.*)", log)
    return m.group(1) if m else None


def _st_field(log, label):
    m = re.search(rf"^\[{_ST_SYM}\]   {re.escape(label)}\s*: (.*)$", log, re.M)
    return m.group(1) if m else None


class _StScenarios:
    """Chaque scénario est évalué une seule fois (l'évaluation est le poste de coût)."""
    _cache = {}

    @classmethod
    def get(cls, name, builder):
        if name not in cls._cache:
            m1 = builder()
            cls._cache[name] = (m1, _st_eval(_E, m1))
        return cls._cache[name]

    @classmethod
    def bull(cls, seed=3):
        return cls.get(f"bull{seed}", lambda: _st_path(_st_zigzag(2000, _st_trend_legs(seed, 14, 1)), 0.35, seed))

    @classmethod
    def bear(cls, seed=3):
        return cls.get(f"bear{seed}", lambda: _st_path(_st_zigzag(2400, _st_trend_legs(seed, 14, -1)), 0.35, seed))

    @classmethod
    def flip(cls, seed=4):   # 8 jambes haussières puis 12 jambes baissières
        legs = _st_trend_legs(seed, 8, 1) + _st_trend_legs(seed + 100, 12, -1)
        return cls.get(f"flip{seed}", lambda: _st_path(_st_zigzag(2000, legs), 0.35, seed)), sum(d for d, _ in legs[:16])

    @classmethod
    def chop(cls, seed=1):
        return cls.get(f"chop{seed}", lambda: _st_path(_st_zigzag(2000, _st_chop_legs(seed)), 0.35, seed, waves=((37, 1.6), (23, .8))))


# ----------------------------------------------------------------------------- tests
class TestRetracement(unittest.TestCase):
    """H1 + M15 alignés, M5 à contre-sens = retracement interne : jamais un signal dans le sens du M5."""

    def _check(self, m1, res, ctx):
        # ctx = 1 : contexte haussier (M5 baissier = retracement) ; ctx = -1 : contexte baissier (M5 haussier)
        bucket = [(ev, sig, log) for ev, sig, log in res
                  if _st_state(m1, ev["i"] + 1)[:3] == (ctx, ctx, -ctx)]
        with_ctx = [b for b in bucket if b[0]["dir"] == ctx]        # CHoCH M1 dans le sens du contexte, M5 pas encore retourné
        against = [b for b in bucket if b[0]["dir"] == -ctx]        # CHoCH M1 dans le sens du M5 = contre le contexte
        self.assertGreater(len(with_ctx), 5, "scénario non représentatif : pas assez de CHoCH dans le sens du contexte")
        self.assertGreater(len(against), 5, "scénario non représentatif : pas assez de CHoCH contre le contexte")
        self.assertEqual([b for b in bucket if b[1]], [], "aucun signal ne doit sortir pendant un retracement M5")
        for _, _, log in against:
            self.assertIn(f"contre le contexte {'haussier' if ctx == 1 else 'baissier'}", _st_reason(log))
            self.assertEqual(_st_field(log, "retracement"), "—", "l'étape M5 ne doit même pas être atteinte")
        for _, _, log in with_ctx:
            self.assertIn("retracement M5 en cours", _st_reason(log))
            self.assertEqual(_st_field(log, "retracement"), "EN COURS")
        # sur tout le scénario : jamais de signal à contre-sens d'un contexte H1 + M15 aligné
        for ev, sig, _ in res:
            h1, m15, _, _ = _st_state(m1, ev["i"] + 1)
            if sig and h1 == m15 != 0:
                self.assertEqual(sig["dir"], h1)
        return len(with_ctx), len(against)

    def test_retracement_bullish_pas_de_sell(self):
        m1, res = _StScenarios.bull()
        n_buy, n_sell = self._check(m1, res, 1)
        print(f"\n  retracement bullish : {n_sell} CHoCH SELL + {n_buy} CHoCH BUY pendant M5 baissier -> 0 signal")

    def test_retracement_bearish_pas_de_buy(self):
        m1, res = _StScenarios.bear()
        n_sell, n_buy = self._check(m1, res, -1)
        print(f"\n  retracement bearish : {n_buy} CHoCH BUY + {n_sell} CHoCH SELL pendant M5 haussier -> 0 signal")


class TestContinuation(unittest.TestCase):
    """Contexte aligné + fin de retracement (CHoCH M5 avec sweep/réaction, ou M5 déjà dans le sens) + POI + CHoCH M1."""

    def _accepted(self, paths, side, ctx):
        acc, states = [], Counter()
        for m1, res in paths:
            for ev, sig, log in res:
                if not sig:
                    continue
                acc.append(sig)
                h1, m15, m5, _ = _st_state(m1, ev["i"] + 1)
                self.assertEqual((h1, m15, m5), (ctx, ctx, ctx), "signal accepté hors contexte H1/M15/M5 aligné")
                self.assertEqual(sig["side"], side)
                self.assertEqual(sig["setup"], "CONTINUATION")
                self.assertIsNotNone(sig["poi"])
                self.assertIn("ACCEPTÉ", _st_field(log, "état final"))
                states[_st_field(log, "retracement")] += 1
                for lbl in ("HTF bias", "liquidité ext.", "liquidité int.", "retracement", "CHoCH M5", "trigger M1", "POI"):
                    self.assertIsNotNone(_st_field(log, lbl), f"ligne de log manquante : {lbl}")
        return acc, states

    def test_continuation_buy(self):
        paths = [_StScenarios.bull(s) for s in (1, 2, 3)]
        acc, states = self._accepted(paths, "BUY", 1)
        self.assertGreaterEqual(len(acc), 3)
        self.assertEqual(set(states), {"AUCUN", "TERMINÉ"}, f"attendu : continuation (AUCUN) et fin de retracement (TERMINÉ) : {states}")
        print(f"\n  continuation BUY : {len(acc)} signaux, états retracement {dict(states)}")

    def test_continuation_sell(self):
        paths = [_StScenarios.bear(s) for s in (1, 2, 3)]
        acc, states = self._accepted(paths, "SELL", -1)
        self.assertGreaterEqual(len(acc), 3)
        self.assertTrue(set(states) <= {"AUCUN", "TERMINÉ"}, states)
        print(f"\n  continuation SELL : {len(acc)} signaux, états retracement {dict(states)}")

    def test_pas_de_signal_sans_fin_de_retracement_prouvee(self):
        """CHoCH M5 dans le sens du contexte mais sans sweep ni réaction : le M1 ne suffit pas."""
        seen = 0
        for s in (1, 2, 3):
            m1, res = _StScenarios.bull(s)
            for ev, sig, log in res:
                if _st_field(log, "retracement") == "NON CONFIRMÉ":
                    seen += 1
                    self.assertIsNone(sig)
                    self.assertIn("sans balayage de liquidité interne ni réaction", _st_reason(log))
        self.assertGreater(seen, 0)


class TestInvalidationHTF(unittest.TestCase):
    """Le biais ne change que lorsque H1 ET M15 sont réellement retournés ; entre les deux : aucun trade."""

    def test_invalidation(self):
        (m1, res), switch = _StScenarios.flip()
        by_ctx = Counter()
        first_bear_aligned = None
        for ev, sig, log in res:
            h1, m15, _, _ = _st_state(m1, ev["i"] + 1)
            by_ctx[((h1, m15), bool(sig))] += 1
            if sig:
                self.assertEqual((h1, m15), (sig["dir"], sig["dir"]), "signal accepté sans H1 et M15 alignés dans son sens")
            if h1 != m15:                                   # transition : un seul des deux a cassé -> contexte non confirmé
                self.assertIsNone(sig)
                self.assertIn("indéterminé" if h1 == 0 else "non confirmé", _st_reason(log))
            if (h1, m15) == (-1, -1) and first_bear_aligned is None:
                first_bear_aligned = ev["i"]
        buys = [s for _, s, _ in res if s and s["side"] == "BUY"]
        sells = [s for _, s, _ in res if s and s["side"] == "SELL"]
        self.assertGreater(len(buys), 0, "il doit y avoir des BUY avant l'invalidation")
        self.assertGreater(len(sells), 0, "il doit y avoir des SELL après l'invalidation")
        # chronologie : tous les BUY avant les SELL ; aucun SELL tant que H1 et M15 ne sont pas tous deux baissiers
        self.assertLess(max(s["t"] for s in buys), min(s["t"] for s in sells))
        self.assertGreaterEqual(min(s["t"] for s in sells), _ST_T0 + first_bear_aligned * 60)
        self.assertGreater(first_bear_aligned, switch, "H1 et M15 ne doivent basculer qu'après la cassure de structure")
        print(f"\n  invalidation HTF : {len(buys)} BUY (contexte haussier) puis {len(sells)} SELL (après H1+M15 baissiers) ; "
              f"transition (H1 != M15) : {sum(v for (k, _), v in by_ctx.items() if k[0] != k[1])} CHoCH, 0 signal")


class TestChop(unittest.TestCase):
    """Marché en oscillation : peu de contextes alignés, chaque signal reste conforme, les protections d'origine sont conservées."""

    def test_chop(self):
        m1, res = _StScenarios.chop()
        n = len(res)
        acc = [(ev, sig, log) for ev, sig, log in res if sig]
        reasons = Counter(re.sub(r"[-+]?\d+\.\d+", "X", _st_reason(log) or "")[:40] for _, sig, log in res if not sig)
        for ev, sig, log in acc:
            h1, m15, m5, _ = _st_state(m1, ev["i"] + 1)
            self.assertEqual((h1, m15, m5), (sig["dir"],) * 3)
        self.assertGreater(n, 200)
        self.assertLessEqual(len(acc), 0.06 * n, f"trop de signaux en chop : {len(acc)}/{n}")
        blocked_ctx = sum(v for k, v in reasons.items() if "contre le contexte" in k or "non confirmé" in k)
        self.assertGreater(blocked_ctx, 0.4 * n, "la plupart des CHoCH M1 doivent être stoppés par le contexte")
        print(f"\n  chop : {n} CHoCH M1 -> {len(acc)} signaux ({100 * len(acc) / n:.1f} %) ; "
              f"{blocked_ctx} stoppés par le contexte H1/M15")


class TestM1NeDecidePasSeul(unittest.TestCase):
    def test_m1_seul(self):
        m1 = _StScenarios.bull()[0]
        res = _st_eval(_E, m1, only_m1=True)
        self.assertGreater(len(res), 100)
        self.assertEqual([r for r in res if r[1]], [])
        self.assertTrue(all("indéterminé" in _st_reason(r[2]) for r in res), "sans H1, le M1 ne doit jamais fournir le biais")


class TestLogs(unittest.TestCase):
    def test_lignes_de_log(self):
        m1, res = _StScenarios.bull()
        acc = next(r for r in res if r[1])
        rej = next(r for r in res if not r[1] and _st_field(r[2], "retracement") == "EN COURS")
        for _, _, log in (acc, rej):
            for lbl in ("HTF bias", "liquidité ext.", "liquidité int.", "retracement", "CHoCH M5", "trigger M1", "POI", "état final"):
                self.assertIsNotNone(_st_field(log, lbl), lbl)
        self.assertIn("✅ ACCEPTÉ", _st_field(acc[2], "état final"))
        self.assertIn("⛔ REJETÉ", _st_field(rej[2], "état final"))
        self.assertIn("contexte HAUSSIER", _st_field(rej[2], "HTF bias"))
        print("\n--- log d'un signal accepté ---\n" + acc[2].rstrip() + "\n--- log d'un CHoCH rejeté (retracement M5 en cours) ---\n" + rej[2].rstrip())


class TestEntreeM5(unittest.TestCase):
    """UT d'entrée M5 : l'étape M5 est sautée (M5 est alors le trigger), H1 + M15 restent le contexte."""

    def test_smoke(self):
        m1 = _StScenarios.bull()[0]
        try:
            _E._apply_timeframe(5)
            m5 = _st_agg(m1, 5, limit=10_000)
            _E.get_candles = lambda symbol, minutes=None: _st_agg(m1[:feed["n"]], minutes or 5)
            feed, out = {"n": len(m1)}, []
            for ev in _E.analyze(m5):
                if ev["kind"] != "CHOCH":
                    continue
                feed["n"] = (ev["i"] + 1) * 5
                _E._htf_cache.clear()
                _E._ctx_cache.clear()
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    sig = _E.build_signal(m5[:ev["i"] + 1], ev, _ST_SYM)
                out.append((sig, buf.getvalue()))
            self.assertGreater(len(out), 20)
            self.assertTrue(any("non applicable" in (_st_field(log, "retracement") or "") for _, log in out))
            for sig, _ in out:
                self.assertTrue(sig is None or sig["setup"] == "CONTINUATION")
        finally:
            _E._apply_timeframe(1)


@unittest.skipUnless(_ST_OLD, "_ST_OLD non défini : comparaison avec l'ancien moteur ignorée")
class TestNonRegression(unittest.TestCase):
    def test_poi_reaction_identique(self):
        """Le refactor (_turn_index / _sweep_at) ne doit rien changer à poi_reaction, dans les conditions réelles du moteur :
        CHoCH M1 évalués face aux POI M5 / M15 / H1 (les 4 issues possibles doivent apparaître, sinon le test serait vide)."""
        old = _st_load(_ST_OLD, "engine_old")
        outcomes = Counter()
        for seed in (1, 2, 3):
            m1 = _StScenarios.bull(seed)[0]
            for ev in (e for e in _E.analyze(m1) if e["kind"] == "CHOCH"):
                c = m1[:ev["i"] + 1]
                for tf in (5, 15, 60):
                    for p in _E.find_pois(_st_agg(c, tf), tf, ev["dir"]):
                        got = _E.poi_reaction(c, ev, p)
                        self.assertEqual(got, old.poi_reaction(c, ev, p))
                        outcomes[got] += 1
        self.assertEqual(set(outcomes), {(False, None), (True, None), (True, "rejet du POI"), (True, "balayage de liquidité")})

    def test_nouveau_sous_ensemble_de_l_ancien(self):
        old = _st_load(_ST_OLD, "engine_old2")
        for name, (m1, res) in (("bull", _StScenarios.bull()), ("chop", _StScenarios.chop())):
            res_old = _st_eval(old, m1, tolerate=True)
            crashes = sum(1 for r in res_old if "EXCEPTION" in r[2])
            kn = {r[0]["t"] for r in res if r[1]}
            ko = {r[0]["t"] for r in res_old if r[1]}
            self.assertTrue(kn <= ko, f"{name} : un signal du nouveau moteur n'existe pas dans l'ancien")
            rev = sum(1 for r in res_old if r[1] and r[1]["setup"] == "REVERSAL FROM POI")
            print(f"\n  {name} : ancien {len(ko)} signaux (dont {rev} REVERSAL contre H1, {crashes} CHoCH en exception) -> nouveau {len(kn)}")




def _selftest():
    suite, loader = unittest.TestSuite(), unittest.TestLoader()
    for cls in (TestRetracement, TestContinuation, TestInvalidationHTF, TestChop, TestM1NeDecidePasSeul,
                TestLogs, TestEntreeM5, TestNonRegression):
        suite.addTests(loader.loadTestsFromTestCase(cls))
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(_selftest())
    main()
