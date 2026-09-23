

#!/usr/bin/env python3
"""AlphaBot BOS + CHoCH — Gold & BTC — version en un seul fichier.

Signaux Telegram sur XAUUSD et BTCUSD, timeframe modifiable à tout moment via /timeframe (M1 par défaut, réglable via DEFAULT_TIMEFRAME),
entrée directe sur CHoCH.
Prix : Gold via l'API publique Deriv (WebSocket), BTC via l'API publique Binance. Aucune clé de prix.

Lancer :
    pip install requests python-dotenv matplotlib websocket-client flask metaapi-cloud-sdk
    python main.py

Déploiement Render (Web Service) : le process écoute sur $PORT et expose GET /health,
pendant que la boucle de trading et le polling Telegram tournent en arrière-plan.
Variables utiles : TELEGRAM_TOKEN, CHAT_ID_GROUPE, CHAT_ID_ADMIN, TIMEFRAME,
DEFAULT_RISK_USD, DEFAULT_LEVERAGE, PORT, METAAPI_TOKEN, METAAPI_ACCOUNT_ID.

Le bot tourne en permanence (pas de /start /stop) : scan continu, une analyse à chaque
nouvelle bougie clôturée du timeframe choisi.

Risque : le lot est calculé à partir du risque $ choisi + SL, SANS solde de compte.
Le levier ne sert qu'à afficher une marge indicative (jamais utilisé pour le lot).
Grille de sortie par défaut : paliers RR1/RR2/RR3 notifiés au groupe, BE (SL -> entrée) à
RR2, TP finale à RR4 (réglable : BE depuis Telegram (/be), ou via BE_RR / TP_RR / RR_LEVELS). Aucun PnL $ n'est affiché,
ni au groupe ni en privé Leader — uniquement des R et des taux de réussite par RR.

Réglages : section 1 (CONFIGURATION) ou fichier .env (voir README) — ex. TIMEFRAME=M15.
Le timeframe se change aussi à chaud depuis Telegram (/timeframe) : le dernier choix est mémorisé, sauf si la
variable TIMEFRAME est modifiée sur Render (elle reprend alors la main au démarrage suivant).
RR (RR1-RR4), timeframe d'entrée (M1/M5/M15), filtre HTF (OFF / M15 / COMPLET), niveau du BE et nombre de signaux
simultanés par actif se règlent aussi à chaud depuis le menu
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
import asyncio
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

# UT supplémentaire utilisée UNIQUEMENT comme UT de liquidité (jamais une UT d'entrée) : ne pas fusionner dans
# TF_LABELS, qui est itéré par les boutons /timeframe.
HTF_ONLY_LABELS = {240: "H4"}


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
        "min_sl_pct": 0.0010,       # plancher SL absolu : ~0.10% (ex. ~2.6 pts sur du Gold à 2 600)
        "be_fees_buffer": 0.05,     # défaut BE : SL à entrée +/- 0.05 pt (couvre spread/commission), voir BE_FEES_BUFFER
    },
    "BTCUSD": {
        "source": "binance", "binance_symbol": "BTCUSDT",
        "value_per_point": 1.0,     # $ par point pour 1 lot (1 BTC) - à ajuster selon le broker
        "min_lot": 0.001, "lot_step": 0.001, "decimals": 0,
        "min_sl_pct": 0.0020,       # plancher SL absolu : ~0.20% (ex. ~160 pts sur du BTC à 80 000) -- plus volatile/mèches larges que le Gold
        "be_fees_buffer": 5.0,      # défaut BE : SL à entrée +/- 5 pts (couvre spread/commission), voir BE_FEES_BUFFER
    },
}


def get_be_fees_buffer(symbol):
    """Marge (en points, prix brut) ajoutée au-delà de l'entrée pure lors du déplacement du SL au
    BE, pour couvrir le spread / la commission. Priorité : variable d'env BE_FEES_BUFFER (globale,
    tous symboles) si définie, sinon le défaut propre à SYMBOLS[symbol]["be_fees_buffer"]."""
    env_val = os.getenv("BE_FEES_BUFFER", "").strip()
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            print(f"[be] BE_FEES_BUFFER={env_val!r} invalide (nombre attendu) — défaut symbole utilisé.")
    return SYMBOLS.get(symbol, {}).get("be_fees_buffer", 0.0)


BE_TARGET_USD = _env_float("BE_TARGET_USD", 0.01)   # résultat net minimum visé au BE (0.00 possible)
BE_COMMISSION_USD_PER_LOT = _env_float("BE_COMMISSION_USD_PER_LOT", 0.0)  # commission round-turn/lot std si connue


def _be_price_buffer(symbol, lot):
    """Buffer de prix à ajouter au-delà de l'entrée pour le BE, calculé pour viser un résultat NET
    minimum de BE_TARGET_USD (défaut +0.01$) sur CE lot précis — remplace l'ancien buffer fixe en
    points (0.05 pt), qui se traduisait par une perte nette sur certains lots (frais/commission non
    couverts). buffer_price = (BE_TARGET_USD + commission_estimée) / (value_per_point x lot)."""
    cfg = SYMBOLS.get(symbol, {})
    vpp = cfg.get("value_per_point") or 0.0
    lot = float(lot or 0.0)
    if vpp <= 0 or lot <= 0:
        return get_be_fees_buffer(symbol)   # repli sur l'ancien réglage fixe si lot/valeur inconnus
    commission = BE_COMMISSION_USD_PER_LOT * lot
    return (BE_TARGET_USD + commission) / (vpp * lot)

# --- Stratégie BOS + CHoCH ---------------------------------------------------
SWING_DEPTH = _env_int("SWING_DEPTH", 3)
ATR_PERIOD = 14
ENTRY_CHOCH1 = _env_bool("ENTRY_CHOCH1", True)    # CHoCH qui suit un BOS (retournement classique)
ENTRY_CHOCH2 = _env_bool("ENTRY_CHOCH2", True)    # CHoCH + CHoCH (CHoCH qui suit directement un CHoCH)
DEBUG_CHOCH = _env_bool("DEBUG_CHOCH", True)      # logge chaque CHoCH ignoré (et pourquoi) au lieu de rien dire
HTF_FILTER = _env_bool("HTF_FILTER", True)   # ON : HTF = contexte, POI = zone de réaction, CHoCH = déclencheur (htf_poi_setup)
HTF_MINUTES = _env_int("HTF_MINUTES", 60)    # unité de temps de référence pour la tendance ("liquidité externe")
# Mode du filtre HTF quand il est actif (réglable depuis Telegram : OFF / M15 / COMPLET) :
#   M15  = tendance de fond simple : le CHoCH d'entrée doit aller dans le sens de la dernière cassure de structure de l'UT de
#          référence (HTF_REF_TF : M15 pour une entrée M1/M3/M5, H1 pour M15/M30, H4 pour H1). M15 baissier -> que des SELL.
#   FULL = cascade complète H1 + M15 + M5 + POI (htf_poi_setup).
HTF_MODE = os.getenv("HTF_MODE", "M15").strip().upper()   # mode utilisé quand le filtre est ON et qu'aucun choix Telegram n'existe
if HTF_MODE == "COMPLET":
    HTF_MODE = "FULL"
if HTF_MODE not in ("M15", "FULL"):
    HTF_MODE = "M15"
HTF_MODES = ("OFF", "M15", "FULL")
HTF_REF_TF = {1: 15, 3: 15, 5: 15, 15: 60, 30: 60, 60: 240}   # UT d'entrée -> UT de la tendance de fond (mode M15)
# Cascade du filtre HTF : H1 (biais + liquidité externe) -> M15 (confirmation du contexte) -> M5 (retracement, liquidité interne,
# fin du mouvement) -> UT d'entrée (trigger final uniquement). Seules les UT > UT d'entrée portent des POI.
HTF_CASCADE = tuple(sorted({HTF_MINUTES, 15, 5}, reverse=True))
# UT d'entrée -> UT de liquidité externe par défaut (liquidité interne = l'UT d'entrée elle-même).
# Réglable uniquement pour l'entrée M5, via get_ext_tf() / set_ext_tf_m5() (réglage 'ext_tf_m5' en base).
LIQ_EXT = {1: 15, 3: 15, 5: 60, 15: 240, 30: 240, 60: 240}

# --- Filtre Fibonacci HTF Premium/Discount (indépendant de HTF_MODE / htf_poi_setup) -----------
# use_mtf_fibonacci_pd_filter = true/false : activable/désactivable à tout moment (ENV MTF_FIBONACCI_PD_FILTER,
# ou à chaud via /mtffib sur Telegram -- le choix Telegram est mémorisé en base et prime au redémarrage suivant).
# N'ajoute AUCUNE nouvelle règle de score/OB/FVG : ne fait que filtrer les signaux M1/M5 déjà produits par la
# logique existante, selon la position du prix dans le Fibonacci du swing HTF (mèches High/Low) :
#   tendance HTF haussière -> seuls les BUY autorisés, et seulement si le retracement a atteint >= 50% (Discount)
#   tendance HTF baissière -> seuls les SELL autorisés, et seulement si le retracement a atteint >= 50% (Premium)
# DÉFAUT = OFF (False) : si la base SQLite est perdue (Render sans disque persistant : redémarrage / redéploiement /
# mise en veille), le réglage Telegram disparaît et on retombe ICI -- le filtre ne doit donc JAMAIS se réactiver seul.
# Pour l'activer volontairement de façon permanente : variable Render MTF_FIBONACCI_PD_FILTER=true (ou /mtffib on).
USE_MTF_FIBONACCI_PD_FILTER = _env_bool("MTF_FIBONACCI_PD_FILTER", _env_bool("USE_MTF_FIBONACCI_PD_FILTER", False))
# UT d'entrée -> UT du Fibonacci HTF : M1/M3 -> M15, M5 -> H1, M15 -> H4 (imposé, indépendant de HTF_REF_TF/LIQ_EXT).
MTF_FIB_TF = {1: 15, 3: 15, 5: 60, 15: 240}
MTF_FIB_MIN_RETRACE = 0.5   # niveau minimum (50%) à atteindre pour considérer la zone Premium/Discount comme mitigée

EXT_DEPTH = _env_int("EXT_DEPTH", 5)   # profondeur (bougies de chaque côté) pour confirmer un pivot swing de liquidité externe
EQ_TOL_ATR = 0.1                       # tolérance EQH/EQL : 2 pivots à moins de EQ_TOL_ATR ATR sont considérés au même niveau
POI_MAX_AGE = _env_int("POI_MAX_AGE", 60)                  # âge max d'un POI (OB / FVG), en bougies de sa propre UT
POI_MIN_ATR = _env_float("POI_MIN_ATR", 0.15)              # taille mini d'un POI, en ATR de son UT (écarte les micro-gaps)
SWEEP_LOOKBACK = _env_int("SWEEP_LOOKBACK", 10)            # bougies d'entrée comparées pour détecter un balayage de liquidité
SL_BUFFER_ATR = _env_float("SL_BUFFER_ATR", 0.2)   # buffer au-delà de la ligne du BOS (0.1 laissait le SL pile sur la mèche -> balayé par le bruit)
MIN_SL_ATR = _env_float("MIN_SL_ATR", 0.8)         # SL minimum (en ATR) -- 0.3 donnait des SL de quelques points sur BTC en M1/M5, balayés par le spread/bruit
MAX_SL_ATR = _env_float("MAX_SL_ATR", 6.0)         # au-delà : signal ignoré (BOS trop ancien)
# Plancher ABSOLU du SL, en % du prix d'entrée : filet de sécurité indépendant de l'ATR (l'ATR d'une UT basse comme M1
# peut lui-même être minuscule en marché calme -> MIN_SL_ATR seul ne suffit pas à empêcher un SL de quelques points).
# Réglable par actif ci-dessous (SYMBOLS[...]["min_sl_pct"]), sinon valeur par défaut MIN_SL_PCT.
MIN_SL_PCT = _env_float("MIN_SL_PCT", 0.0015)      # 0.15% par défaut (ex. ~120 pts sur du BTC à 80 000)
BE_RR = _env_float("BE_RR", 2.0)          # RR auquel le SL est déplacé à l'entrée (BE) : valeur de départ, ensuite /be sur Telegram
BE_RR_CHOICES = (0.5, 1, 1.5, 2, 3)       # boutons du menu BE (n'importe quelle valeur > 0 via /be 0.75)
TP_RR = _env_float("TP_RR", 4.0)          # RR de la TP finale (clôture complète, pas de TP1/TP2)
RR_LEVELS = (1.0, 2.0, 3.0)                # paliers intermédiaires notifiés au groupe (hors TP finale)
# ⚙️ PARAMÈTRES SIGNAL (menu Telegram) : TP_RR, HTF_FILTER et le timeframe ci-dessus ne sont que les valeurs de
# DÉPART ; le choix fait sur Telegram est mémorisé en base (table settings) et prime après chaque redémarrage.
SIGNAL_RR_CHOICES = (1, 2, 3, 4)     # boutons « RR1 | RR2 | RR3 | RR4 »
SIGNAL_TF_CHOICES = (1, 5, 15)       # boutons « M1 | M5 | M15 » (timeframe du déclenchement final)
MAX_POSITIONS = _env_int("MAX_POSITIONS", 1)     # signaux ouverts max par actif (1 = aucun nouveau signal tant que le précédent n'est pas clôturé) ; ensuite /maxpos
MAX_POSITIONS_CHOICES = (1, 2, 3)               # boutons du menu « signaux simultanés »

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
# Garde-fou spread (trading manuel uniquement) : refuse l'ordre si le spread au moment du clic
# dépasse ce % du prix — vise les moments anormaux (news, ouverture de session, faible liquidité),
# pas le scalping M1 normal (le spread y reste d'habitude bien sous ce seuil). Réglable par env ;
# 0 désactive complètement le contrôle (l'ordre part quel que soit le spread).
MAX_SPREAD_PCT = _env_float("MAX_SPREAD_PCT", 0.0015)   # 0.15% par défaut (ex. ~6.4 pts sur Gold à 4290)
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

# --- MetaApi (exécution MT5) ------------------------------------------------
# Remplace les deux valeurs ci-dessous par les tiennes (dashboard MetaApi.cloud), ou laisse-les
# vides et utilise les variables d'environnement METAAPI_TOKEN / METAAPI_ACCOUNT_ID (Render / .env) —
# la variable d'environnement, si définie, garde toujours la priorité sur ces valeurs par défaut.
# SÉCURITÉ : ne jamais coder le token / account ID en dur ici (risque de fuite si le fichier est
# partagé, uploadé ou versionné). Utilise exclusivement les variables d'environnement Render/.env.
_DEFAULT_METAAPI_TOKEN = ""        # <-- ne pas coller de token ici, utiliser METAAPI_TOKEN (env)
_DEFAULT_METAAPI_ACCOUNT_ID = ""   # <-- ne pas coller d'account id ici, utiliser METAAPI_ACCOUNT_ID (env)
METAAPI_TOKEN = os.getenv("METAAPI_TOKEN", "") or _DEFAULT_METAAPI_TOKEN
METAAPI_ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "") or _DEFAULT_METAAPI_ACCOUNT_ID
# Mappe les symboles internes (XAUUSD, BTCUSD) vers ceux du broker si différents,
# ex. MT5_SYMBOL_MAP={"XAUUSD":"XAUUSD.m","BTCUSD":"BTCUSDm"}. Vide -> pas de mapping (identité).
try:
    MT5_SYMBOL_MAP = json.loads(os.getenv("MT5_SYMBOL_MAP", "") or '{"XAUUSD":"XAUUSDm","BTCUSD":"BTCUSDm"}')
except Exception:
    print("[metaapi] MT5_SYMBOL_MAP invalide (JSON attendu) — mapping ignoré.")
    MT5_SYMBOL_MAP = {"XAUUSD": "XAUUSDm", "BTCUSD": "BTCUSDm"}

# Tolérance de slippage (en points de prix, prix brut) transmise à MetaApi pour chaque ordre
# MARKET : au-delà de cet écart entre le prix demandé et le prix d'exécution, le broker (Exness)
# rejette l'ordre plutôt que de l'exécuter à un prix trop éloigné. Réglable par variable d'env
# MT5_SLIPPAGE_XAUUSD / MT5_SLIPPAGE_BTCUSD (sinon défaut ci-dessous par actif).
MT5_SLIPPAGE_DEFAULT = {"XAUUSD": 0.5, "BTCUSD": 30.0}


def get_mt5_slippage(symbol):
    env_val = os.getenv(f"MT5_SLIPPAGE_{symbol}", "").strip()
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            print(f"[metaapi] MT5_SLIPPAGE_{symbol}={env_val!r} invalide (nombre attendu) — défaut utilisé.")
    return MT5_SLIPPAGE_DEFAULT.get(symbol, 1.0)


# Écart (en % du prix) entre sig["entry"] (prix Deriv/Binance) et le prix d'ouverture réel côté
# Exness au-delà duquel l'admin est notifié (l'ordre reste ouvert : seule une alerte est envoyée).
MT5_PRICE_GAP_ALERT_PCT = {"XAUUSD": 0.0015, "BTCUSD": 0.0005}


# ============================================================================
# 1bis. METAAPI — connexion au compte MT5 (exécution réelle des ordres)
#
# Connexion RPC vers MetaApi.cloud, utilisée par l'exécution d'ordres (section
# suivante). Ne bloque jamais le process principal : si METAAPI_TOKEN /
# METAAPI_ACCOUNT_ID sont absents ou que la connexion échoue, on logge et on
# continue (le bot reste fonctionnel en mode "signal only").
# ============================================================================

try:
    from metaapi_cloud_sdk import MetaApi
except ImportError:  # metaapi-cloud-sdk optionnel : absent -> pas d'exécution MT5
    MetaApi = None

_metaapi_instance = None
_metaapi_connection = None
_metaapi_connected_at = None   # timestamp (time.time()) de la dernière (re)synchronisation réussie
_metaapi_lock = asyncio.Lock()   # verrou asyncio (une seule boucle MT5 partagée, voir _run_mt5)

# --- UNE SEULE boucle asyncio permanente pour tout MetaApi ---------------------------------------
# Correctif : avant, chaque appel faisait asyncio.run(...), qui crée puis FERME une boucle. Les
# tâches internes du SDK (websocket, abonnements) mouraient à la fermeture et la connexion restait
# liée à une boucle morte. Désormais toutes les coroutines MetaApi tournent sur la même boucle.
_mt5_loop = asyncio.new_event_loop()
threading.Thread(target=_mt5_loop.run_forever, daemon=True, name="mt5-loop").start()


def _run_mt5(coro, timeout=None):
    """Exécute une coroutine MetaApi sur la boucle permanente et attend le résultat (appel bloquant)."""
    return asyncio.run_coroutine_threadsafe(coro, _mt5_loop).result(timeout)


async def _rpc_alive_async(connection):
    """Test de vie réel : une connexion RPC n'a pas de terminal_state, on interroge le compte."""
    try:
        await asyncio.wait_for(connection.get_account_information(), timeout=15)
        return True
    except Exception:
        return False

# Délai minimum (s) à laisser au terminal MT5 après "wait_synchronized" avant d'autoriser un
# trade : le flag "synchronized" peut être vrai avant que le cache des specs de symboles (stops
# level, filling modes...) soit entièrement peuplé. Réglable via MT5_SYNC_WAIT_SEC.
MT5_SYNC_WAIT_SEC = _env_float("MT5_SYNC_WAIT_SEC", 3.0)

# Nombre de tentatives de connexion/synchronisation MetaApi avant d'abandonner : un
# wait_synchronized() qui timeout ne veut pas dire que le compte n'est pas Connected/Deployed
# côté dashboard MetaApi — on retente proprement (nouvelle connexion RPC à chaque tentative)
# plutôt que d'abandonner sur le 1er timeout. Réglable via MT5_SYNC_RETRIES.
MT5_SYNC_RETRIES = max(1, _env_int("MT5_SYNC_RETRIES", 3))
# Timeout (s) passé à wait_synchronized() à chaque tentative. Réglable via MT5_SYNC_TIMEOUT_SEC.
MT5_SYNC_TIMEOUT_SEC = _env_float("MT5_SYNC_TIMEOUT_SEC", 60.0)
# Backoff (s) entre deux tentatives de (re)connexion, multiplié par le numéro de la tentative
# (1re relance après MT5_RECONNECT_BACKOFF_SEC, 2e après 2x, ...). Réglable via MT5_RECONNECT_BACKOFF_SEC.
MT5_RECONNECT_BACKOFF_SEC = _env_float("MT5_RECONNECT_BACKOFF_SEC", 2.0)

# Marge de sécurité appliquée au-delà du stopsLevel minimum imposé par le broker, pour ne pas
# coller pile au seuil (le prix peut légèrement bouger entre le calcul du signal et l'envoi de
# l'ordre). Réglable via MT5_STOP_BUFFER (1.0 = pas de marge, 1.2 = +20%).
MT5_STOP_BUFFER = _env_float("MT5_STOP_BUFFER", 1.0)   # 1.0 = pas de marge ajoutée par défaut (diagnostic d'abord)


# --- Connexion MetaApi PERSISTANTE en arrière-plan ---------------------------------------------
# Problème résolu ici : avant, la connexion/synchronisation MetaApi (jusqu'à 3 tentatives x 60s =
# ~3 minutes) se déclenchait AU MOMENT du signal, ce qui est bien trop lent pour un timeframe M1 et
# bloque l'envoi du signal. Désormais : un thread d'arrière-plan connecte et resynchronise MetaApi
# en continu dès le démarrage du bot, indépendamment des signaux. Au moment d'un trade, on ne fait
# QUE consulter l'état courant (prêt / pas prêt) — jamais de nouvelle attente de synchronisation.
_metaapi_ready = threading.Event()          # set() <=> connexion RPC connectée ET synchronisée, prête à trader
MT5_BG_RETRY_SEC = _env_float("MT5_BG_RETRY_SEC", 15.0)   # intervalle entre 2 tentatives de reconnexion en arrière-plan
MT5_BG_HEALTHCHECK_SEC = _env_float("MT5_BG_HEALTHCHECK_SEC", 10.0)  # intervalle de vérif santé une fois connecté


# ============================================================================
# MODE REST (par défaut) : plus AUCUN websocket vers MetaApi.
#
# Le SDK (metaapi_cloud_sdk) ouvre un websocket socket.io permanent + attend « wait_synchronized ». Depuis Render ce
# websocket échoue en boucle (« socket client failed to connect to the server », « Failed to subscribe »), même quand
# le compte est Connected/Deployed côté MetaApi : c'est le TRANSPORT qui tombe, pas le compte ni le broker.
# En mode REST, chaque action (ordre, positions, specs, BE, deals) est un simple appel HTTPS vers l'API REST MetaApi
# (mt-client-api-v1.<region>.agiliumtrade.ai). Rien à connecter, rien à synchroniser côté bot, donc plus de time-out
# de synchronisation : l'ordre part directement au moment du signal.
# METAAPI_MODE=sdk remet l'ancien fonctionnement (websocket) ; METAAPI_REGION (défaut : auto puis london) et
# METAAPI_REST_URL (URL complète, prioritaire) permettent de forcer l'hôte.
# ============================================================================
METAAPI_MODE = os.getenv("METAAPI_MODE", "rest").strip().lower()
METAAPI_REST_TIMEOUT_SEC = _env_float("METAAPI_REST_TIMEOUT_SEC", 25.0)
_MT5_TRADE_OK_CODES = {10008, 10009, 10010}   # PLACED / DONE / DONE_PARTIAL
_rest_base_cache = None

# 13. Codes d'erreur définitifs MT5 : jamais de retentative, juste une explication en français.
_DEFINITIVE_ERROR_HINTS = {
    "INVALID_STOPS": "SL/TP invalide : trop proche du prix (stopsLevel/freezeLevel du broker) "
                      "ou stop du mauvais côté du prix.",
    "INVALID_VOLUME": "Volume (lot) hors des bornes volumeMin/volumeMax/volumeStep réelles du broker.",
    "MARKET_CLOSED": "Marché fermé pour ce symbole à cet instant (hors horaires de trading broker).",
    "TRADE_DISABLED": "Trading désactivé pour ce symbole ou ce compte côté broker (tradeMode).",
    "NO_MONEY": "Marge libre insuffisante sur le compte pour ouvrir cette position.",
}
# INVALID_FILL n'est PAS dans cette liste : c'est le seul cas où une retentative automatique avec
# un autre filling mode a un sens (voir _execute_mt5_order_async) — tous les autres codes ci-dessus
# sont définitifs et ne doivent jamais déclencher de nouvel envoi.


def _classify_trade_error(msg):
    """13. Reconnaît un code d'erreur MT5 DÉFINITIF (hors INVALID_FILL) dans un message MetaApi ->
    (code, explication_fr), ou (None, None) si aucun code définitif connu (dans ce cas seul un cas
    précis, INVALID_FILL, peut justifier une retentative corrigée — voir l'appelant)."""
    msg_u = str(msg or "").upper()
    for code, explication in _DEFINITIVE_ERROR_HINTS.items():
        if code in msg_u:
            return code, explication
    return None, None


class MetaApiUnknownRetcodeError(RuntimeError):
    """14. TRADE_RETCODE_UNKNOWN / numericCode=-1 : le broker a renvoyé un code que MetaApi ne
    reconnaît pas dans sa table. Ce n'est PAS une erreur "essayons encore" : c'est un résultat
    INCERTAIN — l'ordre a pu passer malgré la réponse UNKNOWN. NE JAMAIS renvoyer automatiquement
    la même requête ni en tenter une variante : l'appelant doit seulement vérifier si une position
    existe déjà (anti-doublon), logguer un diagnostic complet, puis relever l'exception telle
    quelle. Porte la réponse brute + le payload envoyé pour ce diagnostic."""

    def __init__(self, raw_response, sent_body):
        self.raw_response = raw_response
        self.sent_body = sent_body
        super().__init__(
            f"TRADE_RETCODE_UNKNOWN (numericCode=-1) — réponse brute MetaApi={raw_response!r} | "
            f"payload envoyé={json.dumps(sent_body, default=str)}")


def _metaapi_rest_base():
    """URL de base de l'API REST : METAAPI_REST_URL, sinon METAAPI_REGION, sinon région lue sur le compte
    (API de provisioning), sinon london (région vue dans les logs)."""
    global _rest_base_cache
    if _rest_base_cache:
        return _rest_base_cache
    url = os.getenv("METAAPI_REST_URL", "").strip().rstrip("/")
    if not url:
        region = os.getenv("METAAPI_REGION", "").strip().lower()
        if not region:
            try:
                r = requests.get("https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts/"
                                 + METAAPI_ACCOUNT_ID, headers={"auth-token": METAAPI_TOKEN}, timeout=15)
                region = str((r.json() or {}).get("region") or "").strip().lower() if r.ok else ""
            except Exception as e:
                print(f"[metaapi-rest] région du compte non lue ({e}) — london par défaut.")
        region = region or "london"
        url = f"https://mt-client-api-v1.{region}.agiliumtrade.ai"
    _rest_base_cache = url
    print(f"[metaapi-rest] API REST : {url}")
    return url


class _MetaApiRestConnection:
    """Même interface que la connexion RPC du SDK (get_positions, create_market_*_order, modify_position, ...) mais en
    HTTPS pur : le reste du bot n'a rien à changer. Chaque appel est indépendant (aucun état de connexion)."""

    def _call(self, method, path, body=None):
        url = f"{_metaapi_rest_base()}/users/current/accounts/{METAAPI_ACCOUNT_ID}{path}"
        if path == "/trade" and isinstance(body, dict):
            # Log de diagnostic temporaire (aucun secret : ni token ni account ID ici) — permet de
            # voir EXACTEMENT ce qui part sur le fil avant tout envoi, sans deviner.
            types = {k: type(v).__name__ for k, v in body.items()}
            print(f"[metaapi-trade] -> {method} {path} | payload={json.dumps(body, default=str)} | types={types}")
        r = requests.request(method, url, json=body, timeout=METAAPI_REST_TIMEOUT_SEC,
                             headers={"auth-token": METAAPI_TOKEN, "Accept": "application/json"})
        if path == "/trade":
            print(f"[metaapi-trade] <- HTTP {r.status_code} | raw={r.text[:2000]}")
        try:
            data = r.json()
        except ValueError:
            data = None
        if r.status_code >= 400:
            if isinstance(data, dict):
                details = data.get("details")
                base = data.get("message") or data.get("stringCode") or str(data)[:500]
                msg = f"{base} — details: {details}" if details else base
            else:
                msg = r.text[:500]
            raise RuntimeError(f"MetaApi REST {method} {path} -> HTTP {r.status_code} : {msg}")
        return data

    async def _acall(self, method, path, body=None):
        return await asyncio.get_running_loop().run_in_executor(None, lambda: self._call(method, path, body))

    async def get_account_information(self):
        return await self._acall("GET", "/account-information")

    async def get_symbol_specification(self, symbol):
        return await self._acall("GET", f"/symbols/{symbol}/specification")

    async def get_positions(self):
        return await self._acall("GET", "/positions") or []

    async def get_deals_by_position(self, position_id):
        return await self._acall("GET", f"/history-deals/position/{position_id}")

    async def _trade(self, body):
        res = await self._acall("POST", "/trade", body)
        code = res.get("numericCode") if isinstance(res, dict) else None
        if code is not None and code not in _MT5_TRADE_OK_CODES:
            string_code = res.get("stringCode") if isinstance(res, dict) else None
            message = res.get("message") if isinstance(res, dict) else None
            # 8/14. UNKNOWN : jamais un message générique, jamais une retentative aveugle ensuite —
            # exception dédiée traitée par _execute_mt5_order_async (vérif anti-doublon, jamais de retry).
            if code == -1 or str(string_code or "").upper() == "TRADE_RETCODE_UNKNOWN":
                raise MetaApiUnknownRetcodeError(res, body)
            raise RuntimeError(f"ordre refusé par le broker : {string_code} — {message} "
                                f"(numericCode={code}) | payload envoyé={json.dumps(body, default=str)}")
        return res

    async def _market(self, action, symbol, volume, sl, tp, options):
        o = dict(options or {})
        # Normalisation stricte des nombres : jamais de string/None/NaN/inf envoyé à MetaApi.
        volume = float(volume)
        if not math.isfinite(volume) or volume <= 0:
            raise ValueError(f"volume invalide avant envoi MetaApi : {volume!r}")
        body = {"actionType": action, "symbol": str(symbol), "volume": volume}
        if sl:
            sl = float(sl)
            if math.isfinite(sl) and sl > 0:
                body["stopLoss"] = sl
        if tp:
            tp = float(tp)
            if math.isfinite(tp) and tp > 0:
                body["takeProfit"] = tp
        for k in ("comment", "clientId", "slippage"):
            if o.get(k) is not None:
                body[k] = o[k]
        # 1. Ne plus forcer systématiquement ORDER_FILLING_IOC : "fillingModes" n'est envoyé QUE si
        # un ou plusieurs modes réellement supportés ont été déterminés depuis les specs du symbole
        # (voir _pick_filling_modes) — sinon on omet la clé et MetaApi/le broker choisit lui-même.
        modes = o.get("fillingModes")
        if modes:
            body["fillingModes"] = list(modes)
        return await self._trade(body)

    async def create_market_buy_order(self, symbol, volume, stop_loss=None, take_profit=None, options=None):
        return await self._market("ORDER_TYPE_BUY", symbol, volume, stop_loss, take_profit, options)

    async def create_market_sell_order(self, symbol, volume, stop_loss=None, take_profit=None, options=None):
        return await self._market("ORDER_TYPE_SELL", symbol, volume, stop_loss, take_profit, options)

    async def modify_position(self, position_id, stop_loss=None, take_profit=None):
        body = {"actionType": "POSITION_MODIFY", "positionId": str(position_id)}
        if stop_loss is not None:
            body["stopLoss"] = stop_loss
        if take_profit is not None:
            body["takeProfit"] = take_profit
        return await self._trade(body)

    async def close_position(self, position_id):
        """Ferme intégralement une position ouverte (POSITION_CLOSE_ID) — utilisé par le mode
        trading manuel Telegram (bouton 🛑 Fermer)."""
        body = {"actionType": "POSITION_CLOSE_ID", "positionId": str(position_id)}
        return await self._trade(body)

    async def get_symbol_price(self, symbol):
        """Prix bid/ask réel actuellement coté côté broker (endpoint /current-price MetaApi) —
        utilisé par le mode trading manuel pour ne jamais calculer un lot/SL/TP sur un prix Deriv/
        Binance qui diffère de celui où l'ordre part réellement."""
        return await self._acall("GET", f"/symbols/{symbol}/current-price")

    async def close(self):
        return None


_rest_connection = _MetaApiRestConnection()


def _notify_mt5_recovered(since_ts):
    """MetaApi de nouveau joignable : prévient l'admin et liste les signaux publiés pendant la panne, dont l'ordre
    n'a PAS été exécuté (exec_status FAILED, sans ticket MT5) -- à ouvrir/ignorer à la main."""
    try:
        rows = _q("SELECT id, symbol, side, kind FROM trades WHERE exec_status='FAILED' AND opened_ts>=? "
                  "AND (mt5_position_id IS NULL OR mt5_position_id='') ORDER BY id", (since_ts,)).fetchall()
        txt = "✅ Exécution MT5 réactivée (MetaApi de nouveau joignable)."
        if rows:
            txt += "\n❌ Signaux détectés pendant la panne, ordre NON exécuté :\n" + "\n".join(
                f"• #{r['id']} {r['symbol']} {r['side']} {r['kind']}" for r in rows)
        to_admin(txt)
    except Exception as e:
        print(f"[metaapi-rest] notification de reprise impossible : {e}")


def _metaapi_rest_background_loop():
    """Mode REST : simple témoin de santé (aucune connexion à maintenir). Ne sert qu'à l'affichage « prêt / pas
    prêt » : les ordres, eux, partent TOUJOURS directement (voir get_ready_connection). 3 échecs de suite pour
    passer en « pas prêt », un seul succès pour repasser en « prêt » ; logs uniquement aux changements d'état."""
    print("[metaapi-rest] Mode REST actif (aucun websocket, aucune synchronisation à attendre).")
    fails = 0
    down_since = None   # début de la dernière panne (None = pas de panne en cours)
    while True:
        try:
            _run_mt5(_rest_connection.get_account_information(), timeout=40)
            fails = 0
            if not _metaapi_ready.is_set():
                _metaapi_ready.set()
                print("[metaapi-rest] ✅ API REST MetaApi joignable — ordres exécutables immédiatement.")
                if down_since is not None:
                    _notify_mt5_recovered(down_since)
                    down_since = None
        except Exception as e:
            fails += 1
            if fails == 3 and _metaapi_ready.is_set():
                _metaapi_ready.clear()
                down_since = now_ts()
                print(f"[metaapi-rest] ⚠️ API REST injoignable 3 fois de suite ({e}).")
                try:
                    to_admin("⚠️ Exécution MT5 INDISPONIBLE (API MetaApi injoignable). Les signaux restent analysés et "
                             "publiés, mais leurs ordres ne seront PAS exécutés tant que MetaApi n'est pas revenu.")
                except Exception:
                    pass
            elif fails == 1 and not _metaapi_ready.is_set():
                print(f"[metaapi-rest] API REST : {e}")
        time.sleep(max(MT5_BG_HEALTHCHECK_SEC, 30.0))


def get_ready_connection():
    """Chemin RAPIDE utilisé au moment d'un trade : ne connecte JAMAIS et n'attend JAMAIS —
    retourne la connexion courante si le thread d'arrière-plan la considère prête, sinon None
    immédiatement (aucun délai de plusieurs dizaines de secondes au moment du signal)."""
    if METAAPI_MODE == "rest":
        return _rest_connection if (METAAPI_TOKEN and METAAPI_ACCOUNT_ID) else None   # stateless : on tente toujours
    if not _metaapi_ready.is_set():
        return None
    return _metaapi_connection


def _metaapi_background_loop():
    """Boucle d'arrière-plan (thread daemon dédié) : connecte/synchronise MetaApi une bonne fois
    au démarrage, puis surveille la connexion en continu et la ré-établit sans jamais bloquer le
    reste du bot (signal M1, Telegram...). C'est la SEULE fonction qui appelle
    get_metaapi_connection() avec ses tentatives/backoff ; le reste du code ne fait que lire
    _metaapi_ready via get_ready_connection()."""
    if METAAPI_MODE == "rest":
        if METAAPI_TOKEN and METAAPI_ACCOUNT_ID:
            _metaapi_rest_background_loop()
        return
    if MetaApi is None or not METAAPI_TOKEN or not METAAPI_ACCOUNT_ID:
        return  # exécution MT5 désactivée : rien à faire en arrière-plan
    print("[metaapi] Thread de connexion persistante démarré.")
    while True:
        try:
            connection = _run_mt5(get_metaapi_connection())
        except Exception as e:
            connection = None
            print(f"[metaapi] Boucle d'arrière-plan : erreur inattendue ({e}).")
        if connection is not None:
            _metaapi_ready.set()
            print("[metaapi] ✅ MetaApi PRÊT (connecté + synchronisé) — signaux exécutables immédiatement.")
            # Une fois prêt, on surveille juste la santé de la connexion (pas de nouvelle
            # tentative bloquante tant qu'elle reste connectée) et on la refait sans attendre
            # dès qu'elle tombe, toujours en arrière-plan.
            while True:
                time.sleep(MT5_BG_HEALTHCHECK_SEC)
                try:
                    still_ok = _run_mt5(_rpc_alive_async(connection), timeout=30)
                except Exception:
                    still_ok = False
                if not still_ok:
                    _metaapi_ready.clear()
                    print("[metaapi] ⚠️ Connexion MetaApi perdue — reconnexion en arrière-plan...")
                    break
        else:
            _metaapi_ready.clear()
            print(f"[metaapi] MetaApi non prêt — nouvelle tentative dans {MT5_BG_RETRY_SEC:.0f}s (en arrière-plan, "
                  f"n'affecte pas les signaux Telegram).")
            time.sleep(MT5_BG_RETRY_SEC)


def start_metaapi_background():
    """À appeler une fois au démarrage du bot (main()) : lance la connexion MetaApi persistante
    dans un thread daemon séparé. Ne bloque jamais le démarrage du bot."""
    threading.Thread(target=_metaapi_background_loop, daemon=True, name="metaapi-bg").start()


class SLTooCloseError(Exception):
    """Levée quand le SL (ou le TP) calculé par le moteur de signal est plus proche du prix
    d'entrée que le stopsLevel minimum imposé par le broker pour ce symbole. Ne doit JAMAIS
    entraîner un déplacement automatique du SL : l'appelant doit rejeter le signal tel quel."""

    def __init__(self, symbol, broker_symbol, which, required, actual):
        self.symbol, self.broker_symbol, self.which = symbol, broker_symbol, which
        self.required, self.actual = required, actual
        super().__init__(
            f"{which} trop proche pour {broker_symbol} : requis >= {required}, calculé {actual}")


async def _close_stale_connection(connection):
    """Ferme proprement une connexion RPC devenue inutilisable avant d'en recréer une —
    best-effort : une erreur ici ne doit jamais empêcher la tentative de reconnexion suivante."""
    if connection is None:
        return
    try:
        await connection.close()
    except Exception:
        pass


async def get_metaapi_connection(force_reconnect=False):
    """Retourne une connexion RPC MetaApi connectée et synchronisée, ou None si indisponible.

    Réutilise la connexion existante si elle est encore active (connexion RPC persistante :
    jamais recréée à chaque ordre). Si elle est absente/caduque, ou si `force_reconnect=True`,
    tente jusqu'à MT5_SYNC_RETRIES connexions propres (nouvelle connexion RPC à chaque tentative,
    avec backoff croissant) : un `wait_synchronized()` qui timeout ne signifie pas forcément que
    le compte n'est pas Connected/Deployed côté dashboard MetaApi, donc on ne renonce jamais sur
    un seul échec. Ne lève jamais d'exception vers l'appelant : toute erreur est logguée et None
    est retourné, pour ne jamais interrompre la boucle de trading / l'envoi des signaux Telegram.
    """
    global _metaapi_instance, _metaapi_connection, _metaapi_connected_at

    if METAAPI_MODE == "rest":   # HTTPS pur : rien à connecter ni à synchroniser
        return _rest_connection if (METAAPI_TOKEN and METAAPI_ACCOUNT_ID) else None

    if MetaApi is None:
        print("[metaapi] SDK non installé (pip install metaapi-cloud-sdk) — exécution MT5 désactivée.")
        return None
    if not METAAPI_TOKEN or not METAAPI_ACCOUNT_ID:
        print("[metaapi] METAAPI_TOKEN / METAAPI_ACCOUNT_ID absents — exécution MT5 désactivée.")
        return None

    async with _metaapi_lock:
        if not force_reconnect and _metaapi_connection is not None:
            if await _rpc_alive_async(_metaapi_connection):
                return _metaapi_connection  # connexion persistante réutilisée telle quelle
            # connexion caduque -> on retente une connexion propre ci-dessous

        stale = _metaapi_connection
        _metaapi_connection = None
        _metaapi_connected_at = None
        await _close_stale_connection(stale)

        last_err = None
        for attempt in range(1, MT5_SYNC_RETRIES + 1):
            connection = None
            try:
                if _metaapi_instance is None:
                    _metaapi_instance = MetaApi(METAAPI_TOKEN)

                account = await _metaapi_instance.metatrader_account_api.get_account(METAAPI_ACCOUNT_ID)

                if account.state not in ("DEPLOYED",):
                    print(f"[metaapi] Déploiement du compte {METAAPI_ACCOUNT_ID}...")
                    await account.deploy()

                print(f"[metaapi] Connexion au compte {METAAPI_ACCOUNT_ID} "
                      f"(tentative {attempt}/{MT5_SYNC_RETRIES})...")
                await account.wait_connected()

                connection = account.get_rpc_connection()
                await connection.connect()
                await connection.wait_synchronized(timeout_in_seconds=MT5_SYNC_TIMEOUT_SEC)

                _metaapi_connection = connection
                _metaapi_connected_at = time.time()
                print(f"[metaapi] Connecté et synchronisé (tentative {attempt}/{MT5_SYNC_RETRIES}).")
                return _metaapi_connection

            except Exception as e:
                last_err = e
                print(f"[metaapi] Échec connexion/synchronisation (tentative {attempt}/{MT5_SYNC_RETRIES}) : {e}")
                await _close_stale_connection(connection)
                _metaapi_connection = None
                _metaapi_connected_at = None
                if attempt < MT5_SYNC_RETRIES:
                    await asyncio.sleep(MT5_RECONNECT_BACKOFF_SEC * attempt)

        print(f"[metaapi] Connexion abandonnée après {MT5_SYNC_RETRIES} tentatives : {last_err}")
        traceback.print_exc()
        return None


async def _ensure_terminal_ready():
    """Chemin utilisé au moment d'un trade (signal M1) : ne connecte/synchronise JAMAIS ici — la
    connexion persistante est maintenue en continu par le thread d'arrière-plan
    (_metaapi_background_loop / start_metaapi_background). On se contente de lire l'état courant :
    si MetaApi n'est pas prêt (pas encore synchronisé, ou connexion tombée), on retourne None
    IMMÉDIATEMENT (aucune attente de plusieurs dizaines de secondes / minutes sur un signal M1)."""
    connection = get_ready_connection()
    if connection is None:
        return None
    if _metaapi_connected_at is not None:
        remaining = MT5_SYNC_WAIT_SEC - (time.time() - _metaapi_connected_at)
        if remaining > 0:
            await asyncio.sleep(remaining)
    return connection


def _spec_get(spec, *keys, default=None):
    """Lit la 1re clé présente dans `spec` parmi `keys` — les SDK/versions MetaApi n'exposent pas
    toujours les mêmes noms de champs pour les specs de symbole."""
    for k in keys:
        try:
            if isinstance(spec, dict) and k in spec and spec[k] is not None:
                return spec[k]
            v = getattr(spec, k, None)
            if v is not None:
                return v
        except Exception:
            pass
    return default


async def _get_symbol_spec(connection, broker_symbol):
    """2. Étape « specs symbole » : récupère les specs MT5 (stopsLevel, point, filling modes...)
    juste avant l'envoi. Retourne None si indisponible (l'appelant devient alors permissif sur la
    vérif de distance plutôt que de bloquer tous les trades sur un souci d'API)."""
    try:
        return await connection.get_symbol_specification(broker_symbol)
    except Exception as e:
        print(f"[metaapi] Impossible de récupérer les specs de {broker_symbol} : {e} — vérif stop-level ignorée.")
        return None


def _normalize_volume(spec, lot):
    """Arrondit/clampe le volume sur les VRAIES specs du broker (volumeStep/minVolume/maxVolume
    renvoyées par MetaApi pour ce symbole), pas seulement sur SYMBOLS[...]['lot_step'] (config
    statique côté bot qui peut différer du broker réel) — cause plausible d'un 'Validation failed'
    si le pas/min/max réel du compte Exness diffère de ce qui est codé en dur dans SYMBOLS.
    Retourne le lot tel quel si les specs ne sont pas disponibles (jamais bloquant par excès)."""
    if not spec:
        return lot
    step = _spec_get(spec, "volumeStep", "stepVolume", "volume_step", default=None)
    vmin = _spec_get(spec, "minVolume", "volumeMin", "min_volume", default=None)
    vmax = _spec_get(spec, "maxVolume", "volumeMax", "max_volume", default=None)
    out = float(lot)
    if step:
        step = float(step)
        out = round(math.floor(out / step + 1e-9) * step, 8)
    if vmin is not None and out < float(vmin):
        out = float(vmin)
    if vmax is not None and out > float(vmax):
        out = float(vmax)
    if out != lot:
        print(f"[metaapi] Volume ajusté aux specs réelles du broker : {lot} -> {out} "
              f"(step={step}, min={vmin}, max={vmax})")
    return out


def _min_stop_distance(spec, symbol):
    """6. Distance minimale (prix brut) entre l'entrée et un SL/TP, d'après MAX(stopsLevel,
    freezeLevel) (en points) x la taille du point, avec la marge MT5_STOP_BUFFER (1.0 par défaut =
    aucune marge ajoutée, réglable via env si besoin). Retourne 0.0 si les specs ne permettent pas
    de calculer ce seuil (vérif alors ignorée, jamais bloquante par excès de prudence)."""
    stops_level = _spec_get(spec, "stopsLevel", "tradeStopsLevel", "stops_level", default=0) or 0
    freeze_level = _spec_get(spec, "freezeLevel", "tradeFreezeLevel", "freeze_level", default=0) or 0
    level = max(float(stops_level), float(freeze_level))
    if not level:
        return 0.0
    dec = SYMBOLS.get(symbol, {}).get("decimals", 2)
    point = _spec_get(spec, "point", "tickSize", "tick_size", default=None)
    if not point:
        digits = _spec_get(spec, "digits", default=dec)
        point = 10 ** (-digits)
    return level * float(point) * MT5_STOP_BUFFER


def _pick_filling_modes(spec):
    """3. Modes de filling RÉELLEMENT supportés par le symbole/broker, dans l'ordre de préférence
    (IOC en tête s'il est supporté) — au lieu de forcer un seul mode à l'aveugle. Conversion du
    préfixe MQL5 SYMBOL_FILLING_* (renvoyé par les specs) vers ORDER_FILLING_* (attendu par /trade).
    Renvoie None si les specs ne donnent aucune info : dans ce cas "fillingModes" est omis du
    payload (voir _market) et MetaApi/le broker choisit lui-même — ne JAMAIS supposer IOC/FOK sans
    confirmation, c'est la cause la plus probable d'un TRADE_RETCODE_UNKNOWN."""
    override = os.getenv("MT5_FILLING_MODE", "").strip()
    if override:
        return [override]
    modes = _spec_get(spec, "fillingModes", "filling_modes", default=None) or []
    modes = [m.replace("SYMBOL_FILLING_", "ORDER_FILLING_") for m in modes if isinstance(m, str)]
    if not modes:
        return None
    if "ORDER_FILLING_IOC" in modes:
        modes = ["ORDER_FILLING_IOC"] + [m for m in modes if m != "ORDER_FILLING_IOC"]
    return modes


def _normalize_price(spec, symbol, price):
    """4. Arrondit un prix (SL/TP) sur le nombre RÉEL de digits du broker (spec.digits), pas
    seulement SYMBOLS[...]['decimals'] (config statique côté bot qui peut différer du broker réel).
    Ne modifie jamais la valeur de SL/TP au-delà de cet arrondi (point 12 : jamais de changement de
    SL/TP/RR imposé par la stratégie)."""
    if price is None:
        return price
    dec = SYMBOLS.get(symbol, {}).get("decimals", 2)
    digits = _spec_get(spec, "digits", default=dec) if spec else dec
    try:
        digits = int(digits)
    except (TypeError, ValueError):
        digits = dec
    return round(float(price), max(digits, 0))


def _check_trade_geometry(symbol, side, price, sl, tp):
    """5. Vérifie que SL/TP sont du bon côté du prix avant tout envoi :
    BUY -> SL < prix < TP ; SELL -> SL > prix > TP.
    Ne recalcule/déplace JAMAIS le SL ou le TP : lève ValueError (rejet net) si la géométrie est
    incohérente — à traiter comme un échec d'exécution classique par l'appelant."""
    if not price:
        return
    if side == "BUY":
        if sl and sl >= price:
            raise ValueError(f"Géométrie invalide {symbol} BUY : SL={sl} doit être < prix d'entrée={price}.")
        if tp and tp <= price:
            raise ValueError(f"Géométrie invalide {symbol} BUY : TP={tp} doit être > prix d'entrée={price}.")
    elif side == "SELL":
        if sl and sl <= price:
            raise ValueError(f"Géométrie invalide {symbol} SELL : SL={sl} doit être > prix d'entrée={price}.")
        if tp and tp >= price:
            raise ValueError(f"Géométrie invalide {symbol} SELL : TP={tp} doit être < prix d'entrée={price}.")


def _log_symbol_diag(symbol, broker_symbol, spec):
    """7. Diagnostic détaillé des specs du symbole, affiché AVANT chaque ordre — c'est ce log qui
    doit permettre de confirmer (ou d'infirmer) toute hypothèse sur la cause d'un UNKNOWN, plutôt
    que de la supposer."""
    diag = {
        "digits": _spec_get(spec, "digits", default=None),
        "point": _spec_get(spec, "point", "tickSize", "tick_size", default=None),
        "volumeMin": _spec_get(spec, "minVolume", "volumeMin", "min_volume", default=None),
        "volumeMax": _spec_get(spec, "maxVolume", "volumeMax", "max_volume", default=None),
        "volumeStep": _spec_get(spec, "volumeStep", "stepVolume", "volume_step", default=None),
        "tradeMode": _spec_get(spec, "tradeMode", default=None),
        "executionMode": _spec_get(spec, "executionMode", default=None),
        "fillingModes": _spec_get(spec, "fillingModes", "filling_modes", default=None),
        "stopsLevel": _spec_get(spec, "stopsLevel", "tradeStopsLevel", "stops_level", default=None),
        "freezeLevel": _spec_get(spec, "freezeLevel", "tradeFreezeLevel", "freeze_level", default=None),
    }
    print(f"[metaapi-symbol] symbol={broker_symbol} (interne {symbol}) "
          + " ".join(f"{k}={v}" for k, v in diag.items()))
    return diag


async def _account_snapshot(connection):
    """8b. État du compte pour diagnostic avant toute décision après une erreur — best-effort,
    ne lève jamais d'exception (utilisé uniquement pour enrichir les logs/messages admin)."""
    try:
        info = await connection.get_account_information()
        return {"balance": info.get("balance"), "equity": info.get("equity"),
                "margin": info.get("margin"), "freeMargin": info.get("freeMargin"),
                "tradeAllowed": info.get("tradeAllowed"), "connected": info.get("connected")}
    except Exception as e:
        return {"erreur_lecture_compte": str(e)}


async def _execute_mt5_stop_check_only(symbol, entry, sl, tp):
    """Étapes (1) et (2) seules, sans passer d'ordre : utilisée par publish_signal pour rejeter un
    signal AVANT toute création de trade si le SL/TP est trop proche du stopsLevel du broker."""
    connection = await _ensure_terminal_ready()
    if connection is None:
        raise RuntimeError("MetaApi non synchronisé (connexion persistante pas encore prête)")
    broker_symbol = MT5_SYMBOL_MAP.get(symbol, symbol)
    spec = await _get_symbol_spec(connection, broker_symbol)
    min_dist = _min_stop_distance(spec, symbol)
    if min_dist > 0:
        sl_dist = abs(entry - sl) if sl else None
        tp_dist = abs(entry - tp) if tp else None
        if sl_dist is not None and sl_dist < min_dist:
            raise SLTooCloseError(symbol, broker_symbol, "SL", min_dist, sl_dist)
        if tp_dist is not None and tp_dist < min_dist:
            raise SLTooCloseError(symbol, broker_symbol, "TP", min_dist, tp_dist)


async def _find_position_by_signature(connection, symbol, side, client_id):
    """9. Anti-doublon basé sur symbol + direction + clientId (le clientId, ex. "AB<trade_id>",
    encode déjà le signal_id unique côté appelant — voir _run_mt5_execution/place_manual_order).
    Ne renvoie une position que si elle correspond AUSSI au symbole et à la direction, pas
    seulement au clientId, pour ne jamais réutiliser par erreur une position d'un autre trade dont
    le commentaire tronqué coïnciderait. Utilisé avant toute décision après une erreur, pour ne
    jamais renvoyer un 2e ordre si le 1er a en fait été exécuté côté MT5 malgré une erreur réseau/
    UNKNOWN côté RPC. Retourne le dict position, ou None si rien trouvé (best-effort)."""
    if not client_id:
        return None
    try:
        positions = await connection.get_positions()
    except Exception as e:
        print(f"[metaapi] Vérif anti-doublon impossible (get_positions a échoué) : {e}")
        return None
    broker_symbol = MT5_SYMBOL_MAP.get(symbol, symbol)
    want_type = "POSITION_TYPE_BUY" if side == "BUY" else "POSITION_TYPE_SELL"
    for p in positions or []:
        matches_id = str(p.get("clientId") or "") == client_id or client_id in str(p.get("comment") or "")
        if not matches_id:
            continue
        if str(p.get("symbol") or "") not in (symbol, broker_symbol):
            continue
        if p.get("type") and p.get("type") != want_type:
            continue
        return p
    return None


async def _execute_mt5_order_async(symbol, side, lot, entry, sl, tp, client_id=None):
    """Passe un ordre MARKET MT5 via MetaApi, dans l'ordre :
    (1) attente sync terminal (connexion RPC persistante),
    (2) récupération des specs + diagnostic complet AVANT tout envoi (digits, point, volumes,
        tradeMode, executionMode, fillingModes, stopsLevel, freezeLevel — voir _log_symbol_diag),
    (3) normalisation volume/SL/TP sur les VRAIES specs broker + vérif géométrie + vérif distance
        minimale (stopsLevel/freezeLevel) — rejet net (SLTooCloseError / ValueError), jamais de
        déplacement automatique du SL/TP,
    (4) sélection des filling modes réellement supportés par les specs (jamais IOC forcé sans
        confirmation),
    (5) envoi de l'ordre.

    Après l'envoi, AUCUNE retentative n'est automatique par défaut :
    - TRADE_RETCODE_UNKNOWN (numericCode=-1) : résultat INCERTAIN, jamais "essayons encore" — on
      vérifie seulement si la position existe déjà (symbol+direction+clientId), on logue le
      diagnostic complet (réponse brute + état du compte), puis on relève l'exception.
    - Erreur définitive (INVALID_STOPS, INVALID_VOLUME, MARKET_CLOSED, TRADE_DISABLED, NO_MONEY) :
      jamais de retry, cause explicite en français.
    - INVALID_FILL : SEUL cas où une retentative automatique a un sens (essai du filling mode
      suivant de la liste réellement supportée par les specs).
    - Toute autre erreur (réseau/timeout) : vérification anti-doublon puis on relève l'exception,
      jamais de renvoi aveugle de la même requête.

    Retourne le dict résultat MetaApi (positionId/orderId) en cas de succès, ou lève une exception."""
    connection = await _ensure_terminal_ready()
    if connection is None:
        raise RuntimeError("MetaApi non synchronisé (connexion persistante pas encore prête)")

    broker_symbol = MT5_SYMBOL_MAP.get(symbol, symbol)

    spec = await _get_symbol_spec(connection, broker_symbol)
    _log_symbol_diag(symbol, broker_symbol, spec)   # 7. diagnostic AVANT tout envoi

    lot = _normalize_volume(spec, lot)
    sl = _normalize_price(spec, symbol, sl)
    tp = _normalize_price(spec, symbol, tp)

    _check_trade_geometry(symbol, side, entry, sl, tp)   # 5. rejet net si géométrie incohérente

    min_dist = _min_stop_distance(spec, symbol)          # 6. stopsLevel + freezeLevel
    if min_dist > 0:
        sl_dist = abs(entry - sl) if sl else None
        tp_dist = abs(entry - tp) if tp else None
        if sl_dist is not None and sl_dist < min_dist:
            raise SLTooCloseError(symbol, broker_symbol, "SL", min_dist, sl_dist)
        if tp_dist is not None and tp_dist < min_dist:
            raise SLTooCloseError(symbol, broker_symbol, "TP", min_dist, tp_dist)

    slippage = get_mt5_slippage(symbol)
    filling_modes = _pick_filling_modes(spec)   # 1/3. jamais IOC forcé si les specs ne le confirment pas
    # MetaApi : longueur totale comment + clientId limitée à 30 caractères si les deux sont
    # fournis (31 sinon) — voir metaapi.cloud/docs/client/clientIdUsage.
    cid = (client_id or "")[:32]
    max_comment = 30 - len(cid) - 1 if cid else 31
    comment = f"AlphaBot {symbol}"[:max(max_comment, 0)]
    options = {"comment": comment, "slippage": slippage}
    if filling_modes:
        options["fillingModes"] = filling_modes
    if cid:
        options["clientId"] = cid

    async def _place(opts):
        if side == "BUY":
            return await connection.create_market_buy_order(broker_symbol, lot, sl, tp, options=opts)
        elif side == "SELL":
            return await connection.create_market_sell_order(broker_symbol, lot, sl, tp, options=opts)
        else:
            raise ValueError(f"side invalide : {side!r} (attendu BUY ou SELL)")

    try:
        return await _place(options)

    except SLTooCloseError:
        raise

    except MetaApiUnknownRetcodeError as e:
        # UNKNOWN = résultat incertain, jamais "essayons encore". On vérifie seulement si l'ordre
        # est en fait passé, puis on relève l'exception dans tous les cas (pas de nouvel envoi).
        print(f"[metaapi] UNKNOWN — aucune nouvelle tentative automatique. Vérification position...")
        existing = await _find_position_by_signature(connection, symbol, side, client_id)
        if existing is not None:
            print(f"[metaapi] ✅ Position retrouvée après UNKNOWN : symbol={symbol} side={side} "
                  f"clientId={client_id} — réutilisation, pas de nouvel ordre.")
            return existing
        acc = await _account_snapshot(connection)
        print(f"[metaapi] ⚠️ UNKNOWN sans position retrouvée | symbol={broker_symbol} side={side} "
              f"lot={lot} sl={sl} tp={tp} fillingModes={filling_modes} account={acc} "
              f"raw={e.raw_response!r}")
        raise

    except Exception as e:
        msg = str(e)
        code, explication_fr = _classify_trade_error(msg)

        if code:   # 13. erreur définitive : jamais de retry
            raise RuntimeError(f"{code} — {explication_fr} | détail brut : {e}") from e

        if "INVALID_FILL" in msg.upper() and filling_modes and len(filling_modes) > 1:
            # Seul cas où changer de filling mode a un sens : on ne remplace QUE ce paramètre,
            # jamais après UNKNOWN ni après une erreur réseau/timeout.
            print(f"[metaapi] INVALID_FILL — nouvel essai avec fillingModes={filling_modes[1:]}.")
            next_options = dict(options)
            next_options["fillingModes"] = filling_modes[1:]
            return await _place(next_options)

        # Réseau/timeout/autre : vérifier d'abord si l'ordre est passé, jamais le renvoyer aveuglément.
        print(f"[metaapi] Erreur d'exécution ({e}) — vérification anti-doublon avant toute décision.")
        existing = await _find_position_by_signature(connection, symbol, side, client_id)
        if existing is not None:
            print(f"[metaapi] ✅ Position déjà ouverte malgré l'erreur : {existing}")
            return existing
        raise


async def _confirm_position_async(connection, position_id):
    """Confirme réellement, côté MT5 (pas seulement via la réponse de création d'ordre), qu'une
    position positionId/orderId existe bien. Best-effort : si la vérification elle-même échoue
    (API indisponible), on ne conclut PAS à un échec d'exécution — seule l'absence confirmée de
    la position (position introuvable) doit faire douter du succès de l'ordre. Retourne aussi le
    dict position (ou None) pour que l'appelant vérifie que le SL/TP demandés sont réellement
    attachés côté broker, pas seulement que le HTTP 200 a été reçu."""
    if not position_id:
        return False, "pas d'identifiant de position renvoyé par MetaApi", None
    try:
        positions = await connection.get_positions()
    except Exception as e:
        return None, f"vérification impossible ({e})", None
    pos = next((p for p in positions or [] if str(p.get("id")) == str(position_id)), None)
    return (pos is not None), None, pos


def execute_mt5_order(symbol, side, lot, entry, sl, tp, client_id=None):
    """Wrapper sync : exécute un ordre MARKET côté MT5 avec le lot/SL/TP déjà calculés par le
    moteur de signal, puis confirme réellement l'existence de la position créée (positionId/
    orderId) avant de la considérer comme ouverte. Laisse remonter SLTooCloseError telle quelle (à
    l'appelant de décider du rejet du signal) ; toute autre erreur ne lève jamais d'exception :
    retourne le position_id (str) en cas de succès confirmé, ou None en cas d'échec (loggé +
    admin notifié par l'appelant). `client_id` (ex. l'id du trade en base) sert à la déduplication
    lors d'une éventuelle retentative — voir _execute_mt5_order_async."""
    try:
        result = _run_mt5(_execute_mt5_order_async(symbol, side, lot, entry, sl, tp, client_id))
    except SLTooCloseError as e:
        # Rare : le prix a bougé entre la pré-vérif (publish_signal) et l'exécution, faisant
        # basculer le SL sous le stopsLevel entre-temps. Jamais de déplacement automatique du
        # SL : on traite ça comme un échec d'exécution classique, notifié par l'appelant.
        print(f"[metaapi] Ordre refusé à l'exécution ({e}) — signal déjà envoyé, à traiter manuellement.")
        return None
    except MetaApiUnknownRetcodeError as e:
        # 14. Diagnostic complet en cas d'UNKNOWN persistant (aucune retentative n'a été tentée).
        print(f"[metaapi] Échec d'exécution {symbol} {side} lot={lot} — TRADE_RETCODE_UNKNOWN persistant : {e}")
        traceback.print_exc(limit=-3)
        send(CHAT_ID_ADMIN,
             f"❌ {symbol} {side} lot={lot} — le broker a renvoyé TRADE_RETCODE_UNKNOWN (numericCode=-1).\n"
             f"Réponse brute : {e.raw_response}\n"
             f"Payload envoyé : {e.sent_body}\n"
             f"SL={sl} TP={tp}\n"
             f"Vérifie manuellement sur MT5 si une position a été ouverte avant de relancer.")
        return None
    except Exception as e:
        code, explication_fr = _classify_trade_error(str(e))
        print(f"[metaapi] Échec d'exécution {symbol} {side} lot={lot} : {e}")
        traceback.print_exc(limit=-3)
        if code:
            send(CHAT_ID_ADMIN, f"❌ {symbol} {side} lot={lot} — ordre rejeté ({code}) : {explication_fr}\n"
                                 f"SL={sl} TP={tp} | détail brut : {e}")
        return None

    position_id = str(result.get("positionId") or result.get("orderId") or result.get("id") or "")
    fill_price = result.get("price") or result.get("openPrice")

    if not position_id:
        print(f"[metaapi] Échec d'exécution {symbol} {side} lot={lot} : "
              f"aucun positionId/orderId dans la réponse MetaApi ({result!r}).")
        return None

    # Confirmation réelle côté MT5 (pas seulement la réponse de création d'ordre) : si la position
    # est confirmée absente (found=False), c'est un échec net ; si la vérif elle-même échoue
    # (found=None, ex. API indisponible), on ne bloque pas sur ce doute, l'ordre a déjà un ticket.
    try:
        connection = get_ready_connection()
        found, reason, pos = _run_mt5(_confirm_position_async(connection, position_id)) \
            if connection is not None else (None, "connexion MetaApi indisponible pour la confirmation", None)
    except Exception as e:
        found, reason, pos = None, f"confirmation impossible ({e})", None

    if found is False:
        print(f"[metaapi] Position {position_id} introuvable côté broker après exécution "
              f"({symbol} {side} lot={lot}) — traité comme échec d'exécution.")
        return None
    if found is None:
        print(f"[metaapi] Ticket {position_id} obtenu mais non re-confirmé ({reason}) — "
              f"considéré exécuté (ticket déjà attribué par MetaApi).")
    else:
        print(f"[metaapi] Position {position_id} confirmée côté broker.")
        # Vérifie que le SL/TP demandés sont RÉELLEMENT attachés (pas seulement un HTTP 200) :
        # un écart peut survenir si le broker a accepté l'ordre mais rejeté silencieusement le SL/TP
        # (rare, mais possible selon le trade mode du symbole).
        if pos is not None:
            broker_sl, broker_tp = pos.get("stopLoss"), pos.get("takeProfit")
            if sl and not broker_sl:
                print(f"[metaapi] ⚠️ Position {position_id} ouverte MAIS sans SL attaché côté broker "
                      f"(demandé {sl}) — à vérifier/corriger manuellement.")
                send(CHAT_ID_ADMIN, f"⚠️ {symbol} {side} — position {position_id} ouverte SANS SL attaché "
                                     f"(demandé {sl}). Vérifie et corrige manuellement sur MT5/Telegram.")
            if tp and not broker_tp:
                print(f"[metaapi] ⚠️ Position {position_id} ouverte MAIS sans TP attaché côté broker "
                      f"(demandé {tp}).")

    print(f"[metaapi] Ordre exécuté : {symbol} {side} lot={lot} entry~{entry} fill={fill_price} SL={sl} TP={tp} "
          f"-> ticket {position_id}")

    if fill_price:
        try:
            gap_pct = abs(float(fill_price) - entry) / entry
        except (TypeError, ZeroDivisionError):
            gap_pct = None
        if gap_pct is not None and gap_pct > MT5_PRICE_GAP_ALERT_PCT.get(symbol, 0.001):
            send(CHAT_ID_ADMIN,
                 f"⚠️ Écart de prix {symbol} {side} : signal @ {entry:.2f} / exécuté @ {float(fill_price):.2f} "
                 f"({gap_pct * 100:.2f}%) — ticket {position_id}. SL/TP restent basés sur le prix du signal, "
                 f"pense à vérifier le risque réel de cette position.")

    return position_id


async def _move_to_breakeven_async(position_id, entry, side, symbol, lot):
    """Modifie le SL d'une position MT5 déjà ouverte pour le placer au BE (entrée +/- un buffer
    dynamique visant BE_TARGET_USD net sur CE lot précis — voir _be_price_buffer). Lève une
    exception en cas d'échec — à charge de l'appelant sync (move_to_breakeven) de l'attraper."""
    connection = await get_metaapi_connection()
    if connection is None:
        raise RuntimeError("connexion MetaApi indisponible")

    buffer = _be_price_buffer(symbol, lot)
    dec = SYMBOLS.get(symbol, {}).get("decimals", 2)

    if side == "BUY":
        new_sl = entry + buffer   # au-dessus de l'entrée : vise un résultat net positif sur un long
    elif side == "SELL":
        new_sl = entry - buffer   # en-dessous de l'entrée : vise un résultat net positif sur un short
    else:
        raise ValueError(f"side invalide : {side!r} (attendu BUY ou SELL)")
    new_sl = round(new_sl, dec)

    await connection.modify_position(position_id, stop_loss=new_sl)
    print(f"[metaapi] BE dynamique {symbol} {side} lot={lot} buffer={buffer:.5f} "
          f"(cible {BE_TARGET_USD}$"
          + (f" + commission {BE_COMMISSION_USD_PER_LOT}$/lot" if BE_COMMISSION_USD_PER_LOT else "")
          + f") -> SL={new_sl}")
    return new_sl


def move_to_breakeven(position_id, entry, side, symbol, lot):
    """Déplace le SL d'une position MT5 ouverte (identifiée par position_id) au BE + un buffer
    calculé pour viser un résultat NET minimum de BE_TARGET_USD (défaut +0.01$, réglable via env ;
    0.00 pour viser pile l'entrée) sur le lot réel de cette position — remplace l'ancien buffer
    fixe en points qui pouvait se traduire par une perte nette selon le lot/la commission.

    Ne lève jamais d'exception : retourne True en cas de succès, False sinon (loggé)."""
    if not position_id:
        print("[metaapi] move_to_breakeven : pas de position_id MT5 (ordre non exécuté côté broker) — ignoré.")
        return False
    try:
        _run_mt5(_move_to_breakeven_async(position_id, entry, side, symbol, lot))
    except Exception as e:
        print(f"[metaapi] Échec move_to_breakeven position={position_id} : {e}")
        traceback.print_exc(limit=-3)
        return False
    return True


async def _close_position_async(position_id):
    connection = await get_metaapi_connection()
    if connection is None:
        raise RuntimeError("connexion MetaApi indisponible")
    return await connection.close_position(position_id)


def close_mt5_position(position_id):
    """Ferme une position MT5 via MetaApi. Ne lève jamais d'exception : (True, None) si fermée,
    (False, message d'erreur lisible) sinon — à afficher tel quel dans Telegram."""
    try:
        _run_mt5(_close_position_async(position_id))
    except Exception as e:
        print(f"[metaapi] Échec fermeture position {position_id} : {e}")
        traceback.print_exc(limit=-3)
        return False, str(e)
    return True, None


async def _account_summary_async():
    connection = await get_metaapi_connection()
    if connection is None:
        return None
    info = await connection.get_account_information() or {}
    positions = await connection.get_positions() or []
    return {
        "connected": True,
        "broker": info.get("broker") or info.get("server") or "Broker inconnu",
        "server": info.get("server"),
        "balance": info.get("balance"),
        "equity": info.get("equity"),
        "free_margin": info.get("freeMargin"),
        "currency": info.get("currency") or "",
        "positions": positions,
    }


def get_account_summary():
    """Résumé compte/broker pour Telegram (🏦 COMPTE / BROKER). Ne lève jamais d'exception :
    retourne {"connected": False} si l'API n'est pas joignable, pour ne jamais afficher "Exness"
    en dur si l'API réellement connectée est différente (ou absente)."""
    try:
        res = _run_mt5(_account_summary_async())
    except Exception as e:
        print(f"[metaapi] Résumé compte impossible : {e}")
        return {"connected": False}
    return res or {"connected": False}


REV_MT5_SYMBOL_MAP = {v: k for k, v in MT5_SYMBOL_MAP.items()}


def _internal_symbol(broker_symbol):
    """Symbole interne (XAUUSD/BTCUSD) depuis le symbole broker (ex. XAUUSDm) — l'inverse de
    MT5_SYMBOL_MAP, pour afficher les positions manuelles avec les mêmes libellés que le reste du bot."""
    return REV_MT5_SYMBOL_MAP.get(broker_symbol, broker_symbol)


async def _manual_price_async(symbol):
    connection = await _ensure_terminal_ready()
    if connection is None:
        return None, "MetaApi non synchronisé (connexion pas encore prête)"
    broker_symbol = MT5_SYMBOL_MAP.get(symbol, symbol)
    try:
        price = await connection.get_symbol_price(broker_symbol)
    except Exception as e:
        return None, f"prix indisponible côté broker ({e})"
    if not price or price.get("bid") is None or price.get("ask") is None:
        return None, "réponse prix broker invalide (pas de bid/ask)"
    return price, None


def place_manual_order(symbol, side):
    """Exécute un trade MANUEL déclenché depuis Telegram (📈 TRADE → BUY/SELL), en une seule
    action, sans confirmation intermédiaire (voir consignes) :
      1. prix réel broker (MetaApi current-price, pas Deriv/Binance)
      2. SL via le plancher de risque déjà existant (manual_sl_distance)
      3. lot via calc_lot (même fonction que le moteur automatique) selon le risque $ configuré
      4. TP via le RR configuré (get_tp_rr)
      5. envoi de l'ordre (execute_mt5_order fait déjà toutes les validations techniques : specs,
         stopsLevel, volume min/max/step, filling mode — et ne lève jamais, retourne None si refusé)
    Retourne un dict {"ok": True, ...détails...} ou {"ok": False, "error": "..."} — jamais d'exception."""
    if side not in ("BUY", "SELL"):
        return {"ok": False, "error": f"side invalide : {side!r}"}
    if symbol not in SYMBOLS:
        return {"ok": False, "error": f"symbole inconnu : {symbol!r}"}

    try:
        price, err = _run_mt5(_manual_price_async(symbol))
    except Exception as e:
        price, err = None, str(e)
    if err:
        return {"ok": False, "error": err}

    entry = float(price["ask"]) if side == "BUY" else float(price["bid"])
    spread = abs(float(price["ask"]) - float(price["bid"]))
    spread_pct = spread / entry if entry else 0
    max_spread = get_max_spread_pct()
    if max_spread > 0 and spread_pct > max_spread:
        return {"ok": False, "error": f"spread trop large au moment du clic ({spread:.2f} pts, "
                                       f"{spread_pct * 100:.3f}% > seuil {max_spread * 100:.3f}%) — "
                                       f"probablement une news ou un marché peu liquide, réessaie dans un instant."}
    sl_distance = manual_sl_distance(symbol, entry)
    if not sl_distance or sl_distance <= 0:
        return {"ok": False, "error": "distance SL non calculable (ATR/plancher indisponible)"}
    d = 1 if side == "BUY" else -1
    sl = entry - d * sl_distance
    rr = get_tp_rr()
    tp = entry + d * sl_distance * rr

    lot_info = calc_lot(symbol, get_risk(), sl_distance)
    if lot_info["lot"] <= 0:
        return {"ok": False, "error": "lot calculé nul (risque ou distance SL invalide)"}

    # clientId MetaApi : format court strategyId_positionId_orderId, alphanumérique uniquement
    # (pas de tirets) — voir metaapi.cloud/docs/client/clientIdUsage. "MAN" = stratégie manuelle,
    # code 3 lettres du symbole + 1ère lettre du sens = positionId, fin du timestamp ms = orderId.
    code = next(k for k, v in _MANUAL_SYMS.items() if v == symbol) if symbol in _MANUAL_SYMS.values() \
        else symbol[:3].upper()
    client_id = f"MAN_{code}{side[0]}_{str(int(time.time() * 1000))[-8:]}"
    position_id = execute_mt5_order(symbol, side, lot_info["lot"], entry, sl, tp, client_id=client_id)
    if not position_id:
        return {"ok": False, "error": "ordre refusé par le broker (voir logs serveur pour le détail exact)"}

    return {"ok": True, "position_id": position_id, "symbol": symbol, "side": side,
            "entry": entry, "sl": sl, "tp": tp, "lot": lot_info["lot"],
            "risk_usd": lot_info["real_risk"], "rr": rr, "spread": spread,
            "raised_to_min": lot_info["raised_to_min"]}


async def _mt5_open_position_ids_async():
    """Retourne l'ensemble des IDs (str) de toutes les positions actuellement ouvertes côté broker,
    ou None si MetaApi est indisponible (pour ne jamais conclure à tort à une clôture)."""
    connection = await get_metaapi_connection()
    if connection is None:
        return None
    positions = await connection.get_positions()
    return {str(p.get("id")) for p in positions}


async def _mt5_position_deals_async(position_id):
    """Historique des deals MT5 d'une position (utilisé pour déduire pourquoi/comment elle a été
    fermée côté broker : SL, TP, ou clôture manuelle/autre)."""
    connection = await get_metaapi_connection()
    if connection is None:
        return None
    deals = await connection.get_deals_by_position(position_id)
    if isinstance(deals, dict):
        deals = deals.get("deals", [])
    return deals or []


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
# RR d'armement du BE et mode du filtre HTF FIGÉS à la création du signal (changer le réglage n'affecte pas un trade ouvert).
_ensure_column("trades", "be_rr", "REAL")
_ensure_column("trades", "htf_mode", "TEXT")
# Exécution MT5 (MetaApi) : ticket de la position ouverte côté broker, si l'exécution a réussi.
_ensure_column("trades", "mt5_position_id", "TEXT")
# Résultat de l'exécution MT5 : PENDING (en cours) / OK / FAILED. Indépendant du SUIVI du signal (statut OPEN).
_ensure_column("trades", "exec_status", "TEXT")
# message_id Telegram du signal d'ouverture dans le groupe : toutes les notifications de suivi (RR1/RR2/
# BE/TP/SL) répondent (reply_to_message_id) à CE message précis, jamais au dernier message du groupe.
_ensure_column("trades", "signal_message_id", "INTEGER")


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


def get_manual_mode():
    return get_setting("manual_mode", "0") == "1"


def set_manual_mode(on):
    set_setting("manual_mode", "1" if on else "0")


def get_max_spread_pct():
    return float(get_setting("max_spread_pct", MAX_SPREAD_PCT))


def set_max_spread_pct(v):
    set_setting("max_spread_pct", v)


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


def get_htf_mode():
    """Mode du filtre HTF : 'OFF' | 'M15' | 'FULL'. Dernier choix Telegram ('htf_mode' en base) ; sinon l'ancien réglage
    ON/OFF ('htf_filter', versions précédentes : ON = HTF_MODE) ; sinon HTF_FILTER / HTF_MODE (Render / défaut)."""
    v = get_setting("htf_mode")
    if v in HTF_MODES:
        return v
    legacy = get_setting("htf_filter")
    if legacy is not None:
        return HTF_MODE if str(legacy) == "1" else "OFF"
    return HTF_MODE if HTF_FILTER else "OFF"


def set_htf_mode(mode):
    if mode in HTF_MODES:
        set_setting("htf_mode", mode)


def get_htf_filter():
    """Filtre HTF actif ? (M15 ou COMPLET)."""
    return get_htf_mode() != "OFF"


def set_htf_filter(on):
    set_htf_mode(HTF_MODE if on else "OFF")   # compatibilité : ON = mode par défaut


def get_mtf_fib_filter():
    """Filtre Fibonacci HTF Premium/Discount : ON/OFF, réglable à tout moment. Dernier choix Telegram
    ('mtf_fib_filter' en base) sinon USE_MTF_FIBONACCI_PD_FILTER (ENV / défaut)."""
    v = get_setting("mtf_fib_filter")
    if v is not None:
        return str(v).strip().lower() in ("1", "true", "yes", "on")
    return USE_MTF_FIBONACCI_PD_FILTER


def set_mtf_fib_filter(on):
    set_setting("mtf_fib_filter", "1" if on else "0")


def get_max_positions():
    """Signaux ouverts max par actif : dernier choix Telegram, sinon MAX_POSITIONS (Render / défaut = 1)."""
    try:
        v = int(float(get_setting("max_positions", MAX_POSITIONS)))
    except (TypeError, ValueError):
        return max(1, MAX_POSITIONS)
    return v if v >= 1 else max(1, MAX_POSITIONS)


def set_max_positions(n):
    set_setting("max_positions", int(n))


def get_be_rr():
    """RR d'armement du BE des NOUVEAUX signaux : dernier choix Telegram, sinon BE_RR (Render / défaut)."""
    try:
        v = float(get_setting("be_rr", BE_RR))
    except (TypeError, ValueError):
        return BE_RR
    return v if v > 0 else BE_RR


def set_be_rr(v):
    set_setting("be_rr", float(v))


def trade_be_rr(trade):
    """RR d'armement du BE d'UN trade, fixé à la création du signal (changer /be n'affecte jamais un trade ouvert).
    Repli : BE_RR pour les anciens trades sans valeur enregistrée."""
    try:
        v = trade.get("be_rr")
        if v is not None and float(v) > 0:
            return float(v)
    except (TypeError, ValueError):
        pass
    return BE_RR


def get_ext_tf(entry_tf):
    """UT de liquidité externe pour l'UT d'entrée `entry_tf` (minutes) : réglable (H1/H4) uniquement pour
    l'entrée M5 — dernier choix Telegram ('ext_tf_m5' en base), H1 par défaut ; fixe (LIQ_EXT) pour toute
    autre UT d'entrée."""
    if entry_tf == 5:
        try:
            v = int(get_setting("ext_tf_m5", LIQ_EXT[5]))
        except (TypeError, ValueError):
            v = LIQ_EXT[5]
        return v if v in (60, 240) else LIQ_EXT[5]
    return LIQ_EXT.get(entry_tf, HTF_MINUTES)


def set_ext_tf_m5(minutes):
    set_setting("ext_tf_m5", int(minutes))


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
    # 'EXECUTING' inclus : un trade MARKET en cours de confirmation MT5 doit réserver sa place,
    # sinon un nouveau signal pourrait être publié (et un 2e ordre envoyé) avant confirmation.
    return _q("SELECT COUNT(*) n FROM trades WHERE symbol=? AND status IN ('OPEN','PENDING','EXECUTING')",
              (symbol,)).fetchone()["n"]


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
BINANCE_INTERVALS = {1: "1m", 3: "3m", 5: "5m", 15: "15m", 30: "30m", 60: "1h", 240: "4h"}


def _only_closed(candles, tf_sec=None):
    now = time.time()
    tf_sec = tf_sec or TF_SEC
    return sorted((c for c in candles if c["t"] + tf_sec + CLOSE_GRACE_SEC <= now),
                  key=lambda c: c["t"])


# --- BTC : Binance (klines) avec BASCULE AUTOMATIQUE vers Bybit puis OKX -----------------------
# Binance bannit régulièrement les IP partagées des hébergeurs (erreur 418 / 429 / 451). Dans ce cas
# on met Binance en pause quelques minutes (insister prolonge le ban) et on passe sur Bybit / OKX.
BYBIT_INTERVALS = {1: "1", 3: "3", 5: "5", 15: "15", 30: "30", 60: "60", 240: "240"}
OKX_INTERVALS = {1: "1m", 3: "3m", 5: "5m", 15: "15m", 30: "30m", 60: "1H", 240: "4H"}
BINANCE_PAUSE_SEC = _env_int("BINANCE_PAUSE_SEC", 300)
_binance_blocked_until = 0.0


def _binance_ban(err):
    """Met Binance en pause si l'erreur ressemble à un ban / blocage IP (418, 429, 451, 403)."""
    global _binance_blocked_until
    code = getattr(getattr(err, "response", None), "status_code", None)
    if code in (418, 429, 451, 403):
        _binance_blocked_until = time.time() + BINANCE_PAUSE_SEC


def _binance_only(symbol, minutes=None):
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
            _binance_ban(e)
    raise RuntimeError(f"Binance indisponible : {last_err}")


def _bybit(symbol, minutes=None):
    r = requests.get("https://api.bybit.com/v5/market/kline",
                     params={"category": "linear", "symbol": symbol,
                             "interval": BYBIT_INTERVALS[minutes or TIMEFRAME_MIN],
                             "limit": min(CANDLES_LIMIT, 1000)}, timeout=15)
    r.raise_for_status()
    j = r.json()
    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit : {j.get('retMsg')}")
    return [{"t": int(k[0]) // 1000, "o": float(k[1]), "h": float(k[2]),
             "l": float(k[3]), "c": float(k[4])} for k in j["result"]["list"]]


def _okx(symbol, minutes=None):
    inst = symbol.replace("USDT", "-USDT")   # BTCUSDT -> BTC-USDT
    r = requests.get("https://www.okx.com/api/v5/market/candles",
                     params={"instId": inst, "bar": OKX_INTERVALS[minutes or TIMEFRAME_MIN],
                             "limit": min(CANDLES_LIMIT, 300)}, timeout=15)
    r.raise_for_status()
    j = r.json()
    if j.get("code") != "0":
        raise RuntimeError(f"OKX : {j.get('msg')}")
    return [{"t": int(k[0]) // 1000, "o": float(k[1]), "h": float(k[2]),
             "l": float(k[3]), "c": float(k[4])} for k in j["data"]]


def _binance(symbol, minutes=None):
    """Bougies BTC : Binance si dispo, sinon Bybit, sinon OKX (ordre chronologique dans tous les cas)."""
    errors = []
    sources = [("Binance", _binance_only), ("Bybit", _bybit), ("OKX", _okx)]
    for name, fn in sources:
        if name == "Binance" and time.time() < _binance_blocked_until:
            errors.append("Binance en pause (IP bloquée)")
            continue
        try:
            raw = sorted(fn(symbol, minutes), key=lambda c: c["t"])
            if raw:
                if name != "Binance":
                    print(f"[BTC] source de secours utilisée : {name}")
                return raw
        except Exception as e:
            errors.append(f"{name} : {e}")
    raise RuntimeError("Aucune source BTC disponible — " + " | ".join(errors))


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
                if "candles" in msg or "history" in msg:
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


def get_live_price(symbol):
    """Prix instantané (dernier tick), pour la surveillance BE temps réel — un seul point, pas
    un historique de bougies : appel volontairement léger pour pouvoir tourner toutes les 5-10 s
    sans peser sur les APIs publiques. Retourne un float, ou None si indisponible (réseau, etc.)."""
    cfg = SYMBOLS[symbol]
    try:
        if cfg["source"] == "binance":
            errors = []
            if time.time() >= _binance_blocked_until:
                for base in BINANCE_BASES:
                    try:
                        r = requests.get(base + "/api/v3/ticker/price",
                                          params={"symbol": cfg["binance_symbol"]}, timeout=6)
                        r.raise_for_status()
                        return float(r.json()["price"])
                    except Exception as e:
                        errors.append(str(e))
                        _binance_ban(e)
            try:   # secours : Bybit puis OKX
                r = requests.get("https://api.bybit.com/v5/market/tickers",
                                 params={"category": "linear", "symbol": cfg["binance_symbol"]}, timeout=6)
                r.raise_for_status()
                return float(r.json()["result"]["list"][0]["lastPrice"])
            except Exception as e:
                errors.append(f"Bybit : {e}")
            try:
                r = requests.get("https://www.okx.com/api/v5/market/ticker",
                                 params={"instId": cfg["binance_symbol"].replace("USDT", "-USDT")}, timeout=6)
                r.raise_for_status()
                return float(r.json()["data"][0]["last"])
            except Exception as e:
                errors.append(f"OKX : {e}")
            raise RuntimeError("prix BTC indisponible — " + " | ".join(errors))
        if cfg["source"] == "deriv":
            msg = _deriv_request({
                "ticks_history": cfg["deriv_symbol"], "style": "ticks",
                "count": 1, "end": "latest"})
            return float(msg["history"]["prices"][-1])
        raise ValueError(f"Source de prix inconnue pour {symbol} : {cfg['source']}")
    except Exception as e:
        print(f"[{symbol}] get_live_price indisponible : {e}")
        return None


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


def _flag_eq(pivots, atr_now):
    """Marque eq=True sur chaque pivot (highs OU lows, jamais mélangés) ayant au moins un autre pivot du même
    côté à moins de EQ_TOL_ATR ATR — repère les zones EQH / EQL (liquidité égale, plusieurs mèches au même niveau)."""
    tol = EQ_TOL_ATR * atr_now
    for pv in pivots:
        pv["eq"] = False
    for i, a in enumerate(pivots):
        for b in pivots[i + 1:]:
            if abs(a["level"] - b["level"]) < tol:
                a["eq"] = b["eq"] = True


def ext_map(symbol):
    """Carte de liquidité externe (fonction pure, lecture seule — ne modifie rien et n'influence JAMAIS l'acceptation d'un
    signal : build_signal / htf_poi_setup ne s'en servent que pour l'affichage, sig["ext"] et le log) : pivots swing (profondeur EXT_DEPTH) sur l'UT de liquidité externe de
    l'UT d'entrée courante (get_ext_tf), NON balayés (aucune mèche ultérieure au-delà, cf. `_flag_eq`/sweep
    ci-dessous — un pivot balayé n'est plus une liquidité disponible et est exclu).

    Retourne {"tf": UT utilisée, "highs": [...], "lows": [...]} — chaque pivot : {"i", "t", "level", "eq"},
    "eq" = True si un autre pivot du même côté est à moins de EQ_TOL_ATR ATR (EQH côté highs, EQL côté lows).
    Les deux listes sont triées par distance croissante au dernier prix clôturé."""
    tf = get_ext_tf(TIMEFRAME_MIN)
    raw = _ctx_candles(symbol, tf)
    if len(raw) <= 2 * EXT_DEPTH + 1:
        return {"tf": tf, "highs": [], "lows": []}
    atr = atr_series(raw)
    highs, lows = [], []
    for p in range(EXT_DEPTH, len(raw) - EXT_DEPTH):
        win_h = [x["h"] for x in raw[p - EXT_DEPTH:p + EXT_DEPTH + 1]]
        win_l = [x["l"] for x in raw[p - EXT_DEPTH:p + EXT_DEPTH + 1]]
        if raw[p]["h"] == max(win_h) and all(raw[p]["h"] > raw[k]["h"] for k in range(p - EXT_DEPTH, p)):
            if not any(k["h"] > raw[p]["h"] for k in raw[p + 1:]):   # non balayé depuis
                highs.append({"i": p, "t": raw[p]["t"], "level": raw[p]["h"]})
        if raw[p]["l"] == min(win_l) and all(raw[p]["l"] < raw[k]["l"] for k in range(p - EXT_DEPTH, p)):
            if not any(k["l"] < raw[p]["l"] for k in raw[p + 1:]):   # non balayé depuis
                lows.append({"i": p, "t": raw[p]["t"], "level": raw[p]["l"]})
    a_now = atr[-1] if atr else 0.0
    _flag_eq(highs, a_now)
    _flag_eq(lows, a_now)
    price = raw[-1]["c"]
    highs.sort(key=lambda pv: abs(pv["level"] - price))
    lows.sort(key=lambda pv: abs(pv["level"] - price))
    return {"tf": tf, "highs": highs, "lows": lows}


def int_sweep(c, ev):
    """Liquidité interne sur les bougies d'entrée `c`, pour le CHoCH `ev` (`analyze`) — fonction pure, lecture
    seule : n'influence JAMAIS l'acceptation d'un signal (affichage : sig["sweep"] et log). Aucune redétection : réutilise `_turn_index`
    (point de retournement) et `_sweep_at` (balayage), comme `m5_retracement`.

    Retourne {"swept": bool, "level": niveau balayé | None, "extreme": index du point de retournement, "eq":
    True si au moins 2 des SWEEP_LOOKBACK bougies précédant le retournement touchent ce niveau à moins de
    EQ_TOL_ATR ATR (EQL / EQH interne — liquidité renforcée par plusieurs mèches)}."""
    d = ev["dir"]
    e = _turn_index(c, ev)
    lvl, swept = _sweep_at(c, e, d)
    eq = False
    if lvl is not None:
        tol = EQ_TOL_ATR * ev["atr"]
        prior = c[max(0, e - SWEEP_LOOKBACK):e]
        touches = sum(1 for k in prior if abs((k["l"] if d == 1 else k["h"]) - lvl) < tol)
        eq = touches >= 2
    return {"swept": swept, "level": lvl, "extreme": e, "eq": eq}


# --- lecture ext / int : AFFICHAGE UNIQUEMENT (graphique, message, log, analyse). Rien ici ne décide d'un signal. ----------
def _liq_lbl(side, eq):
    """Libellé d'un niveau de liquidité : « haut » / « bas », ou EQH / EQL quand plusieurs mèches sont au même niveau."""
    return ("EQH" if side == "haut" else "EQL") if eq else side


def ext_target(symbol, d, price, m=None):
    """Liquidité externe VISÉE par un trade de sens d (1 = BUY -> plus proche plus haut non balayé au-dessus du prix ;
    -1 = SELL -> plus proche plus bas non balayé en dessous). Lecture de `ext_map` (m : carte déjà calculée, optionnelle).
    Retourne {"tf", "side", "level", "eq", "dist"} ou None si aucune liquidité externe dans cette direction."""
    m = m or ext_map(symbol)
    pool = [pv for pv in (m["highs"] if d == 1 else m["lows"]) if (pv["level"] - price) * d > 0]
    if not pool:
        return None
    pv = min(pool, key=lambda q: abs(q["level"] - price))
    return {"tf": m["tf"], "side": "haut" if d == 1 else "bas", "level": pv["level"], "eq": bool(pv["eq"]),
            "dist": abs(pv["level"] - price)}


def sweep_reading(c, ev, tf_lbl=None):
    """Liquidité interne de l'UT d'entrée pour le CHoCH `ev`, lue via `int_sweep` : {"tf", "side", "level", "swept", "eq",
    "t" (bougie du point de retournement), "i"}. side = « bas » pour un CHoCH haussier (liquidité sous les plus bas),
    « haut » pour un CHoCH baissier."""
    r = int_sweep(c, ev)
    return {"tf": tf_lbl or TF_LABEL, "side": "bas" if ev["dir"] == 1 else "haut", "level": r["level"],
            "swept": bool(r["swept"]), "eq": bool(r["eq"]), "t": c[r["extreme"]]["t"], "i": r["extreme"]}


def liquidity_reading(symbol, c, ev, entry):
    """(ext, sweep) d'un signal : sig["ext"] / sig["sweep"]. Ne lève jamais : un souci de lecture (données H/M15 absentes,
    réseau) laisse simplement la clé à None, le signal part quand même — c'est de l'affichage, pas une condition."""
    ext = sweep = None
    try:
        ext = ext_target(symbol, ev["dir"], entry)
    except Exception as e:
        print(f"[{symbol}] lecture liquidité externe impossible ({type(e).__name__}) : signal sans ligne « Ext. »")
    try:
        sweep = sweep_reading(c, ev)
    except Exception as e:
        print(f"[{symbol}] lecture liquidité interne impossible ({type(e).__name__}) : signal sans ligne « Int. »")
    return ext, sweep


def _ext_short(x, dec):
    return f"{_liq_lbl(x['side'], x['eq'])} {_fmt(x['level'], dec)} (à {_fmt(x['dist'], dec)} pts)"


def _ext_txt(x, dec):
    """« M15 haut 80 900 (à 134 pts) » / « M15 EQL 80 500 (à 266 pts) »."""
    return f"{_tf_lbl(x['tf'])} {_ext_short(x, dec)}"


def _sweep_txt(x, dec):
    """« M1 EQL 80 780 balayé » / « M1 haut 80 780 non balayé »."""
    if x.get("level") is None:
        return f"{x['tf']} — (pas d'historique)"
    return f"{x['tf']} {_liq_lbl(x['side'], x['eq'])} {_fmt(x['level'], dec)} " + ("balayé" if x["swept"] else "non balayé")


def _liq_lines(sig, dec):
    """Lignes « Ext. : … » et « Int. : … » d'un message de signal ; chaîne vide si sig n'a pas ces clés (ex. renvoi différé)."""
    out = []
    if sig.get("ext"):
        out.append("Ext. : " + _ext_txt(sig["ext"], dec))
    if sig.get("sweep") and sig["sweep"].get("level") is not None:
        out.append("Int. : " + _sweep_txt(sig["sweep"], dec))
    return "".join(line + "\n" for line in out)


def _ext_trace_txt(symbol, d, price, dec):
    """Ligne « liquidité ext. » du log multi-UT : `ext_map` (visée dans le sens d), à défaut l'extrême de la jambe H1."""
    try:
        x = ext_target(symbol, d, price)
    except Exception:
        x = None
    return _ext_txt(x, dec) if x else _ext_liquidity(symbol, d, dec)


def _poi_log(symbol, msg):
    if DEBUG_CHOCH:
        print(f"[{symbol}]   {msg}")


def _tf_lbl(tf):
    return TF_LABELS.get(tf) or HTF_ONLY_LABELS.get(tf, f"M{tf}")


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
    try:   # liquidité interne de l'UT d'entrée (int_sweep) : affichage, ne décide de rien
        int_txt = _sweep_txt(sweep_reading(c, ev), dec)
    except Exception:
        int_txt = "—"
    tr["int"] = int_txt

    # 1) contexte : H1 (biais) confirmé par M15
    bias = htf_bias(symbol)
    use_m15 = TIMEFRAME_MIN < 15
    m15 = _last_dir(_ctx_candles(symbol, 15)) if use_m15 else bias
    ctx_txt = f"{htf} {_dir_txt(bias)}" + (f" · M15 {_dir_txt(m15)}" if use_m15 else "")
    if bias:
        tr["ext"] = _ext_trace_txt(symbol, bias, c[ev["i"]]["c"], dec)   # ext_map ; repli : jambe H1
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
        tr.update({"int": f"{int_txt} · {r['liq']}", "retr": r["state"], "choch5": r["choch"]})
        if r["state"] == "INDÉTERMINÉ":
            return setup, None, "structure M5 indéterminée -- retracement impossible à évaluer"
        if r["state"] == "EN COURS":
            return setup, None, (f"retracement M5 en cours (M5 encore {_dir_txt(-d)}) -- {tfl} seul ne suffit pas, "
                                 f"attendre un CHoCH M5 {_dir_txt(d)}")
        if r["state"] == "NON CONFIRMÉ":
            return setup, None, "CHoCH M5 sans balayage de liquidité interne ni réaction sur POI -- fin du retracement non confirmée"
    else:
        tr.update({"retr": f"non applicable (UT d'entrée {tfl})", "choch5": "—"})

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


def htf_ref_tf(entry_tf=None):
    """UT de la tendance de fond utilisée par le mode M15 : M15 pour une entrée M1/M3/M5, H1 pour M15/M30, H4 pour H1."""
    return HTF_REF_TF.get(entry_tf or TIMEFRAME_MIN, 15)


def htf_trend_check(symbol, ev, trace=None):
    """Filtre HTF en mode M15 : le CHoCH d'entrée doit aller dans le sens de la tendance de fond (direction de la dernière
    cassure de structure de l'UT de référence, règles d'`analyze`). Aucun autre barrage (ni H1, ni M5, ni POI).
    Retourne (tendance 1 / -1 / 0, motif_de_refus) ; motif_de_refus vaut None quand le CHoCH suit la tendance de fond.
    Tendance indéterminée (pas assez de bougies, lecture impossible) -> refus : pas de signal sans contexte lisible."""
    tf = htf_ref_tf()
    lbl = _tf_lbl(tf)
    d = ev["dir"]
    side = "BUY" if d == 1 else "SELL"
    trend = _last_dir(_ctx_candles(symbol, tf))
    if trace is not None:
        trace["htf"] = f"{lbl} {_dir_txt(trend)}"
    if trend == 0:
        return 0, f"tendance de fond {lbl} indéterminée -- pas de signal sans contexte {lbl} lisible"
    if d != trend:
        return trend, (f"{side} contre la tendance de fond {lbl} ({_dir_txt(trend)}) -- seuls les "
                       f"{'BUY' if trend == 1 else 'SELL'} sont autorisés tant que {lbl} ne casse pas sa structure")
    return trend, None


_mtf_fib_cache = {}   # symbole -> {"tf", "leg_id", "dir", "hi", "lo", "mid", "mitigated"}


def mtf_fib_tf(entry_tf=None):
    """UT du Fibonacci HTF pour l'UT d'entrée `entry_tf` (minutes) : M1/M3 -> M15, M5 -> H1, M15 -> H4."""
    return MTF_FIB_TF.get(entry_tf or TIMEFRAME_MIN, 15)


def mtf_fib_zone(symbol, tf=None):
    """Un seul Fibonacci HTF actif par symbole : construit (mèches High/Low) sur le dernier swing HTF
    confirmé (dernier événement de structure d'`analyze`, UT `tf`), et mis à jour UNIQUEMENT quand ce
    swing change -- jamais plusieurs Fibonacci empilés/qui se chevauchent.

    0% / 50% / 100% sont toujours calculés du Low vers le High (Discount = 0-50%, Premium = 50-100%).
    'mitigated' mémorise qu'un retracement a atteint au moins 50% depuis que ce swing existe : une fois
    vrai, cela reste vrai même si le prix ressort ensuite de la zone (mitigation mémorisée, pas ponctuelle).
    None si pas assez de bougies HTF ou lecture impossible (le filtre doit alors refuser, jamais laisser passer)."""
    tf = tf or mtf_fib_tf()
    raw = _ctx_candles(symbol, tf)
    if len(raw) <= 2 * SWING_DEPTH + 1:
        return None
    evs = analyze(raw)
    if not evs:
        return None
    ev = evs[-1]
    i0 = ev.get("src")
    if i0 is None or i0 >= len(raw) - 1:
        return None
    seg = raw[i0:]                              # du swing d'origine (mèche) jusqu'à la dernière bougie HTF clôturée
    hi, lo = max(x["h"] for x in seg), min(x["l"] for x in seg)
    if hi <= lo:
        return None
    mid = (hi + lo) / 2.0
    d = ev["dir"]
    leg_id = (ev["t"], d)                        # identifie le swing HTF courant -> ne change que sur un nouveau swing
    cached = _mtf_fib_cache.get(symbol)
    if not cached or cached["tf"] != tf or cached["leg_id"] != leg_id:
        cached = {"tf": tf, "leg_id": leg_id, "dir": d, "hi": hi, "lo": lo, "mid": mid, "mitigated": False}
    else:
        cached.update(dir=d, hi=hi, lo=lo, mid=mid)   # même swing : les mèches peuvent encore s'étendre un peu
    reached_50 = (lo <= mid - 1e-9 and min(x["l"] for x in seg) <= mid) if d == 1 else (max(x["h"] for x in seg) >= mid)
    if reached_50:
        cached["mitigated"] = True
    _mtf_fib_cache[symbol] = cached
    return cached


def mtf_fib_pd_filter(symbol, ev, trace=None):
    """Filtre Fibonacci HTF Premium/Discount (voir get_mtf_fib_filter) : indépendant de htf_mode / htf_poi_setup,
    ne fait que filtrer un signal M1/M5 déjà produit. Retourne (autorisé: bool, motif_de_refus ou None)."""
    tf = mtf_fib_tf()
    lbl = _tf_lbl(tf)
    fib = mtf_fib_zone(symbol, tf)
    if fib is None:
        if trace is not None:
            trace["mtf_fib"] = f"Fibo HTF {lbl} indisponible"
        return False, f"Fibonacci HTF {lbl} indisponible -- pas assez de bougies / lecture impossible"
    d = ev["dir"]
    side = "BUY" if d == 1 else "SELL"
    zone = "Discount (0-50%)" if fib["dir"] == 1 else "Premium (50-100%)"
    if trace is not None:
        trace["mtf_fib"] = (f"{lbl} {_dir_txt(fib['dir'])} -- 0% {fib['lo']:.2f} / 50% {fib['mid']:.2f} / "
                             f"100% {fib['hi']:.2f} -- {zone} {'mitigée' if fib['mitigated'] else 'non mitigée'}")
    if d != fib["dir"]:
        return False, (f"{side} contre la tendance Fibonacci HTF {lbl} ({_dir_txt(fib['dir'])}) -- seuls les "
                        f"{'BUY' if fib['dir'] == 1 else 'SELL'} sont autorisés (filtre Premium/Discount)")
    if not fib["mitigated"]:
        return False, (f"retracement HTF {lbl} < 50% (50% = {fib['mid']:.2f}) -- zone {zone} pas encore "
                        f"atteinte, signal {side} ignoré tant que le retracement n'a pas atteint 50%")
    return True, None


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
    htf_mode, tp_rr, be_rr = get_htf_mode(), get_tp_rr(), get_be_rr()   # réglages Telegram, lus à chaque signal : effet immédiat
    htf_on = htf_mode != "OFF"
    setup = poi = None
    if htf_on and symbol:
        if htf_mode == "FULL":
            # HTF = contexte · POI = zone de réaction · CHoCH = déclencheur : plus de blocage automatique contre le biais H1
            setup, poi, why = htf_poi_setup(symbol, c, ev, trace)
            if why:
                return _rej(why)
        else:   # M15 : le CHoCH doit suivre la tendance de fond, rien d'autre
            trend, why = htf_trend_check(symbol, ev)
            if why:
                return _rej(why)
            setup = f"TENDANCE {_tf_lbl(htf_ref_tf())} {_dir_txt(trend).upper()}"
    # Filtre Fibonacci HTF Premium/Discount : totalement indépendant de htf_mode ci-dessus (peut être actif
    # même si htf_mode == OFF, et inversement) -- ne fait que filtrer un signal M1/M5 déjà produit par la
    # logique existante, jamais de nouvelle règle de score/OB/FVG. Voir use_mtf_fibonacci_pd_filter.
    mtf_fib_on = bool(symbol and get_mtf_fib_filter())
    if mtf_fib_on:
        ok, why = mtf_fib_pd_filter(symbol, ev, trace)
        if not ok:
            return _rej(why)
    d, a = ev["dir"], ev["atr"]
    entry = c[ev["i"]]["c"]
    sl = ev["sl_level"] - d * SL_BUFFER_ATR * a
    if (d == 1 and sl >= entry) or (d == -1 and sl <= entry):
        return _rej(f"SL structurel du mauvais côté de l'entrée (sl={sl:.2f}, entrée={entry:.2f})")
    risk = abs(entry - sl)
    # Le contrôle "BOS trop ancien/éloigné" doit juger la distance STRUCTURELLE d'origine (avant tout plancher) :
    # sinon, sur une UT basse (M1) où l'ATR est petit, le plancher en % ci-dessous dépasserait quasi toujours
    # 6xATR et bloquerait TOUS les signaux, même valides (c'est le bug qui a fait disparaître les signaux).
    if risk > MAX_SL_ATR * a:
        return _rej(f"SL trop large : {risk:.2f} pts > {MAX_SL_ATR}xATR ({MAX_SL_ATR * a:.2f} pts) "
                    f"-- niveau de référence trop ancien/éloigné")
    # Double plancher : ATR (MIN_SL_ATR) ET % du prix (min_sl_pct/MIN_SL_PCT) -- le plus grand des deux gagne.
    # Sans le plancher en %, un ATR minuscule (marché calme sur une UT basse comme M1) laissait passer des SL de
    # quelques points, balayés par le moindre bruit/spread : c'est ce plancher absolu qui l'empêche.
    min_pct = SYMBOLS.get(symbol, {}).get("min_sl_pct", MIN_SL_PCT) if symbol else MIN_SL_PCT
    min_risk = max(MIN_SL_ATR * a, entry * min_pct)
    if risk < min_risk:
        risk = min_risk
        sl = entry - d * risk
    sig = {
        "dir": d, "side": "BUY" if d == 1 else "SELL", "type": ev["type"],
        "order": "MARKET", "ref_price": entry,
        "entry": entry, "sl": sl, "risk": risk,
        "tp": entry + d * risk * tp_rr,
        "t": ev["t"], "bos_level": ev["sl_level"],
        "rr": tp_rr, "htf": htf_on, "htf_mode": htf_mode, "be_rr": be_rr, "tf": TF_LABEL,   # affichés sur le signal
        "setup": setup, "poi": poi,   # CONTINUATION (COMPLET) / TENDANCE M15 ... (M15) ; None si filtre HTF OFF
        "mtf_fib_on": mtf_fib_on, "mtf_fib": trace.get("mtf_fib"),   # affichage filtre Fibonacci HTF Premium/Discount
    }
    # Entrée LIMIT : retest du niveau cassé (la ligne du CHoCH), avec le même SL structurel.
    lvl = ev["level"]
    want_limit = ENTRY_MODE == "LIMIT" or (ENTRY_MODE == "BOTH" and abs(entry - lvl) > LIMIT_EXT_ATR * a)
    if want_limit and ((d == 1 and sl < lvl < entry) or (d == -1 and entry < lvl < sl)):
        risk_l = abs(lvl - sl)
        min_risk_l = max(MIN_SL_ATR * a, lvl * min_pct)
        if risk_l < min_risk_l:
            risk_l = min_risk_l
            sl = lvl - d * risk_l
        # si le prix a déjà dépassé le seuil d'annulation, la limit n'a plus de sens : on reste en DIRECT
        if (entry - lvl) * d < LIMIT_CANCEL_RR * risk_l:
            sig.update(order="LIMIT", entry=lvl, sl=sl, risk=risk_l, tp=lvl + d * risk_l * tp_rr)
    # Lecture liquidité externe / interne : AFFICHAGE (graphique + message), jamais une condition d'acceptation.
    sig["ext"], sig["sweep"] = liquidity_reading(symbol, c, ev, sig["entry"]) if symbol else (None, None)
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


def manual_sl_distance(symbol, entry):
    """Distance SL (en points, prix brut) pour un trade MANUEL déclenché depuis Telegram, en
    réutilisant le même plancher que le moteur automatique (build_signal) : max(MIN_SL_ATR x ATR,
    entrée x min_sl_pct). Un trade manuel n'a pas de CHoCH/structure à mesurer (pas de sl_level) —
    on ne réinvente donc pas un système de SL, on applique le plancher de risque déjà en place,
    qui est la seule partie de la logique SL existante réutilisable sans contexte de signal.
    Retourne None si l'ATR n'est pas calculable (pas assez de bougies)."""
    try:
        candles = get_candles(symbol)
        atr = atr_series(candles)[-1] if candles else None
    except Exception as e:
        print(f"[manuel] ATR indisponible pour {symbol} ({e}) — plancher % seul utilisé.")
        atr = None
    min_pct = SYMBOLS.get(symbol, {}).get("min_sl_pct", MIN_SL_PCT)
    candidates = [entry * min_pct]
    if atr:
        candidates.append(MIN_SL_ATR * atr)
    return max(candidates)


# ============================================================================
# 6. SUIVI DES POSITIONS
#
# Suivi des positions sur les bougies clôturées.
#
# Grille : RR{be_rr du trade} -> SL à l'entrée (BE) | RR{TP_RR} -> clôture finale (TP), pas de clôture partielle.
# Si SL et objectif sont touchés dans la même bougie, le SL est compté en premier (prudent).
# ============================================================================

def track_trade(trade, candles):
    """Fait avancer un trade avec les nouvelles bougies. Retourne la liste des événements.

    Grille simple : RR{be_rr du trade} -> SL déplacé à l'entrée (BE) ; RR{TP_RR} -> clôture finale (TP).
    Le RR du BE est propre à chaque trade (figé à la création du signal, comme le TP). Pas de clôture partielle.
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
    be_rr = trade_be_rr(trade)   # idem pour le BE
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

        # 2) progression : BE (armé au RR propre au trade), paliers RR1/RR2/RR3 (notification groupe), TP finale
        r_fav = (favorable - entry) * side / risk
        be_arm = False    # le BE vient d'être armé sur CETTE bougie
        if not trade["be_hit"] and be_rr < tp_rr and r_fav >= be_rr:
            trade["be_hit"], trade["sl"] = 1, entry
            be_arm = True
            pos_id = trade.get("mt5_position_id")
            if pos_id:   # position réellement exécutée côté broker (MetaApi) : on synchronise son SL
                move_to_breakeven(pos_id, entry, trade["side"], trade["symbol"], trade["lot"])
        be_told = False   # ... et il est annoncé avec un palier RR (sinon : événement BE_MOVED seul)
        for lvl in RR_LEVELS:
            if lvl > tp_rr:
                continue   # palier au-delà de la TP de ce trade : jamais atteint (trade clôturé avant)
            field = f"rr{int(lvl)}_hit"
            if not trade[field] and r_fav >= lvl:
                trade[field] = 1
                if lvl >= tp_rr:
                    continue   # palier = TP finale : annoncé par l'événement TP, pas en double
                be_now = be_arm and not be_told and lvl >= be_rr
                be_told = be_told or be_now
                events.append({"name": f"RR{int(lvl)}", "be_moved": be_now, "trade": dict(trade)})
        # BE armé à un RR qui n'est pas un palier annoncé (ex. 0.5, 1.5) : message dédié
        if be_arm and not be_told and r_fav < tp_rr:
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


# --- réconciliation broker (MetaApi) : détecte les positions fermées côté MT5 ------------------
# Indépendant du suivi RR ci-dessus (track_trade, qui avance sur les bougies clôturées) : ici on
# vérifie juste, côté broker, qu'une position censée être ouverte l'est toujours. Utile si le SL/TP
# a été exécuté par MT5 entre deux bougies, ou en cas de clôture manuelle/autre côté broker — cas
# que track_trade (qui ne regarde que les bougies) ne peut pas voir de lui-même.
MT5_SYNC_INTERVAL = _env_int("MT5_SYNC_INTERVAL", 60)   # secondes entre 2 réconciliations MetaApi


def _mt5_close_outcome(trade, deals):
    """Déduit l'issue (SL/TP/BE/CLOSED_EXT) et le résultat ($ et R) d'une position MT5 fermée côté
    broker, à partir de son historique de deals MetaApi. Best-effort : si la raison de clôture est
    absente ou inconnue (clôture manuelle, stop-out, etc.), l'issue retombe sur CLOSED_EXT plutôt
    que de deviner TP/SL à tort."""
    profit = sum((d.get("profit") or 0) + (d.get("commission") or 0) + (d.get("swap") or 0) for d in deals)
    reason = None
    for d in reversed(deals):   # dernier deal de sortie (DEAL_ENTRY_OUT / DEAL_ENTRY_OUT_BY) = celui qui a clôturé
        if d.get("entryType") in ("DEAL_ENTRY_OUT", "DEAL_ENTRY_OUT_BY"):
            reason = d.get("reason")
            break
    if reason == "DEAL_REASON_SL":
        outcome = "BE" if trade.get("be_hit") else "SL"   # SL déjà déplacé à l'entrée -> c'est un BE, pas une perte
    elif reason == "DEAL_REASON_TP":
        outcome = "TP"
    else:
        outcome = "CLOSED_EXT"   # clôture manuelle / stop-out / raison inconnue : pas de TP/SL supposé
    risk_usd = trade.get("risk_usd") or 0
    result_r = (profit / risk_usd) if risk_usd else None
    return outcome, result_r, profit


def sync_mt5_positions():
    """Réconciliation périodique, à appeler depuis la boucle principale (indépendamment du scan par
    symbole) : compare les trades OPEN en base ayant un mt5_position_id à la liste des positions
    réellement ouvertes côté broker (MetaApi). Si une position a disparu côté broker, le trade
    correspondant est marqué CLOSED en base avec l'issue déduite de l'historique MetaApi.

    Ne duplique jamais la grille RR (sl/be_hit/rrN_hit/paliers RR1-RR3), qui reste entièrement gérée
    par track_trade ci-dessus ; ne touche pas non plus aux trades sans mt5_position_id (mode
    "signal only", sans exécution MT5)."""
    trades = [t for t in open_trades() if t.get("mt5_position_id") and t["status"] == "OPEN"]
    if not trades:
        return

    try:
        broker_ids = _run_mt5(_mt5_open_position_ids_async())
    except Exception as e:
        print(f"[metaapi] sync positions : échec récupération des positions ouvertes : {e}")
        traceback.print_exc(limit=-3)
        return
    if broker_ids is None:
        return   # MetaApi indisponible : on ne conclut à aucune clôture plutôt que de clôturer à tort

    for t in trades:
        pos_id = str(t["mt5_position_id"])
        if pos_id in broker_ids:
            continue   # toujours ouverte côté broker : rien à faire ici (track_trade s'en occupe)

        try:
            deals = _run_mt5(_mt5_position_deals_async(pos_id))
        except Exception as e:
            print(f"[metaapi] sync positions : échec historique deals position {pos_id} : {e}")
            traceback.print_exc(limit=-3)
            deals = []

        outcome, result_r, profit = _mt5_close_outcome(t, deals or [])
        update_trade(t["id"], status="CLOSED", closed_ts=int(time.time()),
                     outcome=outcome, result_r=result_r, pnl_usd=profit)
        r_txt = f", {result_r:+.2f} R" if result_r is not None else ""
        print(f"[metaapi] Position {pos_id} fermée côté broker ({t['symbol']} {t['side']}) -> {outcome}{r_txt} "
              f"(base mise à jour).")
        to_admin(f"📒 {t['symbol']} {t['side']} — position fermée côté broker (MT5) : <b>{outcome}</b>{r_txt}")
        # Le trade n'est plus OPEN en base : track_trade ne l'annoncera donc jamais. On prévient le groupe ici
        # pour qu'un TP / SL / BE exécuté par MT5 entre deux bougies ne passe jamais sous silence.
        if outcome in ("TP", "SL", "BE"):
            try:
                row = _q("SELECT * FROM trades WHERE id=?", (t["id"],)).fetchone()
                tr = dict(row)
                if tr.get("result_r") is None:
                    tr["result_r"] = {"TP": trade_tp_rr(tr), "SL": -1.0, "BE": 0.0}[outcome]
                ev = {"name": outcome, "trade": tr}
                dec = SYMBOLS[t["symbol"]]["decimals"]
                to_group(group_event(ev, dec), reply_to_message_id=tr.get("signal_message_id"))
                group_event_media(outcome)
            except Exception:
                traceback.print_exc()


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

    # Lecture ext / int (sig["ext"], sig["sweep"]) : absente pour make_event_chart / anciens signaux -> simplement pas dessinée.
    ext, sw = sig.get("ext"), sig.get("sweep")
    if sw and sw.get("swept") and sw.get("level") is not None:   # niveau interne balayé : « SWEEP » (+ EQH / EQL si égaux)
        k = next((j for j, cc in enumerate(view) if cc["t"] == sw.get("t")), None)
        if k is not None:
            col = "#e040fb"
            ax.plot([max(0, k - SWEEP_LOOKBACK), k], [sw["level"]] * 2, color=col, lw=1.0)
            ax.plot([k], [sw["level"]], marker="x", color=col, ms=5)
            eq_txt = f" {_liq_lbl(sw['side'], True)}" if sw.get("eq") else ""
            ax.text(k, sw["level"], f"SWEEP{eq_txt}", color=col, fontsize=7, ha="center",
                    va="bottom" if sw["side"] == "haut" else "top")

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
    ext_edge = None
    if ext:   # liquidité externe visée : ligne pointillée si elle tient dans le cadre, sinon simple repère au bord du graphique
        lv, span = ext["level"], (hi - lo) or 1.0
        ext_lbl = f"EXT {_tf_lbl(ext['tf'])} {_liq_lbl(ext['side'], ext['eq'])} {lv:.{decimals}f}"
        if lo - 0.6 * span <= lv <= hi + 0.6 * span:
            lo, hi = min(lo, lv), max(hi, lv)
            ax.axhline(lv, color="#f5a623", lw=1.0, ls=(0, (1, 2.5)))
            ax.text(x1 + 0.5, lv, ext_lbl, color="#f5a623", fontsize=8, va="bottom")
        else:
            ext_edge = (lv > hi, ext_lbl)
    pad = (hi - lo) * 0.05
    ax.set_ylim(lo - pad, hi + pad)
    if ext_edge:
        up, lbl = ext_edge
        ax.text(0, (hi + pad) if up else (lo - pad), f"{lbl} {'↑' if up else '↓'}", color="#f5a623", fontsize=7,
                va="top" if up else "bottom")
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

    try:   # liquidité externe (ext_map : plus proche pivot non balayé de chaque côté, EQH / EQL signalés)
        m_ext = ext_map(symbol)
        sides = []
        for d_, word in ((1, "haut"), (-1, "bas")):
            x = ext_target(symbol, d_, price, m=m_ext)
            sides.append(_ext_short(x, dec) if x else f"{word} —")
        ext_map_txt = f"Externe ({_tf_lbl(m_ext['tf'])}) : " + " · ".join(sides)
    except Exception:
        ext_map_txt = "Externe (carte) : indisponible"
    try:   # liquidité interne (int_sweep) : dernier CHoCH M1 -> niveau balayé ou non
        chochs = [e_ for e_ in data[1]["events"] if e_["kind"] == "CHOCH"]
        if chochs:
            last_ch = chochs[-1]
            int_m1 = (f"Interne (M1) : dernier CHoCH {_dir_txt(last_ch['dir'])} — "
                      f"{_sweep_txt(sweep_reading(data[1]['raw'], last_ch, tf_lbl='M1'), dec)}")
        else:
            int_m1 = "Interne (M1) : aucun CHoCH M1 dans l'historique lu"
    except Exception:
        int_m1 = "Interne (M1) : indisponible"

    text = (
        f"📈 <b>ANALYSE TECHNIQUE — {symbol}</b>\n"
        f"Prix actuel : <b>{price:.{dec}f}</b>\n\n"
        f"<b>Structure multi-UT</b>\n" + "\n".join(struct_lines) + "\n\n"
        f"<b>Liquidité</b>\n"
        f"Externe ({htf_lbl}) : {ext_txt}\n"
        f"{ext_map_txt}\n"
        f"Interne (M5) : {retr['liq']}\n"
        f"{int_m1}\n\n"
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


def send(chat_id, text, photo=None, reply_markup=None, reply_to_message_id=None):
    if not chat_id:
        return None
    data = {"chat_id": chat_id, "parse_mode": "HTML"}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    if reply_to_message_id:
        # Répond au message original du signal (jamais recherché visuellement, jamais "le dernier du groupe") :
        # allow_sending_without_reply évite un crash Telegram si ce message a été supprimé entre-temps.
        data["reply_to_message_id"] = reply_to_message_id
        data["allow_sending_without_reply"] = True
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


def to_group(text, photo=None, reply_to_message_id=None):
    return send(CHAT_ID_GROUPE, text, photo, reply_to_message_id=reply_to_message_id)


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


def _htf_txt(mode):
    """Libellé du mode HTF : OFF / ON (M15) / ON (complet)."""
    if mode == "OFF":
        return "OFF"
    return "ON (complet)" if mode == "FULL" else f"ON ({_tf_lbl(htf_ref_tf())})"


def _params_lines(sig, tf=None):
    """Les 3 paramètres affichés sur un nouveau signal."""
    mode = sig.get("htf_mode")
    if mode not in HTF_MODES:
        htf = sig.get("htf")
        mode = get_htf_mode() if htf is None else (HTF_MODE if htf else "OFF")
    lines = (f"Timeframe : {tf or sig.get('tf') or TF_LABEL}\n"
             f"RR : {_sig_rr(sig):g}\n"
             f"Filtre HTF : {_htf_txt(mode)}")
    fib_line = _mtf_fib_line(sig)
    return f"{lines}\n{fib_line}" if fib_line else lines


def _mtf_fib_line(sig):
    """Ligne « Fibo HTF » d'un signal : None si le filtre Fibonacci HTF Premium/Discount est OFF."""
    if not sig.get("mtf_fib_on"):
        return None
    return f"Fibo HTF : {sig.get('mtf_fib') or 'ON'}"


def _be_line(sig):
    """Ligne « BE → RRx » d'un signal ; « BE : non utilisé » si le RR du BE n'est pas sous la TP finale."""
    be_rr = sig.get("be_rr") or BE_RR
    return f"BE → RR{be_rr:g}" if be_rr < _sig_rr(sig) else "BE : non utilisé (RR du BE ≥ TP)"


def group_signal(symbol, sig, position_n, dec, tf=None):
    """Message groupe ultra court : sens, entrée, SL, TP."""
    icon = "🟢" if sig["side"] == "BUY" else "🔴"
    limit = sig.get("order") == "LIMIT"
    label = f"{sig['side']} LIMIT" if limit else sig["side"]
    return (
        f"{icon} <b>{label} {symbol}</b> · {tf or TF_LABEL}\n"
        f"Entrée : {'limit ' if limit else '≈ '}{_fmt(sig['entry'], dec)}\n"
        f"🛡 SL : {_fmt(sig['sl'], dec)}\n"
        f"🎯 TP : {_fmt(sig['tp'], dec)}\n"
        f"{_be_line(sig)}"
    )


def admin_signal(symbol, sig, risk_usd, leverage, lot_info, margin, dec):
    """Message privé/admin : 1re ligne = devise + LOT (aperçu de notification), puis le détail."""
    label = f"{sig['side']} LIMIT" if sig.get("order") == "LIMIT" else sig["side"]
    icon = "🟢" if sig["side"] == "BUY" else "🔴"
    base = "ta limit" if sig.get("order") == "LIMIT" else "ton prix réel"
    txt = (
        f"<b>{symbol} · LOT {lot_info['lot']:g}</b>\n"
        f"{icon} {label} · Entrée ≈ {_fmt(sig['entry'], dec)}\n"
        f"🛡 SL {_fmt(sig['sl'], dec)} · 🎯 TP {_fmt(sig['tp'], dec)} (RR{_sig_rr(sig):g})\n"
        f"📐 Depuis {base} : SL {_dist(sig, sig['sl'], dec)} pts / TP {_dist(sig, sig['tp'], dec)} pts\n"
        f"Risque {risk_usd:g} $ · Levier {leverage:g}x · Marge ≈ {margin:.2f} $\n"
        f"{_be_line(sig)}"
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
    """Messages de suivi du groupe : une ligne, le strict nécessaire."""
    t, name = ev["trade"], ev["name"]
    head = f"{t['symbol']} {t['side']}"
    if name.startswith("RR"):
        return f"🟡 <b>{name} ✅</b> — {head}" + (" · SL → BE 🔒" if ev.get("be_moved") else "")
    if name == "FILLED":
        return f"✅ <b>LIMIT exécuté</b> — {head} @ {_fmt(t['entry'], dec)}"
    if name == "CANCELLED":
        return f"❌ <b>LIMIT annulé</b> — {head}"
    if name == "BE_MOVED":
        return f"🔒 <b>BE armé</b> — {head} · SL → entrée"
    r = t.get("result_r")
    r_txt = f" ({r:+.1f}R)" if r is not None else ""
    if name == "TP":
        return f"🎯 <b>TP ✅</b> — {head}{r_txt}"
    if name == "BE":
        return f"🟰 <b>BE touché</b> — {head} (0R)"
    return f"🔴 <b>SL ❌</b> — {head}{r_txt}"


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


# --- ⚙️ PARAMÈTRES SIGNAL : RR / timeframe d'entrée / filtre HTF / BE / signaux simultanés -----------
def _htf_mode_txt(mode=None):
    mode = mode or get_htf_mode()
    return {"OFF": "🔴 OFF", "M15": f"🟢 ON — tendance {_tf_lbl(htf_ref_tf())}", "FULL": "🟣 ON — complet"}[mode]


def _signal_text():
    return (f"⚙️ <b>PARAMÈTRES SIGNAL</b>\n\n"
            f"🎯 RR actuel : RR{get_tp_rr():g}\n"
            f"⏱ Timeframe : {TF_LABEL}\n"
            f"🧠 Filtre HTF : {_htf_mode_txt()}\n"
            f"📐 Filtre Fibo HTF : {'ON' if get_mtf_fib_filter() else 'OFF'}\n"
            f"🔒 BE à : RR{get_be_rr():g}\n"
            f"📍 Signaux ouverts max par actif : {get_max_positions()}")


def _signal_keyboard():
    return {"inline_keyboard": [
        [{"text": "🎯 RR", "callback_data": "sig:rr"}, {"text": "⏱ TIMEFRAME", "callback_data": "sig:tf"},
         {"text": "🧠 FILTRE HTF", "callback_data": "sig:htf"}],
        [{"text": "🔒 BE", "callback_data": "sig:be"}, {"text": "📍 SIGNAUX MAX", "callback_data": "sig:max"}],
        [{"text": "🌐 LIQ. EXTERNE", "callback_data": "sig:ext"}, {"text": "📐 FIBO HTF", "callback_data": "sig:fib"}],
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
    ref = _tf_lbl(htf_ref_tf())
    return (f"🧠 Filtre HTF : {_htf_mode_txt()}\n\n"
            f"🟢 {ref} : le CHoCH d'entrée doit suivre la tendance de fond {ref} (dernière cassure de structure {ref}). "
            f"{ref} baissier = uniquement des SELL, {ref} haussier = uniquement des BUY, rien d'autre n'est exigé. "
            f"(Entrée M1/M5 → M15 · entrée M15/M30 → H1 · entrée H1 → H4.)\n\n"
            f"🟣 COMPLET : H{HTF_MINUTES // 60 or 1} = biais, M15 = confirmation du contexte, M5 = fin du retracement, "
            f"POI (OB / FVG) mitigé = zone de réaction, CHoCH d'entrée = trigger final.\n\n"
            f"🔴 OFF : aucun filtre, tout CHoCH valide devient un signal (BUY ou SELL). Effet immédiat.")


def _signal_htf_keyboard():
    cur = get_htf_mode()
    return _signal_back([[
        {"text": ("✅ " if cur == "M15" else "") + f"🟢 {_tf_lbl(htf_ref_tf())}", "callback_data": "shtf:M15"},
        {"text": ("✅ " if cur == "FULL" else "") + "🟣 COMPLET", "callback_data": "shtf:FULL"},
        {"text": ("✅ " if cur == "OFF" else "") + "🔴 OFF", "callback_data": "shtf:OFF"}]])


def _signal_fib_text():
    on = get_mtf_fib_filter()
    return (f"📐 Filtre Fibonacci HTF Premium/Discount : <b>{'ON' if on else 'OFF'}</b>\n\n"
            f"UT Fibo : {_tf_lbl(mtf_fib_tf())} (auto selon le timeframe d'entrée {TF_LABEL})\n\n"
            f"🟢 ON : tendance HTF haussière → seuls les BUY sont autorisés (et seulement si le retracement a "
            f"atteint la zone Discount, 0-50%) ; tendance HTF baissière → seuls les SELL (zone Premium, 50-100%).\n\n"
            f"🔴 OFF : aucune contrainte Premium/Discount, tout CHoCH valide devient un signal (BUY ou SELL).\n\n"
            f"Indépendant du filtre HTF ci-dessus — les deux peuvent être combinés ou activés séparément. "
            f"Effet immédiat sur les NOUVEAUX signaux ; les trades déjà ouverts ne sont pas affectés.\n"
            f"Aussi réglable via /mtffib on ou /mtffib off.")


def _signal_fib_keyboard():
    on = get_mtf_fib_filter()
    return _signal_back([[
        {"text": ("✅ " if on else "") + "🟢 ON", "callback_data": "sfib:on"},
        {"text": ("✅ " if not on else "") + "🔴 OFF", "callback_data": "sfib:off"}]])


def _signal_be_text():
    return (f"🔒 BE actuel : <b>RR{get_be_rr():g}</b>\n"
            f"Quand le prix atteint ce RR, le SL est déplacé à l'entrée. Si le prix revient sur l'entrée : "
            f"« BE touché » (0 R), pas un SL. S'applique aux NOUVEAUX signaux : les trades déjà ouverts gardent leur BE. "
            f"Un BE égal ou supérieur au RR de la TP finale est ignoré. Autre valeur : /be 0.75")


def _signal_be_keyboard():
    cur = get_be_rr()
    return _signal_back([[{"text": ("✅ " if abs(cur - v) < 1e-9 else "") + f"RR{v:g}", "callback_data": f"sbe:{v}"}
                          for v in BE_RR_CHOICES]])


def _signal_max_text():
    return (f"📍 Signaux ouverts max par actif : <b>{get_max_positions()}</b>\n"
            f"1 = aucun nouveau signal sur un actif tant que le signal précédent n'est pas clôturé (TP, SL ou BE). "
            f"Effet immédiat. Autre valeur : /maxpos 4")


def _signal_max_keyboard():
    cur = get_max_positions()
    return _signal_back([[{"text": ("✅ " if cur == n else "") + str(n), "callback_data": f"smax:{n}"}
                          for n in MAX_POSITIONS_CHOICES]])


def _signal_ext_text():
    entry, ext = TIMEFRAME_MIN, get_ext_tf(TIMEFRAME_MIN)
    pair = f"{_tf_lbl(entry)} → ext. {_tf_lbl(ext)} / int. {_tf_lbl(entry)}"
    if entry == 5:
        return (f"🌐 <b>Liquidité externe</b>\n{pair}\n"
                f"UT de référence pour le biais + la liquidité externe, quand l'UT d'entrée est M5. Effet immédiat.")
    fixed = ", ".join(f"{_tf_lbl(k)}→{_tf_lbl(v)}" for k, v in LIQ_EXT.items() if k != 5)
    return (f"🌐 <b>Liquidité externe</b>\n{pair}\n"
            f"Fixe pour l'UT d'entrée {_tf_lbl(entry)} ({fixed}). Réglable (H1/H4) uniquement quand l'UT d'entrée "
            f"est M5 (menu ⏱ Timeframe → M5).")


def _signal_ext_keyboard():
    if TIMEFRAME_MIN != 5:
        return _signal_back([])
    cur = get_ext_tf(5)
    return _signal_back([[{"text": ("✅ " if cur == 60 else "") + "H1", "callback_data": "sext:60"},
                          {"text": ("✅ " if cur == 240 else "") + "H4", "callback_data": "sext:240"}]])


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


# --- 🏦 TRADING MANUEL (Telegram) --------------------------------------------------------------
# Section isolée, ajoutée sans toucher au moteur de signal automatique. Utilise l'API déjà
# connectée (MetaApi REST / MT5) — jamais de 2e connexion, jamais de nom de broker codé en dur :
# le nom réel vient de get_account_information() (voir get_account_summary).
_MANUAL_SYMS = {"XAU": "XAUUSD", "BTC": "BTCUSD"}


def _compte_text():
    s = get_account_summary()
    if not s.get("connected"):
        return "🔴 <b>API : DÉCONNECTÉE</b>\n\nMetaApi n'est pas joignable pour le moment — réessaie dans quelques secondes."
    cur = s.get("currency") or ""
    n_pos = len(s.get("positions") or [])
    return (
        f"🟢 <b>API : CONNECTÉ</b>\n"
        f"🏦 Broker : {s.get('broker')}\n"
        f"💰 Balance : {s.get('balance'):,.2f} {cur}\n".replace(",", " ")
        + f"📊 Equity : {s.get('equity'):,.2f} {cur}\n".replace(",", " ")
        + f"💳 Marge disponible : {s.get('free_margin'):,.2f} {cur}\n".replace(",", " ")
        + f"📌 Positions ouvertes : {n_pos}"
    )


def _compte_keyboard():
    return _with_back({"inline_keyboard": [
        [{"text": "🔄 Actualiser", "callback_data": "acc:home"}, {"text": "📌 Positions", "callback_data": "mpos:home"}]]})


def _manuel_text():
    auto_on = True   # le moteur automatique tourne toujours (pas d'ON/OFF global sur ce bot) — affiché pour repère
    return (f"🤖 AUTO : {'ON' if auto_on else 'OFF'}\n"
            f"👤 MANUEL : {'ON' if get_manual_mode() else 'OFF'}\n\n"
            f"Quand MANUEL est OFF, les boutons BUY/SELL du menu 📈 TRADE n'exécutent aucun ordre.\n"
            f"💵 Risque par trade : {get_risk():g} $ (menu 💰 Risque)\n"
            f"🎯 RR : {get_tp_rr():g} (menu ⚙️ Paramètres signal)")


def _manuel_keyboard():
    on = get_manual_mode()
    return _with_back({"inline_keyboard": [[
        {"text": ("✅ " if on else "") + "🟢 ON", "callback_data": "man:on"},
        {"text": ("✅ " if not on else "") + "🔴 OFF", "callback_data": "man:off"}]]})


def _trade_home_text():
    return "📈 <b>TRADE</b>\n\nChoisis l'actif." + ("" if get_manual_mode() else
           "\n\n⚠️ Trading manuel OFF (menu 👤 Manuel) — aucun ordre ne partira tant qu'il n'est pas activé.")


def _trade_home_keyboard():
    return _with_back({"inline_keyboard": [
        [{"text": "🟠 XAUUSD", "callback_data": "mtr:sym:XAU"}, {"text": "₿ BTCUSD", "callback_data": "mtr:sym:BTC"}]]})


def _trade_side_text(symbol):
    return (f"📈 <b>{symbol}</b>\n\n"
            f"💵 Risque : {get_risk():g} $ · 🎯 RR : {get_tp_rr():g}\n"
            f"Le lot, le SL et le TP sont calculés automatiquement au prix réel du broker au moment du clic.\n\n"
            f"⚡️ Le clic exécute directement l'ordre (pas de confirmation).")


def _trade_side_keyboard(symbol):
    code = next(k for k, v in _MANUAL_SYMS.items() if v == symbol)
    return {"inline_keyboard": [
        [{"text": "🟢 BUY", "callback_data": f"mtr:buy:{code}"}, {"text": "🔴 SELL", "callback_data": f"mtr:sell:{code}"}],
        [{"text": "🔙 Trade", "callback_data": "mtr:home"}, {"text": "🔙 Menu", "callback_data": "menu:home"}]]}


def _manual_order_text(r):
    if not r["ok"]:
        return f"❌ <b>Ordre refusé</b>\n\nCause : {r['error']}"
    dec = SYMBOLS[r["symbol"]]["decimals"]
    warn = "\n⚠️ Lot minimum appliqué : risque réel différent du montant configuré." if r["raised_to_min"] else ""
    return (f"✅ <b>{r['symbol']} {r['side']} exécuté</b>\n\n"
            f"Entrée : {_fmt(r['entry'], dec)}\n"
            f"Lot : {r['lot']:g}\n"
            f"SL : {_fmt(r['sl'], dec)}\n"
            f"TP : {_fmt(r['tp'], dec)}\n"
            f"Spread : {_fmt(r['spread'], dec)}\n"
            f"Risque : {r['risk_usd']:.2f} $\n"
            f"RR : 1:{r['rr']:g}{warn}")


def _pos_side(p):
    return "BUY" if str(p.get("type", "")).endswith("BUY") else "SELL"


def _mpositions_text():
    s = get_account_summary()
    if not s.get("connected"):
        return "🔴 API déconnectée — positions indisponibles."
    positions = s.get("positions") or []
    if not positions:
        return "📌 <b>POSITIONS (broker)</b>\n\nAucune position ouverte."
    lines = []
    for p in positions:
        sym = _internal_symbol(p.get("symbol", ""))
        dec = SYMBOLS.get(sym, {}).get("decimals", 2)
        side = _pos_side(p)
        icon = "🟢" if side == "BUY" else "🔴"
        sl = p.get("stopLoss")
        tp = p.get("takeProfit")
        lines.append(f"{icon} <b>{sym} {side}</b> · lot {p.get('volume')}\n"
                     f"Entrée : {_fmt(p.get('openPrice', 0), dec)}\n"
                     f"SL : {_fmt(sl, dec) if sl else '—'} · TP : {_fmt(tp, dec) if tp else '—'}\n"
                     f"P&L : {p.get('profit', 0):.2f} $ · ticket {p.get('id')}")
    return "📌 <b>POSITIONS (broker)</b>\n\n" + "\n\n".join(lines)


def _mpositions_keyboard():
    s = get_account_summary()
    rows = []
    for p in (s.get("positions") or []) if s.get("connected") else []:
        pid = str(p.get("id"))
        sym = _internal_symbol(p.get("symbol", ""))
        rows.append([{"text": f"🟢 BE+frais {sym} #{pid}", "callback_data": f"mpos:be:{pid}"},
                     {"text": f"🛑 Fermer #{pid}", "callback_data": f"mpos:close:{pid}"}])
    rows.append([{"text": "🔄 Actualiser", "callback_data": "mpos:home"}])
    return _with_back({"inline_keyboard": rows})


def _menu_keyboard():
    return {"inline_keyboard": [
        [{"text": "💰 Risque", "callback_data": "menu:risque"}, {"text": "⚙️ Levier", "callback_data": "menu:levier"}],
        [{"text": "⏱ Timeframe", "callback_data": "menu:timeframe"}, {"text": "🎭 Médias", "callback_data": "menu:medias"}],
        [{"text": "📊 Stats", "callback_data": "menu:stats"}, {"text": "📈 Positions", "callback_data": "menu:trades"}],
        [{"text": "📊 Analyse", "callback_data": "ana:home"}],
        [{"text": "⚙️ Paramètres signal", "callback_data": "menu:signal"}],
        [{"text": "🏦 Compte", "callback_data": "acc:home"}, {"text": "👤 Manuel", "callback_data": "man:home"}],
        [{"text": "📈 Trade", "callback_data": "mtr:home"}, {"text": "📌 Positions broker", "callback_data": "mpos:home"}],
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
               "/signal — paramètres du signal (RR, timeframe, filtre HTF, BE, signaux max)\n"
               "/be [RR] — RR auquel le SL passe à l'entrée (ex. /be 1)\n"
               "/maxpos [n] — signaux ouverts max par actif (1 = pas de doublon)\n"
               "/mtffib [on|off] — filtre Fibonacci HTF Premium/Discount\n"
               "/medias — stickers / images / GIF du groupe (TP, SL, BE, motivation)\n"
               "/analyse — analyse technique / fondamentale à la demande (BTCUSD, XAUUSD)\n"
               "/compte — statut API/broker, balance, equity, marge\n"
               "/manuel [on|off] — active/désactive le trading manuel Telegram\n"
               "/trade — menu BUY/SELL manuel (exécution directe, sans confirmation)\n"
               "/mpositions — positions réelles du broker + BE/Fermer\n"
               "/spread [%] — seuil de spread max au-delà duquel un ordre manuel est refusé\n"
               "/slset <ticket> <valeur> — modifie le SL d'une position\n"
               "/tpset <ticket> <valeur> — modifie le TP d'une position\n"
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
    if cmd == "/be":
        if len(parts) > 1:
            try:
                v = float(parts[1].replace(",", "."))
                if not 0 < v <= 20:
                    raise ValueError
                set_be_rr(v)
            except ValueError:
                return ("RR invalide. Exemple : /be 1 ou /be 0.5", None)
        return (_signal_be_text(), _signal_be_keyboard())
    if cmd in ("/mtffib", "/fibohtf"):
        if len(parts) > 1:
            arg = parts[1].strip().lower()
            if arg in ("on", "1", "true", "activer", "activé"):
                set_mtf_fib_filter(True)
            elif arg in ("off", "0", "false", "desactiver", "désactiver"):
                set_mtf_fib_filter(False)
            else:
                return ("Valeur invalide. Exemple : /mtffib on  ou  /mtffib off", None)
        on = get_mtf_fib_filter()
        return (f"📐 Filtre Fibonacci HTF Premium/Discount : <b>{'ON' if on else 'OFF'}</b>\n"
                f"UT Fibo : {_tf_lbl(mtf_fib_tf())} (auto selon le timeframe d'entrée {TF_LABEL})\n"
                f"Change avec /mtffib on ou /mtffib off.", None)
    if cmd in ("/maxpos", "/signauxmax"):
        if len(parts) > 1:
            try:
                v = int(parts[1])
                if not 1 <= v <= 10:
                    raise ValueError
                set_max_positions(v)
            except ValueError:
                return ("Valeur invalide (1 à 10). Exemple : /maxpos 1", None)
        return (_signal_max_text(), _signal_max_keyboard())
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
    if cmd == "/compte":
        return (_compte_text(), _compte_keyboard())
    if cmd == "/manuel":
        if len(parts) > 1:
            arg = parts[1].strip().lower()
            if arg in ("on", "1", "true", "activer", "activé"):
                set_manual_mode(True)
            elif arg in ("off", "0", "false", "desactiver", "désactiver"):
                set_manual_mode(False)
        return (_manuel_text(), _manuel_keyboard())
    if cmd == "/trade":
        return (_trade_home_text(), _trade_home_keyboard())
    if cmd == "/mpositions":
        return (_mpositions_text(), _mpositions_keyboard())
    if cmd == "/spread":
        if len(parts) > 1:
            try:
                v = float(parts[1].replace(",", ".").replace("%", "")) / 100
                if not 0 <= v <= 0.05:
                    raise ValueError
                set_max_spread_pct(v)
            except ValueError:
                return ("Valeur invalide (0 à 5). Exemple : /spread 0.15  (0 = désactive le contrôle)", None)
        cur = get_max_spread_pct()
        return (f"📏 Seuil spread max (trading manuel) : <b>{cur * 100:.3f}%</b>\n"
                f"Un ordre manuel est refusé si le spread au clic dépasse ce %. 0 = jamais refusé.\n"
                f"Change avec /spread 0.15 (en %).", None)
    if cmd in ("/slset", "/tpset"):
        if len(parts) < 3:
            return (f"Exemple : {cmd} 123456789 2650.50 (ticket, nouvelle valeur)", None)
        try:
            pos_id, value = parts[1], float(parts[2].replace(",", "."))
        except ValueError:
            return ("Valeur invalide. Exemple : /slset 123456789 2650.50", None)
        try:
            async def _do():
                conn = await get_metaapi_connection()
                if conn is None:
                    raise RuntimeError("connexion MetaApi indisponible")
                if cmd == "/slset":
                    return await conn.modify_position(pos_id, stop_loss=value)
                return await conn.modify_position(pos_id, take_profit=value)
            _run_mt5(_do())
        except Exception as e:
            return (f"❌ Modification refusée : {e}", None)
        label = "SL" if cmd == "/slset" else "TP"
        return (f"✅ {label} de la position {pos_id} mis à jour : {value:g}", None)
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
            elif page == "ext":
                _edit(cq, _signal_ext_text(), _signal_ext_keyboard())
            elif page == "be":
                _edit(cq, _signal_be_text(), _signal_be_keyboard())
            elif page == "max":
                _edit(cq, _signal_max_text(), _signal_max_keyboard())
            elif page == "fib":
                _edit(cq, _signal_fib_text(), _signal_fib_keyboard())
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
            arg = data[5:]
            mode = {"1": HTF_MODE, "0": "OFF"}.get(arg, arg)   # « 1 » / « 0 » : anciens boutons d'un message déjà envoyé
            if mode in HTF_MODES:
                set_htf_mode(mode)
                _edit(cq, _signal_text(), _signal_keyboard())
                ack = "Filtre HTF : " + _htf_txt(mode)
        elif data.startswith("sfib:"):
            arg = data[5:]
            if arg in ("on", "off"):
                set_mtf_fib_filter(arg == "on")
                _edit(cq, _signal_text(), _signal_keyboard())
                ack = "Filtre Fibo HTF : " + ("ON" if arg == "on" else "OFF")
        elif data.startswith("sbe:"):
            v = float(data[4:])
            if any(abs(v - x) < 1e-9 for x in BE_RR_CHOICES):
                set_be_rr(v)
                _edit(cq, _signal_text(), _signal_keyboard())
                ack = f"BE à RR{v:g}"
        elif data.startswith("smax:"):
            n = int(data[5:])
            if n in MAX_POSITIONS_CHOICES:
                set_max_positions(n)
                _edit(cq, _signal_text(), _signal_keyboard())
                ack = f"{n} signal(s) max"
        elif data.startswith("sext:"):
            v = int(data[5:])
            if TIMEFRAME_MIN == 5 and v in (60, 240):
                set_ext_tf_m5(v)
                _edit(cq, _signal_ext_text(), _signal_ext_keyboard())
                ack = _tf_lbl(v)
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
        elif data == "acc:home":
            _edit(cq, _compte_text(), _compte_keyboard())
            ack = "Actualisé"
        elif data == "man:home":
            _edit(cq, _manuel_text(), _manuel_keyboard())
        elif data == "man:on":
            set_manual_mode(True)
            _edit(cq, _manuel_text(), _manuel_keyboard())
            ack = "Manuel : ON"
        elif data == "man:off":
            set_manual_mode(False)
            _edit(cq, _manuel_text(), _manuel_keyboard())
            ack = "Manuel : OFF"
        elif data == "mtr:home":
            _edit(cq, _trade_home_text(), _trade_home_keyboard())
        elif data.startswith("mtr:sym:"):
            sym = _MANUAL_SYMS.get(data[8:])
            if sym:
                _edit(cq, _trade_side_text(sym), _trade_side_keyboard(sym))
        elif data.startswith("mtr:buy:") or data.startswith("mtr:sell:"):
            side = "BUY" if data.startswith("mtr:buy:") else "SELL"
            code = data.split(":")[2]
            sym = _MANUAL_SYMS.get(code)
            if not sym:
                pass
            elif not get_manual_mode():
                _post("answerCallbackQuery",
                      {"callback_query_id": cq["id"], "text": "👤 Manuel OFF — active-le d'abord.", "show_alert": True})
                return
            else:
                _post("answerCallbackQuery", {"callback_query_id": cq["id"], "text": f"⏳ {side} {sym}…"})
                r = place_manual_order(sym, side)
                _edit(cq, _manual_order_text(r), _trade_side_keyboard(sym))
                if r["ok"]:
                    to_admin(f"👤 Trade manuel : {_manual_order_text(r)}")
                return
        elif data == "mpos:home":
            _edit(cq, _mpositions_text(), _mpositions_keyboard())
            ack = "Actualisé"
        elif data.startswith("mpos:be:"):
            pid = data[8:]
            positions = (get_account_summary().get("positions") or [])
            p = next((p for p in positions if str(p.get("id")) == pid), None)
            if p is None:
                ack = "Position introuvable (déjà fermée ?)"
            else:
                sym = _internal_symbol(p.get("symbol", ""))
                side = _pos_side(p)
                lot = float(p.get("volume") or 0.0)
                ok = move_to_breakeven(pid, float(p.get("openPrice", 0)), side, sym, lot)
                ack = "✅ BE appliqué" if ok else "❌ Échec BE (voir logs)"
            _edit(cq, _mpositions_text(), _mpositions_keyboard())
        elif data.startswith("mpos:close:"):
            pid = data[11:]
            ok, err = close_mt5_position(pid)
            ack = "✅ Position fermée" if ok else f"❌ {err}"
            _edit(cq, _mpositions_text(), _mpositions_keyboard())
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
    max_pos = get_max_positions()
    if n_open >= max_pos:
        print(f"[{symbol}] {sig['side']} {sig['type']} ignoré ({n_open}/{max_pos} signal(s) encore ouvert(s) : pas de doublon)")
        return

    risk_usd = get_risk()
    leverage = get_leverage()
    lot_info = calc_lot(symbol, risk_usd, sig["risk"])
    if lot_info["lot"] <= 0:
        print(f"[{symbol}] signal ignoré : lot invalide")
        return
    margin = calc_margin(symbol, lot_info["lot"], sig["entry"], leverage)

    is_market = sig.get("order", "MARKET") == "MARKET"
    mt5_enabled = bool(METAAPI_TOKEN and METAAPI_ACCOUNT_ID)
    # Le trade est TOUJOURS suivi (OPEN, ou PENDING pour une LIMIT) dès sa création : le suivi RR / BE / TP / SL
    # sur le groupe et le bot ne dépend jamais de l'exécution MT5. L'exécution est tracée à part (exec_status).
    initial_status = "PENDING" if sig.get("order") == "LIMIT" else "OPEN"

    trade_id = add_trade(
        status=initial_status,
        order_type=sig.get("order", "MARKET"), placed_ts=sig["t"], ref_price=sig.get("ref_price"),
        sent_admin=0, sent_group=0,   # passent à 1 seulement quand Telegram a confirmé l'envoi
        signal_key=key, symbol=symbol, side=sig["side"], kind=sig["type"], timeframe=TF_LABEL,
        entry=sig["entry"], sl=sig["sl"], sl_initial=sig["sl"], tp=sig["tp"],
        be_rr=sig.get("be_rr"), htf_mode=sig.get("htf_mode"),
        risk_usd=lot_info["real_risk"],  # risque réel du lot pris (= risque demandé sauf lot minimum)
        lot=lot_info["lot"], leverage=leverage, opened_ts=now_ts(), last_ts=sig["t"],
        exec_status="PENDING" if (is_market and mt5_enabled) else None)
    print(f"[{symbol}] SIGNAL {sig['side']} {sig.get('order', 'MARKET')} {sig['type']} @ {sig['entry']:.{dec}f} "
          f"(bougie {_ts_str(sig['t'])})")

    # 1) ENVOI IMMÉDIAT du signal (admin + groupe), AVANT toute tentative d'exécution MT5.
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

    # 2) Exécution MT5 ensuite, dans un thread dédié : n'interrompt ni le scan ni le suivi des autres trades.
    #    Un échec est notifié à l'admin mais n'arrête JAMAIS le suivi TP / SL / BE du signal.
    if is_market and mt5_enabled:
        threading.Thread(target=_run_mt5_execution, name=f"mt5-exec-{trade_id}", daemon=True,
                         args=(trade_id, symbol, sig, lot_info["lot"])).start()


def _run_mt5_execution(trade_id, symbol, sig, lot):
    """Exécute l'ordre MT5 d'un signal déjà publié et suivi. Ne modifie jamais le statut de suivi (OPEN)."""
    try:
        mt5_position_id = execute_mt5_order(symbol, sig["side"], lot, sig["entry"], sig["sl"], sig["tp"],
                                            client_id=f"AB{trade_id}")
    except Exception as e:   # filet de sécurité : ce thread ne doit jamais mourir en silence
        print(f"[{symbol}] exécution MT5 #{trade_id} : erreur inattendue : {e}")
        traceback.print_exc(limit=-3)
        mt5_position_id = None

    if mt5_position_id:
        update_trade(trade_id, mt5_position_id=mt5_position_id, exec_status="OK")
        # Si le BE a été armé pendant que l'ordre s'exécutait, on répercute le SL sur la position broker.
        row = _q("SELECT status, be_hit, side, entry, symbol, lot FROM trades WHERE id=?", (trade_id,)).fetchone()
        if row and row["status"] == "OPEN" and row["be_hit"]:
            move_to_breakeven(mt5_position_id, row["entry"], row["side"], row["symbol"], row["lot"])
        to_admin(f"✅ {symbol} {sig['side']} — ordre exécuté sur MT5 (ticket {mt5_position_id}).")
        return

    update_trade(trade_id, exec_status="FAILED")
    if not _metaapi_ready.is_set():
        why = "MetaApi indisponible au moment du signal (API injoignable / compte MT5 non connecté)."
    else:
        why = "le broker a refusé l'ordre ou la confirmation a échoué (voir logs)."
    to_admin(f"⚠️ {symbol} {sig['side']} {sig['type']} (trade #{trade_id})\n"
             f"❌ ORDRE NON EXÉCUTÉ sur MT5 : {why}\n"
             f"Le suivi RR / BE / TP / SL continue normalement sur le groupe et ici. "
             f"Ordre à ouvrir manuellement si besoin.")


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
                fields = {f"sent_{who}": 1}
                if who == "group" and res:
                    # Mémorise le message_id RÉEL renvoyé par Telegram : c'est lui, et lui seul, qui sera
                    # utilisé comme reply_to_message_id pour RR1/RR2/BE/TP/SL de CE trade (jamais deviné).
                    msg_id = (res.get("result") or {}).get("message_id")
                    if msg_id:
                        fields["signal_message_id"] = msg_id
                update_trade(trade_id, **fields)
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
               "order": t.get("order_type") or "MARKET", "ref_price": t.get("ref_price"), "tf": t["timeframe"],
               "be_rr": t.get("be_rr"), "htf_mode": t.get("htf_mode")}
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
            # Réponse au message Telegram EXACT du signal d'ouverture de CE trade (jamais le dernier
            # message du groupe) : évite tout mélange entre trades ouverts simultanément (ex. XAUUSD / BTCUSD).
            to_group(group_event(ev, dec), photo=chart,
                     reply_to_message_id=ev["trade"].get("signal_message_id"))
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


BE_WATCH_INTERVAL = _env_int("BE_WATCH_INTERVAL", 7)   # secondes entre deux vérifications BE temps réel (5-10s conseillé)


def check_be_realtime():
    """Vérifie le BE en dehors du cycle de bougies : un tick suffit à armer le SL dès que le RR
    propre au trade (be_rr) est atteint, sans attendre la clôture de la bougie en cours.

    Ne duplique jamais track_trade : ne touche que be_hit/sl (et rr{N}_hit si le palier coïncide),
    jamais TP/SL final (une clôture ne peut être décidée qu'à la clôture de bougie, avec l'historique
    complet — ici on n'a qu'un tick isolé). Notifie Telegram immédiatement (pas d'attente du scan)."""
    dec_by_symbol = {s: SYMBOLS[s]["decimals"] for s in SYMBOLS}
    for trade in open_trades():
        if trade["status"] != "OPEN" or trade["be_hit"]:
            continue
        be_rr, tp_rr = trade_be_rr(trade), trade_tp_rr(trade)
        if be_rr >= tp_rr:
            continue   # BE désactivé pour ce trade (voir _be_line)
        price = get_live_price(trade["symbol"])
        if price is None:
            continue
        side = 1 if trade["side"] == "BUY" else -1
        entry, risk = trade["entry"], abs(trade["entry"] - trade["sl_initial"])
        if risk <= 0:
            continue
        # sécurité : si le SL a déjà été touché entre-temps (tick défavorable), on laisse track_trade
        # gérer la clôture au prochain scan — on n'arme jamais un BE sur un trade déjà perdant du tick.
        adverse_hit = (price <= trade["sl"]) if side == 1 else (price >= trade["sl"])
        if adverse_hit:
            continue
        r_fav = (price - entry) * side / risk
        if r_fav < be_rr:
            continue

        trade["be_hit"], trade["sl"] = 1, entry
        pos_id = trade.get("mt5_position_id")
        if pos_id:
            move_to_breakeven(pos_id, entry, trade["side"], trade["symbol"], trade["lot"])
        update_trade(trade["id"], be_hit=1, sl=entry)
        print(f"[be-realtime] {trade['symbol']} {trade['side']} #{trade['id']} : BE armé à RR{be_rr:g} "
              f"(prix {price:g}) — SL -> entrée.")
        ev = {"name": "BE_MOVED", "trade": dict(trade)}
        dec = dec_by_symbol[trade["symbol"]]
        to_group(group_event(ev, dec))
        txt = admin_event(ev)
        if txt:
            to_admin(txt)


def _be_watch_loop():
    """Boucle dédiée BE, en parallèle de _trading_loop (qui reste cadencée sur les bougies) :
    tourne toutes les BE_WATCH_INTERVAL secondes, ne fait qu'armer le BE au tick — jamais de TP/SL,
    jamais de nouveau signal, pour ne jamais interférer avec la logique bougie par bougie."""
    while True:
        try:
            check_be_realtime()
        except Exception:
            traceback.print_exc()
        time.sleep(BE_WATCH_INTERVAL)


def _trading_loop():
    """La boucle de scan (symboles + rapport quotidien), tourne en continu en arrière-plan."""
    try:
        last_hb = 0.0
        last_mt5_sync = 0.0
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
            if time.time() - last_mt5_sync >= MT5_SYNC_INTERVAL:   # réconciliation broker, indépendante du scan par symbole
                last_mt5_sync = time.time()
                try:
                    sync_mt5_positions()
                except Exception:
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
        threading.Thread(target=_be_watch_loop, daemon=True).start()
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
    threading.Thread(target=_be_watch_loop, daemon=True).start()
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
    start_metaapi_background()   # connexion MT5 persistante en arrière-plan, indépendante des signaux M1
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


class TestGetExtTf(unittest.TestCase):
    """get_ext_tf : UT de liquidité externe par UT d'entrée — fixe (LIQ_EXT) sauf pour M5, réglable en base
    ('ext_tf_m5') pour M5."""

    def test_get_ext_tf(self):
        for entry_tf, expected in LIQ_EXT.items():
            if entry_tf != 5:
                self.assertEqual(get_ext_tf(entry_tf), expected, f"entrée {_tf_lbl(entry_tf)}")
        self.assertEqual(get_ext_tf(5), LIQ_EXT[5], "M5 sans réglage en base -> défaut H1")
        set_ext_tf_m5(240)
        self.assertEqual(get_ext_tf(5), 240, "M5 après réglage en base -> H4")
        set_ext_tf_m5(60)
        self.assertEqual(get_ext_tf(5), 60, "M5 remis à H1")


class TestLiquiditePure(unittest.TestCase):
    """ext_map / int_sweep (section 4) : fonctions pures, ne touchent ni build_signal ni htf_poi_setup."""

    def _seed_ctx(self, tf, candles):
        _ctx_cache[(_ST_SYM, tf)] = (time.time(), candles)

    def test_pivot_balaye_exclu_non_balaye_inclus(self):
        _apply_timeframe(1)   # entrée M1 -> UT externe LIQ_EXT[1] = M15
        tf, base, margin = get_ext_tf(1), 2000.0, EXT_DEPTH + 3

        def _series(sweep_level=None):
            n = 2 * margin + 1
            c = [{"t": _ST_T0 + i * tf * 60, "o": base, "h": base, "l": base, "c": base} for i in range(n)]
            c[margin] = {**c[margin], "h": base + 10}   # pivot high isolé, non balayé
            if sweep_level is not None:
                pad = [{"t": c[-1]["t"] + (i + 1) * tf * 60, "o": base, "h": base, "l": base, "c": base}
                       for i in range(EXT_DEPTH + 2)]
                sweep = [{"t": pad[-1]["t"] + tf * 60, "o": base, "h": sweep_level, "l": base, "c": base}]
                c = c + pad + sweep + [{"t": sweep[-1]["t"] + (i + 1) * tf * 60, "o": base, "h": base,
                                        "l": base, "c": base} for i in range(EXT_DEPTH + 2)]
            return c

        self._seed_ctx(tf, _series())
        highs_ok = ext_map(_ST_SYM)["highs"]
        self.assertTrue(any(pv["level"] == base + 10 for pv in highs_ok), "pivot non balayé absent du résultat")

        self._seed_ctx(tf, _series(sweep_level=base + 20))
        highs_swept = ext_map(_ST_SYM)["highs"]
        self.assertFalse(any(pv["level"] == base + 10 for pv in highs_swept), "pivot balayé n'a pas été exclu")

    def test_eql_detectee(self):
        _apply_timeframe(1)
        tf, base, margin = get_ext_tf(1), 2000.0, EXT_DEPTH + 3

        def _flat(n, t0):
            return [{"t": t0 + i * tf * 60, "o": base, "h": base, "l": base, "c": base} for i in range(n)]

        c = _flat(margin, _ST_T0)
        c.append({"t": c[-1]["t"] + tf * 60, "o": base, "h": base, "l": base - 10, "c": base})   # creux 1
        c += _flat(2 * margin, c[-1]["t"] + tf * 60)   # écart large : fenêtres de confirmation disjointes
        c.append({"t": c[-1]["t"] + tf * 60, "o": base, "h": base, "l": base - 10, "c": base})   # creux 2, même niveau
        c += _flat(margin, c[-1]["t"] + tf * 60)

        self._seed_ctx(tf, c)
        lows = ext_map(_ST_SYM)["lows"]
        eql = [pv for pv in lows if pv["level"] == base - 10]
        self.assertEqual(len(eql), 2, "les deux creux au même niveau doivent être détectés")
        self.assertTrue(all(pv["eq"] for pv in eql), "les deux creux au même niveau doivent être marqués eq (EQL)")

    def test_sweep_interne(self):
        base = 2000.0
        prior = [{"t": _ST_T0 + i * 60, "o": base, "h": base, "l": base, "c": base} for i in range(SWEEP_LOOKBACK)]
        prior[2]["l"] = base - 5   # deux creux internes au même niveau avant le retournement -> EQL interne
        prior[6]["l"] = base - 5
        turn = {"t": prior[-1]["t"] + 60, "o": base, "h": base + 1, "l": base - 6, "c": base + 0.5}   # mèche sous le creux, clôture au-dessus
        c = prior + [turn]
        e = len(c) - 1
        ev = {"dir": 1, "i": e, "src": 0, "atr": 2.0}
        r = int_sweep(c, ev)
        self.assertTrue(r["swept"], "mèche sous le creux + clôture au-dessus -> balayage détecté")
        self.assertEqual(r["level"], base - 5)
        self.assertEqual(r["extreme"], e)
        self.assertTrue(r["eq"], "deux creux internes au même niveau -> eq (EQL interne)")


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




# ----------------------------------------------------------------------------- tests : filtre HTF M15, BE par trade, doublons
class TestHtfM15(unittest.TestCase):
    """Mode M15 : le CHoCH d'entrée doit suivre la dernière cassure M15 ; jamais de signal à contre-sens, jamais sans contexte."""

    def test_sens_du_m15(self):
        old = _E.HTF_MODE
        try:
            _E.HTF_MODE = "M15"
            seen = {}
            for name, m1 in (("bull", _StScenarios.bull()[0]), ("bear", _StScenarios.bear()[0]),
                             ("flip", _StScenarios.flip()[0][0])):
                res = _st_eval(_E, m1)
                self.assertGreater(len(res), 20, name)
                for ev, sig, log in res:
                    m15 = _st_state(m1, ev["i"] + 1)[1]
                    if sig:
                        self.assertEqual(sig["dir"], m15, f"{name} : signal à contre-sens du M15")
                        self.assertEqual(sig["htf_mode"], "M15")
                        seen.setdefault(name, set()).add(sig["side"])
                    elif m15 == 0:
                        self.assertIn("indéterminée", _st_reason(log))
                    elif ev["dir"] != m15:
                        self.assertIn("contre la tendance de fond M15", _st_reason(log), name)
                        seen.setdefault(name + "-refus", set()).add("BUY" if ev["dir"] == 1 else "SELL")
            # le M15 peut se retourner (brièvement) même dans un scénario globalement haussier : la garantie testée est
            # « chaque signal suit le M15 du moment » (ci-dessus) ; ici on vérifie seulement que le filtre travaille dans les 2 sens
            self.assertIn("BUY", seen.get("bull", set()))
            self.assertIn("SELL", seen.get("bear", set()))
            self.assertTrue(seen.get("bull-refus") or seen.get("bear-refus"), "des CHoCH à contre-sens M15 doivent être refusés")
            print(f"\n  M15 : signaux {({k: sorted(v) for k, v in seen.items()})}")
        finally:
            _E.HTF_MODE = old

    def test_sans_contexte_pas_de_signal(self):
        old = _E.HTF_MODE
        try:
            _E.HTF_MODE = "M15"
            res = _st_eval(_E, _StScenarios.bull()[0], only_m1=True)   # UT supérieures indisponibles
            self.assertEqual([r for r in res if r[1]], [])
            self.assertTrue(all("indéterminée" in _st_reason(r[2]) for r in res))
        finally:
            _E.HTF_MODE = old

    def test_mode_off_laisse_tout_passer(self):
        try:
            _E.set_setting("htf_mode", "OFF")
            m1 = _StScenarios.bull()[0]
            res = _st_eval(_E, m1)
            sides = {r[1]["side"] for r in res if r[1]}
            self.assertEqual(sides, {"BUY", "SELL"}, "OFF = aucun filtre : les deux sens passent")
            self.assertTrue(all(r[1]["htf_mode"] == "OFF" for r in res if r[1]))
        finally:
            _E._q("DELETE FROM settings WHERE key='htf_mode'", commit=True)

    def test_reglage_et_migration(self):
        try:
            for k in ("htf_mode", "htf_filter"):
                _E._q("DELETE FROM settings WHERE key=?", (k,), commit=True)
            self.assertEqual(_E.get_htf_mode(), "FULL" if _E.HTF_FILTER else "OFF")   # défaut = HTF_MODE (FULL en test)
            _E.set_setting("htf_filter", "0")            # ancienne base : OFF
            self.assertEqual(_E.get_htf_mode(), "OFF")
            _E.set_setting("htf_filter", "1")            # ancienne base : ON -> mode par défaut
            self.assertEqual(_E.get_htf_mode(), _E.HTF_MODE)
            _E.set_htf_mode("M15")                       # le choix explicite prime
            self.assertEqual(_E.get_htf_mode(), "M15")
            self.assertTrue(_E.get_htf_filter())
            _E.set_htf_mode("OFF")
            self.assertFalse(_E.get_htf_filter())
        finally:
            for k in ("htf_mode", "htf_filter"):
                _E._q("DELETE FROM settings WHERE key=?", (k,), commit=True)

    def test_ut_de_reference(self):
        self.assertEqual([_E.htf_ref_tf(t) for t in (1, 3, 5, 15, 30, 60)], [15, 15, 15, 60, 60, 240])


def _st_trade(side="BUY", be_rr=None, tp_rr=4.0, entry=100.0, risk=1.0):
    s = 1 if side == "BUY" else -1
    sl = entry - s * risk
    return {"id": -1, "symbol": "XAUUSD", "side": side, "entry": entry, "sl": sl, "sl_initial": sl,
            "tp": entry + s * risk * tp_rr, "status": "OPEN", "be_hit": 0, "rr1_hit": 0, "rr2_hit": 0, "rr3_hit": 0,
            "last_ts": 0, "closed_ts": None, "filled_ts": None, "result_r": None, "pnl_usd": None, "outcome": None,
            "risk_usd": 10.0, "be_rr": be_rr}


def _st_c(t, h, l, o=None, c=None):
    return {"t": t, "o": o if o is not None else (h + l) / 2, "h": h, "l": l, "c": c if c is not None else (h + l) / 2}


def _st_names(events):
    return [e["name"] for e in events]


class TestBE(unittest.TestCase):
    """BE réglable par trade : armement au bon RR, « BE touché » (0 R) distinct du SL, trade ouvert jamais modifié."""

    def test_be_rr1_puis_retour_entree(self):
        t = _st_trade("BUY", be_rr=1)
        ev = _E.track_trade(t, [_st_c(1, 101.2, 100.1)])
        self.assertEqual(_st_names(ev), ["RR1"])
        self.assertTrue(ev[0]["be_moved"])
        self.assertEqual((t["be_hit"], t["sl"]), (1, 100.0))
        ev = _E.track_trade(t, [_st_c(2, 100.6, 99.95)])   # le prix revient toucher l'entrée
        self.assertEqual(_st_names(ev), ["BE"])
        self.assertEqual((t["outcome"], t["result_r"], t["status"]), ("BE", 0.0, "CLOSED"))
        self.assertIn("BE touché", _E.group_event(ev[0], 2))
        self.assertNotIn("SL touché", _E.group_event(ev[0], 2))

    def test_be_demi_r(self):
        t = _st_trade("BUY", be_rr=0.5)
        ev = _E.track_trade(t, [_st_c(1, 100.6, 100.1)])
        self.assertEqual(_st_names(ev), ["BE_MOVED"])
        self.assertIn("RR0.5", _E.group_event(ev[0], 2))
        ev = _E.track_trade(t, [_st_c(2, 100.4, 99.9)])
        self.assertEqual((_st_names(ev), t["result_r"]), (["BE"], 0.0))

    def test_sell_symetrique(self):
        t = _st_trade("SELL", be_rr=1)
        ev = _E.track_trade(t, [_st_c(1, 99.9, 98.7)])
        self.assertEqual((_st_names(ev), t["sl"]), (["RR1"], 100.0))
        ev = _E.track_trade(t, [_st_c(2, 100.05, 99.5)])
        self.assertEqual((_st_names(ev), t["outcome"]), (["BE"], "BE"))

    def test_meme_bougie_pique_puis_sl_reste_un_sl(self):
        """Un pic à +2R puis retour sous l'entrée dans la MÊME bougie : l'ordre des mèches est inconnu -> SL (prudent)."""
        t = _st_trade("BUY", be_rr=1)
        ev = _E.track_trade(t, [_st_c(1, 102.0, 98.9)])
        self.assertEqual((_st_names(ev), t["result_r"]), (["SL"], -1.0))

    def test_sl_initial_sans_be(self):
        t = _st_trade("BUY", be_rr=1)
        ev = _E.track_trade(t, [_st_c(1, 100.5, 98.9)])
        self.assertEqual((_st_names(ev), t["outcome"], t["result_r"]), (["SL"], "SL", -1.0))

    def test_be_au_dela_de_la_tp_ignore(self):
        t = _st_trade("BUY", be_rr=5, tp_rr=4)
        ev = _E.track_trade(t, [_st_c(1, 103.5, 100.2)])
        self.assertEqual(_st_names(ev), ["RR1", "RR2", "RR3"])
        self.assertFalse(any(e.get("be_moved") for e in ev))
        self.assertEqual(t["be_hit"], 0)
        self.assertIn("non utilisé", _E._be_line({"be_rr": 5, "tp": 104.0, "entry": 100.0, "risk": 1.0, "rr": 4}))

    def test_be_1_5_dans_une_bougie_qui_saute_plusieurs_paliers(self):
        t = _st_trade("BUY", be_rr=1.5)
        ev = _E.track_trade(t, [_st_c(1, 103.2, 100.2)])
        self.assertEqual(_st_names(ev), ["RR1", "RR2", "RR3"])
        self.assertEqual([e["be_moved"] for e in ev], [False, True, False])   # annoncé une seule fois, avec RR2

    def test_ancien_trade_sans_be_rr_garde_be_rr_global(self):
        t = _st_trade("BUY", be_rr=None, tp_rr=6)
        g = _E.BE_RR
        ev = _E.track_trade(t, [_st_c(1, 100 + g - 0.1, 100.2)])
        self.assertFalse(t["be_hit"])
        ev = _E.track_trade(t, [_st_c(2, 100 + g + 0.1, 100.2)])
        self.assertEqual(t["be_hit"], 1)

    def test_reglage_be_naffecte_pas_un_trade_ouvert(self):
        try:
            _E.set_be_rr(3)
            t = _st_trade("BUY", be_rr=1)
            _E.track_trade(t, [_st_c(1, 101.2, 100.1)])
            self.assertEqual(t["be_hit"], 1, "le trade garde SON be_rr (1), pas le réglage courant (3)")
        finally:
            _E._q("DELETE FROM settings WHERE key='be_rr'", commit=True)

    def test_commande_be_et_menu(self):
        try:
            txt, kb = _E.handle_command("/be 1.5")
            self.assertEqual(_E.get_be_rr(), 1.5)
            self.assertIn("RR1.5", txt)
            self.assertIn("RR1.5", str(kb))
            self.assertIn("Exemple", _E.handle_command("/be abc")[0])
            self.assertEqual(_E.get_be_rr(), 1.5)
            self.assertIn("BE à : RR1.5", _E._signal_text())
        finally:
            _E._q("DELETE FROM settings WHERE key='be_rr'", commit=True)


class TestSignauxSimultanes(unittest.TestCase):
    """Un seul signal ouvert par actif par défaut : pas de doublon tant que le précédent n'est pas clôturé."""

    def _sig(self, t):
        return {"dir": -1, "side": "SELL", "type": "CHOCH1", "order": "MARKET", "ref_price": 100.0, "entry": 100.0,
                "sl": 101.0, "risk": 1.0, "tp": 96.0, "t": t, "rr": 4.0, "htf": True, "htf_mode": "M15",
                "be_rr": 1.0, "tf": "M1", "setup": "TENDANCE M15 BAISSIER", "poi": None}

    def test_pas_de_doublon(self):
        sent = []
        saved = (_E._deliver_signal, _E.make_chart)
        try:
            _E._q("DELETE FROM trades", commit=True)
            _E._deliver_signal = lambda trade_id, symbol, admin_txt, group_txt, **k: sent.append((trade_id, group_txt))
            _E.make_chart = lambda *a, **k: None
            self.assertEqual(_E.get_max_positions(), _E.MAX_POSITIONS)
            self.assertEqual(_E.MAX_POSITIONS, 1, "défaut : 1 signal ouvert par actif")
            _E.publish_signal("XAUUSD", [], [], self._sig(1000))
            _E.publish_signal("XAUUSD", [], [], self._sig(1060))   # doublon : ignoré tant que le 1er est ouvert
            self.assertEqual(_E.count_open("XAUUSD"), 1)
            self.assertEqual(len(sent), 1)
            self.assertNotIn("Position ", sent[0][1], "avec max = 1 la ligne « Position n/m » n'a pas de sens")
            self.assertIn("BE → RR1", sent[0][1])
            self.assertIn("Filtre HTF : ON (M15)", sent[0][1])
            row = _E._q("SELECT be_rr, htf_mode FROM trades").fetchone()
            self.assertEqual((row["be_rr"], row["htf_mode"]), (1.0, "M15"))
            _E.publish_signal("BTCUSD", [], [], self._sig(1000))   # un autre actif reste libre
            self.assertEqual(_E.count_open("BTCUSD"), 1)
            _E._q("UPDATE trades SET status='CLOSED' WHERE symbol='XAUUSD'", commit=True)   # clôturé (SL, TP ou BE)
            _E.publish_signal("XAUUSD", [], [], self._sig(1120))
            self.assertEqual(_E.count_open("XAUUSD"), 1)
            self.assertEqual(len(sent), 3)
            _E.handle_command("/maxpos 2")
            self.assertEqual(_E.get_max_positions(), 2)
            _E.publish_signal("XAUUSD", [], [], self._sig(1180))
            self.assertEqual(_E.count_open("XAUUSD"), 2)
            self.assertIn("Position 2/2", sent[-1][1])
        finally:
            _E._deliver_signal, _E.make_chart = saved
            _E._q("DELETE FROM trades", commit=True)
            _E._q("DELETE FROM settings WHERE key='max_positions'", commit=True)

    def test_menu_htf(self):
        kb = str(_E._signal_htf_keyboard())
        for cb in ("shtf:M15", "shtf:FULL", "shtf:OFF"):
            self.assertIn(cb, kb)


# ----------------------------------------------------------------------------- tests : lecture liquidité ext / int (affichage)
class TestLectureExtInt(unittest.TestCase):
    """sig["ext"] / sig["sweep"] : affichage uniquement — présents dans le signal, dans le message, le graphique, le log
    multi-UT et l'analyse ; jamais une condition d'acceptation ; absents (renvoi différé, make_event_chart) sans erreur."""

    @staticmethod
    def _off_signals(m1):
        _E.set_setting("htf_mode", "OFF")
        try:
            return _st_eval(_E, m1)
        finally:
            _E._q("DELETE FROM settings WHERE key='htf_mode'", commit=True)

    def test_cles_du_signal(self):
        m1 = _StScenarios.bull()[0]
        sigs = [(ev, sig) for ev, sig, _ in self._off_signals(m1) if sig]
        self.assertGreater(len(sigs), 10)
        times = {c["t"] for c in m1}
        n_ext = 0
        for ev, sig in sigs:
            sw, ext = sig["sweep"], sig["ext"]
            self.assertIsNotNone(sw)
            self.assertEqual(sw["side"], "bas" if sig["dir"] == 1 else "haut")
            self.assertIn(sw["t"], times)
            self.assertEqual(sw["tf"], "M1")
            if ext:
                n_ext += 1
                self.assertEqual(ext["side"], "haut" if sig["dir"] == 1 else "bas")
                self.assertGreater((ext["level"] - sig["entry"]) * sig["dir"], 0, "l'externe visée est dans le sens du trade")
                self.assertEqual(ext["tf"], 15)
        self.assertGreater(n_ext, 0, "au moins un signal doit avoir une liquidité externe M15 visée")

    def test_affichage_seulement_ne_decide_de_rien(self):
        """Mêmes signaux acceptés, que la lecture ext / int fonctionne ou qu'elle plante."""
        m1 = _StScenarios.bull()[0]
        ref = {ev["t"] for ev, sig, _ in self._off_signals(m1) if sig}
        saved = (_E.ext_target, _E.sweep_reading)

        def boom(*a, **k):
            raise RuntimeError("panne de lecture")
        try:
            _E.ext_target, _E.sweep_reading = boom, boom
            res = self._off_signals(m1)
        finally:
            _E.ext_target, _E.sweep_reading = saved
        self.assertEqual({ev["t"] for ev, sig, _ in res if sig}, ref)
        self.assertTrue(all(sig["ext"] is None and sig["sweep"] is None for _, sig, _ in res if sig))

    def _fake_sig(self, **kw):
        sig = {"side": "SELL", "dir": -1, "type": "CHOCH1", "entry": 80766.0, "sl": 80778.0, "risk": 12.0, "tp": 80718.0,
               "rr": 4.0, "order": "MARKET", "htf_mode": "M15", "be_rr": 1.0, "tf": "M1", "t": 1}
        sig.update(kw)
        return sig

    def test_lignes_message(self):
        ext = {"tf": 15, "side": "bas", "level": 80500.0, "eq": True, "dist": 266.0}
        sw = {"tf": "M1", "side": "haut", "level": 80780.0, "swept": True, "eq": True, "t": 1, "i": 1}
        sig = self._fake_sig(ext=ext, sweep=sw)
        lot = {"lot": 0.01, "real_risk": 10.0, "raised_to_min": False}
        for txt in (_E.group_signal("BTCUSD", sig, 1, 0), _E.admin_signal("BTCUSD", sig, 10.0, 100.0, lot, 5.0, 0)):
            self.assertIn("Ext. : M15 EQL 80 500 (à 266 pts)", txt)
            self.assertIn("Int. : M1 EQH 80 780 balayé", txt)
            self.assertLess(txt.index("Ext. :"), txt.index("Int. :"))
            self.assertLess(txt.index("Int. :"), txt.index("SL :"), "les lignes Ext./Int. précèdent SL/TP")
        sig = self._fake_sig(ext={**ext, "eq": False, "side": "haut"}, sweep={**sw, "swept": False, "eq": False})
        txt = _E.group_signal("BTCUSD", sig, 1, 0)
        self.assertIn("Ext. : M15 haut 80 500", txt)
        self.assertIn("Int. : M1 haut 80 780 non balayé", txt)

    def test_cles_absentes_tolerees(self):
        sig = self._fake_sig()   # renvoi différé : pas de sig["ext"] / sig["sweep"]
        lot = {"lot": 0.01, "real_risk": 10.0, "raised_to_min": False}
        for txt in (_E.group_signal("BTCUSD", sig, None, 0), _E.admin_signal("BTCUSD", sig, 10.0, 100.0, lot, 5.0, 0)):
            self.assertNotIn("Ext. :", txt)
            self.assertNotIn("Int. :", txt)
            self.assertIn("SL :", txt)
        sig = self._fake_sig(ext=None, sweep={"tf": "M1", "side": "haut", "level": None, "swept": False, "eq": False})
        self.assertNotIn("Int. :", _E.group_signal("BTCUSD", sig, None, 0))

    @unittest.skipUnless(importlib.util.find_spec("matplotlib"), "matplotlib absent")
    def test_graphiques(self):
        import tempfile
        m1 = _StScenarios.bull()[0]
        acc = [(ev, sig) for ev, sig, _ in self._off_signals(m1) if sig and sig["ext"] and sig["sweep"]["swept"]]
        self.assertTrue(acc, "il faut un signal avec ext + sweep balayé")
        ev, sig = acc[-1]
        candles = m1[:ev["i"] + 1]
        old_dir = _E.CHART_DIR
        _E.CHART_DIR = tempfile.mkdtemp()
        try:
            evs = _E.analyze(candles)
            far = {**sig, "ext": {**sig["ext"], "level": sig["entry"] + 100 * sig["dir"] * sig["risk"]}}   # hors cadre
            for name, sg in (("complet", sig), ("ext hors cadre", far), ("sans lecture", {**sig, "ext": None, "sweep": None}),
                             ("cles absentes", {k: v for k, v in sig.items() if k not in ("ext", "sweep")}),
                             ("sweep non balaye", {**sig, "sweep": {**sig["sweep"], "swept": False}})):
                path = _E.make_chart("XAUUSD", candles, evs, sg, 2)
                self.assertTrue(path and os.path.exists(path), name)
                os.replace(path, os.path.join(_E.CHART_DIR, name.replace(" ", "_") + ".png"))
            trade = {"entry": sig["entry"], "sl": sig["sl"], "tp": sig["tp"], "side": sig["side"], "kind": sig["type"]}
            path = _E.make_event_chart("XAUUSD", candles, evs, trade, 2)   # pas de ext / sweep : toléré
            self.assertTrue(path and os.path.exists(path))
            if os.environ.get("KEEP_CHARTS"):
                import shutil
                shutil.copytree(_E.CHART_DIR, os.environ["KEEP_CHARTS"], dirs_exist_ok=True)
        finally:
            _E.CHART_DIR = old_dir

    def test_log_multi_ut(self):
        m1, res = _StScenarios.bull()   # filtre COMPLET
        acc = [r for r in res if r[1]]
        self.assertTrue(acc)
        for _, _, log in acc:
            self.assertRegex(_st_field(log, "liquidité int."), r"^M1 (haut|bas|EQH|EQL) [\d .]+ (non )?balayé", "int_sweep")
        self.assertTrue(any(re.match(r"^M15 (haut|bas|EQH|EQL) ", _st_field(log, "liquidité ext.") or "") for _, _, log in acc),
                        "ext_map : « M15 haut/bas … » dans au moins un log")

    def test_analyse_technique(self):
        import tempfile
        m1 = _StScenarios.bull()[0]
        _StFeed(_E, m1)
        _E._htf_cache.clear()
        _E._ctx_cache.clear()
        old_dir = _E.CHART_DIR
        _E.CHART_DIR = tempfile.mkdtemp()
        try:
            txt, _chart = _E.build_technical_analysis("XAUUSD")
        finally:
            _E.CHART_DIR = old_dir
        self.assertIn("Externe (M15) :", txt)
        self.assertRegex(txt, r"Interne \(M1\) : dernier CHoCH \w+ — M1 (haut|bas|EQH|EQL) [\d .]+ (non )?balayé")
        print("\n  " + "\n  ".join(l for l in txt.splitlines() if l.startswith(("Externe", "Interne"))))


def _selftest():
    _E.HTF_MODE = "FULL"   # les tests historiques (H1 -> M15 -> M5 -> POI) évaluent la cascade complète ; TestHtfM15 passe en mode M15
    suite, loader = unittest.TestSuite(), unittest.TestLoader()
    for cls in (TestRetracement, TestContinuation, TestInvalidationHTF, TestChop, TestGetExtTf,
                TestLiquiditePure, TestM1NeDecidePasSeul, TestLogs, TestEntreeM5, TestNonRegression,
                TestHtfM15, TestBE, TestSignauxSimultanes, TestLectureExtInt):
        suite.addTests(loader.loadTestsFromTestCase(cls))
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(_selftest())
    main()
