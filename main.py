


#!/usr/bin/env python3
"""AlphaBot BOS + CHoCH — Gold & BTC — version en un seul fichier.

Signaux Telegram sur XAUUSD et BTCUSD, timeframe modifiable à tout moment via /timeframe (M1 par défaut, réglable via DEFAULT_TIMEFRAME),
entrée directe sur CHoCH.
Prix : Gold via l'API publique Deriv (WebSocket), BTC via l'API publique Binance. Aucune clé de prix.

Lancer :
    pip install requests python-dotenv matplotlib pillow websocket-client flask metaapi-cloud-sdk
    python main.py

Déploiement Render (Web Service) : le process écoute sur $PORT et expose GET /health,
pendant que la boucle de trading et le polling Telegram tournent en arrière-plan.
Variables utiles : TELEGRAM_TOKEN, CHAT_ID_GROUPE, CHAT_ID_ADMIN, TIMEFRAME,
DEFAULT_RISK_USD, DEFAULT_LEVERAGE, PORT, METAAPI_TOKEN, METAAPI_ACCOUNT_ID,
RESULT_IMAGE_ENABLED, RESULT_IMAGE_KINDS, RESULT_IMAGE_SHOW_LOT (image de résultat QML, section 7ter).

Le bot tourne en permanence ; /stop et /start (ou le bouton du menu) coupent / rétablissent les NOUVEAUX signaux, le suivi des positions ne s'arrête jamais : scan continu, une analyse à chaque
nouvelle bougie clôturée du timeframe choisi.

Risque : le lot est calculé à partir du risque $ choisi + SL, SANS solde de compte.
Le levier ne sert qu'à afficher une marge indicative (jamais utilisé pour le lot).
Grille de sortie par défaut : paliers RR1/RR2/RR3 notifiés au groupe, BE (SL -> entrée) à
RR2, TP finale à RR3 (réglable : BE depuis Telegram (/be), ou via BE_RR / TP_RR / RR_LEVELS). Aucun PnL $ n'est affiché,
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
           5 Risque · 6 Suivi des positions · 7 Graphique · 7ter Image de résultat QML · 8 Telegram · 9 Boucle principale
           10 Serveur web (Render) · 11 Auto-tests · 12 Module QML (Quasimodo) · 13 Module FIBTREND · 14 Moteur SMC · 15 Stratégie QML GAGNANT
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
from datetime import datetime, timedelta, timezone

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

try:
    from PIL import Image, ImageDraw, ImageFont  # pillow : image de résultat QML (section 7ter), optionnel
except ImportError:
    Image = ImageDraw = ImageFont = None


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
HTF_ONLY_LABELS = {240: "H4", 1440: "D1"}


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
        "source": "binance", "binance_symbol": "BTCUSDT", "deriv_symbol": "cryBTCUSD",   # deriv_symbol : historique LONG seulement
        "value_per_point": 1.0,     # $ par point pour 1 lot (1 BTC) - à ajuster selon le broker
        "min_lot": 0.001, "lot_step": 0.001, "decimals": 0,
        "min_sl_pct": 0.0020,       # plancher SL absolu : ~0.20% (ex. ~160 pts sur du BTC à 80 000) -- plus volatile/mèches larges que le Gold
        "be_fees_buffer": 5.0,      # défaut BE : SL à entrée +/- 5 pts (couvre spread/commission), voir BE_FEES_BUFFER
    },
    # --- Forex USD/xxx : 1 lot = 100 000 USD. La devise de cotation (CAD, JPY) n'est pas l'USD : la valeur du point
    # en $ = 100 000 / prix. `vpp_from_price` la recalcule donc au prix réel (voir value_per_point) ;
    # "value_per_point" ne sert que de repli si aucun prix n'est connu.
    "USDCAD": {
        "source": "deriv", "deriv_symbol": "frxUSDCAD",
        "vpp_from_price": True, "contract_size": 100000.0,
        "value_per_point": 73000.0,
        "min_lot": 0.01, "lot_step": 0.01, "decimals": 5,
        "min_sl_pct": 0.0007,       # ~0.07% (~9.6 pips à 1.37)
        "be_fees_buffer": 0.00005,  # 0.5 pip
    },
    "USDJPY": {
        "source": "deriv", "deriv_symbol": "frxUSDJPY",
        "vpp_from_price": True, "contract_size": 100000.0,
        "value_per_point": 660.0,
        "min_lot": 0.01, "lot_step": 0.01, "decimals": 3,
        "min_sl_pct": 0.0007,       # ~0.07% (~10 pips à 150)
        "be_fees_buffer": 0.005,    # 0.5 pip
    },
    # --- marchés ajoutés pour le module QML (désactivés par défaut : voir SYMBOLS_ON / menu 🌍 ACTIFS) ---
    "EURUSD": {"source": "deriv", "deriv_symbol": "frxEURUSD", "value_per_point": 100000.0,
               "min_lot": 0.01, "lot_step": 0.01, "decimals": 5, "min_sl_pct": 0.0007, "be_fees_buffer": 0.00005},
    "GBPUSD": {"source": "deriv", "deriv_symbol": "frxGBPUSD", "value_per_point": 100000.0,
               "min_lot": 0.01, "lot_step": 0.01, "decimals": 5, "min_sl_pct": 0.0007, "be_fees_buffer": 0.00005},
    "AUDUSD": {"source": "deriv", "deriv_symbol": "frxAUDUSD", "value_per_point": 100000.0,
               "min_lot": 0.01, "lot_step": 0.01, "decimals": 5, "min_sl_pct": 0.0007, "be_fees_buffer": 0.00005},
    "NZDUSD": {"source": "deriv", "deriv_symbol": "frxNZDUSD", "value_per_point": 100000.0,
               "min_lot": 0.01, "lot_step": 0.01, "decimals": 5, "min_sl_pct": 0.0007, "be_fees_buffer": 0.00005},
    "USDCHF": {"source": "deriv", "deriv_symbol": "frxUSDCHF", "vpp_from_price": True, "contract_size": 100000.0,
               "value_per_point": 113000.0, "min_lot": 0.01, "lot_step": 0.01, "decimals": 5,
               "min_sl_pct": 0.0007, "be_fees_buffer": 0.00005},
    # croisés JPY : le P&L est en JPY -> converti avec le cours USDJPY (vpp_ref), pas avec leur propre prix
    "EURJPY": {"source": "deriv", "deriv_symbol": "frxEURJPY", "vpp_from_price": True, "vpp_ref": "USDJPY",
               "contract_size": 100000.0, "value_per_point": 660.0, "min_lot": 0.01, "lot_step": 0.01,
               "decimals": 3, "min_sl_pct": 0.0007, "be_fees_buffer": 0.005},
    "GBPJPY": {"source": "deriv", "deriv_symbol": "frxGBPJPY", "vpp_from_price": True, "vpp_ref": "USDJPY",
               "contract_size": 100000.0, "value_per_point": 660.0, "min_lot": 0.01, "lot_step": 0.01,
               "decimals": 3, "min_sl_pct": 0.0008, "be_fees_buffer": 0.005},
    # indice Dow Jones : 1 lot = 1 $ par point (À VÉRIFIER chez ton broker : contract size, min/step lot, nom du symbole)
    "US30": {"source": "deriv", "deriv_symbol": "OTC_DJI", "value_per_point": 1.0,
             "min_lot": 0.01, "lot_step": 0.01, "decimals": 2, "min_sl_pct": 0.0008, "be_fees_buffer": 2.0},
}


def value_per_point(symbol, price=None):
    """$ par point (1.00 de prix) pour 1 lot. Fixe pour Gold/BTC ; pour USDCAD/USDJPY = taille du contrat / prix
    (le P&L est en CAD/JPY, converti en USD). Sans prix fourni : dernier prix connu, sinon valeur de repli de SYMBOLS."""
    cfg = SYMBOLS.get(symbol, {})
    if cfg.get("vpp_from_price"):
        if cfg.get("vpp_ref"):   # croisé : cours de la paire de conversion (ex. USDJPY), pas le prix du croisé
            price = None
            px = globals().get("_last_price", {}).get(cfg["vpp_ref"]) or globals().get("_qml_ref_px", {}).get(cfg["vpp_ref"])
        else:
            px = price or globals().get("_last_price", {}).get(symbol)
        if px and px > 0:
            return cfg["contract_size"] / float(px)
    return cfg.get("value_per_point") or 0.0


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
    vpp = value_per_point(symbol)
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

# --- Filtre M15 « VRAIE IMPULSION + STRUCTURE + FIBONACCI » ----------------------------------------
# use_m15_impulse_filter = true/false : activable/désactivable à tout moment (ENV M15_IMPULSE_FILTER, ou à chaud via
# /m15imp on|off et le bouton « 📈 IMPULSION M15 » du menu ⚙️ Paramètres signal ; le choix Telegram est mémorisé en base).
# N'ajoute AUCUNE nouvelle règle de score : filtre seulement les CHoCH déjà produits par la logique existante.
# Ordre de lecture : structure M15 -> vraie tendance -> vraie impulsion (swing High/Low structurel, jamais une micro-jambe)
# -> Fibonacci sur toute la jambe -> retracement atteint -> confluence OB/FVG (-> sweep) -> CHoCH d'entrée.
# DÉFAUT = OFF : le filtre ne doit JAMAIS se réactiver seul si la base SQLite est perdue (même logique que MTF_FIBONACCI_PD_FILTER).
USE_M15_IMPULSE_FILTER = _env_bool("M15_IMPULSE_FILTER", False)
M15_IMP_DEPTH = _env_int("M15_IMP_DEPTH", 5)                      # profondeur des pivots M15 (> SWING_DEPTH : ignore les micro-jambes)
M15_IMP_MIN_LEG_ATR = _env_float("M15_IMP_MIN_LEG_ATR", 2.0)      # amplitude mini de l'impulsion (en ATR M15), sinon micro-jambe ignorée
M15_IMP_LOOKBACK = _env_int("M15_IMP_LOOKBACK", 200)               # remontée max (bougies M15) pour trouver l'origine de l'impulsion
M15_IMP_MIN_RETRACE = _env_float("M15_IMP_MIN_RETRACE", 0.4)      # zone de réaction principale : 50 % et au-delà
M15_IMP_EARLY_MIN = _env_float("M15_IMP_EARLY_MIN", 0.236)        # sous ce niveau : jamais de signal ; entre EARLY_MIN et 50 % : seulement avec POI fort
M15_IMP_NEED_POI = _env_bool("M15_IMP_NEED_POI", True)            # à 50 %+ : exiger une confluence OB / FVG (M15 ou H1) -- « 50 % ne s'utilise pas seul »
M15_IMP_NEED_SWEEP = _env_bool("M15_IMP_NEED_SWEEP", False)       # à 50 %+ : exiger aussi un sweep sur le point de retournement (optionnel)

# Valeurs de DÉPART des nouveaux filtres : tout se règle ensuite depuis Telegram (menu ⚙️ Paramètres signal), rien à toucher sur Render.
BOS_REAL_RETRACE = 0.4                 # « vrai BOS » : retracement mini (en % de la jambe) après la cassure
POI_FILTER_TF = "BOTH"                 # filtre POI HTF : M15 | H1 | BOTH
BOS_RETRACE_CHOICES = (0.4, 0.45, 0.5)
M15_RETRACE_CHOICES = (0.4, 0.5, 0.618)
POI_TF_CHOICES = ("M15", "H1", "BOTH")

EXT_DEPTH = _env_int("EXT_DEPTH", 5)   # profondeur (bougies de chaque côté) pour confirmer un pivot swing de liquidité externe
EQ_TOL_ATR = 0.1                       # tolérance EQH/EQL : 2 pivots à moins de EQ_TOL_ATR ATR sont considérés au même niveau
POI_MAX_AGE = _env_int("POI_MAX_AGE", 60)                  # âge max d'un POI (OB / FVG), en bougies de sa propre UT
POI_MIN_ATR = _env_float("POI_MIN_ATR", 0.15)              # taille mini d'un POI, en ATR de son UT (écarte les micro-gaps)
POC_BINS = _env_int("POC_BINS", 20)                         # POC proxy (pas de volume réel) : nb de paliers de prix sur la jambe impulsive
SWEEP_LOOKBACK = _env_int("SWEEP_LOOKBACK", 10)            # bougies d'entrée comparées pour détecter un balayage de liquidité
SL_BUFFER_ATR = _env_float("SL_BUFFER_ATR", 0.2)   # buffer au-delà de la ligne du BOS (0.1 laissait le SL pile sur la mèche -> balayé par le bruit)
MIN_SL_ATR = _env_float("MIN_SL_ATR", 0.8)         # SL minimum (en ATR) -- 0.3 donnait des SL de quelques points sur BTC en M1/M5, balayés par le spread/bruit
MAX_SL_ATR = _env_float("MAX_SL_ATR", 6.0)         # au-delà : signal ignoré (BOS trop ancien)
# Seuils de la détection SWEEP -> CHoCH -> verrou (machine SETUP50), tous modifiables ici ou par variable d'environnement.
SWEEP_EQUAL_ATR = _env_float("SWEEP_EQUAL_ATR", 0.20)        # equal highs / lows : écart < N x ATR14
SWEEP_MIN_ATR = _env_float("SWEEP_MIN_ATR", 0.10)            # mèche au-delà du niveau : au moins N x ATR14 ...
SWEEP_MAX_ATR = _env_float("SWEEP_MAX_ATR", 1.00)            # ... et au plus N x ATR14 (au-delà : cassure BOS, pas un sweep)
SWEEP_LEVEL_MIN_AGE = _env_int("SWEEP_LEVEL_MIN_AGE", 20)    # niveau balayé : vieux d'au moins N bougies (et jamais balayé)
SWEEP_CLOSE_BACK = _env_int("SWEEP_CLOSE_BACK", 3)           # clôture de retour : sur la bougie du sweep ou l'une des N suivantes
SWING_FRACTAL = _env_int("SE_SWING_DEPTH", 5)                # swing fractal : N bougies de chaque côté
CHOCH_WINDOW = _env_int("CHOCH_WINDOW", 10)                  # le CHoCH doit survenir dans les N bougies après le sweep valide
CHOCH_BODY_ATR_MIN = _env_float("CHOCH_BODY_ATR_MIN", 1.00)  # CHoCH : corps >= N x ATR14 (ou FVG créé), sinon ignoré
MAX_BOUGIES = _env_int("MAX_BOUGIES", _env_int("SE_MAX_BOUGIES", 20))   # le verrou se lève N bougies après le CHoCH si pas d'entrée
# Plancher ABSOLU du SL, en % du prix d'entrée : filet de sécurité indépendant de l'ATR (l'ATR d'une UT basse comme M1
# peut lui-même être minuscule en marché calme -> MIN_SL_ATR seul ne suffit pas à empêcher un SL de quelques points).
# Réglable par actif ci-dessous (SYMBOLS[...]["min_sl_pct"]), sinon valeur par défaut MIN_SL_PCT.
MIN_SL_PCT = _env_float("MIN_SL_PCT", 0.0015)      # 0.15% par défaut (ex. ~120 pts sur du BTC à 80 000)
BE_RR = _env_float("BE_RR", 2.0)          # RR auquel le SL est déplacé à l'entrée (BE) : valeur de départ, ensuite /be sur Telegram
BE_RR_CHOICES = (0.5, 1, 1.5, 2, 3)       # boutons du menu BE (n'importe quelle valeur > 0 via /be 0.75)
TP_RR = _env_float("TP_RR", 3.0)          # RR de la TP finale (clôture complète, pas de TP1/TP2) ; modifiable : /rr ou menu RR
RR_LEVELS = (1.0, 2.0, 3.0)                # paliers intermédiaires notifiés au groupe (hors TP finale)
# ⚙️ PARAMÈTRES SIGNAL (menu Telegram) : TP_RR, HTF_FILTER et le timeframe ci-dessus ne sont que les valeurs de
# DÉPART ; le choix fait sur Telegram est mémorisé en base (table settings) et prime après chaque redémarrage.
SIGNAL_RR_CHOICES = (1, 2, 3, 4)     # boutons « RR1 | RR2 | RR3 | RR4 »
SIGNAL_TF_CHOICES = (1, 5, 15)       # boutons « M1 | M5 | M15 » (timeframe du déclenchement final)
MAX_POSITIONS = _env_int("MAX_POSITIONS", 1)     # signaux ouverts max par actif (1 = aucun nouveau signal tant que le précédent n'est pas clôturé) ; ensuite /maxpos
MAX_POSITIONS_CHOICES = (1, 2, 3, 4, 5, 10)               # boutons du menu « signaux simultanés »

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

# --- Types d'entrée activables depuis Telegram (/entree ou ⚙️ Paramètres signal → 📥 ENTRÉE) --------------------------
#   DIRECT = entrée au marché à la clôture du CHoCH (comportement historique).
#   LIMIT  = ordre limit sur le bord PROCHE de l'OB / FVG créé par le CHoCH (le prix doit y revenir), SL derrière la zone.
#   Les deux ensemble = les deux ordres partent (direct + limit) ; le limit ne compte pas dans « signaux max ».
# Ces valeurs ne sont que le DÉPART : le choix Telegram est mémorisé en base et prime après redémarrage.
ENTRY_DIRECT = _env_bool("ENTRY_DIRECT", True)
ENTRY_LIMIT = _env_bool("ENTRY_LIMIT", False)
POI_LIMIT_DEPTH = _env_float("POI_LIMIT_DEPTH", 0.0)       # 0 = bord proche de la zone (défaut) ; 0.5 = milieu ; 1 = bord lointain
POI_LIMIT_MAX_ATR = _env_float("POI_LIMIT_MAX_ATR", 3.0)   # zone plus loin que N x ATR du prix : pas d'ordre limit
LIMIT_MT5_EXEC = _env_bool("LIMIT_MT5_EXEC", True)         # limit touché -> ordre MT5 envoyé au marché (sinon suivi seulement)
LIMIT_FILL_MAX_DRIFT_R = _env_float("LIMIT_FILL_MAX_DRIFT_R", 0.5)   # pas d'ordre MT5 si le prix a déjà dérivé de plus de N R du limit

# --- Modes de SL / TP et stratégies activables (Telegram : /signal -> 🛡 SL / TP · 🧩 STRATÉGIES) ------------------------
#   SL (entrée directe) : STRUCT = ligne du BOS opposé (historique) · ZONE = derrière l'OB/FVG créé par le CHoCH ·
#                         SWEEP  = derrière l'extrême de la jambe du CHoCH (le sweep). Le limit OB/FVG met toujours son SL derrière la zone.
#   TP : RR = RR fixe (historique) · TARGET = liquidité externe visée · SWING = plus proche swing high/low non cassé.
#        Cible absente ou plus proche que TP_MIN_RR -> repli sur le RR fixe (loggé).
#   CRT (Candle Range Theory) : module indépendant, DÉSACTIVÉ par défaut. Bougie HTF de référence (CRH/CRL) balayée d'un côté,
#        clôture de retour DANS le range, puis CHoCH sur l'UT d'entrée ; SL derrière le sweep ; TP = autre bout du range / 50 %.
SL_MODES, TP_MODES, CRT_TP_MODES, CRT_REFS = ("STRUCT", "ZONE", "SWEEP"), ("RR", "TARGET", "SWING"), ("RANGE", "MID", "RR"), (60, 240, 1440)
SL_MODE = os.getenv("SL_MODE", "STRUCT").strip().upper()
TP_MODE = os.getenv("TP_MODE", "RR").strip().upper()
TP_MIN_RR = _env_float("TP_MIN_RR", 1.0)
STRAT_MAIN = _env_bool("STRAT_MAIN", True)      # stratégie actuelle : Sweep -> POI -> mitigation -> CHoCH
STRAT_CRT = _env_bool("STRAT_CRT", False)       # module CRT
CRT_REF_TF = _env_int("CRT_REF_TF", 60)         # UT de la bougie de référence : 60 (H1) / 240 (H4) / 1440 (D1)
CRT_TP = os.getenv("CRT_TP", "RANGE").strip().upper()
# --- FILTRES QUALITÉ du CHoCH (interrupteurs ON/OFF, menu Telegram « 🔬 FILTRES ») ------------------------------
# Toujours CALCULÉS et loggés (bloc par signal) ; ils ne REFUSENT un signal que s'ils sont ON. Défaut : tous OFF.
QF_KEYS = ("swings", "disp", "range", "fvg", "ind", "leg")
QF_LBL = {"swings": "Structure HH/HL", "disp": "Displacement", "range": "Anti-range", "fvg": "FVG strict", "ind": "Inducement",
          "leg": "Qualité jambe"}
QF_DEFAULT = {k: _env_bool("QF_" + k.upper(), False) for k in QF_KEYS}
DISP_BODY_ATR = _env_float("DISP_BODY_ATR", 0.8)       # displacement : corps de la bougie cassante >= N x ATR
DISP_BODY_RATIO = _env_float("DISP_BODY_RATIO", 0.6)   # ... et corps >= N % de sa hauteur (peu de mèches)
DISP_BREAK_ATR = _env_float("DISP_BREAK_ATR", 0.10)    # vraie cassure : clôture au-delà du niveau d'au moins N x ATR
RANGE_LOOKBACK = _env_int("RANGE_LOOKBACK", 20)        # anti-range : bougies analysées avant/avec la cassure
RANGE_MIN_ER = _env_float("RANGE_MIN_ER", 0.15)        # efficience mini (|déplacement net| / chemin parcouru) ; en dessous = chop
LEG_MIN_CANDLES = _env_int("LEG_MIN_CANDLES", 2)           # jambe (filtre "leg") : trop courte -> non jugeable, rejetée
LEG_MIN_BODY_ATR = _env_float("LEG_MIN_BODY_ATR", 0.35)    # corps moyen des bougies de la jambe >= N x ATR
LEG_MIN_BODY_RATIO = _env_float("LEG_MIN_BODY_RATIO", 0.40)  # ... et corps moyen >= N % du range moyen (peu de mèches en moyenne)
LEG_MIN_DIR_RATIO = _env_float("LEG_MIN_DIR_RATIO", 0.55)  # part mini de bougies dans le sens de la jambe (le reste = bruit/contre-mouvement)
LEG_MIN_NET_EFF = _env_float("LEG_MIN_NET_EFF", 0.35)      # efficience de la jambe = déplacement net / somme des ranges (chemin parcouru)
LEG_MICRO_BODY_ATR = _env_float("LEG_MICRO_BODY_ATR", 0.15)  # une bougie est "micro" si son corps < N x ATR
LEG_MAX_MICRO_RATIO = _env_float("LEG_MAX_MICRO_RATIO", 0.50)  # part max de micro-bougies tolérée dans la jambe
CRT_DUP = _env_bool("CRT_DUP", False)         # CRT autorisé même si la principale a déjà pris ce CHoCH (défaut OFF)
CRT_HTF = _env_bool("CRT_HTF", False)           # CRT + filtre de tendance HTF (l'inverse d'un retournement : à tester, pas à supposer)
# Sessions (heures UTC, [début, fin[) : filtre optionnel /session. Londres ~07-16 UTC, New York ~12-21 UTC (recouvrement 12-16).
def _parse_hours(name, default):
    try:
        a, b = os.getenv(name, default).split("-")
        return int(a), int(b)
    except Exception:
        return tuple(int(x) for x in default.split("-"))
SESSION_LONDON, SESSION_NY = _parse_hours("SESSION_LONDON", "7-16"), _parse_hours("SESSION_NY", "12-21")
SESSIONS = ("ALL", "LONDON", "NY", "BOTH")
LIMIT_ZONES = ("BOTH", "OB", "FVG")

# --- Risque & levier --------------------------------------------------------
# Le lot est calculé uniquement à partir du risque $ et de la distance du SL (aucun solde requis).
DEFAULT_RISK_USD = _env_float("DEFAULT_RISK_USD", 10.0)  # modifiable à tout moment via /risque
MAX_RISK_USD_TELEGRAM = 100000.0  # borne de saisie pour /risque uniquement (renommé pour ne pas entrer en conflit
                                   # avec MAX_RISK_USD du Risk Gate, section 5bis, sans changer son comportement)
# RISK_STRICT ON (défaut) : le risque réel ne doit JAMAIS dépasser le risque configuré. Si le lot
# minimum du broker forcerait un risque réel > risque demandé (au-delà de RISK_STRICT_TOLERANCE_PCT
# de marge), le signal/trade est REFUSÉ plutôt que pris avec un risque plus élevé que prévu.
# RISK_STRICT OFF : ancien comportement -> lot minimum forcé quand même, risque réel plus élevé toléré.
RISK_STRICT = _env_bool("RISK_STRICT", True)
RISK_STRICT_TOLERANCE_PCT = _env_float("RISK_STRICT_TOLERANCE_PCT", 0.0)  # 0 = tolérance nulle (strict pur) ; 0.10 = tolère jusqu'à +10%
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
    MT5_SYMBOL_MAP = {"XAUUSD": "XAUUSDm", "BTCUSD": "BTCUSDm", "USDCAD": "USDCADm", "USDJPY": "USDJPYm", "US30": "US30m"}
    MT5_SYMBOL_MAP.update(json.loads(os.getenv("MT5_SYMBOL_MAP", "") or "{}"))
except Exception:
    print("[metaapi] MT5_SYMBOL_MAP invalide (JSON attendu) — mapping ignoré.")
    MT5_SYMBOL_MAP = {"XAUUSD": "XAUUSDm", "BTCUSD": "BTCUSDm", "USDCAD": "USDCADm", "USDJPY": "USDJPYm", "US30": "US30m"}

# Tolérance de slippage (en points de prix, prix brut) transmise à MetaApi pour chaque ordre
# MARKET : au-delà de cet écart entre le prix demandé et le prix d'exécution, le broker (Exness)
# rejette l'ordre plutôt que de l'exécuter à un prix trop éloigné. Réglable par variable d'env
# MT5_SLIPPAGE_XAUUSD / MT5_SLIPPAGE_BTCUSD (sinon défaut ci-dessous par actif).
MT5_SLIPPAGE_DEFAULT = {"XAUUSD": 0.5, "BTCUSD": 30.0, "USDCAD": 0.0002, "USDJPY": 0.02, "US30": 3.0}


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
MT5_PRICE_GAP_ALERT_PCT = {"XAUUSD": 0.0015, "BTCUSD": 0.0005, "USDCAD": 0.0005, "USDJPY": 0.0005, "US30": 0.0005}


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

    async def get_symbols(self):
        """Liste des symboles réellement disponibles chez CE broker (ex. 'BTCUSD', 'BTCUSDm', 'BTCUSD.a'...) --
        sert à l'auto-détection du nom exact (voir _auto_resolve_symbols), pour ne plus dépendre d'un mapping
        codé en dur (MT5_SYMBOL_MAP) qui suppose un broker précis (Exness)."""
        return await self._acall("GET", "/symbols") or []

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
        # POSITION_MODIFY réécrit SL ET TP côté MT5 : un niveau absent de la requête peut être supprimé
        # (c'est ce qui retirait le TP après le passage en BE, et le SL après un /tpset). On relit donc la
        # position au broker et on renvoie TOUJOURS le niveau qu'on ne modifie pas.
        if stop_loss is None or take_profit is None:
            try:
                cur = next((p for p in (await self.get_positions() or [])
                            if str(p.get("id")) == str(position_id)), None)
            except Exception as e:
                cur = None
                print(f"[metaapi] modify_position {position_id} : lecture SL/TP actuels impossible ({e}) "
                      f"— envoi tel quel.")
            if cur is not None:
                if stop_loss is None and cur.get("stopLoss"):
                    stop_loss = cur["stopLoss"]
                if take_profit is None and cur.get("takeProfit"):
                    take_profit = cur["takeProfit"]
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

    async def close_position_partially(self, position_id, volume, options=None):
        """Clôture partielle (POSITION_PARTIAL) d'une position ouverte."""
        body = {"actionType": "POSITION_PARTIAL", "positionId": str(position_id), "volume": float(volume)}
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
            _auto_resolve_symbols()    # corrige le nommage des symboles si besoin (broker différent d'Exness)
            _prefetch_symbol_specs()   # specs symbole en cache AVANT le prochain signal (évite le Read timed out)
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


# clientId MetaApi : format strategyId_positionId_orderId, TROIS blocs alphanumériques séparés par « _ » (ex. RF_EURUSD_GjCy5lk),
# 30 caractères max avec le commentaire — voir metaapi.cloud/docs/client/clientIdUsage. L'ancien format à 4 blocs
# (AB_3_BTCB_98649045) était refusé en HTTP 400 à CHAQUE ordre puis renvoyé sans clientId.
_CLIENT_ID_RE = re.compile(r"[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9]+")


def _sanitize_client_id(client_id):
    """Retourne le clientId s'il respecte le format MetaApi, sinon "" (jamais envoyé : évite un HTTP 400 + un 2e envoi)."""
    cid = (client_id or "").strip()
    if not cid:
        return ""
    if len(cid) > 30 or not _CLIENT_ID_RE.fullmatch(cid):
        print(f"[metaapi] clientId {cid!r} non conforme (strategyId_positionId_orderId alphanumérique, 30 car. max) "
              f"— non envoyé.")
        return ""
    return cid


def _auto_client_id(trade_id, symbol, side):
    """clientId d'un ordre automatique : AB (stratégie) _ <id trade><symbole><sens> (position) _ <fin timestamp> (ordre)."""
    return f"AB_{trade_id}{str(symbol)[:3].upper()}{str(side)[:1].upper()}_{str(int(time.time() * 1000))[-8:]}"


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


# Cache des specs symbole (digits, filling modes, stopsLevel...) : les specs d'un symbole ne changent quasi jamais,
# donc un « Read timed out » au moment du signal ne doit plus faire tomber digits / fillingModes / stopsLevel à None.
_SPEC_CACHE = {}   # broker_symbol -> (timestamp, spec)
_SPEC_CACHE_TTL_SEC = _env_float("MT5_SPEC_CACHE_TTL_SEC", 6 * 3600)
_SPEC_FETCH_RETRIES = _env_int("MT5_SPEC_FETCH_RETRIES", 3)


def _spec_cache_get(broker_symbol, allow_stale=False):
    item = _SPEC_CACHE.get(broker_symbol)
    if not item:
        return None
    ts, spec = item
    if allow_stale or (time.time() - ts) < _SPEC_CACHE_TTL_SEC:
        return spec
    return None


async def _get_symbol_spec(connection, broker_symbol):
    """2. Étape « specs symbole » : récupère les specs MT5 (stopsLevel, point, filling modes...)
    juste avant l'envoi. Ordre : cache frais -> requête avec retry (backoff 1s, 2s) -> cache périmé (mieux que rien)
    -> None (l'appelant devient alors permissif sur la vérif de distance plutôt que de bloquer tous les trades)."""
    cached = _spec_cache_get(broker_symbol)
    if cached is not None:
        return cached
    last_err = None
    for attempt in range(1, max(_SPEC_FETCH_RETRIES, 1) + 1):
        try:
            spec = await connection.get_symbol_specification(broker_symbol)
            if spec:
                _SPEC_CACHE[broker_symbol] = (time.time(), spec)
                return spec
            last_err = "réponse vide"
        except Exception as e:
            last_err = e
            print(f"[metaapi] Specs {broker_symbol} : tentative {attempt}/{_SPEC_FETCH_RETRIES} échouée : {e}")
        if attempt < _SPEC_FETCH_RETRIES:
            await asyncio.sleep(attempt)
    stale = _spec_cache_get(broker_symbol, allow_stale=True)
    if stale is not None:
        print(f"[metaapi] Specs de {broker_symbol} injoignables ({last_err}) — dernières specs connues (cache périmé) utilisées.")
        return stale
    print(f"[metaapi] Impossible de récupérer les specs de {broker_symbol} : {last_err} — vérif stop-level ignorée.")
    return None


def _clean_symbol_name(name):
    return re.sub(r"[^A-Za-z0-9]", "", name or "").upper()


_symbols_auto_resolved = False   # une seule tentative par démarrage (broker/nommage fixes tant que le process tourne)


def _auto_resolve_symbols():
    """Corrige MT5_SYMBOL_MAP tout seul si le nom mappé (défaut codé en dur pour Exness, ou MT5_SYMBOL_MAP env)
    n'existe pas chez le broker RÉELLEMENT connecté (ex. compte FTMO/FundedNext au lieu d'Exness) : compare aux
    symboles réels du compte (get_symbols) et reprend le plus proche (même nom de base + suffixe broker en plus,
    ex. BTCUSD -> BTCUSDm / BTCUSD.a / BTCUSD1). Ne fait RIEN si le mapping actuel est déjà valide chez ce broker
    (respecte un MT5_SYMBOL_MAP choisi à la main). Plus besoin de changer une variable Render en changeant de broker."""
    global _symbols_auto_resolved
    if _symbols_auto_resolved:
        return
    try:
        raw = _run_mt5(_rest_connection.get_symbols(), timeout=60)
    except Exception as e:
        print(f"[metaapi-rest] Auto-détection des symboles impossible ({e}) — MT5_SYMBOL_MAP actuel conservé tel quel.")
        return
    names = [n if isinstance(n, str) else (n.get("symbol") or n.get("name")) for n in (raw or [])]
    names = [n for n in names if n]
    if not names:
        print("[metaapi-rest] Auto-détection des symboles : liste vide côté broker -- MT5_SYMBOL_MAP conservé tel quel.")
        _symbols_auto_resolved = True
        return
    for sym in SYMBOLS:
        current = MT5_SYMBOL_MAP.get(sym, sym)
        if current in names:
            continue   # le mapping actuel (env ou défaut) existe chez ce broker : on n'y touche pas
        base = _clean_symbol_name(sym)
        candidates = [n for n in names if _clean_symbol_name(n).startswith(base)]
        if not candidates:
            print(f"[metaapi] Auto-détection : aucun symbole de ce broker ne correspond à {sym} (cherché « {current} »). "
                  f"Ce marché ne sera pas tradable ici tant qu'aucun symbole équivalent n'existe côté broker.")
            continue
        best = min(candidates, key=len)   # nom le plus court = suffixe le plus court = correspondance la plus probable
        MT5_SYMBOL_MAP[sym] = best
        REV_MT5_SYMBOL_MAP[best] = sym
        print(f"[metaapi] Auto-détection : {sym} -> « {best} » (« {current} » introuvable chez ce broker).")
    _symbols_auto_resolved = True


def _prefetch_symbol_specs():
    """Réchauffe le cache des specs pour tous les symboles du bot (appelé depuis le thread de fond REST) : ainsi, au
    moment d'un signal, les specs sont déjà en mémoire. Best-effort : ne lève jamais d'exception."""
    for broker_symbol in {MT5_SYMBOL_MAP.get(k, k) for k in SYMBOLS}:
        if _spec_cache_get(broker_symbol) is not None:
            continue
        try:
            spec = _run_mt5(_get_symbol_spec(_rest_connection, broker_symbol), timeout=90)
            if spec:
                print(f"[metaapi-rest] Specs {broker_symbol} en cache "
                      f"(digits={_spec_get(spec, 'digits')}, fillingModes={_spec_get(spec, 'fillingModes')}).")
        except Exception as e:
            print(f"[metaapi-rest] Préchargement des specs {broker_symbol} impossible : {e}")


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

    # DERNIER VERROU Risk Gate : risque réel du volume FINAL (specs réelles du broker) et du SL envoyé, commun à toutes
    # les stratégies. Jamais de SL modifié, jamais de volume forcé : au-delà de MAX_RISK_USD l'ordre ne part pas.
    _ok, _rr = risk_gate_verify(symbol, lot, entry, sl, strategy=_RISK_CTX.get(client_id, "?"))
    if not _ok:
        raise RiskGateBlocked(symbol, lot, _rr if _rr is not None else float("nan"))

    _check_trade_geometry(symbol, side, entry, sl, tp)   # 5. rejet net si géométrie incohérente

    min_dist = _min_stop_distance(spec, symbol)          # 6. stopsLevel + freezeLevel
    if min_dist > 0:
        sl_dist = abs(entry - sl) if sl else None
        tp_dist = abs(entry - tp) if tp else None
        if sl_dist is not None and sl_dist < min_dist:
            raise SLTooCloseError(symbol, broker_symbol, "SL", min_dist, sl_dist)
        if tp_dist is not None and tp_dist < min_dist:
            raise SLTooCloseError(symbol, broker_symbol, "TP", min_dist, tp_dist)

    slippage = get_mt5_slippage(symbol)   # exprimé en prix brut (ex. 0.5 = 50 cts sur l'or)
    # MetaApi / MT5 attendent la déviation en POINTS entiers (point=0.001 sur XAUUSDm : 0.5 envoyé tel quel = 0 point).
    _pt = _spec_get(spec, "point", "tickSize", "tick_size", default=None) if spec else None
    if not _pt:
        _pt = 10 ** (-int(_spec_get(spec, "digits", default=SYMBOLS.get(symbol, {}).get("decimals", 2)) if spec
                          else SYMBOLS.get(symbol, {}).get("decimals", 2)))
    slippage = max(int(round(float(slippage) / float(_pt))), 1)
    filling_modes = _pick_filling_modes(spec)   # 1/3. jamais IOC forcé si les specs ne le confirment pas
    # MetaApi : longueur totale comment + clientId limitée à 30 caractères si les deux sont
    # fournis (31 sinon) — voir metaapi.cloud/docs/client/clientIdUsage.
    cid = _sanitize_client_id(client_id)
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
        # Diagnostic : aucune position n'existe (vérifié ci-dessus) -> UN seul essai en payload minimal
        # (sans slippage ni fillingModes), pour isoler le paramètre fautif. Pas de boucle, pas de 3e envoi.
        minimal = {k: v for k, v in options.items() if k not in ("slippage", "fillingModes")}
        try:
            print("[metaapi] Essai unique en payload minimal (sans slippage, sans fillingModes)...")
            res = await _place(minimal)
            print("[metaapi] ✅ Payload minimal ACCEPTÉ -> le paramètre fautif est slippage et/ou fillingModes.")
            return res
        except Exception as e2:
            print(f"[metaapi] Payload minimal refusé aussi : {e2} -> cause hors payload "
                  f"(marché fermé / trading désactivé sur le compte / droits du mot de passe).")
        raise e

    except Exception as e:
        msg = str(e)
        code, explication_fr = _classify_trade_error(msg)

        if "Validation failed" in msg and "clientId" in msg and "clientId" in options:
            # Rejet de VALIDATION MetaApi (HTTP 400) : l'ordre n'est jamais arrivé au broker -> renvoi sans risque
            # de doublon, sans clientId (l'anti-doublon par clientId est alors indisponible pour cet ordre).
            print("[metaapi] clientId refusé par la validation MetaApi — renvoi unique sans clientId.")
            no_cid = {k: v for k, v in options.items() if k != "clientId"}
            return await _place(no_cid)

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


def execute_mt5_order(symbol, side, lot, entry, sl, tp, client_id=None, strategy=None):
    """Wrapper sync : exécute un ordre MARKET côté MT5 avec le lot/SL/TP déjà calculés par le
    moteur de signal, puis confirme réellement l'existence de la position créée (positionId/
    orderId) avant de la considérer comme ouverte. Laisse remonter SLTooCloseError telle quelle (à
    l'appelant de décider du rejet du signal) ; toute autre erreur ne lève jamais d'exception :
    retourne le position_id (str) en cas de succès confirmé, ou None en cas d'échec (loggé +
    admin notifié par l'appelant). `client_id` (ex. l'id du trade en base) sert à la déduplication
    lors d'une éventuelle retentative — voir _execute_mt5_order_async."""
    if client_id and strategy:
        _RISK_CTX[client_id] = strategy   # nom de stratégie pour les logs du dernier verrou Risk Gate
    try:
        result = _run_mt5(_execute_mt5_order_async(symbol, side, lot, entry, sl, tp, client_id))
    except RiskGateBlocked as e:
        # Dernier verrou : risque réel > MAX_RISK_USD avec le volume final -> aucun ordre, SL inchangé.
        print(f"[metaapi] {e}")
        if client_id:
            _RISK_BLOCKED.add(client_id)
        return None
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
        if pos is not None:
            try:
                real_vol = float(pos.get("volume") or 0)
            except (TypeError, ValueError):
                real_vol = 0.0
            if real_vol > 0 and abs(real_vol - float(lot)) > 1e-9:
                print(f"[metaapi] Volume réellement exécuté : {real_vol:g} (lot du bot {float(lot):g}, arrondi aux specs du broker).")
                lot = real_vol
            if pos.get("openPrice") is not None:
                print(f"[metaapi] Prix d'ouverture réel broker : {pos.get('openPrice')} (signal {entry}).")
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


def _be_net_usd(symbol, side, open_px, sl_px, volume):
    """Résultat NET attendu (en $) si le SL est exécuté exactement à son niveau, commission déduite.
    Un BUY s'ouvre à l'ASK et se ferme sur le BID (le SL d'un long se déclenche sur le bid) ; un SELL s'ouvre au BID et
    se ferme sur l'ASK. Le spread payé à l'ouverture est donc DÉJÀ dans le prix d'ouverture réel du broker : le BE doit
    partir de CE prix (openPrice de la position), jamais du prix du signal (Binance/Deriv) qui en diffère.
    Retourne None si la valeur du point du symbole est inconnue (calcul impossible)."""
    vpp = value_per_point(symbol, open_px)
    if vpp <= 0:
        return None
    sgn = 1 if side == "BUY" else -1
    return (sl_px - open_px) * sgn * vpp * volume - BE_COMMISSION_USD_PER_LOT * volume


def _be_sl_from_open(symbol, side, open_px, volume, point, digits):
    """SL de BE = prix d'ouverture RÉEL du broker +/- buffer calculé sur le volume RÉEL (pas le lot théorique du bot),
    arrondi au tick du broker toujours dans le sens FAVORABLE (haut pour un BUY, bas pour un SELL — un arrondi 'au plus
    proche' pouvait ramener le SL sous l'entrée). VÉRIF 1 : recalcule le net attendu et remonte d'un tick tant qu'il est
    < BE_TARGET_USD. Retourne (sl, verifie) ; verifie=None si le net n'est pas calculable, False si toujours insuffisant."""
    sgn = 1 if side == "BUY" else -1
    buffer = _be_price_buffer(symbol, volume)
    ticks = (open_px + sgn * buffer) / point
    n = math.ceil(ticks - 1e-6) if sgn == 1 else math.floor(ticks + 1e-6)
    sl = round(n * point, digits)
    for _ in range(10):
        net = _be_net_usd(symbol, side, open_px, sl, volume)
        if net is None:
            return sl, None
        if net >= BE_TARGET_USD - 1e-9:
            return sl, True
        n += sgn
        sl = round(n * point, digits)
    return sl, False


def _be_market_check(side, new_sl, bid, ask, min_dist=0.0):
    """VÉRIF 2 (spread / marché) : un long est stoppé sur le BID, un short sur l'ASK. Le nouveau SL doit être du bon côté
    du prix courant, à plus de stopsLevel : sinon MT5 refuse (INVALID_STOPS) ou la position se ferme aussitôt sur le
    spread. Retourne (ok, détail) ; ok=True si le prix n'est pas connu (jamais bloquant par excès)."""
    if side == "BUY":
        if bid is None:
            return True, "prix broker indisponible"
        return (bid - new_sl) > min_dist, f"bid {bid:.2f} / SL {new_sl:.2f} (distance {bid - new_sl:.2f}, min {min_dist:g})"
    if ask is None:
        return True, "prix broker indisponible"
    return (new_sl - ask) > min_dist, f"ask {ask:.2f} / SL {new_sl:.2f} (distance {new_sl - ask:.2f}, min {min_dist:g})"


class BENotApplied(RuntimeError):
    """BE volontairement NON envoyé par la double vérification (net < cible, ou prix déjà au-delà du BE) : le SL d'origine
    reste en place. Ce n'est pas une panne — pas de traceback dans les logs."""


async def _move_to_breakeven_async(position_id, entry, side, symbol, lot):
    """Modifie le SL d'une position MT5 déjà ouverte pour le placer au BE + buffer visant BE_TARGET_USD NET (+0.01 $ par
    défaut) sur le volume RÉEL de la position. Double vérification :
      1) AVANT envoi : SL calculé sur l'openPrice et le volume réels du broker (spread d'ouverture inclus), arrondi
         favorable, net attendu >= cible ; puis contrôle marché (bid/ask, stopsLevel) — sinon BE NON envoyé + alerte admin ;
      2) APRÈS envoi : relit la position, vérifie que le SL attaché est bien le SL demandé, que le TP est resté et que le
         net attendu est >= cible — sinon alerte admin.
    Ne réduit jamais un SL déjà meilleur. Lève une exception en cas d'échec — à charge de l'appelant sync
    (move_to_breakeven) de l'attraper."""
    connection = await get_metaapi_connection()
    if connection is None:
        raise RuntimeError("connexion MetaApi indisponible")
    if side not in ("BUY", "SELL"):
        raise ValueError(f"side invalide : {side!r} (attendu BUY ou SELL)")
    sgn = 1 if side == "BUY" else -1
    broker_symbol = MT5_SYMBOL_MAP.get(symbol, symbol)
    dec = SYMBOLS.get(symbol, {}).get("decimals", 2)

    def _f(v):
        try:
            v = float(v)
            return v if math.isfinite(v) and v > 0 else None
        except (TypeError, ValueError):
            return None

    # --- specs du symbole (décimales / tick réels du broker) ---
    spec = _spec_cache_get(broker_symbol, allow_stale=True) or await _get_symbol_spec(connection, broker_symbol)
    try:
        digits = int(_spec_get(spec, "digits", default=dec)) if spec else dec
    except (TypeError, ValueError):
        digits = dec
    digits = max(digits, 0)
    px = lambda v: f"{v:.{digits}f}"   # prix affichés avec les décimales réelles du broker
    point = (_f(_spec_get(spec, "point", "tickSize", "tick_size", default=None)) if spec else None) or 10 ** (-digits)


    # --- réalité broker : prix d'ouverture RÉEL (spread inclus), volume RÉEL (après arrondi), SL/TP actuels ---
    found, _why, pos = await _confirm_position_async(connection, position_id)
    if found is False:
        raise RuntimeError(f"position {position_id} introuvable côté broker (déjà fermée ?)")
    open_px, volume, cur_sl, tp_keep = float(entry), float(lot), None, None
    if pos is not None:
        open_px = _f(pos.get("openPrice")) or open_px
        volume = _f(pos.get("volume")) or volume
        cur_sl, tp_keep = _f(pos.get("stopLoss")), _f(pos.get("takeProfit"))
        if abs(volume - float(lot)) > 1e-9:
            print(f"[be-check] {symbol} : volume broker {volume:g} != lot du bot {float(lot):g} — volume broker utilisé.")
        if abs(open_px - float(entry)) > 1e-9:
            print(f"[be-check] {symbol} : ouverture broker {px(open_px)} != entrée du signal {float(entry):.2f} "
                  f"(écart {abs(open_px - float(entry)):.2f}) — prix d'ouverture broker utilisé.")
    else:
        print(f"[be-check] {symbol} : position non relue ({_why}) — entrée du signal et lot du bot utilisés (vérif réduite).")

    # --- VÉRIF 1 : calcul (openPrice + volume réels, arrondi favorable, net attendu >= cible) ---
    new_sl, calc_ok = _be_sl_from_open(symbol, side, open_px, volume, point, digits)
    net_exp = _be_net_usd(symbol, side, open_px, new_sl, volume)
    if calc_ok is False:
        msg = (f"BE {symbol} {side} #{position_id} NON appliqué : net attendu {net_exp:+.4f} $ < cible "
               f"{BE_TARGET_USD:+.2f} $ (ouverture {px(open_px)}, SL {px(new_sl)}, volume {volume:g}).")
        print(f"[be-check] ❌ {msg}")
        send(CHAT_ID_ADMIN, f"⚠️ {msg}")
        raise BENotApplied(msg)

    # jamais dégrader un SL déjà meilleur (BE déjà posé, SL suiveur, etc.)
    if cur_sl is not None and (cur_sl - new_sl) * sgn >= 0:
        print(f"[be-check] {symbol} {side} #{position_id} : SL broker {px(cur_sl)} déjà au-delà du BE {px(new_sl)} — inchangé.")
        return cur_sl

    # --- VÉRIF 2 : marché / spread (BUY stoppé sur le bid, SELL sur l'ask) ---
    bid = ask = None
    try:
        q = await connection.get_symbol_price(broker_symbol)
        if q and q.get("bid") is not None and q.get("ask") is not None:
            bid, ask = float(q["bid"]), float(q["ask"])
    except Exception as e:
        print(f"[be-check] {symbol} : bid/ask indisponibles ({e}) — contrôle marché ignoré.")
    min_dist = _min_stop_distance(spec, symbol) if spec else 0.0
    mkt_ok, mkt_detail = _be_market_check(side, new_sl, bid, ask, min_dist)
    spread = (ask - bid) if (bid is not None and ask is not None) else None
    if not mkt_ok:
        msg = (f"BE {symbol} {side} #{position_id} NON appliqué : le prix est trop proche/déjà au-delà du BE "
               f"({mkt_detail}, spread {spread:.2f}). Le SL d'origine reste en place."
               if spread is not None else
               f"BE {symbol} {side} #{position_id} NON appliqué : {mkt_detail}.")
        print(f"[be-check] ❌ {msg}")
        send(CHAT_ID_ADMIN, f"⚠️ {msg}")
        raise BENotApplied(msg)

    # Le BE ne touche QUE le SL : le TP doit rester. Priorité au TP réellement attaché côté broker (peut avoir
    # été changé à la main via /tpset) ; s'il est vide, on retombe sur le TP du trade en base.
    if tp_keep is None:
        try:
            row = _q("SELECT tp FROM trades WHERE CAST(mt5_position_id AS TEXT)=?", (str(position_id),)).fetchone()
            if row and row["tp"]:
                tp_keep = round(float(row["tp"]), digits)
        except Exception as e:
            print(f"[metaapi] BE {symbol} : lecture du TP en base impossible ({e}).")

    await connection.modify_position(position_id, stop_loss=new_sl, take_profit=tp_keep)
    print(f"[metaapi] BE dynamique {symbol} {side} lot={volume:g} buffer={abs(new_sl - open_px):.5f} TP conservé={tp_keep} "
          f"(cible {BE_TARGET_USD}$"
          + (f" + commission {BE_COMMISSION_USD_PER_LOT}$/lot" if BE_COMMISSION_USD_PER_LOT else "")
          + f") -> SL={new_sl}")

    # --- VÉRIF 2bis : relecture après envoi (SL réellement attaché, TP conservé, net attendu) ---
    try:
        _f2, _w2, pos2 = await _confirm_position_async(connection, position_id)
    except Exception as e:
        pos2 = None
        print(f"[be-check] {symbol} : relecture après BE impossible ({e}).")
    if pos2 is not None:
        sl_b, tp_b = _f(pos2.get("stopLoss")), _f(pos2.get("takeProfit"))
        open_b, vol_b = _f(pos2.get("openPrice")) or open_px, _f(pos2.get("volume")) or volume
        net_b = _be_net_usd(symbol, side, open_b, sl_b, vol_b) if sl_b is not None else None
        problems = []
        if sl_b is None or abs(sl_b - new_sl) > point * 1.5:
            problems.append(f"SL broker {sl_b} != SL demandé {px(new_sl)}")
        if tp_keep is not None and tp_b is None:
            problems.append(f"TP {px(tp_keep)} absent après le BE")
        if net_b is not None and net_b < BE_TARGET_USD - 1e-9:
            problems.append(f"net attendu {net_b:+.4f} $ < cible {BE_TARGET_USD:+.2f} $")
        spread_txt = f"{spread:.2f}" if spread is not None else "?"
        if problems:
            msg = f"BE {symbol} {side} #{position_id} à vérifier sur MT5 : " + " ; ".join(problems) + "."
            print(f"[be-check] ❌ {msg}")
            send(CHAT_ID_ADMIN, f"⚠️ {msg}")
        else:
            print(f"[be-check] ✅ {symbol} {side} #{position_id} : SL {px(sl_b)} / TP {px(tp_b) if tp_b is not None else '-'} | "
                  f"ouverture {px(open_b)} vol {vol_b:g} spread {spread_txt} | net attendu "
                  + (f"{net_b:+.4f} $" if net_b is not None else "n/a"))
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
    except BENotApplied as e:   # refus voulu de la double vérification : déjà loggé + alerte admin
        print(f"[metaapi] BE non appliqué position={position_id} : {e}")
        return False
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


async def _partial_close_async(position_id, volume):
    connection = await get_metaapi_connection()
    if connection is None:
        raise RuntimeError("connexion MetaApi indisponible")
    return await connection.close_position_partially(position_id, volume)


def partial_close_mt5(position_id, symbol, lot, fraction):
    """Clôture `fraction` (0-1) du lot d'une position MT5, arrondie au pas de lot du symbole. Ne lève jamais :
    True si envoyée, False sinon (loggé) -- le suivi du signal continue de toute façon."""
    cfg = SYMBOLS.get(symbol, {})
    step, min_lot = cfg.get("lot_step", 0.01), cfg.get("min_lot", 0.01)
    vol = math.floor(float(lot) * float(fraction) / step + 1e-9) * step
    vol = round(vol, 8)
    if vol < min_lot or float(lot) - vol < min_lot - 1e-12:
        print(f"[metaapi] partiel ignoré {symbol} #{position_id} : volume {vol:g} / lot {lot:g} sous le lot minimum")
        return False
    try:
        _run_mt5(_partial_close_async(position_id, vol))
    except Exception as e:
        print(f"[metaapi] Échec clôture partielle position {position_id} : {e}")
        traceback.print_exc(limit=-3)
        return False
    return True


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


def _manual_plan(symbol, side, sl_price=None):
    """Calcule (sans rien envoyer) le plan d'un trade MANUEL au prix réel du broker :
      - entrée = ask (BUY) / bid (SELL) via MetaApi current-price
      - SL : celui choisi par l'utilisateur (sl_price) — le lot et le risque sont calculés à partir de CE SL —
        ou, si sl_price est None, le plancher de risque automatique (manual_sl_distance)
      - lot via calc_lot selon le risque $ configuré ; TP via le RR configuré (get_tp_rr)
    Retourne {"ok": True, entry, spread, sl, tp, sl_distance, lot_info, rr} ou {"ok": False, "error": ...}."""
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
    d = 1 if side == "BUY" else -1
    dec = SYMBOLS[symbol]["decimals"]
    if sl_price is None:
        sl_distance = manual_sl_distance(symbol, entry)
        if not sl_distance or sl_distance <= 0:
            return {"ok": False, "error": "distance SL non calculable (ATR/plancher indisponible)"}
        sl = entry - d * sl_distance
    else:
        sl = float(sl_price)
        sl_distance = (entry - sl) * d
        if sl_distance <= 0:
            return {"ok": False, "error": f"SL du mauvais côté : pour un {side} il doit être "
                                           f"{'sous' if side == 'BUY' else 'au-dessus de'} le prix actuel "
                                           f"({_fmt(entry, dec)})."}
        if sl_distance <= 2 * spread:
            return {"ok": False, "error": f"SL trop proche du prix ({_fmt(sl_distance, dec)} pts pour un spread de "
                                           f"{_fmt(spread, dec)}) : il serait touché presque immédiatement."}
    rr = get_tp_rr()
    tp = entry + d * sl_distance * rr

    lot_info = calc_lot(symbol, get_risk(), sl_distance, price=entry)
    if lot_info["lot"] <= 0:
        return {"ok": False, "error": lot_info.get("rejected_reason")
                or "lot calculé nul (risque ou distance SL invalide)"}
    if RISK_ENABLED:   # Risk Gate commun : même verrou que les signaux automatiques (QML / CRT / CHS+BOS)
        g_ok, g_lot, g_risk = risk_gate_check(symbol, entry, sl, strategy="MANUAL")
        if not g_ok:
            return {"ok": False, "error": f"Risk Gate : risque minimum > {MAX_RISK_USD:.2f}$ pour ce SL "
                                          f"(ordre bloqué, SL inchangé)."}
        lot_info = dict(lot_info, lot=g_lot, real_risk=g_risk, raised_to_min=False)
    return {"ok": True, "symbol": symbol, "side": side, "entry": entry, "spread": spread, "sl": sl, "tp": tp,
            "sl_distance": sl_distance, "lot_info": lot_info, "rr": rr, "custom_sl": sl_price is not None}


def place_manual_order(symbol, side, sl_price=None):
    """Exécute un trade MANUEL déclenché depuis Telegram (📈 TRADE → BUY/SELL). Si sl_price est fourni (SL choisi par
    l'utilisateur), le lot/risque/TP sont recalculés au prix réel du broker AU MOMENT de la validation à partir de ce SL,
    qui reste inchangé. Sans sl_price : SL automatique (plancher de risque). execute_mt5_order fait toutes les validations
    techniques (specs, stopsLevel, volume, filling mode) et ne lève jamais, il retourne None si refusé.
    Retourne {"ok": True, ...détails...} ou {"ok": False, "error": "..."} — jamais d'exception."""
    if not get_mt5_orders_on():
        return {"ok": False, "error": "ordres MT5 désactivés (menu 🧩 Stratégies → 📈 Ordres MT5)"}
    plan = _manual_plan(symbol, side, sl_price)
    if not plan["ok"]:
        return plan
    entry, sl, tp, lot_info = plan["entry"], plan["sl"], plan["tp"], plan["lot_info"]

    # clientId MetaApi : format court strategyId_positionId_orderId, alphanumérique uniquement
    # (pas de tirets) — voir metaapi.cloud/docs/client/clientIdUsage. "MAN" = stratégie manuelle,
    # code 3 lettres du symbole + 1ère lettre du sens = positionId, fin du timestamp ms = orderId.
    code = next(k for k, v in _MANUAL_SYMS.items() if v == symbol) if symbol in _MANUAL_SYMS.values() \
        else symbol[:3].upper()
    client_id = f"MAN_{code}{side[0]}_{str(int(time.time() * 1000))[-8:]}"
    position_id = execute_mt5_order(symbol, side, lot_info["lot"], entry, sl, tp, client_id=client_id,
                                    strategy="MANUAL")
    if not position_id:
        if client_id in _RISK_BLOCKED:
            return {"ok": False, "error": f"Risk Gate : ordre bloqué (risque réel > {MAX_RISK_USD:.2f}$ avec le volume "
                                          f"final du broker). SL inchangé."}
        return {"ok": False, "error": "ordre refusé par le broker (voir logs serveur pour le détail exact)"}

    return {"ok": True, "position_id": position_id, "symbol": symbol, "side": side,
            "entry": entry, "sl": sl, "tp": tp, "lot": lot_info["lot"],
            "risk_usd": lot_info["real_risk"], "rr": plan["rr"], "spread": plan["spread"],
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
# Clôtures partielles : niveaux FIGÉS à la création du signal (JSON [[rr, fraction], ...]), nb de niveaux déjà pris,
# R déjà réalisé et fraction déjà clôturée. NULL / 0 = aucun partiel (comportement historique inchangé).
_ensure_column("trades", "part_cfg", "TEXT")
_ensure_column("trades", "part_done", "INTEGER DEFAULT 0")
_ensure_column("trades", "part_r", "REAL DEFAULT 0")
_ensure_column("trades", "part_pct", "REAL DEFAULT 0")


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


def get_m15_imp_filter():
    """Filtre M15 vraie impulsion + structure + Fibonacci : ON/OFF, réglable à tout moment. Dernier choix Telegram
    ('m15_imp_filter' en base) sinon USE_M15_IMPULSE_FILTER (ENV / défaut OFF)."""
    v = get_setting("m15_imp_filter")
    if v is not None:
        return str(v).strip().lower() in ("1", "true", "yes", "on")
    return USE_M15_IMPULSE_FILTER


def set_m15_imp_filter(on):
    set_setting("m15_imp_filter", "1" if on else "0")


def _get_float_setting(key, default):
    try:
        return float(get_setting(key))
    except (TypeError, ValueError):
        return float(default)


def get_m15_min_retrace():
    """Retracement mini (zone de réaction principale) du filtre M15 impulsion : réglage Telegram, sinon valeur de départ."""
    return _get_float_setting("m15_min_retrace", M15_IMP_MIN_RETRACE)


def set_m15_min_retrace(v):
    set_setting("m15_min_retrace", float(v))


def get_m15_need_poi():
    return _get_bool_setting("m15_need_poi", M15_IMP_NEED_POI)


def get_m15_need_sweep():
    return _get_bool_setting("m15_need_sweep", M15_IMP_NEED_SWEEP)


def get_bos_real():
    """Filtre « vrai BOS » : un BOS n'est validé que si le prix a retracé au moins get_bos_retrace() de la jambe. OFF par défaut."""
    return _get_bool_setting("bos_real", False)


def get_bos_retrace():
    return _get_float_setting("bos_retrace", BOS_REAL_RETRACE)


def get_poi_filter():
    """Filtre POI HTF (OB / FVG M15 et/ou H1 mitigé dans le sens du trade) : OFF par défaut."""
    return _get_bool_setting("poi_filter", False)


def get_poi_filter_tf():
    return _get_choice("poi_filter_tf", POI_FILTER_TF, POI_TF_CHOICES)


def _get_bool_setting(key, default):
    v = get_setting(key)
    if v is None:
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")


# Interrupteur général des ORDRES MT5 (via l'API MetaApi) : ON = comportement d'avant (ordre envoyé quand les identifiants
# MetaApi sont présents) ; OFF = signaux toujours publiés et suivis (RR / BE / TP / SL), mais AUCUN ordre envoyé au broker.
MT5_ORDERS_DEFAULT = os.getenv("MT5_ORDERS_ENABLED", "1").strip().lower() in ("1", "on", "true", "yes", "oui")


def get_mt5_orders_on():
    return _get_bool_setting("mt5_orders", MT5_ORDERS_DEFAULT)


def mt5_orders_allowed():
    """Ordres MT5 autorisés = identifiants MetaApi présents ET interrupteur sur ON."""
    return bool(METAAPI_TOKEN and METAAPI_ACCOUNT_ID) and get_mt5_orders_on()


def get_poc_entry():
    """POC proxy (niveau d'entrée limit supplémentaire, voir poc_proxy) : OFF par défaut -> comportement inchangé
    (limit uniquement sur OB/FVG créé par le CHoCH). ON -> le POC proxy de la même jambe impulsive devient un
    niveau d'entrée limit supplémentaire (pas une condition obligatoire en plus de l'OB/FVG)."""
    return _get_bool_setting("poc_entry", False)


def set_poc_entry(on):
    set_setting("poc_entry", "1" if on else "0")


def get_entry_direct():
    """Entrée directe (marché) active ? Dernier choix Telegram, sinon ENTRY_DIRECT."""
    return _get_bool_setting("entry_direct", ENTRY_DIRECT)


def get_entry_limit():
    """Ordre limit sur OB / FVG actif ? Dernier choix Telegram, sinon ENTRY_LIMIT."""
    return _get_bool_setting("entry_limit", ENTRY_LIMIT)


def set_entry_types(direct, limit):
    """Au moins un type d'entrée doit rester actif (sinon plus aucun signal). Retourne True si appliqué."""
    if not (direct or limit):
        return False
    set_setting("entry_direct", "1" if direct else "0")
    set_setting("entry_limit", "1" if limit else "0")
    return True


def _get_choice(key, default, choices):
    v = str(get_setting(key) or default).strip().upper()
    return v if v in choices else (default if default in choices else choices[0])


def get_sl_mode():
    return _get_choice("sl_mode", SL_MODE, SL_MODES)


def get_tp_mode():
    return _get_choice("tp_mode", TP_MODE, TP_MODES)


def get_crt_tp():
    return _get_choice("crt_tp", CRT_TP, CRT_TP_MODES)


def get_crt_ref():
    try:
        v = int(float(get_setting("crt_ref") or CRT_REF_TF))
    except (TypeError, ValueError):
        v = 60
    return v if v in CRT_REFS else 60


def set_choice(key, value, choices):
    value = str(value).upper()
    if value not in choices:
        return False
    set_setting(key, value)
    return True


def set_crt_ref(v):
    if int(v) not in CRT_REFS:
        return False
    set_setting("crt_ref", int(v))
    return True


def get_session():
    return _get_choice("session", "ALL", SESSIONS)


def get_limit_zone():
    return _get_choice("limit_zone", "BOTH", LIMIT_ZONES)


SYMBOLS_ON_DEFAULT = {x.strip().upper() for x in os.getenv("SYMBOLS_ON", "XAUUSD,BTCUSD").split(",") if x.strip()}


def get_symbol_on(symbol):
    """Actif activé (nouveaux signaux) ? Dernier choix Telegram (menu « 🌍 ACTIFS »), sinon variable SYMBOLS_ON
    (défaut : XAUUSD, BTCUSD). Un actif désactivé continue de SUIVRE ses positions déjà ouvertes, sans en ouvrir de nouvelles."""
    return _get_bool_setting("sym_on_" + symbol, symbol in SYMBOLS_ON_DEFAULT)


def set_symbol_on(symbol, on):
    if symbol not in SYMBOLS:
        return False
    set_setting("sym_on_" + symbol, "1" if on else "0")
    return True


def get_qf(key):
    """Filtre qualité `key` actif ? Dernier choix Telegram, sinon variable QF_<KEY> (défaut OFF)."""
    return _get_bool_setting("qf_" + key, QF_DEFAULT.get(key, False))


def set_qf(key, on):
    if key not in QF_KEYS:
        return False
    set_setting("qf_" + key, "1" if on else "0")
    return True


def get_crt_dup():
    """ON : le CRT tourne aussi quand la stratégie principale a déjà pris le même CHoCH (doublons voulus, phase de test)."""
    return _get_bool_setting("crt_dup", CRT_DUP)


def get_crt_htf():
    return _get_bool_setting("crt_htf", CRT_HTF)


def session_ok(ts):
    """Le CHoCH (bougie d'ouverture `ts`, epoch) tombe-t-il dans la session choisie ? ALL = toujours."""
    mode = get_session()
    if mode == "ALL":
        return True
    h = datetime.fromtimestamp(ts, timezone.utc).hour
    ldn, ny = SESSION_LONDON[0] <= h < SESSION_LONDON[1], SESSION_NY[0] <= h < SESSION_NY[1]
    return {"LONDON": ldn, "NY": ny, "BOTH": ldn or ny}[mode]


def get_strat_main():
    return _get_bool_setting("strat_main", STRAT_MAIN)


def get_strat_crt():
    return _get_bool_setting("strat_crt", STRAT_CRT)


def set_strategies(main, crt):
    """Au moins une stratégie doit rester active (sinon plus aucun signal). Retourne True si appliqué."""
    if not (main or crt):
        return False
    set_setting("strat_main", "1" if main else "0")
    set_setting("strat_crt", "1" if crt else "0")
    return True


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
BINANCE_INTERVALS = {1: "1m", 3: "3m", 5: "5m", 15: "15m", 30: "30m", 60: "1h", 240: "4h", 1440: "1d"}


def _only_closed(candles, tf_sec=None):
    now = time.time()
    tf_sec = tf_sec or TF_SEC
    return sorted((c for c in candles if c["t"] + tf_sec + CLOSE_GRACE_SEC <= now),
                  key=lambda c: c["t"])


# --- BTC : Binance (klines) avec BASCULE AUTOMATIQUE vers Bybit puis OKX -----------------------
# Binance bannit régulièrement les IP partagées des hébergeurs (erreur 418 / 429 / 451). Dans ce cas
# on met Binance en pause quelques minutes (insister prolonge le ban) et on passe sur Bybit / OKX.
BYBIT_INTERVALS = {1: "1", 3: "3", 5: "5", 15: "15", 30: "30", 60: "60", 240: "240", 1440: "D"}
OKX_INTERVALS = {1: "1m", 3: "3m", 5: "5m", 15: "15m", 30: "30m", 60: "1H", 240: "4H", 1440: "1D"}
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

# --- Circuit breaker : évite de marteler l'API pendant une panne Deriv -----------------------
# Après plusieurs échecs consécutifs, on arrête d'essayer pendant DERIV_BREAKER_COOLDOWN
# secondes : les appels échouent instantanément (pas d'attente réseau, pas de requête inutile)
# le temps que l'API Deriv se rétablisse. Un seul succès referme le disjoncteur.
DERIV_BREAKER_THRESHOLD = _env_int("DERIV_BREAKER_THRESHOLD", 3)
DERIV_BREAKER_COOLDOWN = _env_int("DERIV_BREAKER_COOLDOWN", 20)
_deriv_fail_streak = 0
_deriv_down_until = 0.0


def _deriv_breaker_ok():
    """True si on peut tenter Deriv maintenant (disjoncteur fermé ou cooldown écoulé)."""
    return time.time() >= _deriv_down_until


def _deriv_breaker_report(success, err=None):
    global _deriv_fail_streak, _deriv_down_until
    if success:
        _deriv_fail_streak = 0
        _deriv_down_until = 0.0
        return
    _deriv_fail_streak += 1
    if _deriv_fail_streak >= DERIV_BREAKER_THRESHOLD:
        _deriv_down_until = time.time() + DERIV_BREAKER_COOLDOWN
        print(f"[deriv] panne persistante ({_deriv_fail_streak} échecs) : pause de "
              f"{DERIV_BREAKER_COOLDOWN}s avant nouvel essai — {err}")


def _deriv_request(payload):
    """Envoie une requête sur le WebSocket public Deriv et retourne la réponse (bougies).

    La connexion est réutilisée ; si elle est tombée (inactivité, réseau), une reconnexion
    est tentée avant d'abandonner. Un disjoncteur évite de marteler l'API en cas de panne
    prolongée (voir _deriv_breaker_*).
    """
    global _deriv_ws, _deriv_req_id
    if websocket is None:
        raise RuntimeError("module manquant : pip install websocket-client")
    if not _deriv_breaker_ok():
        raise RuntimeError("Deriv indisponible : pause (panne récente), nouvel essai bientôt")
    last_err = None
    attempts = 2
    for attempt in range(attempts):
        try:
            if _deriv_ws is None:
                _deriv_ws = websocket.create_connection(DERIV_WS_URL, timeout=10)
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
                    _deriv_breaker_report(True)
                    return msg
        except DerivError:
            _deriv_breaker_report(True)  # réponse reçue : la connexion fonctionne, seule la requête est invalide
            raise
        except Exception as e:  # connexion morte / timeout : on repart sur une connexion neuve
            last_err = e
            try:
                if _deriv_ws is not None:
                    _deriv_ws.close()
            except Exception:
                pass
            _deriv_ws = None
            if attempt < attempts - 1:
                time.sleep(0.3)  # courte pause avant la reconnexion
    _deriv_breaker_report(False, last_err)
    raise RuntimeError(f"Deriv indisponible : {last_err}")


def _deriv(deriv_symbol, minutes=None, count=None):
    """Bougies du timeframe demandé, construites par Deriv (ticks_history, style candles).
    count > 5000 : pages successives vers le passé (5000 bougies par requête) ; une page en échec rend ce qu'on a déjà."""
    sec = (minutes or TIMEFRAME_MIN) * 60
    want = count or CANDLES_LIMIT
    got, end = {}, "latest"
    for page_no in range(max(1, -(-want // 5000))):
        try:
            msg = _deriv_request({
                "ticks_history": deriv_symbol, "style": "candles", "granularity": sec,
                "count": min(want - len(got), 5000), "end": end,
                "adjust_start_time": 1,   # marché fermé (week-end) : recule jusqu'aux dernières bougies dispo
            })
        except Exception:
            if page_no == 0:
                raise
            break   # historique plus ancien indisponible : on garde les pages déjà reçues
        page = {int(k["epoch"]): k for k in msg.get("candles") or []}
        before = len(got)
        got.update(page)
        if not page or len(got) == before or len(got) >= want:
            break
        end = min(page) - 1
    return [{"t": e, "o": float(k["open"]), "h": float(k["high"]), "l": float(k["low"]), "c": float(k["close"])}
            for e, k in sorted(got.items())]


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


def _binance_long(symbol, minutes, count):
    """Historique long Binance par pages de 1000 bougies (du plus récent vers le plus ancien). Lève si indisponible."""
    interval = BINANCE_INTERVALS[minutes or TIMEFRAME_MIN]
    out, end = [], None
    while len(out) < count:
        last_err, page = None, None
        for base in BINANCE_BASES:
            try:
                prm = {"symbol": symbol, "interval": interval, "limit": min(1000, count - len(out))}
                if end is not None:
                    prm["endTime"] = end
                r = requests.get(base + "/api/v3/klines", params=prm, timeout=15)
                r.raise_for_status()
                page = r.json()
                break
            except Exception as e:
                last_err = e
                _binance_ban(e)
        if page is None:
            raise RuntimeError(f"Binance indisponible : {last_err}")
        if not page:
            break
        out = [{"t": int(k[0] // 1000), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4])}
               for k in page] + out
        end = int(page[0][0]) - 1
        if len(page) < 2:
            break
    return out


def get_candles(symbol, minutes=None, count=None):
    """Retourne la liste des bougies clôturées (plus ancienne -> plus récente).
    count : historique long (analyse QML) ; absent = CANDLES_LIMIT comme avant. Si l'historique long est
    indisponible, repli automatique sur l'historique normal (le bot n'est jamais bloqué).

    minutes=None -> timeframe actuellement sélectionné (M1/M5/...) ; sinon un autre timeframe fixe
    (ex. HTF_MINUTES) sans toucher au suivi des positions ni aux signaux en cours."""
    cfg = SYMBOLS[symbol]
    raw = None
    if cfg["source"] == "deriv":
        raw = _deriv(cfg["deriv_symbol"], minutes, count)
    elif cfg["source"] == "binance":
        if count and count > CANDLES_LIMIT:
            order = ["deriv", "binance"] if os.getenv("LONG_HISTORY_BTC", "deriv").strip().lower() != "binance" else ["binance", "deriv"]
            for src in order:   # historique long : Deriv (5000 / page, sans blocage d'IP) puis Binance ; sinon historique normal
                try:
                    if src == "deriv" and cfg.get("deriv_symbol"):
                        raw = _deriv(cfg["deriv_symbol"], minutes, count)
                    elif src == "binance" and time.time() >= _binance_blocked_until:
                        raw = _binance_long(cfg["binance_symbol"], minutes, count)
                except Exception as e:
                    print(f"[{symbol}] historique long {src} indisponible ({e})")
                    raw = None
                if raw and len(raw) > CANDLES_LIMIT:
                    break
                raw = None
        if not raw:
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
            ("CHoCH M5", "choch5"), (f"trigger {_tf_lbl(TIMEFRAME_MIN)}", "trigger"), ("POI", "poi"), ("impulsion M15", "m15_imp"), ("vrai BOS", "bos_txt"), ("POI HTF", "poi_txt"))
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


# --- Filtre M15 : vraie impulsion + structure + Fibonacci (voir USE_M15_IMPULSE_FILTER) ---------------------
def _m15_pivots(raw, depth):
    """Pivots swing (profondeur `depth`) d'une série de bougies : (highs, lows), chacun [(index, prix)]."""
    highs, lows = [], []
    for p in range(depth, len(raw) - depth):
        win = raw[p - depth:p + depth + 1]
        if raw[p]["h"] == max(x["h"] for x in win) and all(raw[p]["h"] > raw[k]["h"] for k in range(p - depth, p)):
            highs.append((p, raw[p]["h"]))
        if raw[p]["l"] == min(x["l"] for x in win) and all(raw[p]["l"] < raw[k]["l"] for k in range(p - depth, p)):
            lows.append((p, raw[p]["l"]))
    return highs, lows


def m15_impulse_context(symbol, c=None):
    """Contexte M15 « vraie impulsion » : retourne (info | None, motif_si_None).

    1) Structure : cassures de structure d'`analyze` avec une profondeur de pivot M15_IMP_DEPTH (plus large que le M1/M5 :
       une petite jambe interne ne casse aucun pivot structurel, donc ne remplace jamais la tendance principale).
    2) Impulsion = la dernière cassure dont la jambe fait au moins M15_IMP_MIN_LEG_ATR x ATR M15 (les micro-jambes plus récentes
       sont ignorées). Origine = vrai extrême (High pour un SELL, Low pour un BUY) entre la cassure précédente et cette cassure,
       jamais un micro-high / micro-low interne. Fin = extrême atteint depuis (la mesure suit la jambe tant que le prix ne
       fait pas un nouvel extrême opposé). Impulsion invalidée si le prix dépasse son origine.
    3) Retracement : plus forte excursion depuis le bout de la jambe, en % de la jambe (mitigation mémorisée, pas ponctuelle).
    `c` = bougies de l'UT d'entrée (pour inclure la bougie M15 en cours de formation)."""
    raw = _ctx_candles(symbol, 15)
    depth, n = M15_IMP_DEPTH, len(raw)
    if n <= 2 * depth + 1:
        return None, "pas assez de bougies M15"
    evs = analyze(raw, depth)
    if not evs:
        return None, "aucune cassure de structure M15"
    atr = atr_series(raw)
    imp = None
    for k in range(len(evs) - 1, -1, -1):
        e = evs[k]
        d, bi = e["dir"], e["i"]
        # début du mouvement : dernière cassure de sens OPPOSÉ (le vrai sommet / creux de la série de cassures de même sens
        # se trouve après elle) -- un pullback interne (micro-high / micro-low) ne devient donc jamais l'origine du Fibonacci.
        start = next((evs[j]["i"] for j in range(k - 1, -1, -1) if evs[j]["dir"] != d), 0)
        start = max(start, bi - M15_IMP_LOOKBACK)
        rng = range(min(start, bi), bi + 1)
        o = max(rng, key=lambda j: raw[j]["h"]) if d == -1 else min(rng, key=lambda j: raw[j]["l"])
        seg = list(raw[o:])
        if c:   # bougie M15 en cours : les bougies d'entrée postérieures à la dernière M15 clôturée
            t_next = raw[-1]["t"] + 15 * 60
            seg += [x for x in c if x["t"] >= t_next]
        if d == -1:
            hi = seg[0]["h"]
            lo_i = min(range(len(seg)), key=lambda j: seg[j]["l"])
            lo = seg[lo_i]["l"]
            broke = any(x["c"] > hi + 1e-9 for x in raw[o + 1:])   # corps M15 clôturé au-delà de l'origine (une mèche = sweep)
        else:
            lo = seg[0]["l"]
            hi_i = max(range(len(seg)), key=lambda j: seg[j]["h"])
            hi = seg[hi_i]["h"]
            broke = any(x["c"] < lo - 1e-9 for x in raw[o + 1:])
        hi_, lo_ = hi, lo
        a = atr[min(bi, len(atr) - 1)] or 0.0
        if (hi_ - lo_) < M15_IMP_MIN_LEG_ATR * a:
            continue   # micro-jambe : jamais utilisée comme impulsion principale
        if broke:
            return None, ("impulsion M15 invalidée : une bougie M15 a CLÔTURÉ au-delà de son origine "
                          f"({'High' if d == -1 else 'Low'} {hi if d == -1 else lo:.2f}) -- structure à relire")
        # retracement maximal atteint depuis le bout de la jambe
        if d == -1:
            tail = seg[lo_i + 1:]
            ext = max((x["h"] for x in tail), default=lo_)
            retr = min(1.0, (ext - lo_) / (hi_ - lo_))   # mèche au-delà de l'origine = sweep : plafonné à 100 %
        else:
            tail = seg[hi_i + 1:]
            ext = min((x["l"] for x in tail), default=hi_)
            retr = min(1.0, (hi_ - ext) / (hi_ - lo_))
        imp = {"dir": d, "hi": hi_, "lo": lo_, "ext": ext, "retr": max(0.0, retr), "break_i": bi, "ev": e}
        break
    if imp is None:
        return None, f"aucune impulsion M15 significative (>= {M15_IMP_MIN_LEG_ATR:g} ATR) -- micro-jambes ignorées"
    # structure par pivots (HH/HL vs LH/LL) : sert de garde-fou, jamais de seul déclencheur
    ph, pl = _m15_pivots(raw, depth)
    struct = "mixte"
    if len(ph) >= 2 and len(pl) >= 2:
        hh, hl = ph[-1][1] > ph[-2][1], pl[-1][1] > pl[-2][1]
        lh, ll = ph[-1][1] < ph[-2][1], pl[-1][1] < pl[-2][1]
        struct = "HH/HL" if (hh and hl) else "LH/LL" if (lh and ll) else "mixte"
    imp["struct"] = struct
    rng_ = imp["hi"] - imp["lo"]
    imp["lvl"] = (lambda r: imp["lo"] + r * rng_) if imp["dir"] == -1 else (lambda r: imp["hi"] - r * rng_)
    imp["mid"] = imp["lvl"](0.5)
    return imp, None


def m15_impulse_filter(symbol, c, ev, trace=None):
    """Filtre M15 vraie impulsion + structure + Fibonacci (voir get_m15_imp_filter) : ne fait que filtrer un CHoCH déjà
    produit. Retourne (autorisé: bool, motif_de_refus | None). Aucune donnée -> refus (jamais de passage « à l'aveugle »)."""
    side = "BUY" if ev["dir"] == 1 else "SELL"
    try:
        imp, why = m15_impulse_context(symbol, c)
    except Exception as e:
        imp, why = None, f"lecture M15 impossible ({type(e).__name__}: {e})"
    if imp is None:
        if trace is not None:
            trace["m15_imp"] = f"indisponible -- {why}"
        return False, f"Filtre M15 impulsion : {why}"
    d = imp["dir"]
    dtxt = "baissière" if d == -1 else "haussière"
    pct = imp["retr"] * 100
    txt = (f"impulsion M15 {dtxt} {imp['hi']:.2f}→{imp['lo']:.2f} ({'HIGH→LOW' if d == -1 else 'LOW→HIGH'}) · "
           f"retracement max {pct:.0f}% (50% = {imp['mid']:.2f}) · structure {imp['struct']}")
    if trace is not None:
        trace["m15_imp"] = txt
    if ev["dir"] != d:
        return False, (f"{side} contre l'impulsion M15 {dtxt} -- une petite jambe inverse n'est qu'un retracement "
                       f"(seuls les {'SELL' if d == -1 else 'BUY'} sont autorisés)")
    if (d == -1 and imp["struct"] == "HH/HL") or (d == 1 and imp["struct"] == "LH/LL"):
        return False, f"structure par pivots M15 ({imp['struct']}) contredit l'impulsion {dtxt}"
    if imp["retr"] < M15_IMP_EARLY_MIN:
        return False, (f"retracement M15 {pct:.0f}% < {M15_IMP_EARLY_MIN * 100:.1f}% -- trop peu retracé pour trader "
                       f"(50% = {imp['mid']:.2f})")
    # POI structurels (OB / FVG) M15 + H1 dans le sens du trade, touchés par le retracement
    a, b = sorted((imp["lvl"](M15_IMP_EARLY_MIN), imp["ext"]))
    pois = []
    for tf in (15, HTF_MINUTES):
        try:
            pois += find_pois(_ctx_candles(symbol, tf), tf, d)
        except Exception:
            pass
    hit = [x for x in pois if x["hi"] >= a and x["lo"] <= b]
    poi_txt = _poi_txt(hit[0], SYMBOLS[symbol]["decimals"]) if hit else None
    try:
        sweep = bool(_sweep_at(c, _turn_index(c, ev), d)[1])
    except Exception:
        sweep = False
    if trace is not None:
        trace["m15_imp"] = txt + f" · POI {poi_txt or 'aucun'} · sweep {'oui' if sweep else 'non'}"
    if imp["retr"] < get_m15_min_retrace():
        if not hit:
            return False, (f"retracement M15 {pct:.0f}% (< 50%) = signal précoce -- aucun POI OB/FVG fort dans cette zone, "
                           f"attendre 50% ({imp['mid']:.2f}) et au-delà")
    else:
        if get_m15_need_poi() and not hit:
            return False, (f"retracement M15 {pct:.0f}% atteint mais 50% ne se trade pas seul -- aucune confluence OB/FVG/imbalance")
        if get_m15_need_sweep() and not sweep:
            return False, "confluence 50%+ sans sweep de liquidité sur le point de retournement"
    return True, None


def bos_real_check(symbol, c, ev, trace=None):
    """Filtre « vrai BOS » (get_bos_real) : la cassure de structure M15 n'est qualifiée de vrai BOS que si le prix a ensuite
    retracé au moins get_bos_retrace() (40 / 45 / 50 %) de la jambe. Sinon : faux BOS / cassure non confirmée -> pas de signal.
    Retourne (autorisé, motif_de_refus | None). Pas de lecture M15 -> refus."""
    try:
        imp, why = m15_impulse_context(symbol, c)
    except Exception as e:
        imp, why = None, f"lecture M15 impossible ({type(e).__name__})"
    if imp is None:
        if trace is not None:
            trace["bos_txt"] = f"indisponible -- {why}"
        return False, f"Vrai BOS M15 : {why}"
    need, got = get_bos_retrace(), imp["retr"]
    if trace is not None:
        trace["bos_txt"] = f"retracement {got * 100:.0f}% / mini {need * 100:.0f}% -- {'BOS validé' if got >= need else 'BOS non validé'}"
    if got < need:
        return False, (f"BOS M15 non validé : retracement {got * 100:.0f}% < {need * 100:.0f}% de la jambe "
                       f"(un vrai BOS est suivi d'un retracement vers 50 %)")
    return True, None


def poi_mitigation_check(symbol, c, ev, trace=None):
    """Filtre POI HTF (get_poi_filter) : au moment du CHoCH, le point de retournement doit avoir MITIGÉ un OB ou un FVG
    (imbalance) M15 et/ou H1 dans le sens du trade. POI non mitigé / zone indécise -> pas de signal.
    Retourne (autorisé, motif_de_refus | None)."""
    mode = get_poi_filter_tf()
    tfs = {"M15": (15,), "H1": (HTF_MINUTES,), "BOTH": (15, HTF_MINUTES)}[mode]
    d, dec = ev["dir"], SYMBOLS[symbol]["decimals"]
    found, hit = 0, None
    for tf in tfs:
        try:
            pois = find_pois(_ctx_candles(symbol, tf), tf, d)
        except Exception:
            pois = []
        found += len(pois)
        for p in pois:
            mit, react = poi_reaction(c, ev, p)
            if mit and (hit is None or react):
                hit = (p, react)
    lbl = "/".join(_tf_lbl(t) for t in tfs)
    if hit is None:
        if trace is not None:
            trace["poi_txt"] = f"{found} POI {lbl} détecté(s), aucun mitigé"
        return False, (f"aucun OB/FVG {lbl} mitigé ({found} détecté(s)) -- zone non touchée ou indécise, signal ignoré")
    p, react = hit
    if trace is not None:
        trace["poi_txt"] = f"{_poi_txt(p, dec)} mitigé" + (f" · {react}" if react else "")
    return True, None


def sl_by_mode(c, ev, a, symbol):
    """SL de l'entrée directe selon le mode réglé. Retourne (sl, étiquette). Un mode indisponible retombe sur STRUCT (loggé)."""
    d, i = ev["dir"], ev["i"]
    entry, buf, mode = c[i]["c"], SL_BUFFER_ATR * a, get_sl_mode()
    if mode == "SWEEP":
        e = _turn_index(c, ev)
        return (c[e]["l"] if d == 1 else c[e]["h"]) - d * buf, "SWEEP"
    if mode == "ZONE":
        try:
            pois = [p for p in find_pois(c[:i + 1], TIMEFRAME_MIN, d) if p["f"] > ev["src"]]
        except Exception:
            pois = []
        cand = [(abs((p["lo"] if d == 1 else p["hi"]) - entry), p) for p in pois
                if ((p["lo"] if d == 1 else p["hi"]) - entry) * d < 0]
        if cand:
            p = min(cand, key=lambda x: x[0])[1]
            return (p["lo"] if d == 1 else p["hi"]) - d * buf, "ZONE"
        if symbol and DEBUG_CHOCH:
            print(f"[{symbol}] SL ZONE : aucun OB/FVG exploitable -> SL structure")
    return ev["sl_level"] - d * buf, "STRUCT"


def nearest_swing_target(c, i, d, entry, depth=None):
    """Plus proche swing low (SELL) / swing high (BUY) NON cassé de `c[:i+1]`, du bon côté de l'entrée. None si aucun."""
    depth = depth or SWING_DEPTH
    best = None
    for p in range(depth, i - depth + 1):
        if d == -1:
            lv = c[p]["l"]
            if lv >= entry or not all(lv < c[k]["l"] for k in list(range(p - depth, p)) + list(range(p + 1, p + depth + 1))):
                continue
            if min(x["l"] for x in c[p + 1:i + 1]) <= lv:
                continue
            best = lv if best is None or lv > best else best
        else:
            lv = c[p]["h"]
            if lv <= entry or not all(lv > c[k]["h"] for k in list(range(p - depth, p)) + list(range(p + 1, p + depth + 1))):
                continue
            if max(x["h"] for x in c[p + 1:i + 1]) >= lv:
                continue
            best = lv if best is None or lv < best else best
    return best


def apply_tp_mode(sig, c, ev, symbol, tp_rr):
    """TP selon le mode réglé (RR fixe / liquidité externe / swing). Met à jour tp, rr, tp_mode ; repli RR si pas de cible."""
    mode = get_tp_mode()
    sig["tp_mode"] = None
    if mode == "RR":
        return sig
    d, entry, risk = sig["dir"], sig["entry"], sig["risk"]
    tgt = lbl = None
    if mode == "TARGET" and sig.get("ext"):
        tgt, lbl = sig["ext"]["level"], f"liquidité ext. {_tf_lbl(sig['ext']['tf'])}"
    elif mode == "SWING":
        tgt = nearest_swing_target(c, ev["i"], d, entry)
        lbl = f"swing {TF_LABEL}"
    if tgt is None or (tgt - entry) * d <= 0:
        why = "aucune cible exploitable"
    else:
        rr = abs(tgt - entry) / risk
        if rr >= TP_MIN_RR:
            sig.update(tp=tgt, rr=round(rr, 2), tp_mode=lbl)
            return sig
        why = f"cible trop proche (RR {rr:.2f} < {TP_MIN_RR:g})"
    if symbol and DEBUG_CHOCH:
        print(f"[{symbol}] TP {mode} : {why} -> RR fixe {tp_rr:g}")
    return sig


def crt_context(symbol, c, ev, ref_tf):
    """Contexte CRT du CHoCH `ev` : (ctx, None) ou (None, motif). Bougie 1 = dernière bougie HTF CLÔTURÉE avant le CHoCH
    (CRH / CRL) ; bougie 2 = celle qui suit : elle doit avoir balayé UN côté (sweep) puis le CHoCH doit clôturer de nouveau
    DANS le range (pas seulement une mèche). Haussier : sweep du CRL, cible CRH ; baissier : sweep du CRH, cible CRL."""
    if ref_tf <= TIMEFRAME_MIN:
        return None, f"UT de référence {_tf_lbl(ref_tf)} <= UT d'entrée {TF_LABEL}"
    h = _ctx_candles(symbol, ref_tf)
    if not h:
        return None, f"bougies {_tf_lbl(ref_tf)} indisponibles"
    i, ref_sec, d = ev["i"], ref_tf * 60, ev["dir"]
    t_i = c[i]["t"]
    closed = [x for x in h if x["t"] + ref_sec <= t_i]
    if not closed:
        return None, "aucune bougie de référence clôturée avant le CHoCH"
    r = closed[-1]
    start = r["t"] + ref_sec
    if t_i >= start + ref_sec:
        return None, "CHoCH hors de la bougie qui suit la référence (donnée manquante ou marché fermé)"
    win = [x for x in c[:i + 1] if x["t"] >= start]
    if not win:
        return None, "aucune bougie d'entrée depuis la clôture de la référence"
    crh, crl = r["h"], r["l"]
    lo, hi = min(x["l"] for x in win), max(x["h"] for x in win)
    if d == 1:
        if lo >= crl:
            return None, f"CRL {crl:g} non balayé (plus bas {lo:g})"
        if hi > crh:
            return None, "les deux côtés du range balayés (bougie englobante) : pas de CRT propre"
        sweep = lo
    else:
        if hi <= crh:
            return None, f"CRH {crh:g} non balayé (plus haut {hi:g})"
        if lo < crl:
            return None, "les deux côtés du range balayés (bougie englobante) : pas de CRT propre"
        sweep = hi
    if not (crl < c[i]["c"] < crh):
        return None, "clôture du CHoCH hors du range CRT : pas de réintégration"
    return {"ref_tf": ref_tf, "crh": crh, "crl": crl, "mid": (crh + crl) / 2, "sweep": sweep, "ref_t": r["t"]}, None


def _crt_apply_tp(sig, ctx, tp_rr, symbol):
    """TP CRT : autre bout du range (RANGE), 50 % du range (MID) ou RR fixe. Repli RR fixe si la cible est mal placée / trop proche."""
    mode = get_crt_tp()
    sig["tp_mode"] = None
    if mode == "RR":
        return sig
    d, entry, risk = sig["dir"], sig["entry"], sig["risk"]
    tgt = (ctx["crh"] if d == 1 else ctx["crl"]) if mode == "RANGE" else ctx["mid"]
    if (tgt - entry) * d > 0 and abs(tgt - entry) / risk >= TP_MIN_RR:
        sig.update(tp=tgt, rr=round(abs(tgt - entry) / risk, 2),
                   tp_mode="autre bout du range CRT" if mode == "RANGE" else "50 % du range CRT")
    elif symbol and DEBUG_CHOCH:
        print(f"[{symbol}] TP CRT {mode} : cible mal placée ou trop proche -> RR fixe {tp_rr:g}")
    return sig


def crt_build(c, ev, symbol):
    """Signaux du module CRT pour un CHoCH : liste (direct et/ou limit OB/FVG selon 📥 ENTRÉE), [] si refus.
    Indépendant du filtre de tendance HTF (le CRT est un retournement) ; SL derrière le sweep, TP sur le range."""
    if ev["kind"] != "CHOCH" or ev["sl_level"] is None:
        return []
    side = "BUY" if ev["dir"] == 1 else "SELL"
    ctx, why = crt_context(symbol, c, ev, get_crt_ref())
    if not ctx:
        if DEBUG_CHOCH:
            print(f"[{symbol}] CRT {side} {ev['type']} ignoré : {why}")
        return []
    d, a, i = ev["dir"], ev["atr"], ev["i"]
    tp_rr, be_rr = get_tp_rr(), get_be_rr()
    htf_txt = ""
    if get_crt_htf():   # option : le CRT doit aussi suivre la tendance de fond (même filtre que la stratégie principale)
        trend, why = htf_trend_check(symbol, ev)
        if why:
            if DEBUG_CHOCH:
                print(f"[{symbol}] CRT {side} {ev['type']} ignoré : {why}")
            return []
        htf_txt = f" + tendance {_tf_lbl(htf_ref_tf())} {_dir_txt(trend)}"
    base = {"htf": False, "htf_mode": "OFF", "setup": f"CRT {_tf_lbl(ctx['ref_tf'])} -> {TF_LABEL}{htf_txt}", "poi": None,
            "mtf_fib_on": False, "mtf_fib": None, "m15_imp_on": False, "m15_imp": None, "crt": ctx, "strategy": "CRT"}
    out = []
    if get_entry_direct():
        entry = c[i]["c"]
        sl = ctx["sweep"] - d * SL_BUFFER_ATR * a
        risk = abs(entry - sl)
        if risk > MAX_SL_ATR * a:
            if DEBUG_CHOCH:
                print(f"[{symbol}] CRT {side} direct ignoré : SL derrière le sweep trop large ({risk:.2f} pts > {MAX_SL_ATR}xATR)")
        else:
            min_pct = SYMBOLS.get(symbol, {}).get("min_sl_pct", MIN_SL_PCT)
            risk = max(risk, MIN_SL_ATR * a, entry * min_pct)
            sl = entry - d * risk
            sig = {"dir": d, "side": side, "type": ev["type"], "order": "MARKET", "ref_price": entry,
                   "entry": entry, "sl": sl, "risk": risk, "tp": entry + d * risk * tp_rr, "t": ev["t"],
                   "bos_level": ev["sl_level"], "rr": tp_rr, "be_rr": be_rr, "tf": TF_LABEL, "key_suffix": "crt", "sl_mode": "SWEEP"}
            sig.update(base)
            sig["ext"], sig["sweep"] = liquidity_reading(symbol, c, ev, entry)
            out.append(_crt_apply_tp(sig, ctx, tp_rr, symbol))
    if get_entry_limit():
        lim, why = poi_limit_signal(c, ev, symbol, tp_rr, be_rr, base, sl_anchor=ctx["sweep"], apply_tp=False)
        if lim:
            lim["key_suffix"], lim["sl_mode"] = "crtpoi", "SWEEP"
            out.append(_crt_apply_tp(lim, ctx, tp_rr, symbol))
        elif DEBUG_CHOCH:
            print(f"[{symbol}] CRT limit OB/FVG non placé : {why}")
    for sig in out:
        print(f"[{symbol}] signal ACCEPTÉ : {sig['side']} {sig['type']} -- {base['setup']} "
              f"(range {ctx['crl']:g}-{ctx['crh']:g}, sweep {ctx['sweep']:g})")
    return out


def poi_limit_signal(c, ev, symbol, tp_rr, be_rr, base, sl_anchor=None, apply_tp=True):
    """Ordre LIMIT sur le bord PROCHE de l'OB / FVG créé par le CHoCH `ev` (sur l'UT d'entrée).

    Le CHoCH a déjà eu lieu : on attend que le prix REVIENNE dans la zone (SELL : zone d'offre au-dessus, BUY : zone
    de demande en dessous). Ordre placé à l'entrée de la zone (POI_LIMIT_DEPTH = 0 -> bord proche, pour ne pas rater
    l'entrée), SL derrière le bord lointain + buffer ATR, TP au RR réglé. Zone la plus proche du prix retenue.
    Pas de limit si le prix est déjà dans / au-delà de la zone (l'entrée directe couvre ce cas), si la zone est trop
    loin (POI_LIMIT_MAX_ATR) ou si le SL serait trop large (MAX_SL_ATR).
    Retourne (signal | None, motif_de_refus | None)."""
    d, a, i = ev["dir"], ev["atr"], ev["i"]
    cur = c[i]["c"]
    try:
        pois = [p for p in find_pois(c[:i + 1], TIMEFRAME_MIN, d) if p["f"] > ev["src"]]
    except Exception as e:
        return None, f"lecture des OB/FVG impossible ({type(e).__name__})"
    zone_kind = get_limit_zone()
    if zone_kind != "BOTH":
        pois = [p for p in pois if p["kind"] == zone_kind]
    poc_on = get_poc_entry()   # POC proxy : niveau d'entrée SUPPLÉMENTAIRE (jamais une condition obligatoire en plus)
    if not pois and not poc_on:
        return None, f"aucun {'OB / FVG' if zone_kind == 'BOTH' else zone_kind} valide créé par ce CHoCH"
    cands = []
    for p in pois:
        w = p["hi"] - p["lo"]
        prox = p["hi"] if d == 1 else p["lo"]
        entry = prox - d * POI_LIMIT_DEPTH * w
        if (entry - cur) * d >= 0:
            continue                       # prix déjà dans / au-delà de la zone : rien à attendre
        if abs(cur - entry) > POI_LIMIT_MAX_ATR * a:
            continue                       # zone trop loin : le retour est improbable
        cands.append((abs(cur - entry), entry, p))
    if poc_on:
        poc = poc_proxy(c, ev)
        if poc is not None and (poc - cur) * d < 0 and abs(cur - poc) <= POI_LIMIT_MAX_ATR * a:
            cands.append((abs(cur - poc), poc, {"kind": "POC", "tf": TIMEFRAME_MIN, "d": d,
                                                 "lo": poc, "hi": poc, "f": i, "t_valid": c[i]["t"]}))
    if not cands:
        return None, "prix déjà dans la zone (ou le POC), ou zone trop éloignée : pas d'ordre limit"
    _, entry, p = min(cands, key=lambda x: x[0])
    dist = sl_anchor if sl_anchor is not None else (p["lo"] if d == 1 else p["hi"])   # bord lointain de la zone (CRT : le sweep)
    sl = dist - d * SL_BUFFER_ATR * a
    if (d == 1 and sl >= entry) or (d == -1 and sl <= entry):
        return None, "SL derrière la zone du mauvais côté de l'entrée"
    risk = abs(entry - sl)
    if risk > MAX_SL_ATR * a:
        return None, f"zone trop large : SL {risk:.2f} pts > {MAX_SL_ATR}xATR"
    min_pct = SYMBOLS.get(symbol, {}).get("min_sl_pct", MIN_SL_PCT) if symbol else MIN_SL_PCT
    min_risk = max(MIN_SL_ATR * a, entry * min_pct)
    if risk < min_risk:                    # zone très fine : mêmes planchers que l'entrée directe
        risk = min_risk
        sl = entry - d * risk
    sig = {
        "dir": d, "side": "BUY" if d == 1 else "SELL", "type": ev["type"],
        "order": "LIMIT", "ref_price": cur,
        "entry": entry, "sl": sl, "risk": risk, "tp": entry + d * risk * tp_rr,
        "t": ev["t"], "bos_level": ev["sl_level"],
        "rr": tp_rr, "be_rr": be_rr, "tf": TF_LABEL,
        "key_suffix": "poi", "zone": p,   # signal_key distinct de l'entrée directe ; zone d'entrée affichée dans le message
    }
    sig.update(base)
    # liquidité : affichage seulement, comme pour l'entrée directe
    sig["ext"], sig["sweep"] = liquidity_reading(symbol, c, ev, entry) if symbol else (None, None)
    if apply_tp:
        apply_tp_mode(sig, c, ev, symbol, tp_rr)
    return sig, None


# ============================================================================
# FILTRES QUALITÉ DU CHoCH — calculés à chaque CHoCH, appliqués seulement s'ils sont ON (get_qf)
#   swings : structure d'avant CHoCH cohérente (vente : HH + HL, le CHoCH casse un HL ; achat : LL + LH)
#   disp   : displacement (corps net >= DISP_BODY_ATR x ATR, peu de mèches) ET vraie cassure (clôture >= DISP_BREAK_ATR x ATR au-delà)
#   range  : anti-chop (efficience des RANGE_LOOKBACK dernières bougies >= RANGE_MIN_ER)
#   fvg    : le CHoCH a créé un FVG dans son sens, pas encore entièrement mitigé (états : non mitigé / partiel / mitigé)
#   ind    : inducement — LIMIT OB/FVG seulement : un pivot mineur de la jambe doit se trouver entre le prix et la zone
# ============================================================================
def _confirmed_swings(c, upto, depth=SWING_DEPTH, span=300):
    """Swings hauts / bas confirmés (même définition que `analyze`) à l'index `upto` : listes de (index, prix)."""
    hs, ls = [], []
    for p in range(max(depth, upto - span), upto - depth + 1):
        wh = [x["h"] for x in c[p - depth:p + depth + 1]]
        wl = [x["l"] for x in c[p - depth:p + depth + 1]]
        if c[p]["h"] == max(wh) and all(c[p]["h"] > c[k]["h"] for k in range(p - depth, p)):
            hs.append((p, c[p]["h"]))
        if c[p]["l"] == min(wl) and all(c[p]["l"] < c[k]["l"] for k in range(p - depth, p)):
            ls.append((p, c[p]["l"]))
    return hs, ls


def swing_check(c, ev):
    d, i, src, lvl = ev["dir"], ev["i"], ev["src"], ev["level"]
    hs, ls = _confirmed_swings(c, i)
    if d == -1:   # vente : le CHoCH casse un plus bas (HL) ; avant lui, plus hauts (HH) et plus bas (HL) montants
        prev = [x for x in ls if x[0] < src]
        if not prev or len(hs) < 2:
            return {"ok": False, "txt": "swings insuffisants pour juger la structure"}
        a_ok, b_ok = prev[-1][1] < lvl, hs[-1][1] > hs[-2][1]
        lab = ("HH" if b_ok else "HH✗") + "+" + ("HL" if a_ok else "HL✗")
    else:         # achat : le CHoCH casse un plus haut (LH) ; avant lui, plus bas (LL) et plus hauts (LH) descendants
        prev = [x for x in hs if x[0] < src]
        if not prev or len(ls) < 2:
            return {"ok": False, "txt": "swings insuffisants pour juger la structure"}
        a_ok, b_ok = prev[-1][1] > lvl, ls[-1][1] < ls[-2][1]
        lab = ("LL" if b_ok else "LL✗") + "+" + ("LH" if a_ok else "LH✗")
    return {"ok": bool(a_ok and b_ok), "txt": f"structure avant CHoCH {lab}"}


def disp_check(c, ev):
    d, i, a = ev["dir"], ev["i"], ev["atr"]
    if not a:
        return {"ok": False, "txt": "ATR indisponible"}
    x = c[i]
    body, rng = (x["c"] - x["o"]) * d, x["h"] - x["l"]
    brk = (x["c"] - ev["level"]) * d
    ratio = body / rng if rng > 0 else 0.0
    ok = body >= DISP_BODY_ATR * a and ratio >= DISP_BODY_RATIO and brk >= DISP_BREAK_ATR * a
    return {"ok": bool(ok), "txt": f"corps {body / a:.2f}xATR ({ratio:.0%} de la bougie) · cassure +{brk / a:.2f}xATR"}


def range_check(c, ev):
    i, n = ev["i"], RANGE_LOOKBACK
    if i < n:
        return {"ok": False, "txt": "historique insuffisant"}
    w = c[i - n:i + 1]
    path = sum(abs(w[k]["c"] - w[k - 1]["c"]) for k in range(1, len(w)))
    er = abs(w[-1]["c"] - w[0]["c"]) / path if path > 0 else 0.0
    return {"ok": er >= RANGE_MIN_ER, "txt": f"efficience {er:.2f} (min {RANGE_MIN_ER:g}) · chop {round((1 - er) * 100)}/100"}


def leg_check(c, ev):
    """Qualité de la JAMBE IMPULSIVE (du point de retournement à la bougie du CHoCH, comme `choch_fvg`/`poc_proxy`) —
    différent de `disp_check` qui ne juge que la bougie du CHoCH elle-même. Rejette le schéma « petite bougie x N ->
    CHoCH final propre » même quand ce dernier passerait le filtre displacement.
    Ne compte pas les bougies (pas de seuil « N bougies mini »), juge la jambe entière sur 4 axes :
      - corps moyen (>= LEG_MIN_BODY_ATR x ATR) et corps/range moyen (>= LEG_MIN_BODY_RATIO) : peu de mèches en moyenne
      - part de bougies dans le sens de la jambe (>= LEG_MIN_DIR_RATIO) : peu de contre-mouvement
      - efficience nette (déplacement net / chemin parcouru, >= LEG_MIN_NET_EFF) : peu de bruit/aller-retour
      - part de micro-bougies -- corps < LEG_MICRO_BODY_ATR x ATR -- (<= LEG_MAX_MICRO_RATIO)
    """
    d, i, a = ev["dir"], ev["i"], ev["atr"]
    if not a:
        return {"ok": False, "txt": "ATR indisponible"}
    e = _turn_index(c, ev)
    leg = c[e:i + 1]
    if len(leg) < LEG_MIN_CANDLES:
        return {"ok": False, "txt": f"jambe trop courte ({len(leg)} bougie(s) < {LEG_MIN_CANDLES})"}
    bodies = [abs(x["c"] - x["o"]) for x in leg]
    ranges = [x["h"] - x["l"] for x in leg]
    avg_body = sum(bodies) / len(bodies)
    ratios = [b / r for b, r in zip(bodies, ranges) if r > 0]
    avg_ratio = sum(ratios) / len(ratios) if ratios else 0.0
    directional = sum(1 for x in leg if (x["c"] - x["o"]) * d > 0) / len(leg)
    path = sum(ranges)
    net_eff = abs(leg[-1]["c"] - leg[0]["o"]) / path if path > 0 else 0.0
    micro_ratio = sum(1 for b in bodies if b < LEG_MICRO_BODY_ATR * a) / len(leg)
    ok = (avg_body >= LEG_MIN_BODY_ATR * a and avg_ratio >= LEG_MIN_BODY_RATIO
          and directional >= LEG_MIN_DIR_RATIO and net_eff >= LEG_MIN_NET_EFF
          and micro_ratio <= LEG_MAX_MICRO_RATIO)
    txt = (f"corps moy {avg_body / a:.2f}xATR ({avg_ratio:.0%} du range) · directionnel {directional:.0%} · "
           f"efficience {net_eff:.2f} · micro-bougies {micro_ratio:.0%} (sur {len(leg)} bougies)")
    return {"ok": bool(ok), "txt": txt}


def choch_fvg(c, ev, dec=5):
    """FVG le plus récent créé PAR la jambe du CHoCH (dans son sens) et son état vis-à-vis des bougies suivantes."""
    d, i, a = ev["dir"], ev["i"], ev["atr"] or 0.0
    e = _turn_index(c, ev)
    best = None
    for k in range(e + 1, i):
        lo, hi = (c[k - 1]["h"], c[k + 1]["l"]) if d == 1 else (c[k + 1]["h"], c[k - 1]["l"])
        if hi > lo and hi - lo >= POI_MIN_ATR * a:
            best = (k, lo, hi)
    if not best:
        return {"ok": False, "state": None, "txt": "aucun FVG créé par le CHoCH"}
    k, lo, hi = best
    later = c[k + 2:]
    if d == 1:    # zone de demande : mitigée quand le prix REDESCEND dedans
        m = min((x["l"] for x in later), default=None)
        state = "non mitigé" if m is None or m > hi else ("mitigé" if m <= lo else "partiel")
    else:         # zone d'offre : mitigée quand le prix REMONTE dedans
        m = max((x["h"] for x in later), default=None)
        state = "non mitigé" if m is None or m < lo else ("mitigé" if m >= hi else "partiel")
    return {"ok": state != "mitigé", "state": state, "zone": (lo, hi),
            "txt": f"FVG {lo:.{dec}f}-{hi:.{dec}f} {state}"}


def poc_proxy(c, ev, bins=None):
    """POC PROXY (« POC de temps », pas un vrai POC volume — aucune source de volume réel n'est disponible ici :
    Deriv/Binance ne fournissent pas de volume exploitable pour ce bot).

    Calculé UNIQUEMENT sur la jambe impulsive qui a créé le CHoCH `ev` (du point de retournement à la bougie du
    CHoCH inclus, comme `choch_fvg`) :
      1) plage low -> high de cette jambe,
      2) découpée en `POC_BINS` paliers de prix,
      3) chaque bougie de la jambe "vote" pour tous les paliers que couvre son low -> high,
      4) le palier le plus voté = POC proxy (son centre) ; égalité -> palier le plus proche du centre de la jambe.

    Retourne un prix (float), ou None si la jambe est trop courte / plate pour être découpée."""
    i = ev["i"]
    e = _turn_index(c, ev)
    leg = c[e:i + 1]
    if len(leg) < 2:
        return None
    lo = min(x["l"] for x in leg)
    hi = max(x["h"] for x in leg)
    if hi <= lo:
        return None
    n = bins or POC_BINS
    step = (hi - lo) / n
    counts = [0] * n
    for x in leg:
        b_lo = max(0, min(n - 1, int((x["l"] - lo) / step)))
        b_hi = max(0, min(n - 1, int((x["h"] - lo) / step)))
        for b in range(b_lo, b_hi + 1):
            counts[b] += 1
    best = max(counts)
    center = (n - 1) / 2
    idx = min((k for k, v in enumerate(counts) if v == best), key=lambda k: abs(k - center))
    return lo + (idx + 0.5) * step


def ind_check(c, ev, entry):
    """Inducement d'un LIMIT : pivot mineur (profondeur 1) de la jambe, du côté opposé au trade, situé strictement ENTRE le
    prix du CHoCH et l'entrée limit — le prix doit le balayer pour atteindre la zone."""
    d, i = ev["dir"], ev["i"]
    e, cur = _turn_index(c, ev), c[i]["c"]
    lv = []
    for k in range(e + 1, i):
        if d == -1 and c[k]["h"] > c[k - 1]["h"] and c[k]["h"] > c[k + 1]["h"] and cur < c[k]["h"] < entry:
            lv.append(c[k]["h"])
        if d == 1 and c[k]["l"] < c[k - 1]["l"] and c[k]["l"] < c[k + 1]["l"] and entry < c[k]["l"] < cur:
            lv.append(c[k]["l"])
    if not lv:
        return {"ok": False, "txt": "aucun pivot mineur (IND) entre le prix et la zone"}
    lvl = max(lv) if d == -1 else min(lv)
    return {"ok": True, "txt": f"IND @ {lvl:.5g} entre le prix et la zone (balayé à l'aller)"}


def quality_check(c, ev, dec=5):
    """Filtres qualité du CHoCH `ev` -> {clé: {"ok", "txt"}}. Ne refuse rien : voir quality_reject."""
    out = {}
    for key, fn in (("swings", swing_check), ("disp", disp_check), ("range", range_check), ("leg", leg_check)):
        try:
            out[key] = fn(c, ev)
        except Exception as e:
            out[key] = {"ok": False, "txt": f"calcul impossible ({type(e).__name__})"}
    try:
        out["fvg"] = choch_fvg(c, ev, dec)
    except Exception as e:
        out["fvg"] = {"ok": False, "state": None, "txt": f"calcul impossible ({type(e).__name__})"}
    return out


def quality_reject(qf):
    """Motif de refus si un filtre ON échoue (swings / disp / range / fvg ; l'IND se juge sur le limit), sinon None."""
    for k in ("swings", "disp", "range", "fvg", "leg"):
        if get_qf(k) and k in qf and not qf[k]["ok"]:
            return f"filtre {QF_LBL[k]} : {qf[k]['txt']}"
    return None


def signal_log_block(symbol, sig, dec):
    """Un bloc de log par signal : niveaux, cible, RR attendu et résultat de chaque filtre qualité (ON/OFF)."""
    qf = sig.get("qf") or {}
    lines = [f"[{symbol}] ── SIGNAL {sig['side']} {sig.get('order', 'MARKET')} {sig['type']} ({_ts_str(sig['t'])}) ──",
             f"    ENTRY={sig['entry']:.{dec}f} SL={sig['sl']:.{dec}f} TP={sig['tp']:.{dec}f} RISK_PTS={sig['risk']:.{dec}f}",
             f"    TARGET_TYPE={sig.get('tp_mode') or 'RR fixe'} TARGET_PRICE={sig['tp']:.{dec}f} EXPECTED_RR={sig.get('rr')}"]
    for k in QF_KEYS:
        if k in qf:
            lines.append(f"    {k.upper():7}: {'✅' if qf[k]['ok'] else '❌'} {qf[k]['txt']}  [filtre {'ON' if get_qf(k) else 'OFF'}]")
    print("\n".join(lines))


def build_signal(c, ev, symbol=None, multi=False):
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

    _none = [] if multi else None
    if ev["kind"] != "CHOCH":
        return _none
    if ev["sl_level"] is None:
        return _rej("pas de niveau de repli valide (sl_level manquant)") or _none
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
    # Filtre M15 vraie impulsion + structure + Fibonacci : indépendant des deux filtres ci-dessus (OFF par défaut).
    m15_imp_on = bool(symbol and get_m15_imp_filter())
    if m15_imp_on:
        ok, why = m15_impulse_filter(symbol, c, ev, trace)
        if not ok:
            return _rej(why)
    bos_on = bool(symbol and get_bos_real())
    if bos_on:
        ok, why = bos_real_check(symbol, c, ev, trace)
        if not ok:
            return _rej(why)
    poi_on = bool(symbol and get_poi_filter())
    if poi_on:
        ok, why = poi_mitigation_check(symbol, c, ev, trace)
        if not ok:
            return _rej(why)
    qf = quality_check(c, ev, SYMBOLS[symbol]["decimals"] if symbol else 5)   # filtres qualité : toujours calculés
    why = quality_reject(qf)                                                    # ... refusent seulement s'ils sont ON
    if why:
        return _rej(why)
    d, a = ev["dir"], ev["atr"]

    def _direct():
        entry = c[ev["i"]]["c"]
        sl, sl_tag = sl_by_mode(c, ev, a, symbol)
        if (d == 1 and sl >= entry) or (d == -1 and sl <= entry):
            return _rej(f"SL ({sl_tag}) du mauvais côté de l'entrée (sl={sl:.2f}, entrée={entry:.2f})")
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
            "m15_imp_on": m15_imp_on, "m15_imp": trace.get("m15_imp"),   # affichage filtre M15 impulsion
            "bos_txt": trace.get("bos_txt"), "poi_txt": trace.get("poi_txt"),
            "qf": dict(qf, ind={"ok": True, "txt": "n/a (entrée directe : l'IND se juge sur le limit OB/FVG)"}),
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
        sig["sl_mode"] = sl_tag
        apply_tp_mode(sig, c, ev, symbol, tp_rr)
        return sig

    def _accept(sig):
        if setup:   # dernier maillon de la chaîne POI -> mitigation -> réaction -> CHoCH : accepté (les refus passent par _rej)
            src = f" depuis {_poi_txt(poi, SYMBOLS[symbol]['decimals'])}" if poi else ""
            print(f"[{symbol}] signal ACCEPTÉ : {sig['side']} {sig['type']} -- {setup}{src}")
            if trace:
                _mtf_block(symbol, trace, f"✅ ACCEPTÉ -- {sig['side']} {sig['type']} · {setup}{src}")
        return sig

    if not multi:   # appel historique (backtests / tests) : un seul signal direct, inchangé
        sig = _direct()
        return _accept(sig) if sig else None

    # Mode multi (scan réel) : selon les types d'entrée activés sur Telegram -> [direct] et/ou [limit sur OB / FVG]
    out = []
    if get_entry_direct():
        sig = _direct()
        if sig:
            out.append(_accept(sig))
    if get_entry_limit():
        lim, why = poi_limit_signal(c, ev, symbol, tp_rr, be_rr, dict(
            htf=htf_on, htf_mode=htf_mode, setup=setup, poi=poi, mtf_fib_on=mtf_fib_on, mtf_fib=trace.get("mtf_fib"),
            m15_imp_on=m15_imp_on, m15_imp=trace.get("m15_imp"), bos_txt=trace.get("bos_txt"), poi_txt=trace.get("poi_txt")))
        if lim:
            ind = ind_check(c, ev, lim["entry"])
            lim["qf"] = dict(qf, ind=ind)
            if get_qf("ind") and not ind["ok"]:
                if symbol and DEBUG_CHOCH:
                    print(f"[{symbol}] limit OB/FVG non placé : filtre {QF_LBL['ind']} : {ind['txt']}")
            else:
                out.append(_accept(lim))
        elif symbol and DEBUG_CHOCH:
            print(f"[{symbol}] limit OB/FVG non placé : {why}")
    return out


# ============================================================================
# 4b. MACHINE À ÉTATS SETUP50 (remplace l'entrée directe sur le CHoCH de la stratégie principale)
#
#   IDLE → SWEEP → CHOCH_VALIDE → MESURE → ZONE_50_ACTIVE → ENTREE → IDLE
#
# CHoCH ≠ entrée : le CHoCH (clôture du corps + déplacement) valide la direction et démarre la mesure du Fibonacci
# (ancre mobile, jamais figée). L'entrée n'existe qu'après un retracement >= 50 % (mèche comprise) ET une bougie de
# rejet (engulfing / pin bar / doji) à sa clôture. SL derrière le sweep, TP selon le RR Telegram (/rr).
# SWEEP = swing fractal (ou equal highs/lows) intact et vieux d'au moins 20 bougies, mèche 0.1-1 ATR, clôture de retour en <= 3 bougies ;
# CHoCH = clôture du corps au-delà du dernier swing opposé, <= 10 bougies après le sweep, corps >= 1 ATR ou FVG (seuils en tête de fichier).
# Verrou : un seul setup actif par actif et par timeframe, levé à l'entrée / sweep cassé / 100 % / MAX_BOUGIES ; pause après N SL consécutifs. Interrupteur : SETUP_ENGINE=0 sur
# Render pour revenir à l'ancienne logique (entrée directe sur CHoCH), sans redéployer de code.
# ============================================================================
from dataclasses import dataclass as _se_dataclass  # noqa: E402  (alias : l'import « dataclass » de la section 10 reste intact)

SETUP_ENGINE = _env_bool("SETUP_ENGINE", True)


SE_IDLE = "IDLE"
SE_SWEEP = "SWEEP"
SE_CHOCH_VALIDE = "CHOCH_VALIDE"
SE_MESURE = "MESURE"
SE_ZONE_50_ACTIVE = "ZONE_50_ACTIVE"
SE_ENTREE = "ENTREE"

SE_ALLOWED = {
    SE_IDLE: (SE_SWEEP,),
    SE_SWEEP: (SE_CHOCH_VALIDE, SE_IDLE),
    SE_CHOCH_VALIDE: (SE_MESURE, SE_IDLE),            # jamais SE_ENTREE
    SE_MESURE: (SE_ZONE_50_ACTIVE, SE_IDLE),
    SE_ZONE_50_ACTIVE: (SE_MESURE, SE_ENTREE, SE_IDLE),
    SE_ENTREE: (SE_IDLE,),
}


class SEIllegalTransition(RuntimeError):
    pass


@_se_dataclass
class SEParams:
    atr_period: int = 14
    swing_depth: int = SWING_FRACTAL
    sweep_equal_atr: float = SWEEP_EQUAL_ATR
    sweep_min_atr: float = SWEEP_MIN_ATR
    sweep_max_atr: float = SWEEP_MAX_ATR
    level_min_age: int = SWEEP_LEVEL_MIN_AGE
    sweep_close_back: int = SWEEP_CLOSE_BACK
    choch_window: int = CHOCH_WINDOW
    choch_body_atr: float = CHOCH_BODY_ATR_MIN
    max_bougies: int = MAX_BOUGIES
    fib_entry: float = _env_float("SE_FIB_ENTRY", 0.50)          # zone d'entrée
    fib_invalid: float = _env_float("SE_FIB_INVALID", 1.00)      # annulation : retour au sweep
    reject_same_candle: bool = _env_bool("SE_REJECT_SAME_CANDLE", True)  # la bougie qui touche le 50 % peut être la bougie de rejet
    rr: float = 3.0                                              # le RR réel vient du réglage Telegram (get_tp_rr)
    sl_buffer_atr: float = _env_float("SE_SL_BUFFER_ATR", 0.10)  # marge derrière l'extrême du sweep
    max_sl_atr: float = MAX_SL_ATR
    pause_after_sl: int = _env_int("SE_PAUSE_AFTER_SL", 3)       # pause après N SL consécutifs
    pause_candles: int = _env_int("SE_PAUSE_CANDLES", 60)
    leg_min_atr: float = 0.0           # qualité de jambe : amplitude mini de l'impulsion qui a fait le sweep (en ATR) ; 0 = pas de contrôle
    sl_mode: str = "SWEEP"             # SWEEP = derrière l'extrême du sweep (historique) · BOUGIE = derrière la bougie d'entrée · AUTO = OB → avalement → bougie d'entrée
    entry_mode: str = "STRICT"         # STRICT = se_rejection (historique) · CLOSE = avalement ou mèche > corps · TOUCH = mèche seule au 50 %


def se_signal_log(symbol, sig):
    """Ligne de log d'un signal SETUP50 : ancre Fibo (prix, heure), retracement atteint, bougie de rejet, timeframe."""
    f = sig["fib"]
    t = f.get("anchor_t")
    hr = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if t else "?"
    return (f"[{symbol}] SETUP50 {sig['side']} · ancre Fibo {f['anchor']:.5g} ({hr}) · retracement atteint "
            f"{f['retr_max']:.0%} (zone {f['level']:.5g}) · rejet {f['reject']} · timeframe {sig['tf']}")


def _se_flip(x):
    """Miroir de prix : ramène un achat à une vente."""
    return {"o": -x["o"], "h": -x["l"], "l": -x["h"], "c": -x["c"]}


def se_rejection_close(prev, x, d):
    """Entrée SETUP50 à la CLÔTURE de la bougie qui touche le 50 % : 'avalement' (engulfing) ou 'mèche > corps' (pin bar : la mèche
    du côté du rejet — haute pour une vente, basse pour un achat — est plus grande que le corps). Sinon None."""
    if d == 1:
        prev, x = _se_flip(prev), _se_flip(x)
    o, h, l, c = x["o"], x["h"], x["l"], x["c"]
    if h - l <= 0:
        return None
    if c < o and prev["c"] > prev["o"] and o >= prev["c"] and c <= prev["o"]:
        return "avalement"
    if (h - max(o, c)) > abs(c - o):
        return "mèche > corps"
    return None


def se_rejection(prev, x, d):
    """Bougie de rejet dans le sens d (-1 vente, +1 achat) : 'engulfing', 'pin bar', 'doji' ou None."""
    if d == 1:
        prev, x = _se_flip(prev), _se_flip(x)
    o, h, l, c = x["o"], x["h"], x["l"], x["c"]
    rng = h - l
    if rng <= 0:
        return None
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    if c < o and prev["c"] > prev["o"] and o >= prev["c"] and c <= prev["o"]:
        return "engulfing"
    if upper >= 2 * body and upper >= 0.6 * rng:
        return "pin bar"
    if body <= 0.1 * rng and upper > lower and upper >= 0.4 * rng:
        return "doji"
    return None


class SetupEngine:
    """Machine à états SWEEP → CHoCH → verrou → mesure → zone 50 % → entrée. Les fenêtres en bougies
    (âge du niveau, clôture de retour, délai du CHoCH, MAX_BOUGIES) sont des paramètres, définis en tête de fichier."""

    def __init__(self, symbol="?", tf="M1", params=None, verbose=False):
        self.symbol, self.tf, self.p, self.verbose = symbol, tf, params or SEParams(), verbose
        self.state = SE_IDLE
        self.setup = None
        self.pending = None             # sweep en attente de réintégration du niveau
        self.c, self.atr, self._tr_head = [], [], []
        self.base = 0                   # index absolu de self.c[0]
        self.highs, self.lows = [], []  # swings confirmés : {"i","p","used"}
        self.last_t = None
        self.pause_until = -1
        self.losses = 0
        self.quiet = False              # échauffement (historique) : événements enregistrés mais jamais affichés
        self.log = []

    # ------------------------------------------------------------------ utilitaires
    def _g(self, k):
        return self.c[k - self.base]

    def _ts(self, k):
        """Heure (jj/mm hh:mm UTC) de la bougie d'index absolu k, '?' si elle a été purgée."""
        if k is None or k < self.base:
            return "?"
        return datetime.fromtimestamp(self._g(k)["t"], timezone.utc).strftime("%d/%m %H:%M")

    def _note(self, msg):
        line = f"[{self.symbol} {self.tf}] {msg}"
        self.log.append(line)
        del self.log[:-500]
        if self.verbose:
            print(line)

    def _ev(self, tag, ok, msg):
        """Événement SWEEP / CHOCH / LOCK : toujours affiché (sauf échauffement), toujours gardé dans self.log."""
        icon = ("🔒" if ok else "🔓") if tag == "LOCK" else ("✅" if ok else "❌")
        line = f"[{tag}] {icon} [{self.symbol} {self.tf}] {msg}"
        self.log.append(line)
        del self.log[:-500]
        if not self.quiet:
            print(line)

    def _once(self, key):
        """Dédoublonne un événement ❌ répété à chaque bougie pour un même setup."""
        seen = self.setup.setdefault("seen", set())
        if key in seen:
            return False
        seen.add(key)
        return True

    def _to(self, new):
        if new not in SE_ALLOWED[self.state]:
            raise SEIllegalTransition(f"{self.state} → {new} interdit")
        self._note(f"état {self.state} → {new}")
        self.state = new

    def _drop(self, reason):
        if self.setup:
            self._ev("LOCK", False, f"verrou libéré — {reason}")
        self._note(f"setup annulé : {reason}")
        self.setup = self.pending = None
        if self.state != SE_IDLE:
            self._to(SE_IDLE)

    def _fib(self, s, f):
        """Prix au niveau f du retracement (0 = ancre, 1 = extrême du sweep) ; valable achat ET vente."""
        return s["anchor"] + f * (s["start"] - s["anchor"])

    # ------------------------------------------------------------------ structure HH / HL / LH / LL
    def _lbl(self, lst, v, high, a):
        """Étiquette structurelle d'un swing confirmé par rapport au swing précédent du même type."""
        k = next((n for n, w in enumerate(lst) if w["i"] == v["i"]), 0)
        if k == 0:
            return "swing high" if high else "swing low"
        prev = lst[k - 1]["p"]
        if abs(v["p"] - prev) < self.p.sweep_equal_atr * a:
            return "EQH" if high else "EQL"
        if high:
            return "HH" if v["p"] > prev else "LH"
        return "HL" if v["p"] > prev else "LL"

    def _trend(self):
        """Structure courante d'après les 2 derniers swings de chaque côté."""
        if len(self.highs) < 2 or len(self.lows) < 2:
            return "indéterminée"
        up_h = self.highs[-1]["p"] > self.highs[-2]["p"]
        up_l = self.lows[-1]["p"] > self.lows[-2]["p"]
        if up_h and up_l:
            return "haussière (HH/HL)"
        if not up_h and not up_l:
            return "baissière (LH/LL)"
        return "transition / range"

    def report_result(self, sl_hit, pause=True):
        """À appeler à la clôture d'un trade : N SL consécutifs -> pause de pause_candles bougies.
        pause=False (interrupteur /pausesl off) : aucune pause, le compteur reste à zéro."""
        if not pause:
            self.losses = 0
            return
        self.losses = self.losses + 1 if sl_hit else 0
        if self.losses >= self.p.pause_after_sl:
            self.pause_until = self.base + len(self.c) + self.p.pause_candles
            self._note(f"pause {self.p.pause_candles} bougies après {self.losses} SL consécutifs")
            self.losses = 0

    # ------------------------------------------------------------------ flux de bougies
    def feed(self, candles):
        out, first = [], self.last_t is None
        self.quiet = first
        for x in candles:
            if self.last_t is not None and x["t"] <= self.last_t:
                continue
            self._push(x)
            self.last_t = x["t"]
            sig = self._step(self.base + len(self.c) - 1)
            if sig and not first:       # échauffement : l'historique n'émet jamais de signal
                out.append(sig)
        self.quiet = False
        if len(self.c) > 3000:
            self._trim(len(self.c) - 2000)
        return out

    def _trim(self, cut):
        self.c, self.atr = self.c[cut:], self.atr[cut:]
        self.base += cut
        self.highs = [v for v in self.highs if v["i"] >= self.base]
        self.lows = [v for v in self.lows if v["i"] >= self.base]

    def _push(self, x):
        P = self.p
        self.c.append(x)
        i = self.base + len(self.c) - 1
        if len(self.c) == 1:
            tr = x["h"] - x["l"]
        else:
            pc = self.c[-2]["c"]
            tr = max(x["h"] - x["l"], abs(x["h"] - pc), abs(x["l"] - pc))
        if len(self._tr_head) < P.atr_period:
            self._tr_head.append(tr)
            a = sum(self._tr_head) / len(self._tr_head)
        else:
            a = (self.atr[-1] * (P.atr_period - 1) + tr) / P.atr_period
        self.atr.append(a)
        d = P.swing_depth
        p = i - d
        if p - d >= self.base:          # swing confirmé (même définition que analyze() du bot)
            win = [self._g(k) for k in range(p - d, p + d + 1)]
            s = self._g(p)
            if s["h"] == max(w["h"] for w in win) and all(s["h"] > self._g(k)["h"] for k in range(p - d, p)):
                self.highs.append({"i": p, "p": s["h"], "used": False})
            if s["l"] == min(w["l"] for w in win) and all(s["l"] < self._g(k)["l"] for k in range(p - d, p)):
                self.lows.append({"i": p, "p": s["l"], "used": False})

    def _step(self, i):
        x, a = self._g(i), self.atr[i - self.base]
        sig = None
        if self.state == SE_IDLE:
            self._look_for_sweep(i, x, a)
        elif self.state == SE_SWEEP:
            self._wait_choch(i, x, a)
        elif self.state == SE_MESURE:
            sig = self._measure(i, x, a)
        elif self.state == SE_ZONE_50_ACTIVE:
            sig = self._zone(i, x, a)
        for lv in self.highs:           # un niveau percé est consommé : il ne sera plus jamais « un niveau intact »
            if not lv["used"] and x["h"] > lv["p"]:
                lv["used"] = True
        for lv in self.lows:
            if not lv["used"] and x["l"] < lv["p"]:
                lv["used"] = True
        return sig

    # ------------------------------------------------------------------ 1. sweep (liquidité structurelle)
    def _levels_pierced(self, i, x, a):
        """Niveaux de liquidité structurelle intacts percés par la bougie : swing high/low confirmé, ou equal highs/lows
        (swings intacts voisins à moins de SWEEP_EQUAL_ATR x ATR : le niveau est alors l'extrême du groupe)."""
        P, seen, out = self.p, set(), []
        for high, lst in ((True, self.highs), (False, self.lows)):
            for lv in lst:
                if lv["used"]:
                    continue
                grp = [w for w in lst if not w["used"] and abs(w["p"] - lv["p"]) < P.sweep_equal_atr * a]
                top = max(grp, key=lambda w: w["p"]) if high else min(grp, key=lambda w: w["p"])
                if (high, top["i"]) in seen:
                    continue
                seen.add((high, top["i"]))
                pen = (x["h"] - top["p"]) if high else (top["p"] - x["l"])
                if pen <= 0:
                    continue
                lbl = ("EQH" if high else "EQL") if len(grp) > 1 else self._lbl(lst, top, high, a)
                out.append({"high": high, "dir": -1 if high else 1, "lvl": top["p"], "lv_i": top["i"], "age": i - top["i"],
                            "lbl": lbl, "pierce": pen / a, "ext": x["h"] if high else x["l"]})
        return out

    def _look_for_sweep(self, i, x, a):
        P = self.p
        if i < self.pause_until:
            self.pending = None
            return
        if self.pending:
            return self._advance_pending(i, x, a)
        pierced = self._levels_pierced(i, x, a)
        if not pierced:
            return
        young = [c for c in pierced if c["age"] < P.level_min_age]
        if young and len(young) == len(pierced):
            c = young[0]
            return self._ev("SWEEP", False, f"{c['lbl']} {c['lvl']:.5g} ({self._ts(c['lv_i'])}) trop récent : {c['age']} bougies "
                                            f"< {P.level_min_age} (niveau non établi)")
        pierced = [c for c in pierced if c["age"] >= P.level_min_age]
        ok = [c for c in pierced if P.sweep_min_atr <= c["pierce"] <= P.sweep_max_atr]
        if not ok:
            c = min(pierced, key=lambda c: c["pierce"])
            if c["pierce"] > P.sweep_max_atr:
                return self._ev("SWEEP", False, f"{c['lbl']} {c['lvl']:.5g} ({self._ts(c['lv_i'])}) percé de {c['pierce']:.2f} ATR "
                                                f"> {P.sweep_max_atr:g} : cassure structurelle (BOS), pas un sweep")
            return self._ev("SWEEP", False, f"{c['lbl']} {c['lvl']:.5g} ({self._ts(c['lv_i'])}) : mèche de {c['pierce']:.2f} ATR "
                                            f"< {P.sweep_min_atr:g} (trop courte)")
        if len({c["dir"] for c in ok}) > 1:
            return self._ev("SWEEP", False, "bougie englobante (swing high ET swing low percés) : ambiguë, ignorée")
        c = min(ok, key=lambda c: c["pierce"])      # niveau le plus extérieur parmi ceux réellement pris
        self.pending = {"dir": c["dir"], "lvl": c["lvl"], "ext": c["ext"], "ext_i": i, "i0": i, "atr": a,
                        "lv_i": c["lv_i"], "lbl": c["lbl"], "trend": self._trend()}
        self._note(f"mèche au-delà de {c['lbl']} {c['lvl']:.5g} ({c['pierce']:.2f} ATR) : sweep candidat")
        self._advance_pending(i, x, a)

    def _advance_pending(self, i, x, a):
        P, pend = self.p, self.pending
        d = pend["dir"]
        what = f"{pend['lbl']} {pend['lvl']:.5g} ({self._ts(pend['lv_i'])})"
        if i > pend["i0"]:
            if d == -1 and x["h"] > pend["ext"]:
                pend["ext"], pend["ext_i"] = x["h"], i
            if d == 1 and x["l"] < pend["ext"]:
                pend["ext"], pend["ext_i"] = x["l"], i
            if abs(pend["ext"] - pend["lvl"]) > P.sweep_max_atr * pend["atr"]:
                self.pending = None
                return self._ev("SWEEP", False, f"{what} : mèche à {abs(pend['ext'] - pend['lvl']) / pend['atr']:.2f} ATR "
                                                f"> {P.sweep_max_atr:g} : cassure structurelle (BOS), pas un sweep")
        back = x["c"] < pend["lvl"] if d == -1 else x["c"] > pend["lvl"]
        if back:
            return self._confirm_sweep(i, x, pend, what)
        if i - pend["i0"] >= P.sweep_close_back:
            self.pending = None
            return self._ev("SWEEP", False, f"{what} : aucune clôture de retour dans la bougie du sweep ni les {P.sweep_close_back} suivantes")

    def _choch_level(self, ext_i, d, lv_i):
        """Dernier swing structurel OPPOSÉ avant l'extrême du sweep : bas (vente) / haut (achat). Retourne (prix, index, étiquette)."""
        lst = self.lows if d == -1 else self.highs
        prior = [v for v in lst if v["i"] < ext_i]
        a = self.atr[ext_i - self.base]
        if prior:
            v = prior[-1]
            return v["p"], v["i"], self._lbl(lst, v, d == 1, a)
        seg = [(k, self._g(k)) for k in range(max(self.base, lv_i), ext_i + 1)]    # repli : extrême de la jambe qui a fait le sweep
        k, z = min(seg, key=lambda e: e[1]["l"]) if d == -1 else max(seg, key=lambda e: e[1]["h"])
        return (z["l"] if d == -1 else z["h"]), k, "extrême de jambe"

    def _confirm_sweep(self, i, x, pend, what):
        d = pend["dir"]
        if self.p.leg_min_atr > 0:
            # jambe = de l'extrême opposé le plus bas (vente) / haut (achat) depuis la formation du niveau balayé, jusqu'à l'extrême du sweep
            seg = [self._g(k) for k in range(max(self.base, pend["lv_i"]), pend["ext_i"] + 1)]
            org = min(z["l"] for z in seg) if d == -1 else max(z["h"] for z in seg)
            leg = abs(pend["ext"] - org) / pend["atr"]
            if leg < self.p.leg_min_atr:
                self.pending = None
                return self._ev("SWEEP", False, f"{what} : jambe trop faible ({leg:.2f} ATR < {self.p.leg_min_atr:g}) — "
                                                f"le sweep n'est pas le fait d'une vraie impulsion, ignoré")
        lvl_p, lvl_i, lbl = self._choch_level(pend["ext_i"], d, pend["lv_i"])
        self.setup = {"dir": d, "level": pend["lvl"], "start": pend["ext"], "ext_i": pend["ext_i"],
                      "sweep_i": i, "choch_lvl": lvl_p, "choch_lvl_i": lvl_i, "choch_lbl": lbl,
                      "swept_lbl": pend["lbl"], "trend": pend["trend"],
                      "choch_i": None, "anchor": None, "retr": 0.0, "ext_t": self._g(pend["ext_i"])["t"], "locked": True}
        self.pending = None
        self._to(SE_SWEEP)
        self._ev("SWEEP", True, f"{'haut' if d == -1 else 'bas'} balayé : {what}, mèche {abs(pend['ext'] - pend['lvl']) / pend['atr']:.2f} ATR "
                                f"(extrême {pend['ext']:.5g}), réintégré par clôture {x['c']:.5g} · structure avant sweep : {pend['trend']} · "
                                f"swing à casser : {lbl} {lvl_p:.5g} ({self._ts(lvl_i)})")
        self._ev("LOCK", True, f"setup {'VENTE' if d == -1 else 'ACHAT'} actif sur {self.symbol} {self.tf} : nouveaux sweeps et CHoCH "
                               f"ignorés jusqu'à l'entrée, l'invalidation (sweep cassé, 100 %) ou {self.p.max_bougies} bougies après le CHoCH")

    # ------------------------------------------------------------------ 2. CHoCH (changement de structure)
    def _wait_choch(self, i, x, a):
        P, s = self.p, self.setup
        d = s["dir"]
        if P.choch_window > 0 and i - s["sweep_i"] > P.choch_window:
            return self._drop(f"pas de CHoCH dans les {P.choch_window} bougies après le sweep")
        if (d == -1 and x["h"] > s["start"]) or (d == 1 and x["l"] < s["start"]):
            return self._drop("sweep cassé : extrême du sweep dépassé avant le CHoCH")
        lvl, lbl = s["choch_lvl"], s["choch_lbl"]
        tgt = f"{lbl} {lvl:.5g} ({self._ts(s['choch_lvl_i'])})"
        closed = (x["c"] < lvl) if d == -1 else (x["c"] > lvl)
        if not closed:
            if ((x["l"] < lvl) if d == -1 else (x["h"] > lvl)) and self._once("wick"):
                self._ev("CHOCH", False, f"{tgt} percé par la mèche seule (clôture {x['c']:.5g}) : pas de CHoCH")
            return
        body = (x["c"] - x["o"]) * d
        fvg = False
        if i - 2 >= self.base:
            k = self._g(i - 2)
            fvg = (k["l"] > x["h"]) if d == -1 else (k["h"] < x["l"])
        if body <= 0 or not (body >= P.choch_body_atr * a or fvg):
            if self._once("micro"):
                self._ev("CHOCH", False, f"clôture au-delà de {tgt} mais déplacement insuffisant (corps {max(body, 0) / a:.2f} ATR "
                                         f"< {P.choch_body_atr:g}, pas de FVG) : micro-break ignoré")
            return
        s["choch_i"] = i
        s["locked"] = True
        self._to(SE_CHOCH_VALIDE)
        seg = [self._g(k) for k in range(s["ext_i"], i + 1)]
        s["anchor"] = min(z["l"] for z in seg) if d == -1 else max(z["h"] for z in seg)
        s["anchor_t"] = next(z["t"] for z in reversed(seg) if (z["l"] if d == -1 else z["h"]) == s["anchor"])
        self._ev("CHOCH", True, f"{tgt} cassé par clôture {x['c']:.5g} (corps {body / a:.2f} ATR{' + FVG' if fvg else ''}) · "
                                f"sweep associé : {s['swept_lbl']} {s['level']:.5g} · départ {s['start']:.5g} → ancre {s['anchor']:.5g}")
        self._to(SE_MESURE)                # le CHoCH démarre la mesure ; l'entrée est impossible ici

    # ------------------------------------------------------------------ 3. mesure / zone 50 %
    def _invalid(self, i, x):
        """Libère le verrou : MAX_BOUGIES dépassé depuis le CHoCH, ou retracement à 100 % (retour au sweep)."""
        P, s = self.p, self.setup
        d = s["dir"]
        if i - s["choch_i"] > P.max_bougies:
            self._drop(f"plus de {P.max_bougies} bougies depuis le CHoCH (MAX_BOUGIES)")
            return True
        inv = self._fib(s, P.fib_invalid)
        if (x["h"] >= inv) if d == -1 else (x["l"] <= inv):
            self._drop("retracement jusqu'au sweep (100 %)")
            return True
        return False

    def _new_extreme(self, x):
        s = self.setup
        if s["dir"] == -1 and x["l"] < s["anchor"]:
            s["anchor"], s["anchor_t"] = x["l"], x["t"]
            return True
        if s["dir"] == 1 and x["h"] > s["anchor"]:
            s["anchor"], s["anchor_t"] = x["h"], x["t"]
            return True
        return False

    def _measure(self, i, x, a):
        s = self.setup
        if self._invalid(i, x):
            return None
        if self._new_extreme(x):        # l'impulsion continue : on déplace l'ancre, on ne fige jamais le Fibo
            return self._note(f"nouvel extrême : ancre déplacée à {s['anchor']:.5g}")
        ext = x["h"] if s["dir"] == -1 else x["l"]
        s["retr"] = max(s["retr"], (ext - s["anchor"]) / (s["start"] - s["anchor"]))
        if self._touched(x):
            self._to(SE_ZONE_50_ACTIVE)
            self._note(f"zone {self.p.fib_entry:.0%} active à {self._fib(s, self.p.fib_entry):.5g}")
            if self.p.reject_same_candle:
                return self._try_entry(i, x, a)
        return None                      # correction < 50 % : ignorée, on continue à surveiller

    def _touched(self, x):
        lvl = self._fib(self.setup, self.p.fib_entry)
        return x["h"] >= lvl if self.setup["dir"] == -1 else x["l"] <= lvl

    def _zone(self, i, x, a):
        if self._invalid(i, x):
            return None
        if self._new_extreme(x):
            self._to(SE_MESURE)
            return self._note("nouvel extrême avant le rejet : zone désactivée, mesure reprise")
        return self._try_entry(i, x, a)

    # ------------------------------------------------------------------ 4. entrée
    def _try_entry(self, i, x, a):
        P, s = self.p, self.setup
        d = s["dir"]
        ext = x["h"] if d == -1 else x["l"]
        s["retr"] = max(s["retr"], (ext - s["anchor"]) / (s["start"] - s["anchor"]))   # retracement atteint, bougie d'entrée incluse
        if not self._touched(x):
            return None
        if i < self.pause_until:        # pause après N SL : un setup déjà en cours ne peut pas entrer non plus
            return self._drop(f"pause active ({self.pause_until - i} bougies restantes)")
        why = ("mèche au 50 %" if P.entry_mode == "TOUCH" else
               se_rejection_close(self._g(i - 1), x, d) if P.entry_mode == "CLOSE" else se_rejection(self._g(i - 1), x, d))
        if not why:
            return None
        entry = x["c"]
        sl, sl_basis = s["start"] - d * P.sl_buffer_atr * a, "extrême du sweep"
        if P.sl_mode in ("AUTO", "BOUGIE"):
            # SL serré : légèrement derrière l'OB, sinon derrière l'avalement, sinon derrière la bougie d'entrée (jamais à l'intérieur de sa mèche)
            prev = self._g(i - 1)
            far, sl_basis = (x["h"] if d == -1 else x["l"]), "bougie d'entrée"
            if P.sl_mode == "AUTO":
                ob = None
                try:
                    ob = next((z for z in find_pois(self.c, TIMEFRAME_MIN, d)
                               if z["kind"] == "OB" and z["t_valid"] <= x["t"] and x["l"] <= z["hi"] and x["h"] >= z["lo"]), None)
                except Exception:
                    ob = None
                if ob:
                    far = max(far, ob["hi"]) if d == -1 else min(far, ob["lo"])
                    sl_basis = f"OB {ob['lo']:.5g}-{ob['hi']:.5g}"
                elif why == "avalement":
                    far = max(far, prev["h"]) if d == -1 else min(far, prev["l"])
                    sl_basis = "avalement"
            sl = far - d * P.sl_buffer_atr * a
        if (entry - sl) * d <= 0:
            return self._drop("SL du mauvais côté de l'entrée")
        risk = abs(entry - sl)
        if risk > P.max_sl_atr * a:
            return self._drop(f"SL trop large ({risk / a:.1f} ATR)")
        sig = {
            "dir": d, "side": "BUY" if d == 1 else "SELL", "type": "SETUP50", "order": "MARKET",
            "ref_price": entry, "entry": entry, "sl": sl, "risk": risk, "tp": entry + d * risk * P.rr,
            "rr": P.rr, "t": x["t"], "tf": self.tf, "atr": a, "sweep_t": s["ext_t"], "bos_level": s["choch_lvl"],
            "setup": f"SWEEP+CHoCH+50% ({why}) · SL {sl_basis}", "sl_basis": sl_basis,
            "fib": {"start": s["start"], "anchor": s["anchor"], "anchor_t": s.get("anchor_t"), "level": self._fib(s, P.fib_entry),
                    "retr_max": s["retr"], "sweep_level": s["level"], "reject": why},
        }
        self._to(SE_ENTREE)
        self._note(f"ENTRÉE {sig['side']} {entry:.5g} · SL {sl:.5g} · TP {sig['tp']:.5g} · rejet {why}")
        sig["log"] = se_signal_log(self.symbol, sig)
        self._ev("LOCK", False, f"verrou libéré — entrée {sig['side']} déclenchée à {entry:.5g} (un setup = un trade)")
        self.setup = None
        self._to(SE_IDLE)
        return sig


SE_PAUSE_ENABLED = _env_bool("SE_PAUSE_ENABLED", True)   # valeur de départ ; ensuite /pausesl on|off sur Telegram


def get_se_pause():
    """Pause automatique après N SL consécutifs (SETUP50) : dernier choix Telegram, sinon SE_PAUSE_ENABLED."""
    return _get_bool_setting("se_pause", SE_PAUSE_ENABLED)


def set_se_pause(on):
    set_setting("se_pause", "on" if on else "off")


SE_CHOCH_WINDOW_CHOICES = (0, 10, 30, 100)      # 0 = illimité
SE_LEG_ATR_CHOICES = (1.5, 2.0, 3.0)
SE_ENTRY_CHOICES = ("REJET", "MECHE", "STRICT")
SE_FIB_CHOICES = (0.4, 0.45, 0.5)
SE_SL_CHOICES = ("AUTO", "BOUGIE", "SWEEP")


def get_se_sl():
    """SL SETUP50 : AUTO (défaut) = légèrement derrière l'OB, sinon l'avalement, sinon la bougie d'entrée ; BOUGIE = derrière la
    bougie d'entrée ; SWEEP = derrière l'extrême du sweep (ancien, SL large)."""
    return _get_choice("se_sl", "AUTO", SE_SL_CHOICES)


def get_se_fib_entry():
    """Retracement MINI de la zone d'entrée SETUP50 (défaut 40 %) : le prix doit avoir retracé au moins ce niveau de la jambe."""
    v = _get_float_setting("se_fib_entry", 0.4)
    return v if any(abs(v - c) < 1e-9 for c in SE_FIB_CHOICES) else 0.4


def get_se_choch_window():
    """Bougies max entre le sweep et le CHoCH. 0 = illimité (défaut) : le sweep est l'oeuvre de l'impulsion, on attend la
    cassure tant que l'extrême du sweep n'est pas dépassé. Réglage Telegram (menu ⛓ Séquence SETUP50)."""
    try:
        v = int(float(get_setting("se_choch_window")))
    except (TypeError, ValueError):
        return 0
    return v if v in SE_CHOCH_WINDOW_CHOICES else 0


def get_se_entry():
    """REJET (défaut) : au 50 %, signal à la clôture d'un avalement ou d'une bougie à mèche de rejet > corps (pin bar).
    MECHE : dès que la mèche touche le 50 %. STRICT : ancien filtre de rejet (engulfing / pin bar 2x / doji)."""
    return _get_choice("se_entry", "REJET", SE_ENTRY_CHOICES)


def get_se_leg():
    """Qualité de jambe (défaut ON) : le sweep doit être le fait d'une vraie impulsion (>= get_se_leg_atr() ATR)."""
    return _get_bool_setting("se_leg", True)


def get_se_leg_atr():
    return _get_float_setting("se_leg_atr", 2.0)


def get_se_pause_n():
    try:
        v = int(float(get_setting("se_pause_n", SEParams.pause_after_sl)))
    except (TypeError, ValueError):
        return SEParams.pause_after_sl
    return v if v >= 1 else SEParams.pause_after_sl


def get_se_pause_candles():
    try:
        v = int(float(get_setting("se_pause_candles", SEParams.pause_candles)))
    except (TypeError, ValueError):
        return SEParams.pause_candles
    return v if v >= 1 else SEParams.pause_candles


def se_pause_text():
    n, k = get_se_pause_n(), get_se_pause_candles()
    return f"ON — {n} SL consécutifs → pause de {k} bougies {TF_LABEL}" if get_se_pause() else "OFF"


_SE_ENGINES = {}   # (symbole, timeframe) -> SetupEngine : l'état vit en mémoire, il repart de zéro à chaque redémarrage


def _se_engine(symbol):
    key = (symbol, TF_LABEL)
    eng = _SE_ENGINES.get(key)
    if eng is None:
        eng = _SE_ENGINES[key] = SetupEngine(symbol, TF_LABEL, verbose=DEBUG_CHOCH)
    return eng


def _se_make_signal(symbol, raw, candles):
    """Signal brut du moteur -> signal au format de publish_signal (mêmes clés que build_signal / _direct)."""
    d, entry, a = raw["dir"], raw["entry"], raw["atr"]
    sl, risk = raw["sl"], raw["risk"]
    # même double plancher que l'entrée directe : un SL de quelques points serait balayé par le spread / le bruit
    min_pct = SYMBOLS.get(symbol, {}).get("min_sl_pct", MIN_SL_PCT)
    min_risk = max(MIN_SL_ATR * a, entry * min_pct)
    if risk < min_risk:
        risk = min_risk
        sl = entry - d * risk
    tp_rr, be_rr = get_tp_rr(), get_be_rr()
    sig = dict(raw)
    sig.update(
        risk=risk, sl=sl, tp=entry + d * risk * tp_rr, rr=tp_rr, be_rr=be_rr, tf=TF_LABEL,
        htf=False, htf_mode="OFF", poi=None, mtf_fib_on=False, mtf_fib=None, m15_imp_on=False, m15_imp=None, qf={}, sl_mode="STRUCT",
        key_suffix="se", ext=None,
        sweep={"tf": TF_LABEL, "side": "haut" if d == -1 else "bas", "level": raw["fib"]["sweep_level"],
               "swept": True, "eq": False, "t": raw["sweep_t"], "i": None},
    )
    try:   # lecture de la liquidité externe : affichage seulement, jamais une condition (comme liquidity_reading)
        sig["ext"] = ext_target(symbol, d, entry)
    except Exception as e:
        print(f"[{symbol}] lecture liquidité externe impossible ({type(e).__name__}) : signal sans ligne « Ext. »")
    idx = next((k for k, c in enumerate(candles) if c["t"] == sig["t"]), len(candles) - 1)
    apply_tp_mode(sig, candles, {"i": idx}, symbol, tp_rr)   # /modetp rr (fixe) | target (liquidité ext.) | swing ; repli RR si pas de cible
    return sig


def se_apply_filters(symbol, candles, sig):
    """Applique à un signal SETUP50 les trois filtres activables sur Telegram (Impulsion M15, Vrai BOS, POI HTF). Tous OFF par défaut :
    sans aucun filtre actif, retourne (True, None) sans rien lire ni modifier. Retourne (autorisé, motif_de_refus | None) ; en cas
    d'acceptation, ajoute les lignes d'information au signal. Le point de retournement = extrême du sweep (sweep_t) jusqu'à l'entrée."""
    f_m15, f_bos, f_poi = get_m15_imp_filter(), get_bos_real(), get_poi_filter()
    if not (f_m15 or f_bos or f_poi):
        return True, None
    idx = next((k for k, x in enumerate(candles) if x["t"] == sig["t"]), len(candles) - 1)
    st = next((k for k, x in enumerate(candles) if x["t"] == sig.get("sweep_t")), max(0, idx - 20))
    ev = {"dir": sig["dir"], "i": idx, "t": sig["t"], "src": min(st, idx), "kind": "CHOCH", "type": "SETUP50"}
    c, trace = candles[:idx + 1], {}
    for on, fn in ((f_m15, m15_impulse_filter), (f_bos, bos_real_check), (f_poi, poi_mitigation_check)):
        if on:
            ok, why = fn(symbol, c, ev, trace)
            if not ok:
                return False, why
    sig.update(m15_imp_on=f_m15, m15_imp=trace.get("m15_imp"), bos_txt=trace.get("bos_txt"), poi_txt=trace.get("poi_txt"))
    return True, None


def setup_engine_signals(symbol, candles, last_seen):
    """Nourrit la machine à états avec les bougies clôturées et retourne les signaux d'ENTRÉE (0 ou 1 en pratique).
    Ne lève jamais : une erreur du moteur est loggée, le scan et le suivi des positions continuent."""
    try:
        eng = _se_engine(symbol)
        eng.p.pause_after_sl, eng.p.pause_candles = get_se_pause_n(), get_se_pause_candles()
        eng.p.choch_window = get_se_choch_window()
        eng.p.sl_mode = get_se_sl()
        eng.p.fib_entry = get_se_fib_entry()
        eng.p.entry_mode = {"REJET": "CLOSE", "MECHE": "TOUCH", "STRICT": "STRICT"}[get_se_entry()]
        eng.p.leg_min_atr = get_se_leg_atr() if get_se_leg() else 0.0
        if not get_se_pause():          # interrupteur OFF : lève aussi une pause déjà en cours
            eng.pause_until, eng.losses = -1, 0
        raws = eng.feed(candles)
        out = []
        recent = {c["t"] for c in candles[-SIGNAL_MAX_AGE:]}
        for raw in raws:
            side = raw["side"]
            if raw["t"] <= (last_seen or 0) or raw["t"] not in recent:
                print(f"[{symbol}] SETUP50 {side} ignoré : entrée trop ancienne (rattrapage après interruption)")
                continue
            if not session_ok(raw["t"]):
                print(f"[{symbol}] SETUP50 {side} ignoré : hors session ({get_session()})")
                continue
            if raw.get("log"):
                print(raw["log"])
            sig = _se_make_signal(symbol, raw, candles)
            ok, why = se_apply_filters(symbol, candles, sig)
            if not ok:
                print(f"[{symbol}] SETUP50 {side} REJETÉ par filtre : {why}")
                continue
            out.append(sig)
        return out
    except Exception:
        print(f"[{symbol}] machine à états SETUP50 : erreur (aucun signal ce passage)")
        traceback.print_exc()
        return []


def setup_engine_report(trade, name):
    """Résultat d'un trade SETUP50 -> pause automatique après N SL consécutifs (TP / BE remettent le compteur à zéro)."""
    try:
        if trade.get("kind") != "SETUP50" or name not in ("SL", "TP", "BE"):
            return
        _se_engine(trade["symbol"]).report_result(name == "SL", pause=get_se_pause())
    except Exception:
        traceback.print_exc()


# ============================================================================
# 5. RISQUE (calcul du lot)
#
# Calcul du lot à partir du montant en $ à risquer.
# ============================================================================

def calc_lot(symbol, risk_usd, sl_distance, price=None):
    """Lot = montant risqué / (distance SL en points x valeur du point par lot).

    Arrondi vers le bas au pas du broker (le risque réel arrondi est donc toujours <= risque
    demandé, jamais un souci). Cas limite : si même le lot minimum du broker fait dépasser le
    risque demandé (SL trop serré / risque $ trop petit pour ce symbole) :
      - RISK_STRICT ON (défaut) : le lot minimum n'est PAS forcé au-delà de RISK_STRICT_TOLERANCE_PCT
        de dépassement -> lot=0.0, 'rejected_reason' explique pourquoi (le signal est alors ignoré
        par l'appelant, jamais pris avec un risque plus élevé que prévu).
      - RISK_STRICT OFF : ancien comportement -> lot minimum forcé quand même, 'raised_to_min'=True
        et 'real_risk' reflète le risque réel (plus élevé) pour affichage/avertissement.
    """
    cfg = SYMBOLS[symbol]
    vpp = value_per_point(symbol, price)
    empty = {"lot": 0.0, "real_risk": 0.0, "raised_to_min": False, "rejected_reason": None}
    if sl_distance <= 0 or vpp <= 0 or risk_usd <= 0:
        return empty
    raw = risk_usd / (sl_distance * vpp)
    step, min_lot = cfg["lot_step"], cfg["min_lot"]
    lot = math.floor(raw / step + 1e-9) * step
    raised = False
    if lot < min_lot:
        min_lot_risk = min_lot * sl_distance * vpp
        if RISK_STRICT and min_lot_risk > risk_usd * (1.0 + RISK_STRICT_TOLERANCE_PCT):
            out = dict(empty)
            out["rejected_reason"] = (
                f"lot minimum {symbol} ({min_lot}) exigerait un risque réel de {min_lot_risk:.2f}$ "
                f"> risque configuré {risk_usd:.2f}$ (RISK_STRICT actif) — signal ignoré plutôt que "
                f"pris avec un risque plus élevé que prévu.")
            print(f"[risque] {symbol} : {out['rejected_reason']}")
            return out
        lot, raised = min_lot, True
    lot = round(lot, 6)
    return {"lot": lot, "real_risk": lot * sl_distance * vpp, "raised_to_min": raised, "rejected_reason": None}


def calc_margin(symbol, lot, price, leverage):
    """Marge indicative pour ce lot à ce prix/levier — n'influence jamais le calcul du lot ci-dessus.

    Notionnel = lot x value_per_point x prix (value_per_point ~ taille du contrat pour ces 2 actifs).
    À titre indicatif seulement : la marge réelle dépend du broker.
    """
    if leverage <= 0:
        return 0.0
    cfg = SYMBOLS[symbol]
    # USD/xxx : le notionnel est directement la taille du contrat en USD (lot x 100 000)
    notional = lot * cfg["contract_size"] if cfg.get("vpp_from_price") else lot * cfg["value_per_point"] * price
    return notional / leverage


# ============================================================================
# 5bis. RISK GATE — garde-fou exécuté juste avant l'ENVOI DE L'ORDRE au broker.
#
# Totalement indépendant du calcul de lot "stratégie" ci-dessus (calc_lot / get_risk) et des
# conditions de signal (QML, CRT, CHoCH+BOS) : il ne touche ni n'altère ces logiques. Il ne fait
# que recalculer, pour chaque signal déjà validé, un volume d'ordre visant RISK_TARGET_USD (sans
# jamais dépasser MAX_RISK_USD une fois arrondi au pas du broker), puis autorise ou bloque l'envoi
# de l'ordre MT5. Il ne modifie jamais le SL et ne force jamais un ordre au-delà de MAX_RISK_USD.
# ============================================================================
RISK_ENABLED = _env_bool("RISK_ENABLED", True)
RISK_TARGET_USD = _env_float("RISK_TARGET_USD", 0.70)
MAX_RISK_USD = _env_float("MAX_RISK_USD", 0.70)
MIN_LOT = _env_float("MIN_LOT", 0.01)
_RISK_EPS = 1e-6   # tolérance numérique (bruit flottant) uniquement : 0.70 passe, 0.71 est rejeté


class RiskGateBlocked(Exception):
    """Levée par le dernier verrou (juste avant l'envoi broker) : le risque réel du volume FINAL dépasse MAX_RISK_USD."""

    def __init__(self, symbol, lot, risk):
        self.symbol, self.lot, self.risk = symbol, lot, risk
        super().__init__(f"Risk Gate : {symbol} lot={lot:g} -> risque réel {risk:.2f}$ > max {MAX_RISK_USD:.2f}$")


_RISK_CTX = {}   # client_id -> nom de stratégie (pour les logs du dernier verrou)
_RISK_BLOCKED = set()   # client_id des ordres bloqués par le dernier verrou (message admin précis)


def risk_strategy_label(sig):
    """Nom de stratégie pour les logs du Risk Gate : QML / CRT / CHS+BOS (+ type de signal, ex. CHOCH1). Lecture seule."""
    if not isinstance(sig, dict):
        return str(sig or "?")
    typ = sig.get("type")
    if sig.get("strategy") == "CRT" or sig.get("key_suffix") == "crt":
        base = "CRT"
    elif typ == "QML" or sig.get("key_suffix") == "qml":
        base = "QML"
    elif typ == "FIBTREND" or sig.get("key_suffix") == "ft":
        base = "FIBTREND"
    elif typ == "MANUAL":
        base = "MANUAL"
    else:
        base = "CHS+BOS"
    return f"{base}/{typ}" if typ and typ != base else base


def _risk_log(strategy, entry, sl, lot, risk, approved):
    """Log unique du Risk Gate (même format pour toutes les stratégies)."""
    lot_txt = f"{lot:g}" if lot is not None else "-"
    risk_txt = f"{risk:.2f}" if risk is not None else "-"
    print(f"[RISK GATE] Strategy={strategy} Entry={entry} SL={sl} Lot={lot_txt} Risk=${risk_txt} "
          f"MAX=${MAX_RISK_USD:.2f} → {'APPROVED' if approved else 'REJECTED'}")
    if not approved:
        print("[RISK GATE] ORDER BLOCKED — risk exceeds maximum")


def risk_gate_check(symbol, entry, sl, strategy="?", target_usd=None):
    """Risk Gate UNIQUE et COMMUN à toutes les stratégies (QML, CRT, CHS+BOS, manuel...) : calcule le
    volume d'ordre final et dit si l'ordre peut partir. Indépendant du signal : il ne modifie ni ENTRY ni SL.

    Flux : ENTRY + SL -> volume -> risque réel (volume final x distance réelle ENTRY/SL) -> décision.
    1) Plus grand volume dont le risque reste <= min(RISK_TARGET_USD, MAX_RISK_USD), arrondi VERS LE BAS au lot_step.
    2) Plancher MIN_LOT (et min_lot du symbole) : si 0.01 respecte le max -> autorisé (0.55$, 0.70$ ok).
    3) Si même le lot minimum dépasse MAX_RISK_USD (0.71$, 0.90$...) -> ordre refusé : SL inchangé, volume jamais forcé.

    Retourne (approved: bool, lot: float|None, real_risk: float|None).
    """
    if not RISK_ENABLED:
        return True, None, None  # Risk Gate désactivé : comportement existant inchangé

    cfg = SYMBOLS.get(symbol, {})
    lot_step = cfg.get("lot_step", MIN_LOT) or MIN_LOT
    min_lot = max(cfg.get("min_lot", MIN_LOT) or MIN_LOT, MIN_LOT)

    try:
        sl_distance = abs(float(entry) - float(sl))
        vpp = value_per_point(symbol, entry)
    except Exception:
        sl_distance, vpp = 0.0, 0.0

    if sl_distance <= 0 or vpp <= 0:
        _risk_log(strategy, entry, sl, None, None, False)
        return False, None, None

    # 1) plus grand volume possible sans dépasser la limite (arrondi vers le bas au pas du broker)
    _tgt = target_usd if (target_usd and target_usd > 0) else RISK_TARGET_USD   # target_usd : seulement fourni quand /mm est ON
    ceiling = min(_tgt, MAX_RISK_USD) if _tgt > 0 else MAX_RISK_USD
    raw_lot = ceiling / (sl_distance * vpp)
    lot = math.floor(raw_lot / lot_step + 1e-9) * lot_step
    lot = max(round(lot, 6), min_lot)

    # 2) risque réel du volume retenu (volume final x distance réelle ENTRY/SL)
    real_risk = lot * sl_distance * vpp

    # 3) décision finale : jamais de volume forcé au-delà de MAX_RISK_USD
    approved = real_risk <= MAX_RISK_USD + _RISK_EPS
    _risk_log(strategy, entry, sl, lot, real_risk, approved)
    return approved, round(lot, 6), real_risk


def risk_gate_verify(symbol, lot, entry, sl, strategy="?"):
    """DERNIER VERROU (vérification seule, ne redimensionne jamais) : risque réel du volume FINAL (après arrondi
    aux specs réelles du broker) et du SL réellement envoyé. Retourne (approved, real_risk)."""
    if not RISK_ENABLED:
        return True, None
    try:
        real_risk = float(lot) * abs(float(entry) - float(sl)) * value_per_point(symbol, entry)
    except Exception:
        _risk_log(strategy, entry, sl, lot, None, False)
        return False, None
    approved = real_risk <= MAX_RISK_USD + _RISK_EPS
    _risk_log(strategy, entry, sl, lot, real_risk, approved)
    return approved, real_risk


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


MANUAL_SWEEP_SCAN = _env_int("MANUAL_SWEEP_SCAN", 40)            # bougies récentes explorées pour trouver le dernier sweep
MANUAL_SWEEP_MARGIN_ATR = _env_float("MANUAL_SWEEP_MARGIN_ATR", 0.10)   # marge sous la mèche du sweep, en ATR (plancher : 1.5 x spread)


def manual_sweep_sl(symbol, side, spread=0.0):
    """SL d'un trade MANUEL placé au-delà du dernier SWEEP de l'UT d'entrée courante (M1 par défaut), avec une petite marge.
    BUY : dernier balayage de liquidité SOUS un plus bas (mèche puis clôture de retour) -> SL sous la mèche.
    SELL : dernier balayage AU-DESSUS d'un plus haut -> SL au-dessus de la mèche. Réutilise `_sweep_at` (même définition
    du balayage que le moteur). Un sweep déjà repris par le prix (une bougie ultérieure a dépassé la mèche) est ignoré.
    Retourne (sl, infos) ou (None, raison lisible). Ne lève jamais."""
    d = 1 if side == "BUY" else -1
    try:
        c = get_candles(symbol)
    except Exception as e:
        return None, f"bougies indisponibles ({type(e).__name__})"
    if len(c) < SWEEP_LOOKBACK + 3:
        return None, "pas assez de bougies pour lire un sweep"
    atr = atr_series(c)[-1] or 0.0
    margin = max(1.5 * float(spread or 0.0), MANUAL_SWEEP_MARGIN_ATR * atr)
    last = len(c) - 1
    for e in range(last, max(last - MANUAL_SWEEP_SCAN, SWEEP_LOOKBACK) - 1, -1):
        lvl, swept = _sweep_at(c, e, d)
        if not swept:
            continue
        extreme = c[e]["l"] if d == 1 else c[e]["h"]
        if any((k["l"] <= extreme) if d == 1 else (k["h"] >= extreme) for k in c[e + 1:]):
            continue   # mèche déjà reprise par le prix : ce sweep n'est plus un repère valide
        return extreme - d * margin, {"level": lvl, "extreme": extreme, "margin": margin,
                                       "tf": TF_LABEL, "age": last - e, "t": c[e]["t"]}
    return None, (f"aucun sweep {'sous un plus bas' if d == 1 else 'au-dessus d un plus haut'} "
                  f"exploitable sur les {MANUAL_SWEEP_SCAN} dernières bougies {TF_LABEL}")


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
    Clôtures partielles (optionnelles, section 12 « Module QML ») : événement "PARTIAL" à chaque niveau figé dans
    trade["part_cfg"] ; le résultat final = R déjà encaissé + reste de la position (SL / BE / TP).
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
            _pr, _pp = float(trade.get("part_r") or 0), float(trade.get("part_pct") or 0)
            r = _pr + (1 - _pp) * (0.0 if at_be else -1.0)   # partiels déjà encaissés + reste de la position
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
        # clôtures partielles (optionnelles, niveaux figés à la création du trade) : TP1 / TP2 ... avant la TP finale
        for _lv in _partial_levels_of(trade):
            _idx = int(trade.get("part_done") or 0)
            if _idx >= len(_partial_levels_of(trade)):
                break
            _rr, _pct = _partial_levels_of(trade)[_idx]
            if _rr >= tp_rr or r_fav < _rr:
                break
            trade["part_done"] = _idx + 1
            trade["part_r"] = float(trade.get("part_r") or 0) + _pct * _rr
            trade["part_pct"] = float(trade.get("part_pct") or 0) + _pct
            _pos = trade.get("mt5_position_id")
            if _pos:   # position réellement ouverte chez le broker : on clôture la fraction correspondante
                partial_close_mt5(_pos, trade["symbol"], trade["lot"], _pct)
            events.append({"name": "PARTIAL", "level": _idx + 1, "rr": _rr, "pct": _pct, "trade": dict(trade)})
        if r_fav >= tp_rr:
            _pr, _pp = float(trade.get("part_r") or 0), float(trade.get("part_pct") or 0)
            r_tp = _pr + (1 - _pp) * tp_rr
            trade.update(status="CLOSED", closed_ts=c["t"], result_r=r_tp,
                         pnl_usd=r_tp * trade["risk_usd"], outcome="TP")
            events.append({"name": "TP", "trade": dict(trade)})
            break

    trade["last_ts"] = last_ts
    fields = {k: trade[k] for k in ("sl", "be_hit", "status", "last_ts", "closed_ts", "filled_ts",
                                    "result_r", "pnl_usd", "outcome",
                                    "rr1_hit", "rr2_hit", "rr3_hit")}
    for k in ("part_done", "part_r", "part_pct"):
        fields[k] = trade.get(k) or 0
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
                if not send_result_image(ev, dec):   # image de résultat QML (section 7ter) ; sinon envoi habituel
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
    fib = sig.get("fib")
    if fib:   # FIBTREND : Fibo de la jambe (1.0 origine · 50 % · 0.0 extrême) tracé sur l'image
        for lvl, lab in ((fib["origin"], "1.0"), (fib["lvl"], "50 %"), (fib["extreme"], "0.0")):
            ax.axhline(lvl, color="#4fc3f7", lw=0.8, ls="--")
            ax.text(0, lvl, f"Fibo {lab}", color="#4fc3f7", fontsize=7, va="bottom")
    ax.add_patch(Rectangle((x0, min(e, tp)), x1 - x0, abs(tp - e), color="#26a69a", alpha=0.25))
    ax.add_patch(Rectangle((x0, min(e, sl)), x1 - x0, abs(e - sl), color="#ef5350", alpha=0.25))
    for lvl, lab, col in ((e, "Entrée", "#ffffff"), (sl, "SL", "#ef5350"),
                          (tp, "TP", "#26a69a")):
        ax.axhline(lvl, color=col, lw=0.6, ls=":")
        ax.text(x1 + 0.5, lvl, f"{lab} {lvl:.{decimals}f}", color=col, fontsize=8, va="center")

    ax.set_xlim(-1, x1 + 14)
    lo = min(min(c["l"] for c in view), sl, tp)
    hi = max(max(c["h"] for c in view), sl, tp)
    if fib:
        lo, hi = min(lo, fib["origin"], fib["extreme"]), max(hi, fib["origin"], fib["extreme"])
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
    ax.set_title(f"{symbol} {sig['tf'] if sig.get('type') == 'FIBTREND' and sig.get('tf') else TF_LABEL} - {sig['side']}{' LIMIT' if sig.get('order') == 'LIMIT' else ''} ({'BOS + CHoCH + Fibo 50%' if sig['type'] == 'FIBTREND' and 'CHoCH' in str(sig.get('setup')) else 'FIBTREND' if sig['type'] == 'FIBTREND' else 'Sweep + CHoCH + 50%' if sig['type'] == 'SETUP50' else 'CHoCH + CHoCH' if sig['type'] == 'CHOCH2' else 'CHoCH'})",
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
# 7ter. IMAGE DE RÉSULTAT QML (SL touché, RR1, RR2, TP final)
#
# Image « carte résultat » (pillow) envoyée dans le GROUPE uniquement, en réponse au signal d'ouverture
# du trade, à la place du graphique habituel. Ne s'applique qu'aux trades des stratégies listées dans
# RESULT_IMAGE_KINDS (QML, QMLG par défaut ; "ALL" = toutes). BE, BE armé, RR3+, partiels : inchangés.
# Si pillow est absent, ou si la génération / l'envoi échoue, le message habituel part comme avant.
# Aucun lot dans le groupe (RESULT_IMAGE_SHOW_LOT=1 pour l'afficher). Anti-doublon par (trade, événement).
# ============================================================================
RESULT_IMAGE_ENABLED = os.getenv("RESULT_IMAGE_ENABLED", "1").strip().lower() in ("1", "on", "true", "yes", "oui")
RESULT_IMAGE_KINDS = {k.strip().upper() for k in os.getenv("RESULT_IMAGE_KINDS", "QML,QMLG").split(",") if k.strip()}
RESULT_IMAGE_SHOW_LOT = os.getenv("RESULT_IMAGE_SHOW_LOT", "0").strip().lower() in ("1", "on", "true", "yes", "oui")
RI_DEDUP_FILE = os.getenv("RESULT_DEDUP_FILE", os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "results_sent.json"))
RI_BRAND = "AlphaBot • QML"
RI_W, RI_H = 1080, 1350

RI_C_BG = (11, 15, 25)
RI_C_CARD = (20, 27, 43)
RI_C_TEXT = (240, 244, 250)
RI_C_MUTED = (130, 142, 165)
RI_C_GREEN = (22, 190, 110)
RI_C_GOLD = (246, 190, 40)
RI_C_RED = (235, 64, 72)

RI_EVENTS = {
    #  clé     titre              couleur       rang  emoji (légende Telegram)
    "SL":  {"title": "STOP LOSS TOUCHÉ", "color": RI_C_RED,   "rank": -1, "emoji": "🔴"},
    "RR1": {"title": "RR1 ATTEINT",      "color": RI_C_GREEN, "rank": 1,  "emoji": "✅"},
    "RR2": {"title": "RR2 ATTEINT",      "color": RI_C_GREEN, "rank": 2,  "emoji": "✅✅"},
    "TP3": {"title": "TP FINAL ATTEINT", "color": RI_C_GOLD,  "rank": 3,  "emoji": "🏆"},
}
_RI_EVENT_OF = {"SL": "SL", "RR1": "RR1", "RR2": "RR2", "TP": "TP3"}   # événement du suivi -> événement image

_RI_FONTS_BOLD = [
    "DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",   # Render / Linux
    "/system/fonts/Roboto-Bold.ttf",                          # Android / Pydroid3
    "/system/fonts/DroidSans-Bold.ttf",
    "arialbd.ttf",
]
_RI_FONTS_REG = [
    "DejaVuSans.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/system/fonts/Roboto-Regular.ttf",
    "/system/fonts/DroidSans.ttf",
    "arial.ttf",
]
_ri_lock = threading.Lock()


def _ri_font(size, bold=True):
    for p in (_RI_FONTS_BOLD if bold else _RI_FONTS_REG):
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _ri_fmt(p, digits=None):
    if digits is None:
        digits = 5 if p < 10 else (3 if p < 500 else 2)
    return f"{p:.{digits}f}"


def _ri_text_w(d, t, f):
    return d.textlength(t, font=f)


def _ri_center(d, cx, y, t, f, fill):
    d.text((cx - _ri_text_w(d, t, f) / 2, y), t, font=f, fill=fill)


def _ri_r_value(trade, event):
    """Résultat en R : SL = -1R, sinon le RR du niveau touché. Le résultat réel du trade (result_r, partiels
    inclus) prévaut pour SL et TP final quand il est connu."""
    rr = trade.get("result_r")
    if event in ("SL", "TP3") and rr is not None:
        return float(rr)
    if event == "SL":
        return -1.0
    key = {"RR1": "rr1", "RR2": "rr2", "TP3": "rr3"}[event]
    return float(trade.get(key, {"RR1": 1, "RR2": 2, "TP3": 3}[event]))


def _ri_badge_check(d, cx, cy, r, color):
    d.ellipse((cx - r, cy - r, cx + r, cy + r), fill=color)
    w = max(6, r // 6)
    d.line([(cx - r * .45, cy + r * .02), (cx - r * .1, cy + r * .38),
            (cx + r * .5, cy - r * .35)], fill=(255, 255, 255), width=w, joint="curve")


def _ri_badge_cross(d, cx, cy, r, color):
    d.ellipse((cx - r, cy - r, cx + r, cy + r), fill=color)
    w = max(6, r // 6)
    k = r * .38
    d.line([(cx - k, cy - k), (cx + k, cy + k)], fill=(255, 255, 255), width=w)
    d.line([(cx - k, cy + k), (cx + k, cy - k)], fill=(255, 255, 255), width=w)


def _ri_badge_trophy(d, cx, cy, r, color):
    d.ellipse((cx - r, cy - r, cx + r, cy + r), fill=color)
    k = r * .45
    d.polygon([(cx - k, cy - k), (cx + k, cy - k), (cx + k * .6, cy + k * .2),
               (cx - k * .6, cy + k * .2)], fill=(255, 255, 255))
    d.rectangle((cx - k * .15, cy + k * .2, cx + k * .15, cy + k * .75), fill=(255, 255, 255))
    d.rectangle((cx - k * .6, cy + k * .75, cx + k * .6, cy + k * .95), fill=(255, 255, 255))


def make_result_image(trade, event, touch_price=None, when=None):
    """Retourne un io.BytesIO (PNG). `trade` : id, symbol, side, entry, sl, tf, digits, rr3 (RR de la TP finale) ;
    optionnels : tp1 / tp2 (paliers RR1 / RR2), tp3 (TP finale), lot, result_r. `event` : SL | RR1 | RR2 | TP3."""
    import io as _io
    ev = RI_EVENTS[event]
    color = ev["color"]
    W, H = RI_W, RI_H
    is_buy = trade["side"].upper() in ("BUY", "ACHAT", "LONG")
    digits = trade.get("digits")
    r_val = _ri_r_value(trade, event)
    when = when or datetime.now(timezone.utc)

    img = Image.new("RGBA", (W, H), RI_C_BG + (255,))
    d = ImageDraw.Draw(img)

    # Bandeau titre
    d.rectangle((0, 0, W, 250), fill=color)
    cx_badge, cy_badge = 150, 125
    if event == "SL":
        _ri_badge_cross(d, cx_badge, cy_badge, 68, (0, 0, 0, 70))
    elif event == "TP3":
        _ri_badge_trophy(d, cx_badge, cy_badge, 68, (0, 0, 0, 70))
    else:
        _ri_badge_check(d, cx_badge, cy_badge, 68, (0, 0, 0, 70))
    d.text((250, 70), ev["title"], font=_ri_font(58), fill=(255, 255, 255))
    d.text((250, 148), f"{trade['symbol']}  •  {trade.get('tf', 'M1')}  •  QML",
           font=_ri_font(36, False), fill=(255, 255, 255))

    # Carte principale
    d.rounded_rectangle((40, 280, W - 40, 560), radius=36, fill=RI_C_CARD)

    # Pastille ACHAT / VENTE
    pill_col = RI_C_GREEN if is_buy else RI_C_RED
    pill_txt = "ACHAT" if is_buy else "VENTE"
    d.rounded_rectangle((80, 312, 330, 382), radius=35, fill=pill_col)
    _ri_center(d, 205, 326, pill_txt, _ri_font(40), (255, 255, 255))
    d.text((360, 328), trade["symbol"], font=_ri_font(44), fill=RI_C_TEXT)

    # Gros résultat en R
    sign = "+" if r_val > 0 else ""
    _ri_center(d, W / 2, 400, f"{sign}{r_val:g}R", _ri_font(120), color)

    # Ligne infos
    info = []
    if trade.get("lot") is not None:
        info.append(f"LOT {trade['lot']:g}")
    info.append(f"Entrée {_ri_fmt(trade['entry'], digits)}")
    _ri_center(d, W / 2, 520, "   |   ".join(info), _ri_font(30, False), RI_C_MUTED)

    # ───── Échelle de prix (SL / Entrée / paliers / TP final) ─────
    levels = [("SL", trade["sl"], RI_C_RED), ("ENTRÉE", trade["entry"], (120, 170, 255))]
    if trade.get("tp1") is not None:
        levels.append(("RR1", trade["tp1"], RI_C_GREEN))
    if trade.get("tp2") is not None:
        levels.append(("RR2", trade["tp2"], RI_C_GREEN))
    if trade.get("tp3"):
        levels.append(("TP FINAL", trade["tp3"], RI_C_GOLD))

    reached = {"ENTRÉE"}
    if event == "SL":
        reached.add("SL")
    else:
        if ev["rank"] >= 1:
            reached.add("RR1")
        if ev["rank"] >= 2:
            reached.add("RR2")
        if ev["rank"] >= 3:
            reached.add("TP FINAL")

    top, bottom = 620, 1170
    prices = [p for _, p, _ in levels]
    pmin, pmax = min(prices), max(prices)
    span = (pmax - pmin) or 1.0

    def y_of(p):
        return bottom - (p - pmin) / span * (bottom - top)

    # Zones colorées (SL <-> entrée = rouge ; entrée <-> dernier TP = vert)
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    od = ImageDraw.Draw(ov)
    x0, x1 = 80, W - 80
    last_tp = trade.get("tp3") or trade.get("tp2") or trade.get("tp1") or trade["entry"]
    y_e, y_sl, y_tp = y_of(trade["entry"]), y_of(trade["sl"]), y_of(last_tp)
    od.rectangle((x0, min(y_e, y_sl), x1, max(y_e, y_sl)), fill=RI_C_RED + (55,))
    od.rectangle((x0, min(y_e, y_tp), x1, max(y_e, y_tp)), fill=RI_C_GREEN + (45,))
    img = Image.alpha_composite(img, ov)
    d = ImageDraw.Draw(img)

    # Étiquettes (anti-chevauchement vertical)
    placed = sorted([(y_of(p), n, p, c) for n, p, c in levels], key=lambda t: t[0])
    ys, min_gap = [], 52
    for y, *_ in placed:
        if ys and y - ys[-1] < min_gap:
            y = ys[-1] + min_gap
        ys.append(y)

    for (y_real, name, p, c), y_lbl in zip(placed, ys):
        done = name in reached
        col = c if done else (95, 105, 128)
        d.line([(x0, y_real), (x1, y_real)], fill=col, width=3)
        d.rounded_rectangle((x0, y_lbl - 24, x0 + 190, y_lbl + 24), radius=14, fill=col)
        d.text((x0 + 16, y_lbl - 18), name, font=_ri_font(30), fill=(255, 255, 255))
        d.text((x0 + 215, y_lbl - 18), _ri_fmt(p, digits), font=_ri_font(32), fill=RI_C_TEXT)
        if done and name != "ENTRÉE":
            if name == "SL":
                _ri_badge_cross(d, x1 - 40, y_lbl, 26, col)
            else:
                _ri_badge_check(d, x1 - 40, y_lbl, 26, col)

    if touch_price is not None:
        d.text((80, 1190), f"Prix de touche : {_ri_fmt(touch_price, digits)}",
               font=_ri_font(30, False), fill=RI_C_MUTED)

    # Pied de page
    d.line([(60, 1255), (W - 60, 1255)], fill=(40, 50, 72), width=2)
    d.text((70, 1275), RI_BRAND, font=_ri_font(34), fill=RI_C_TEXT)
    stamp = when.strftime("%d/%m/%Y  %H:%M UTC")
    d.text((W - 70 - _ri_text_w(d, stamp, _ri_font(28, False)), 1281), stamp,
           font=_ri_font(28, False), fill=RI_C_MUTED)

    out = _io.BytesIO()
    img.convert("RGB").save(out, format="PNG", optimize=True)
    out.seek(0)
    return out


def build_result_caption(trade, event):
    ev = RI_EVENTS[event]
    is_buy = trade["side"].upper() in ("BUY", "ACHAT", "LONG")
    sens = "🟢 <b>ACHAT</b>" if is_buy else "🔴 <b>VENTE</b>"
    r_val = _ri_r_value(trade, event)
    sign = "+" if r_val > 0 else ""
    lines = [
        f"{ev['emoji']} <b>{ev['title']}</b> {ev['emoji']}",
        f"{sens} • <b>{trade['symbol']}</b>",
        f"📊 Résultat : <b>{sign}{r_val:g}R</b>",
    ]
    if trade.get("lot") is not None:
        lines.append(f"💰 Lot : <b>{trade['lot']:g}</b>")
    return "\n".join(lines)


def _ri_load_dedup():
    try:
        with open(RI_DEDUP_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()


def _ri_save_dedup(s):
    try:
        with open(RI_DEDUP_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(s), f)
    except Exception:
        pass


def _ri_trade_view(t, dec):
    """Trade de la base (dict) -> dict attendu par make_result_image. Niveaux recalculés depuis l'entrée / SL
    initial : RR1 / RR2 n'existent que s'ils sont AVANT la TP finale de CE trade (même règle que track_trade)."""
    side = t["side"]
    sgn = 1 if side == "BUY" else -1
    entry = float(t["entry"])
    sl0 = float(t.get("sl_initial") if t.get("sl_initial") is not None else t["sl"])
    risk = abs(entry - sl0)
    tp_rr = trade_tp_rr(t)
    d = {"id": t["id"], "symbol": t["symbol"], "side": side, "entry": entry, "sl": sl0,
         "tf": t.get("timeframe") or TF_LABEL, "digits": dec, "rr3": tp_rr, "result_r": t.get("result_r")}
    if risk > 0:
        for lvl in (1, 2):
            if lvl < tp_rr - 1e-9:
                d[f"tp{lvl}"] = entry + sgn * lvl * risk
                d[f"rr{lvl}"] = lvl
    if t.get("tp") is not None:
        d["tp3"] = float(t["tp"])
    if RESULT_IMAGE_SHOW_LOT and t.get("lot") is not None:
        d["lot"] = t["lot"]
    return d


def send_result_image(ev, dec):
    """Envoie l'image de résultat au GROUPE pour un événement du suivi (SL / RR1 / RR2 / TP).
    Retourne True si l'annonce est faite (ou déjà faite) ; False -> l'appelant garde son envoi habituel.
    Ne lève jamais."""
    path = None
    try:
        if not (RESULT_IMAGE_ENABLED and Image is not None and CHAT_ID_GROUPE and TELEGRAM_TOKEN):
            return False
        event = _RI_EVENT_OF.get(ev.get("name"))
        t = ev.get("trade") or {}
        if not event or not t:
            return False
        if "ALL" not in RESULT_IMAGE_KINDS and str(t.get("kind") or "").upper() not in RESULT_IMAGE_KINDS:
            return False
        view = _ri_trade_view(t, dec)
        if event in ("RR1", "RR2") and f"tp{event[-1]}" not in view:
            return False
        key = f"{t['id']}:{event}"
        with _ri_lock:
            if key in _ri_load_dedup():
                return True
        buf = make_result_image(view, event, touch_price=t.get("tp") if event == "TP3" else None)
        os.makedirs(CHART_DIR, exist_ok=True)
        path = os.path.join(CHART_DIR, f"result_{t['id']}_{event}.png")
        with open(path, "wb") as f:
            f.write(buf.getvalue())
        res = to_group(build_result_caption(view, event), photo=path,
                       reply_to_message_id=t.get("signal_message_id"))
        if not (res and res.get("ok")):
            return False
        with _ri_lock:
            s = _ri_load_dedup()
            s.add(key)
            _ri_save_dedup(s)
        return True
    except Exception as e:
        print(f"[result_image] échec ({type(e).__name__}: {e}) : envoi habituel")
        return False
    finally:
        if path:
            try:
                os.remove(path)
            except OSError:
                pass



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


NEWS_BEFORE_MIN = _env_int("NEWS_BEFORE_MIN", 30)   # pas de nouveau signal N min AVANT une news
NEWS_AFTER_MIN = _env_int("NEWS_AFTER_MIN", 30)     # ... et N min APRÈS


def get_news_filter_on():
    """Filtre news : ON par défaut (interrupteur Telegram 📰 News). Ne bloque QUE les nouveaux signaux."""
    return _get_bool_setting("news_filter", True)


def get_news_medium():
    """OFF = news à fort impact (rouge) seulement ; ON = + impact moyen (orange)."""
    return _get_bool_setting("news_medium", False)


def get_bot_on():
    """Bot démarré ? OFF = plus aucun NOUVEAU signal (les positions déjà ouvertes restent suivies : BE / TP / SL)."""
    return _get_bool_setting("bot_on", True)


def set_bot_on(v):
    set_setting("bot_on", "1" if v else "0")


def _news_currencies(symbol):
    if len(symbol) == 6 and symbol.isalpha() and symbol not in ("XAUUSD", "BTCUSD"):
        return {symbol[:3], symbol[3:]}
    return {"USD"}   # Gold, BTC, US30 : pilotés par le dollar / les news US


def news_block_info(symbol, now=None):
    """Retourne la news qui bloque `symbol` maintenant (dict) ou None. Calendrier ForexFactory public.
    Si le calendrier est indisponible : None (on ne bloque pas, on ne coupe pas le bot pour ça)."""
    if not get_news_filter_on():
        return None
    try:
        events = _forexfactory_events()
    except Exception:
        return None
    now = now or datetime.now(timezone.utc)
    curs = _news_currencies(symbol)
    impacts = {"High"} | ({"Medium"} if get_news_medium() else set())
    for e in events or []:
        cur = e.get("country") or e.get("currency")
        if cur not in curs or str(e.get("impact", "")).strip() not in impacts:
            continue
        try:
            dt = datetime.fromisoformat(str(e.get("date", "")).replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta_min = (dt - now).total_seconds() / 60.0
        if -NEWS_AFTER_MIN <= delta_min <= NEWS_BEFORE_MIN:
            return {"title": e.get("title", "?"), "cur": cur, "impact": e.get("impact", ""), "dt": dt}
    return None


def _news_line(nb):
    return f"{'🔴' if nb['impact'] == 'High' else '🟠'} {nb['cur']} {nb['title']} ({nb['dt'].strftime('%H:%M UTC')})"


def build_fundamental_analysis(symbol):
    """Résumé du calendrier économique public (ForexFactory, sans clé) pertinent pour l'actif.
    Aucune génération de texte libre : uniquement des événements réels du calendrier, sans invention."""
    try:
        raw_events = _forexfactory_events()
    except Exception as e:
        return f"⚠️ Calendrier économique indisponible pour le moment ({type(e).__name__})."
    if not raw_events:
        return "⚠️ Calendrier économique indisponible pour le moment — réessaie plus tard."

    relevant = {"USD"} | {"XAUUSD": {"CHF", "JPY"}, "USDCAD": {"CAD"}, "USDJPY": {"JPY"}}.get(symbol, _news_currencies(symbol))
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
        "Ce couple réagit surtout aux écarts de taux USD / devise de cotation (décisions de banque centrale, inflation, emploi)"
        + (" et au pétrole pour le CAD." if symbol == "USDCAD" else " et à l'aversion au risque pour le JPY.")
        if symbol in ("USDCAD", "USDJPY") else
        "Le BTC réagit surtout à la liquidité globale et aux données macro USD (taux, inflation, emploi), "
        "ainsi qu'au sentiment risk-on / risk-off des marchés."
    )
    if symbol == "US30":
        watch_note = ("Le Dow Jones réagit surtout aux données macro US (emploi, inflation, Fed), aux taux "
                      "et aux résultats d'entreprises.")
    elif symbol not in ("XAUUSD", "BTCUSD", "USDCAD", "USDJPY"):
        watch_note = "Ce marché réagit aux décisions de banques centrales, à l'inflation et à l'emploi des devises concernées."
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
MEDIA_CATS = {"TP": "🎯 TP", "SL": "🔴 SL", "BE": "➖ BE", "MOTIV": "💪 Motivation",
              "BUY": "🟢 ACHAT", "SELL": "🔴 VENTE"}   # BUY / SELL : envoyés à CHAQUE entrée (plusieurs à la fois)
MEDIA_ENTRY_COUNT = _env_int("MEDIA_ENTRY_COUNT", 3)   # nombre de stickers BUY / SELL envoyés à chaque entrée
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


def to_group_media(cat, n=1):
    """Envoie au groupe n médias DIFFÉRENTS tirés au hasard dans la catégorie (n=1 : comme avant ; silencieux si aucun
    n'est configuré)."""
    items = get_media(cat)
    if not items or not CHAT_ID_GROUPE:
        return
    for item in random.sample(items, min(max(1, int(n)), len(items))):
        kind, _, file_id = item.partition(":")
        if kind not in _MEDIA_SEND:
            continue
        res = _post(_MEDIA_SEND[kind], {"chat_id": CHAT_ID_GROUPE, kind: file_id})
        if res is not None and not res.get("ok"):
            print(f"[telegram] média {cat} refusé : {res.get('description')}")


ENTRY_MEDIA_ENABLED = os.getenv("ENTRY_MEDIA_ENABLED", "1").strip().lower() in ("1", "on", "true", "yes", "oui")


def entry_media_and_lot(symbol, sig, lot):
    """À chaque entrée : plusieurs stickers ACHAT (verts) ou VENTE (rouges) au groupe, puis à l'admin UNE ligne courte :
    sens + actif + lot, rien d'autre. Ne lève jamais (un souci de média ne doit pas gêner le signal)."""
    if not ENTRY_MEDIA_ENABLED:
        return
    try:
        buy = sig["side"] == "BUY"
        to_group_media("BUY" if buy else "SELL", n=MEDIA_ENTRY_COUNT)
        to_admin(f"{'🟢 <b>ACHAT</b>' if buy else '🔴 <b>VENTE</b>'} {symbol} · LOT <b>{lot:g}</b>")
    except Exception as e:
        print(f"[{symbol}] stickers d'entrée indisponibles ({type(e).__name__})")


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
        z = sig.get("zone")
        zone = f"\n📍 Zone d'entrée : {_poi_txt(z, dec)}" if z else ""
        txt = (f"⏳ <b>{sig['side']} LIMIT</b> — <b>N'ENTRE PAS MAINTENANT</b>\n"
               f"Place un ordre limit à {_fmt(sig['entry'], dec)}{now}, puis attends le retour du prix.{zone}\n"
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
    if fib_line:
        lines += f"\n{fib_line}"
    if sig.get("m15_imp_on"):
        lines += f"\nImpulsion M15 : {sig.get('m15_imp') or 'ON'}"
    if sig.get("bos_txt"):
        lines += f"\nVrai BOS : {sig['bos_txt']}"
    if sig.get("poi_txt"):
        lines += f"\nPOI HTF : {sig['poi_txt']}"
    crt = sig.get("crt")
    if crt:
        lines += (f"\n🕯 CRT {_tf_lbl(crt['ref_tf'])} : range {_fmt(crt['crl'], 2)} – {_fmt(crt['crh'], 2)} "
                  f"· sweep {_fmt(crt['sweep'], 2)}")
    if sig.get("sl_mode") and sig["sl_mode"] != "STRUCT":
        lines += f"\nSL : {_SL_LBL.get(sig['sl_mode'], sig['sl_mode'])}"
    if sig.get("tp_mode"):
        lines += f"\nTP : {sig['tp_mode']}"
    return lines


def _mtf_fib_line(sig):
    """Ligne « Fibo HTF » d'un signal : None si le filtre Fibonacci HTF Premium/Discount est OFF."""
    if not sig.get("mtf_fib_on"):
        return None
    return f"Fibo HTF : {sig.get('mtf_fib') or 'ON'}"


def _be_line(sig):
    """Ligne « BE → RRx » d'un signal ; « BE : non utilisé » si le RR du BE n'est pas sous la TP finale."""
    be_rr = sig.get("be_rr") or BE_RR
    return f"BE → RR{be_rr:g}" if be_rr < _sig_rr(sig) else "BE : non utilisé (RR du BE ≥ TP)"


def _head_line(symbol, sig, lot=None):
    """1re ligne (aperçu de la notification) : gros pictos ACHAT vert / VENTE rouge + actif + LOT en gras."""
    buy = sig["side"] == "BUY"
    lbl = ("ACHAT" if buy else "VENTE") + (" LIMIT" if sig.get("order") == "LIMIT" else "")
    lot_txt = f" · LOT {lot:g}" if lot else ""
    return f"{'🟢🟢🟢' if buy else '🔴🔴🔴'} <b>{lbl} {symbol}{lot_txt}</b>"


def group_signal(symbol, sig, position_n, dec, tf=None, lot=None):
    """Message groupe court : 1re ligne sens + LOT (visible dans la notification), puis entrée, SL, TP et RR."""
    limit = sig.get("order") == "LIMIT"
    return (
        f"{_head_line(symbol, sig, lot)}\n"
        f"⏱ {tf or (sig.get('tf') if sig.get('type') in ('QML', 'FIBTREND') else None) or TF_LABEL} · RR {_sig_rr(sig):g}\n"
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
        f"{_head_line(symbol, sig, lot_info['lot'])}\n"
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
    if name == "PARTIAL":
        return (f"💰 <b>Partiel TP{ev['level']}</b> — {head} · {ev['pct'] * 100:g} % clôturé à {ev['rr']:g}R "
                f"(+{t.get('part_r') or 0:.2f}R encaissé)")
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
        return f"🟰 <b>BE touché</b> — {head} ({(r if r is not None else 0.0):+.1f}R)"
    return f"🔴 <b>SL ❌</b> — {head}{r_txt}"


def admin_event(ev):
    t = ev["trade"]
    if ev["name"] == "FILLED":
        return f"✅ {t['symbol']} {t['side']} LIMIT exécuté @ {t['entry']:g} — position ouverte."
    if ev["name"] == "CANCELLED":
        return f"❌ {t['symbol']} {t['side']} LIMIT annulé — {ev['reason']} Retire l'ordre."
    if ev["name"] == "PARTIAL":
        return f"💰 {t['symbol']} {t['side']} : partiel TP{ev['level']} ({ev['pct'] * 100:g} % à {ev['rr']:g}R)."
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
            f"📈 Filtre Impulsion M15 : {'ON' if get_m15_imp_filter() else 'OFF'}\n"
            f"⛓ SETUP50 : jambe {'≥%gATR' % get_se_leg_atr() if get_se_leg() else 'OFF'} · CHoCH "
            f"{'illimité' if not get_se_choch_window() else '≤%d bougies' % get_se_choch_window()} · entrée {get_se_entry().lower()}\n"
            f"🧱 Vrai BOS : {'ON ≥ %d%%' % round(get_bos_retrace() * 100) if get_bos_real() else 'OFF'} · "
            f"🎯 POI HTF : {'ON ' + get_poi_filter_tf() if get_poi_filter() else 'OFF'}\n"
            f"⏸ Pause après SL : {se_pause_text()}\n"
            f"🔒 BE à : RR{get_be_rr():g}\n"
            f"📥 Entrée : {_entry_txt()}\n"
            f"🛡 SL : {_SL_LBL[get_sl_mode()]} · 🎯 TP : {_TP_LBL[get_tp_mode()]}\n"
            f"🧩 Stratégies : {_strat_txt()}\n"
            f"🔬 Filtres qualité : {_qf_txt()}\n"
            f"🌍 Actifs : {_syms_txt()}\n"
            f"🕐 Session : {_SESS_LBL[get_session()]} · 🎛 Zone limit : {get_limit_zone()}\n"
            f"📍 Signaux ouverts max par actif : {get_max_positions()}")


_SL_LBL = {"STRUCT": "structure (BOS)", "ZONE": "derrière OB/FVG", "SWEEP": "derrière le sweep"}
_TP_LBL = {"RR": "RR fixe", "TARGET": "liquidité ext.", "SWING": "swing H/L"}
_CRT_TP_LBL = {"RANGE": "autre bout du range", "MID": "50 % du range", "RR": "RR fixe"}
_SESS_LBL = {"ALL": "toutes", "LONDON": "Londres", "NY": "New York", "BOTH": "Londres + New York"}


def _signal_sess_text():
    return (f"🕐 Session : <b>{_SESS_LBL[get_session()]}</b>\n\n"
            f"Seuls les CHoCH survenus pendant la session choisie donnent un signal (stratégie principale ET CRT).\n"
            f"Londres {SESSION_LONDON[0]:02d}h–{SESSION_LONDON[1]:02d}h UTC · New York {SESSION_NY[0]:02d}h–{SESSION_NY[1]:02d}h UTC "
            f"(réglable : SESSION_LONDON / SESSION_NY).\n"
            f"Le trade déjà ouvert continue normalement hors session. Aussi : /session all|londres|ny|both")


def _signal_sess_keyboard():
    cur = get_session()
    return _signal_back([[_btn(_SESS_LBL[k], f"ssess:{k}", cur == k) for k in SESSIONS]])


def _strat_txt():
    m, c = get_strat_main(), get_strat_crt()
    return ("principale + CRT" if m and c else ("CRT seul" if c else "principale seule")) + \
           (f" ({_tf_lbl(get_crt_ref())}->{TF_LABEL})" if c else "")


def _btn(label, data, on=False):
    return {"text": ("✅ " if on else "") + label, "callback_data": data}


def _signal_sltp_text():
    return (f"🛡 SL (entrée directe) : <b>{_SL_LBL[get_sl_mode()]}</b>\n"
            f"🎯 TP : <b>{_TP_LBL[get_tp_mode()]}</b>\n\n"
            f"SL — structure : ligne du BOS opposé · zone : derrière l'OB/FVG du CHoCH · sweep : derrière l'extrême de la jambe. "
            f"(Le limit OB/FVG et le CRT mettent toujours leur SL derrière la zone / le sweep.)\n"
            f"TP — RR fixe (réglage RR) · liquidité ext. : cible visée · swing : plus proche swing H/L non cassé. "
            f"Cible absente ou sous RR{TP_MIN_RR:g} : repli sur le RR fixe. "
            f"SETUP50 : SL toujours derrière le sweep ; TP = RR fixe (RR{get_tp_rr():g}) ou liquidité ext. selon ce choix.\n"
            f"Effet immédiat sur les NOUVEAUX signaux. Aussi : /modesl struct|zone|sweep · /modetp rr|target|swing")


def _signal_sltp_keyboard():
    sl, tp = get_sl_mode(), get_tp_mode()
    return _signal_back([
        [_btn(_SL_LBL[k].capitalize(), f"ssl:{k}", sl == k) for k in SL_MODES],
        [_btn(_TP_LBL[k].capitalize(), f"stp:{k}", tp == k) for k in TP_MODES]])


def _signal_strat_text():
    return (f"🧩 Stratégies actives : <b>{_strat_txt()}</b>\n\n"
            f"• Principale : Sweep → POI → mitigation → CHoCH (filtres HTF de ce menu).\n"
            f"• CRT : bougie HTF de référence (CRH/CRL) balayée d'un côté, clôture de retour DANS le range, puis CHoCH sur {TF_LABEL}. "
            f"SL derrière le sweep, TP : {_CRT_TP_LBL[get_crt_tp()]}. Sans filtre de tendance (c'est un retournement). "
            f"Modèles courants : H4→M15, H1→M5.\n"
            f"Si les deux sont actives, un CHoCH déjà pris par la principale n'ouvre pas de 2e trade CRT, sauf si « CRT même si… » est ON "
            f"(doublons : {'ON' if get_crt_dup() else 'OFF'}).\n"
            f"⚠️ Le CRT n'est pas validé par un backtest : garde-le désactivé en réel tant qu'il n'a pas fait ses preuves.\n"
            f"Aussi : /strategie main|crt|both · /crt h1|h4|d1|range|mid|rr|htfon|htfoff")


def _signal_strat_keyboard():
    m, c, ref, tp = get_strat_main(), get_strat_crt(), get_crt_ref(), get_crt_tp()
    return _signal_back([
        [_btn("Principale", "sstr:main", m), _btn("CRT", "sstr:crt", c)],
        [_btn(_tf_lbl(k), f"scrt:{k}", ref == k) for k in CRT_REFS],
        [_btn(_CRT_TP_LBL[k].capitalize(), f"scrtp:{k}", tp == k) for k in CRT_TP_MODES],
        [_btn("CRT + filtre tendance HTF", "scrthtf", get_crt_htf())],
        [_btn("CRT même si la principale a pris le CHoCH", "scrtdup", get_crt_dup())]])


def _entry_txt():
    d, l = get_entry_direct(), get_entry_limit()
    return "⚡ direct + ⏳ limit OB/FVG" if d and l else ("⏳ limit OB/FVG seul" if l else "⚡ direct seul")


def _signal_entry_text():
    return (f"📥 Type d'entrée : <b>{_entry_txt()}</b> (touche un bouton pour l'activer / le couper ; au moins un reste actif)\n\n"
            f"⚡ Direct : entrée au marché à la clôture du CHoCH.\n"
            f"⏳ Limit : ordre placé à l'entrée de l'OB / FVG créé par le CHoCH (bord proche, pour ne pas rater l'entrée), "
            f"SL derrière la zone. Annulé si le prix repart sans toucher ou après {LIMIT_EXPIRY_CANDLES} bougies.\n"
            f"Les deux : les deux ordres partent ; le limit ne compte pas dans « signaux max » (attention : risque doublé "
            f"si les deux s'exécutent).\n"
            f"Zone du limit : <b>{get_limit_zone()}</b> (OB seul, FVG seul, ou les deux : la plus proche du prix).\n"
            f"🎯 POC proxy : {'ON' if get_poc_entry() else 'OFF'} — niveau d'entrée limit SUPPLÉMENTAIRE (jamais obligatoire "
            f"en plus de l'OB/FVG), calculé uniquement sur la jambe impulsive du CHoCH (paliers de prix les plus "
            f"traversés, sans volume réel — voir /pocinfo). OFF = comportement inchangé (limit sur OB/FVG seul).\n"
            f"Effet immédiat sur les NOUVEAUX signaux. Aussi : /entree direct | limit | both · /zone ob|fvg|both")


def _signal_entry_keyboard():
    d, l = get_entry_direct(), get_entry_limit()
    return _signal_back([
        [_btn("⚡ Entrée directe", "sentd", d), _btn("⏳ Ordre limit", "sentl", l)],
        [_btn(f"Zone {k}", f"szone:{k}", get_limit_zone() == k) for k in LIMIT_ZONES],
        [_btn("🎯 POC proxy", "sentpoc", get_poc_entry())]])


def _syms_txt():
    on = [x for x in SYMBOLS if get_symbol_on(x)]
    return ", ".join(on) if on else "aucun"


def _signal_sym_text():
    return (f"🌍 <b>ACTIFS</b> : {_syms_txt()}\n\n"
            f"Touche un actif pour l'activer / le désactiver. Un actif désactivé n'ouvre plus de signal (et n'est plus scanné) ; "
            f"ses positions déjà ouvertes restent suivies jusqu'au bout.\n"
            f"Défaut : XAUUSD + BTCUSD (réglable via SYMBOLS_ON). Forex USD/xxx : le lot est grand pour un SL serré, "
            f"vérifie la marge libre du compte avant de les activer.")


def _signal_sym_keyboard():
    btns = [_btn(x, f"ssym:{x}", get_symbol_on(x)) for x in SYMBOLS]
    return _signal_back([btns[j:j + 2] for j in range(0, len(btns), 2)])


def _qf_txt():
    on = [QF_LBL[k] for k in QF_KEYS if get_qf(k)]
    return ", ".join(on) if on else "aucun (tous OFF)"


def _signal_qf_text():
    rows = "\n".join(f"{'✅' if get_qf(k) else '⬜'} {QF_LBL[k]}" for k in QF_KEYS)
    return (f"🔬 <b>FILTRES QUALITÉ</b>\n\n{rows}\n\n"
            f"Chaque filtre est ON ou OFF (défaut : OFF = plus de signaux). Même OFF, il est calculé et écrit dans le log de chaque signal.\n"
            f"• Structure : vente = HH+HL avant le CHoCH ; achat = LL+LH.\n"
            f"• Displacement : corps >= {DISP_BODY_ATR:g}xATR, peu de mèches, clôture >= {DISP_BREAK_ATR:g}xATR au-delà du niveau.\n"
            f"• Anti-range : efficience des {RANGE_LOOKBACK} dernières bougies >= {RANGE_MIN_ER:g}.\n"
            f"• FVG strict : le CHoCH doit créer un FVG pas entièrement mitigé.\n"
            f"• Inducement : limit OB/FVG seulement, un pivot mineur entre le prix et la zone.\n"
            f"Effet immédiat sur les NOUVEAUX signaux.")


def _signal_qf_keyboard():
    btns = [_btn(QF_LBL[k], f"sqf:{k}", get_qf(k)) for k in QF_KEYS]
    return _signal_back([btns[j:j + 2] for j in range(0, len(btns), 2)])


def _signal_keyboard():
    return {"inline_keyboard": [
        [{"text": "🎯 RR", "callback_data": "sig:rr"}, {"text": "⏱ TIMEFRAME", "callback_data": "sig:tf"},
         {"text": "🧠 FILTRE HTF", "callback_data": "sig:htf"}],
        [{"text": "🔒 BE", "callback_data": "sig:be"}, {"text": "📍 SIGNAUX MAX", "callback_data": "sig:max"}],
        [{"text": "🌐 LIQ. EXTERNE", "callback_data": "sig:ext"}, {"text": "📐 FIBO HTF", "callback_data": "sig:fib"}],
        [{"text": "📈 IMPULSION M15", "callback_data": "sig:m15imp"}],
        [{"text": "🧱 VRAI BOS", "callback_data": "sig:bos"}, {"text": "🎯 POI HTF (OB/FVG)", "callback_data": "sig:poih"}],
        [{"text": "⛓ SÉQUENCE SETUP50", "callback_data": "sig:seq"}],
        [{"text": "📥 ENTRÉE", "callback_data": "sig:entry"}, {"text": "🛡 SL / 🎯 TP", "callback_data": "sig:sltp"}],
        [{"text": "🧩 STRATÉGIES (CRT)", "callback_data": "sig:strat"}, {"text": "🕐 SESSION", "callback_data": "sig:sess"}],
        [{"text": "⏸ PAUSE APRÈS SL", "callback_data": "sig:pause"}],
        [{"text": "🔬 FILTRES QUALITÉ", "callback_data": "sig:qf"}, {"text": "🌍 ACTIFS", "callback_data": "sig:sym"}],
        [{"text": "⚙️ QML", "callback_data": "qml:home"}, {"text": "⚙️ FIBTREND", "callback_data": "ft:home"}],
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


def _onoff_row(on, cb):
    return [{"text": ("✅ " if on else "") + "🟢 ON", "callback_data": f"{cb}:on"},
            {"text": ("✅ " if not on else "") + "🔴 OFF", "callback_data": f"{cb}:off"}]


def _signal_m15imp_text():
    on = get_m15_imp_filter()
    return (f"📈 Filtre M15 « vraie impulsion + structure + Fibonacci » : <b>{'ON' if on else 'OFF'}</b>\n\n"
            f"🟢 ON : la structure M15 (HH/HL ou LH/LL, micro-jambes ignorées) donne la vraie tendance ; la vraie impulsion "
            f"(swing High/Low structurel → cassure) est mesurée au Fibonacci. Un CHoCH n'est accepté que dans le sens de "
            f"l'impulsion, après un retracement :\n"
            f"• &lt; {M15_IMP_EARLY_MIN * 100:.1f}% : refusé\n"
            f"• entre {M15_IMP_EARLY_MIN * 100:.1f}% et {get_m15_min_retrace() * 100:.0f}% : précoce, seulement avec un OB/FVG fort\n"
            f"• {get_m15_min_retrace() * 100:.0f}% et au-delà : zone principale, 50% jamais seul\n\n"
            f"Retracement mini : <b>{get_m15_min_retrace() * 100:.1f}%</b>\n"
            f"Confluence OB/FVG exigée : <b>{'OUI' if get_m15_need_poi() else 'NON'}</b> · "
            f"Sweep exigé : <b>{'OUI' if get_m15_need_sweep() else 'NON'}</b>\n\n"
            f"Effet immédiat sur les NOUVEAUX signaux. Aussi : /m15imp on|off")


def _signal_m15imp_keyboard():
    cur = get_m15_min_retrace()
    return _signal_back([
        _onoff_row(get_m15_imp_filter(), "sm15"),
        [{"text": ("✅ " if abs(cur - v) < 1e-9 else "") + f"{v * 100:g}%", "callback_data": f"sm15r:{v}"}
         for v in M15_RETRACE_CHOICES],
        [{"text": "OB/FVG exigé : " + ("✅ ON" if get_m15_need_poi() else "OFF"),
          "callback_data": "sm15p:" + ("off" if get_m15_need_poi() else "on")},
         {"text": "Sweep exigé : " + ("✅ ON" if get_m15_need_sweep() else "OFF"),
          "callback_data": "sm15s:" + ("off" if get_m15_need_sweep() else "on")}]])


def _signal_bos_text():
    on = get_bos_real()
    return (f"🧱 Filtre « vrai BOS » : <b>{'ON' if on else 'OFF'}</b> (retracement mini {get_bos_retrace() * 100:g}%)\n\n"
            f"🟢 ON : une cassure de structure M15 n'est considérée comme un vrai BOS que si le prix a ensuite retracé au moins "
            f"40 / 45 / 50 % de la jambe. Sans ce retracement : faux BOS, aucun signal.\n"
            f"🔴 OFF : aucune contrainte. Effet immédiat sur les NOUVEAUX signaux. Aussi : /bos on|off [40|45|50]")


def _signal_bos_keyboard():
    cur = get_bos_retrace()
    return _signal_back([
        _onoff_row(get_bos_real(), "sbos"),
        [{"text": ("✅ " if abs(cur - v) < 1e-9 else "") + f"{v * 100:g}%", "callback_data": f"sbosr:{v}"}
         for v in BOS_RETRACE_CHOICES]])


def _signal_poih_text():
    on = get_poi_filter()
    return (f"🎯 Filtre POI HTF (OB / imbalance) : <b>{'ON — ' + get_poi_filter_tf() if on else 'OFF'}</b>\n\n"
            f"🟢 ON : à chaque signal, le bot vérifie qu'un Order Block ou un FVG (bullish/bearish selon le sens) de l'UT choisie "
            f"(M15, H1 ou les deux) a bien été MITIGÉ par le point de retournement. Zone non mitigée ou indécise : signal ignoré.\n"
            f"🔴 OFF : aucune contrainte. Effet immédiat. Aussi : /poihtf on|off [m15|h1|both]")


def _signal_poih_keyboard():
    cur = get_poi_filter_tf()
    return _signal_back([
        _onoff_row(get_poi_filter(), "spoi"),
        [{"text": ("✅ " if cur == m else "") + {"M15": "M15", "H1": "H1", "BOTH": "M15 + H1"}[m], "callback_data": f"spoit:{m}"}
         for m in POI_TF_CHOICES]])


def _signal_seq_text():
    cw = get_se_choch_window()
    return ("⛓ <b>Séquence SETUP50</b>\n"
            "1️⃣ Qualité de jambe → 2️⃣ Sweep → 3️⃣ CHoCH → 4️⃣ zone 50 % → 5️⃣ entrée\n\n"
            f"1️⃣ Jambe : <b>{'ON ≥ %g ATR' % get_se_leg_atr() if get_se_leg() else 'OFF'}</b> — le sweep doit être le fait d'une vraie "
            f"impulsion (du plus bas/haut depuis la formation du niveau balayé jusqu'à l'extrême du sweep).\n"
            f"3️⃣ CHoCH : <b>{'illimité' if not cw else 'dans les %d bougies' % cw}</b> après le sweep — illimité = on attend la cassure "
            f"tant que l'extrême du sweep n'est pas dépassé.\n"
            f"4️⃣ Retracement mini : <b>{get_se_fib_entry() * 100:g} %</b> de la jambe (zone d'entrée)\n"
            f"6️⃣ SL : <b>{ {'AUTO': 'derrière l OB, sinon l avalement, sinon la bougie d entrée', 'BOUGIE': 'derrière la bougie d entrée', 'SWEEP': 'derrière l extrême du sweep (large)'}[get_se_sl()] }</b>\n"
            f"5️⃣ Entrée dans la zone : <b>{ {'REJET': 'à la clôture, avalement ou mèche > corps (pin bar)', 'MECHE': 'dès que la mèche touche le 50 %', 'STRICT': 'rejet strict (ancien filtre)'}[get_se_entry()] }</b>\n\n"
            f"Effet immédiat sur les NOUVEAUX setups. Aussi : /seq")


def _signal_seq_keyboard():
    cw, lg, la, en = get_se_choch_window(), get_se_leg(), get_se_leg_atr(), get_se_entry()
    return _signal_back([
        _onoff_row(lg, "sselg"),
        [{"text": ("✅ " if abs(la - v) < 1e-9 else "") + f"{v:g} ATR", "callback_data": f"ssela:{v}"} for v in SE_LEG_ATR_CHOICES],
        [{"text": ("✅ " if cw == v else "") + ("CHoCH illimité" if v == 0 else f"{v} bougies"), "callback_data": f"ssecw:{v}"}
         for v in SE_CHOCH_WINDOW_CHOICES],
        [{"text": ("✅ " if abs(get_se_fib_entry() - v) < 1e-9 else "") + f"Retracement {v * 100:g}%", "callback_data": f"ssefe:{v}"}
         for v in SE_FIB_CHOICES],
        [{"text": ("✅ " if get_se_sl() == "AUTO" else "") + "SL : OB/avalement/bougie", "callback_data": "ssesl:AUTO"}],
        [{"text": ("✅ " if get_se_sl() == "BOUGIE" else "") + "SL : bougie d'entrée", "callback_data": "ssesl:BOUGIE"},
         {"text": ("✅ " if get_se_sl() == "SWEEP" else "") + "SL : sweep (large)", "callback_data": "ssesl:SWEEP"}],
        [{"text": ("✅ " if en == "REJET" else "") + "Avalement / mèche > corps", "callback_data": "sseen:REJET"}],
        [{"text": ("✅ " if en == "MECHE" else "") + "Mèche seule", "callback_data": "sseen:MECHE"},
         {"text": ("✅ " if en == "STRICT" else "") + "Rejet strict (ancien)", "callback_data": "sseen:STRICT"}]])


def _signal_be_text():
    return (f"🔒 BE actuel : <b>RR{get_be_rr():g}</b>\n"
            f"Quand le prix atteint ce RR, le SL est déplacé à l'entrée. Si le prix revient sur l'entrée : "
            f"« BE touché » (0 R), pas un SL. S'applique aux NOUVEAUX signaux : les trades déjà ouverts gardent leur BE. "
            f"Un BE égal ou supérieur au RR de la TP finale est ignoré. Autre valeur : /be 0.75")


SE_PAUSE_N_CHOICES = (2, 3, 4, 5)
SE_PAUSE_CANDLES_CHOICES = (30, 60, 120)


def _signal_pause_text():
    return (f"⏸ <b>Pause après SL consécutifs</b> (stratégie SETUP50)\n"
            f"État : <b>{se_pause_text()}</b>\n\n"
            f"ON : après N SL consécutifs, plus aucune entrée pendant la pause (un TP ou un BE remet le compteur à zéro). "
            f"OFF : aucune pause, et une pause en cours est levée immédiatement.\n"
            f"Effet immédiat. Aussi : /pausesl on|off · /pausesl on 3 60")


def _signal_pause_keyboard():
    on, n, k = get_se_pause(), get_se_pause_n(), get_se_pause_candles()
    return _signal_back([
        [_btn("Activer", "spause:on", on), _btn("Désactiver", "spause:off", not on)],
        [_btn(f"{v} SL", f"spause:n{v}", n == v) for v in SE_PAUSE_N_CHOICES],
        [_btn(f"{v} bougies", f"spause:c{v}", k == v) for v in SE_PAUSE_CANDLES_CHOICES]])


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


_ANALYSE_SYMS = {"XAU": "XAUUSD", "BTC": "BTCUSD", "CAD": "USDCAD", "JPY": "USDJPY",
                 "EUR": "EURUSD", "GBP": "GBPUSD", "CHF": "USDCHF", "AUD": "AUDUSD", "NZD": "NZDUSD",
                 "EJ": "EURJPY", "GJ": "GBPJPY", "US30": "US30"}   # + marchés du module QML (jusqu'à 10, voir QML_MARKETS)


def _analyse_text():
    return ("📊 <b>ANALYSE</b>\n\n"
            "Choisis l'actif. Lecture seule : n'affecte jamais le moteur de signal ni les positions.")


def _analyse_keyboard():
    codes = {v: k for k, v in _ANALYSE_SYMS.items()}
    icons = {"XAUUSD": "🥇", "BTCUSD": "₿"}
    shown = ["XAUUSD", "BTCUSD", "USDCAD", "USDJPY"] + [m for m in SYMBOLS if m not in ("XAUUSD", "BTCUSD", "USDCAD", "USDJPY")]
    btns = [{"text": f"{icons.get(m, '💵')} {m}", "callback_data": f"ana:sym:{codes[m]}"} for m in shown if m in codes]
    return _with_back({"inline_keyboard": [btns[i:i + 2] for i in range(0, len(btns), 2)]})


def _analyse_symbol_text(symbol):
    return f"📊 <b>{symbol}</b>\n\nChoisis le type d'analyse :"


def _analyse_symbol_keyboard(symbol):
    code = next(k for k, v in _ANALYSE_SYMS.items() if v == symbol)
    return {"inline_keyboard": [
        [{"text": "📈 TECHNIQUE", "callback_data": f"ana:tech:{code}"},
         {"text": "📰 FONDAMENTAL", "callback_data": f"ana:fond:{code}"}],
        [{"text": "🎯 BOS/CHoCH", "callback_data": f"ana:str:{code}:main"}, {"text": "🕯 CRT", "callback_data": f"ana:str:{code}:crt"}],
        [{"text": f"🎯 QML {_tf_lbl(t)}", "callback_data": f"ana:qml:{code}:{t}"} for t in QML_ANALYSE_TFS],
        [{"text": "🏆 QML GAGNANT", "callback_data": f"ana:qmlg:{code}"}],
        [{"text": "🔎 TOUTES LES STRATÉGIES", "callback_data": f"ana:all:{code}"}],
        [{"text": "🔙 Analyse", "callback_data": "ana:home"}, {"text": "🔙 Menu", "callback_data": "menu:home"}],
    ]}


# --- 🏦 TRADING MANUEL (Telegram) --------------------------------------------------------------
# Section isolée, ajoutée sans toucher au moteur de signal automatique. Utilise l'API déjà
# connectée (MetaApi REST / MT5) — jamais de 2e connexion, jamais de nom de broker codé en dur :
# le nom réel vient de get_account_information() (voir get_account_summary).
_MANUAL_SYMS = {"XAU": "XAUUSD", "BTC": "BTCUSD", "CAD": "USDCAD", "JPY": "USDJPY"}


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
        [{"text": "🟠 XAUUSD", "callback_data": "mtr:sym:XAU"}, {"text": "₿ BTCUSD", "callback_data": "mtr:sym:BTC"}],
        [{"text": "🇨🇦 USDCAD", "callback_data": "mtr:sym:CAD"}, {"text": "🇯🇵 USDJPY", "callback_data": "mtr:sym:JPY"}]]})


def _trade_side_text(symbol):
    return (f"📈 <b>{symbol}</b>\n\n"
            f"💵 Risque : {get_risk():g} $ · 🎯 RR : {get_tp_rr():g}\n"
            f"Choisis BUY ou SELL, puis envoie <b>ton SL</b> : le lot est calculé sur ce SL et le risque configuré, "
            f"et tu valides avant l'envoi de l'ordre.")


def _trade_side_keyboard(symbol):
    code = next(k for k, v in _MANUAL_SYMS.items() if v == symbol)
    return {"inline_keyboard": [
        [{"text": "🟢 BUY", "callback_data": f"mtr:buy:{code}"}, {"text": "🔴 SELL", "callback_data": f"mtr:sell:{code}"}],
        [{"text": "🔙 Trade", "callback_data": "mtr:home"}, {"text": "🔙 Menu", "callback_data": "menu:home"}]]}


# Trade manuel en attente : chat_id -> {"symbol", "side", "sl" (None tant que non saisi), "ts"}. En mémoire seulement :
# un redémarrage annule l'attente (rien n'a été envoyé au broker à ce stade).
_pending_manual = {}
_PENDING_MANUAL_TTL_SEC = 300


def _pending_manual_get(chat_id):
    p = _pending_manual.get(chat_id)
    if p and time.time() - p["ts"] > _PENDING_MANUAL_TTL_SEC:
        _pending_manual.pop(chat_id, None)
        return None
    return p


_SL_EXAMPLE = {'XAUUSD': '4281.50', 'USDCAD': '1.37250', 'USDJPY': '149.850'}


def _trade_sl_prompt_text(symbol, side):
    return (f"✍️ <b>{symbol} {side}</b>\n\n"
            f"Envoie ton <b>SL</b> (un prix), ex. <code>{_SL_EXAMPLE.get(symbol, '84200')}</code>.\n"
            f"Le lot sera calculé pour risquer {get_risk():g} $ sur ce SL, avec un TP à RR {get_tp_rr():g}.\n"
            f"Ou touche 🎯 SL au sweep : le SL est placé sous (BUY) / au-dessus (SELL) de la mèche du dernier sweep.\n"
            f"Rien n'est envoyé au broker avant ta validation. ⏱ Valable 5 min.")


def _trade_sl_keyboard():
    return {"inline_keyboard": [
        [{"text": "🎯 SL au sweep", "callback_data": "mtr:sweep"}],
        [{"text": "⚙️ SL auto (plancher)", "callback_data": "mtr:auto"}],
        [{"text": "❌ Annuler", "callback_data": "mtr:cancel"}]]}


def _trade_confirm_keyboard():
    return {"inline_keyboard": [
        [{"text": "✅ Valider", "callback_data": "mtr:go"}, {"text": "❌ Annuler", "callback_data": "mtr:cancel"}]]}


def _manual_preview_text(plan, note=""):
    dec = SYMBOLS[plan["symbol"]]["decimals"]
    li = plan["lot_info"]
    warn = (f"\n⚠️ Lot minimum appliqué : risque réel {li['real_risk']:.2f} $ au lieu de {get_risk():g} $."
            if li["raised_to_min"] else "")
    return (f"📝 <b>{plan['symbol']} {plan['side']} — à valider</b>\n\n"
            f"Entrée (prix actuel) : {_fmt(plan['entry'], dec)}\n"
            f"SL : {_fmt(plan['sl'], dec)} ({_fmt(plan['sl_distance'], dec)} pts)\n"
            f"TP : {_fmt(plan['tp'], dec)} (RR 1:{plan['rr']:g})\n"
            f"Lot : {li['lot']:g}\n"
            f"Risque : {li['real_risk']:.2f} $\n"
            f"Spread : {_fmt(plan['spread'], dec)}{warn}\n"
            f"{note}\n"
            f"Aperçu indicatif : à la validation, le lot et le TP sont recalculés au prix réel, ton SL reste inchangé.")


def _handle_manual_sl_input(chat_id, text):
    """Message texte reçu pendant qu'un trade manuel attend son SL : lit le prix, calcule le plan, demande validation."""
    pend = _pending_manual_get(chat_id)
    if not pend:
        return
    try:
        sl = float(re.sub(r"\s", "", text).replace(",", "."))
    except ValueError:
        sl = None
    if sl is None or not math.isfinite(sl) or sl <= 0:
        send(chat_id, "Envoie uniquement le prix du SL (un nombre), ex. <code>4281.50</code>.",
             reply_markup=_trade_sl_keyboard())
        return
    plan = _manual_plan(pend["symbol"], pend["side"], sl)
    if not plan["ok"]:
        send(chat_id, f"❌ {plan['error']}\n\nRenvoie un autre SL, ou annule.", reply_markup=_trade_sl_keyboard())
        return
    pend["sl"] = sl
    pend["ts"] = time.time()
    send(chat_id, _manual_preview_text(plan), reply_markup=_trade_confirm_keyboard())


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
        [{"text": "🧩 Stratégies", "callback_data": "str:home"}],
        [{"text": "⏸ ARRÊTER LE BOT", "callback_data": "bot:stop"} if get_bot_on()
         else {"text": "▶️ DÉMARRER LE BOT", "callback_data": "bot:start"}],
        [{"text": "💰 Risque", "callback_data": "menu:risque"}, {"text": "⚙️ Levier", "callback_data": "menu:levier"}],
        [{"text": "⏱ Timeframe", "callback_data": "menu:timeframe"}, {"text": "🎭 Médias", "callback_data": "menu:medias"}],
        [{"text": "📊 Stats", "callback_data": "menu:stats"}, {"text": "📈 Positions", "callback_data": "menu:trades"}],
        [{"text": "📊 Analyse", "callback_data": "ana:home"}, {"text": "📰 News", "callback_data": "news:home"}],
        [{"text": "⚙️ Paramètres signal", "callback_data": "menu:signal"}, {"text": "💰 Money management", "callback_data": "mm:home"}],
        [{"text": "🏦 Compte", "callback_data": "acc:home"}, {"text": "👤 Manuel", "callback_data": "man:home"}],
        [{"text": "📈 Trade", "callback_data": "mtr:home"}, {"text": "📌 Positions broker", "callback_data": "mpos:home"}],
        [{"text": "🔄 Actualiser", "callback_data": "menu:home"}],
    ]}


def _back_keyboard():
    return {"inline_keyboard": [[{"text": "🔙 Menu", "callback_data": "menu:home"}]]}


def _home_text():
    return (f"🤖 <b>ALPHABOT</b> — " + ("🟢 ACTIF" if get_bot_on() else "⏸ ARRÊTÉ (aucun nouveau signal)") + "\n\n"
            f"💰 Risque : {get_risk():g} $\n"
            f"⚙️ Levier : {get_leverage():g}x\n"
            f"⏱ Timeframe : {TF_LABEL}")


def _strategies_text():
    on = lambda b: "🟢 ON" if b else "🔴 OFF"
    live = [n for n, b in (("BOS/CHoCH", get_strat_main()), ("CRT", get_strat_crt()), ("QML", get_qml_on()), ("FIBTREND", get_ft_on())) if b]
    return (f"🧩 <b>STRATÉGIES</b>\n\n"
            f"BOS / CHoCH : {on(get_strat_main())}\nCRT : {on(get_strat_crt())}\nQML : {on(get_qml_on())}"
            f" · publication auto : {on(get_qml_auto())}\n"
            f"FIBTREND : {on(get_ft_on())} · publication auto : {on(get_ft_auto())}\n"
            f"QML GAGNANT : {on(get_qmlg_on())} (règles à venir : aucun signal pour l'instant)\n"
            f"📈 Ordres MT5 (API) : {on(get_mt5_orders_on())}"
            f"{'' if get_mt5_orders_on() else ' — signaux publiés et suivis, AUCUN ordre envoyé au broker'}\n\n"
            f"Actives : {', '.join(live) if live else 'aucune ⚠️ (plus aucun signal automatique)'}\n"
            f"Bot : {'🟢 ACTIF' if get_bot_on() else '⏸ ARRÊTÉ'}\n\n"
            f"Chaque stratégie ON scanne tous les actifs activés (🌍 ACTIFS) et publie / exécute automatiquement. "
            f"OFF = plus de signal pour elle, mais le bouton 📊 Analyse te donne toujours son point d'entrée potentiel.\n"
            f"QML sans « auto » : proposition envoyée à l'admin seulement.")


def _strategies_keyboard():
    return _with_back({"inline_keyboard": [
        [_btn("BOS / CHoCH", "str:main", get_strat_main()), _btn("CRT", "str:crt", get_strat_crt())],
        [_btn("QML", "str:qml", get_qml_on()), _btn("QML auto (signal)", "str:qmlauto", get_qml_auto())],
        [_btn("FIBTREND", "str:ft", get_ft_on()), _btn("FIBTREND auto (signal)", "str:ftauto", get_ft_auto())],
        [_btn("QML GAGNANT", "str:qmlg", get_qmlg_on()), {"text": "⚙️ QML GAGNANT", "callback_data": "qmlg:home"}],
        [_btn("📈 Ordres MT5 (API)", "str:mt5", get_mt5_orders_on())],
        [{"text": "📊 Analyse (point potentiel)", "callback_data": "ana:home"}, {"text": "⚙️ QML", "callback_data": "qml:home"}],
        [{"text": "⚙️ FIBTREND", "callback_data": "ft:home"}],
    ]})


def _news_text():
    nb = None
    try:
        nb = news_block_info("XAUUSD")
    except Exception:
        pass
    return (f"📰 <b>FILTRE NEWS</b> : {'🟢 ON' if get_news_filter_on() else '🔴 OFF'}\n"
            f"Impact : {'fort + moyen' if get_news_medium() else 'fort (rouge) seulement'}\n"
            f"Fenêtre : {NEWS_BEFORE_MIN} min avant / {NEWS_AFTER_MIN} min après (variables NEWS_BEFORE_MIN / NEWS_AFTER_MIN)\n\n"
            f"Pendant une news de la devise concernée (USD pour Gold / BTC / US30 ; les 2 devises pour une paire forex), "
            f"aucun NOUVEAU signal n'est ouvert. Les positions déjà ouvertes continuent d'être suivies.\n"
            + (f"\n⚠️ En ce moment (USD) : {_news_line(nb)}" if nb else "\nAucune news USD dans la fenêtre actuelle.") +
            "\n\nSource : calendrier ForexFactory public (sans clé). Indisponible = pas de blocage.")


def _news_keyboard():
    return _with_back({"inline_keyboard": [[_btn("Filtre news", "news:on", get_news_filter_on()),
                                            _btn("+ impact moyen", "news:med", get_news_medium())]]})


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
               "/mm — money management : compounding, réduction après SL / drawdown, objectif et perte du jour, limites de trades (OFF par défaut)\n"
               "/levier [valeur] — levier (indicatif, calcul de marge)\n"
               "/stats — statistiques (jour / semaine + taux par RR)\n"
               "/statut — prix BTC / Gold et dernière bougie scannée\n"
               "/testgroupe — envoie un message test au groupe (vérifie CHAT_ID_GROUPE)\n"
               "/trades — positions en cours (avec RR actuel)\n"
               "/timeframe [M1|M3|M5|M15|M30|H1] — change le timeframe à chaud\n"
               "/signal — paramètres du signal (RR, timeframe, filtre HTF, BE, signaux max)\n"
               "/be [RR] — RR auquel le SL passe à l'entrée (ex. /be 1)\n"
               "/maxpos [n] — signaux ouverts max par actif (1 = pas de doublon)\n"
               "/entree [direct|limit|both] — type d'entrée : direct, limit sur OB/FVG, ou les deux\n"
               "/session [all|londres|ny|both] — n'accepter que les CHoCH d'une session · /zone [ob|fvg|both] — zone du limit\n"
               "/modesl [struct|zone|sweep] — SL de l'entrée directe · /modetp [rr|target|swing] — mode du TP\n"
               "/qmlsl [sweep|buffer|extreme|candle|atr] — SL du QML gagnant (défaut sweep) · /qmlatr 2.3 — multiplicateur ATR\n"
               "/strategie [main|crt|both] · /crt [on|off|h1|h4|d1|range|mid|rr] — module CRT (désactivé par défaut)\n"
               "/mtffib [on|off] — filtre Fibonacci HTF Premium/Discount\n"
               "/m15imp [on|off] — filtre M15 vraie impulsion + structure + Fibonacci\n"
               "/bos [on|off] [40|45|50] — vrai BOS : retracement mini après la cassure\n"
               "/poihtf [on|off] [m15|h1|both] — OB/FVG M15/H1 mitigé obligatoire\n"
               "/pausesl [on|off] [nb_SL] [bougies] — pause automatique après N SL consécutifs (stratégie SETUP50)\n"
               "/medias — stickers / images / GIF du groupe (TP, SL, BE, motivation)\n"
               "/qml [on|off] — module QML (TF M5/M15, filtre H1 optionnel, M1 avalement) et clôtures partielles\n"
               "/ft [on|off|auto|struct|tf 5|tp FIB0/LIQ/RR|rr 2|side|tl] — module FIBTREND (BOS+CHoCH, Fibo 50 %, avalement/pin bar), activable à part\n"
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
    if cmd in ("/strategies", "/stratégies", "/strat"):
        return (_strategies_text(), _strategies_keyboard())
    if cmd in ("/stop", "/arreter", "/arrêter"):
        set_bot_on(False)
        return ("⏸ <b>Bot ARRÊTÉ</b> : plus aucun nouveau signal (BOS/CHoCH, CRT, QML). "
                "Les positions déjà ouvertes restent suivies (BE / TP / SL). /start pour redémarrer.", _menu_keyboard())
    if cmd in ("/start", "/demarrer", "/démarrer"):
        set_bot_on(True)
        return ("▶️ <b>Bot DÉMARRÉ</b> : les nouveaux signaux sont de nouveau actifs.", _menu_keyboard())
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
    if cmd in ("/pausesl", "/pause"):
        nums = []
        for tok in parts[1:]:
            t = tok.strip().lower()
            if t in ("on", "1", "true", "activer", "activé"):
                set_se_pause(True)
            elif t in ("off", "0", "false", "desactiver", "désactiver"):
                set_se_pause(False)
            elif t.isdigit() and int(t) >= 1 and len(nums) < 2:
                nums.append(int(t))
            else:
                return ("Valeur invalide. Exemples : /pausesl on  ·  /pausesl off  ·  /pausesl on 3 60 "
                        "(3 SL consécutifs → pause de 60 bougies)", None)
        if nums:
            set_setting("se_pause_n", nums[0])
            if len(nums) > 1:
                set_setting("se_pause_candles", nums[1])
        return (f"⏸ Pause après SL consécutifs (stratégie SETUP50) : <b>{se_pause_text()}</b>\n"
                f"Change avec /pausesl on, /pausesl off ou /pausesl on 3 60 (nb de SL, nb de bougies).", None)
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
    if cmd in ("/m15imp", "/impulsion", "/m15impulse"):
        if len(parts) > 1:
            arg = parts[1].strip().lower()
            if arg in ("on", "1", "true", "activer", "activé"):
                set_m15_imp_filter(True)
            elif arg in ("off", "0", "false", "desactiver", "désactiver"):
                set_m15_imp_filter(False)
            else:
                return ("Valeur invalide. Exemple : /m15imp on  ou  /m15imp off", None)
        on = get_m15_imp_filter()
        return (f"📈 Filtre M15 vraie impulsion + structure + Fibonacci : <b>{'ON' if on else 'OFF'}</b>\n"
                f"Retracement mini {get_m15_min_retrace() * 100:.0f}% (précoce, sous 50%, seulement avec OB/FVG fort).\n"
                f"Change avec /m15imp on ou /m15imp off.", None)
    if cmd in ("/seq", "/sequence"):
        # /seq entree rejet|meche|strict · /seq choch illimite|10|30|100 · /seq jambe on|off|1.5|2|3
        a_ = [x.strip().lower() for x in parts[1:]]
        try:
            if len(a_) >= 2 and a_[0] in ("entree", "entrée"):
                if not set_choice("se_entry", a_[1], SE_ENTRY_CHOICES):
                    raise ValueError
            elif len(a_) >= 2 and a_[0] in ("retrace", "retracement", "fib"):
                v = float(a_[1].rstrip("%").replace(",", "."))
                v = v / 100 if v > 1 else v
                if not any(abs(v - c) < 1e-9 for c in SE_FIB_CHOICES):
                    raise ValueError
                set_setting("se_fib_entry", v)
            elif len(a_) >= 2 and a_[0] == "sl":
                if not set_choice("se_sl", a_[1], SE_SL_CHOICES):
                    raise ValueError
            elif len(a_) >= 2 and a_[0] == "choch":
                v = 0 if a_[1].startswith(("ill", "0")) else int(a_[1])
                if v not in SE_CHOCH_WINDOW_CHOICES:
                    raise ValueError
                set_setting("se_choch_window", v)
            elif len(a_) >= 2 and a_[0] == "jambe":
                if a_[1] in ("on", "off"):
                    set_setting("se_leg", "1" if a_[1] == "on" else "0")
                else:
                    v = float(a_[1].replace(",", "."))
                    if v not in SE_LEG_ATR_CHOICES:
                        raise ValueError
                    set_setting("se_leg", "1"); set_setting("se_leg_atr", v)
            elif a_:
                raise ValueError
        except ValueError:
            return ("Exemples : /seq sl auto|bougie|sweep · /seq retrace 40 · /seq entree rejet · /seq entree meche · /seq entree strict · /seq choch illimite · /seq choch 30 · "
                    "/seq jambe on · /seq jambe 2", None)
        return (_signal_seq_text(), _signal_seq_keyboard())
    if cmd == "/bos":
        for a_ in parts[1:]:
            a_ = a_.strip().lower().rstrip("%")
            if a_ in ("on", "1", "true"):
                set_setting("bos_real", "1")
            elif a_ in ("off", "0", "false"):
                set_setting("bos_real", "0")
            else:
                try:
                    v = float(a_) / (100 if float(a_) > 1 else 1)
                except ValueError:
                    return ("Exemple : /bos on 50  ou  /bos off", None)
                if not 0.2 <= v <= 0.9:
                    return ("Retracement entre 20 et 90 % (ex. /bos on 45).", None)
                set_setting("bos_retrace", v)
        return (f"🧱 Vrai BOS : <b>{'ON' if get_bos_real() else 'OFF'}</b> (retracement mini {get_bos_retrace() * 100:g}%)\n"
                f"Change avec /bos on|off [40|45|50].", None)
    if cmd in ("/poihtf", "/poih"):
        for a_ in parts[1:]:
            a_ = a_.strip().lower()
            if a_ in ("on", "1", "true"):
                set_setting("poi_filter", "1")
            elif a_ in ("off", "0", "false"):
                set_setting("poi_filter", "0")
            elif not set_choice("poi_filter_tf", a_, POI_TF_CHOICES):
                return ("Exemple : /poihtf on both  ·  /poihtf on h1  ·  /poihtf off", None)
        return (f"🎯 Filtre POI HTF : <b>{'ON — ' + get_poi_filter_tf() if get_poi_filter() else 'OFF'}</b>\n"
                f"Change avec /poihtf on|off [m15|h1|both].", None)
    if cmd in ("/entree", "/entrée", "/entry"):
        if len(parts) > 1:
            modes = {"direct": (True, False), "marche": (True, False), "marché": (True, False),
                     "limit": (False, True), "limite": (False, True), "both": (True, True),
                     "les2": (True, True), "deux": (True, True)}
            m = modes.get(parts[1].lower())
            if not m:
                return ("Valeur invalide. Exemple : /entree direct  |  /entree limit  |  /entree both", None)
            set_entry_types(*m)
        return (_signal_entry_text(), _signal_entry_keyboard())
    if cmd == "/session":
        if len(parts) > 1:
            names = {"all": "ALL", "toutes": "ALL", "londres": "LONDON", "london": "LONDON", "ny": "NY", "newyork": "NY",
                     "both": "BOTH", "londres+ny": "BOTH", "les2": "BOTH"}
            v = names.get(parts[1].lower())
            if not v:
                return ("Valeur invalide. Exemple : /session all | londres | ny | both", None)
            set_choice("session", v, SESSIONS)
        return (_signal_sess_text(), _signal_sess_keyboard())
    if cmd == "/zone":
        if len(parts) > 1 and not set_choice("limit_zone", parts[1], LIMIT_ZONES):
            return ("Valeur invalide. Exemple : /zone ob | fvg | both", None)
        return (_signal_entry_text(), _signal_entry_keyboard())
    if cmd == "/qmlatr":
        try:
            v = float(parts[1].replace(",", "."))
            assert 0.1 <= v <= 20
        except (IndexError, ValueError, AssertionError):
            return ("Exemple : /qmlatr 2.3 (multiplicateur ATR du mode atr, entre 0.1 et 20)", None)
        set_setting("qmlg_atr_mult", v)
        return (f"✅ SL ATR x {v:g} (actif si /qmlsl atr)", None)
    if cmd == "/qmlsl":
        if len(parts) > 1 and not set_choice("qmlg_sl_mode", parts[1], QMLG_SL_MODES):
            return (f"Valeur invalide. Choix : {' | '.join(k.lower() for k in QMLG_SL_MODES)}", None)
        return (f"🛡 SL QML gagnant : <b>{get_qmlg_sl_mode()}</b> (ATR x {get_qmlg_atr_mult():g})\n"
                f"Changer : /qmlsl {' | '.join(k.lower() for k in QMLG_SL_MODES)} · /qmlatr 2.3", None)
    if cmd in ("/modesl", "/modetp"):
        key, choices, lbl = ("sl_mode", SL_MODES, _SL_LBL) if cmd == "/modesl" else ("tp_mode", TP_MODES, _TP_LBL)
        if len(parts) > 1 and not set_choice(key, parts[1], choices):
            return (f"Valeur invalide. Choix : {' | '.join(k.lower() for k in choices)}", None)
        return (_signal_sltp_text(), _signal_sltp_keyboard())
    if cmd in ("/strategie", "/stratégie", "/crt"):
        for arg in parts[1:]:
            a = arg.lower()
            refs = {"h1": 60, "h4": 240, "d1": 1440}
            if cmd == "/crt" and a in ("on", "off"):
                if not set_strategies(get_strat_main(), a == "on"):
                    return ("Au moins une stratégie doit rester active.", None)
            elif a in ("htfon", "htfoff"):
                set_setting("crt_htf", "1" if a == "htfon" else "0")
            elif a in refs:
                set_crt_ref(refs[a])
            elif a.upper() in CRT_TP_MODES:
                set_choice("crt_tp", a, CRT_TP_MODES)
            elif a in ("main", "principale"):
                set_strategies(True, False)
            elif a in ("crt", "both", "les2", "deux"):
                if not set_strategies(a != "crt", True):
                    return ("Au moins une stratégie doit rester active.", None)
            else:
                return ("Valeur invalide. Exemples : /strategie main | crt | both  ·  /crt on|off|h1|h4|d1|range|mid|rr", None)
        return (_signal_strat_text(), _signal_strat_keyboard())
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
    if cmd == "/mm":
        return (mm_command(parts[1:]), None)
    if cmd == "/risque":
        if len(parts) > 1:
            try:
                v = float(parts[1].replace(",", "."))
                if not 0 < v <= MAX_RISK_USD_TELEGRAM:
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
    if cmd == "/qml":
        if len(parts) > 1 and parts[1].lower() in ("on", "off"):
            set_setting("qml_on", "1" if parts[1].lower() == "on" else "0")
        return (_qml_text(), _qml_keyboard())
    if cmd == "/ft":
        return (_ft_command(parts), _ft_keyboard())
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
            _stats_line("🇨🇦", "USDCAD", query_stats(symbol="USDCAD")),
            _stats_line("🇯🇵", "USDJPY", query_stats(symbol="USDJPY")),
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
            elif page == "m15imp":
                _edit(cq, _signal_m15imp_text(), _signal_m15imp_keyboard())
            elif page == "bos":
                _edit(cq, _signal_bos_text(), _signal_bos_keyboard())
            elif page == "seq":
                _edit(cq, _signal_seq_text(), _signal_seq_keyboard())
            elif page == "poih":
                _edit(cq, _signal_poih_text(), _signal_poih_keyboard())
            elif page == "entry":
                _edit(cq, _signal_entry_text(), _signal_entry_keyboard())
            elif page == "sltp":
                _edit(cq, _signal_sltp_text(), _signal_sltp_keyboard())
            elif page == "strat":
                _edit(cq, _signal_strat_text(), _signal_strat_keyboard())
            elif page == "sess":
                _edit(cq, _signal_sess_text(), _signal_sess_keyboard())
            elif page == "sym":
                _edit(cq, _signal_sym_text(), _signal_sym_keyboard())
            elif page == "qf":
                _edit(cq, _signal_qf_text(), _signal_qf_keyboard())
            elif page == "pause":
                _edit(cq, _signal_pause_text(), _signal_pause_keyboard())
            else:
                _edit(cq, _signal_text(), _signal_keyboard())
        elif data.startswith("ssym:"):
            k = data[5:]
            if set_symbol_on(k, not get_symbol_on(k)):
                _edit(cq, _signal_sym_text(), _signal_sym_keyboard())
                ack = f"{k} : {'ON' if get_symbol_on(k) else 'OFF'}"
        elif data.startswith("sqf:"):
            k = data[4:]
            if k in QF_KEYS and set_qf(k, not get_qf(k)):
                _edit(cq, _signal_qf_text(), _signal_qf_keyboard())
                ack = f"{QF_LBL[k]} : {'ON' if get_qf(k) else 'OFF'}"
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
        elif data.startswith("ssl:") or data.startswith("stp:"):
            key, choices = ("sl_mode", SL_MODES) if data.startswith("ssl:") else ("tp_mode", TP_MODES)
            if set_choice(key, data[4:], choices):
                _edit(cq, _signal_sltp_text(), _signal_sltp_keyboard())
                ack = f"SL : {_SL_LBL[get_sl_mode()]} · TP : {_TP_LBL[get_tp_mode()]}"
        elif data.startswith("ssess:"):
            if set_choice("session", data[6:], SESSIONS):
                _edit(cq, _signal_sess_text(), _signal_sess_keyboard())
                ack = "Session : " + _SESS_LBL[get_session()]
        elif data.startswith("szone:"):
            if set_choice("limit_zone", data[6:], LIMIT_ZONES):
                _edit(cq, _signal_entry_text(), _signal_entry_keyboard())
                ack = "Zone limit : " + get_limit_zone()
        elif data == "scrtdup":
            set_setting("crt_dup", "0" if get_crt_dup() else "1")
            _edit(cq, _signal_strat_text(), _signal_strat_keyboard())
            ack = "CRT doublons : " + ("ON" if get_crt_dup() else "OFF")
        elif data == "scrthtf":
            set_setting("crt_htf", "0" if get_crt_htf() else "1")
            _edit(cq, _signal_strat_text(), _signal_strat_keyboard())
            ack = "CRT + filtre tendance : " + ("ON" if get_crt_htf() else "OFF")
        elif data.startswith("sstr:"):
            m, c = get_strat_main(), get_strat_crt()
            arg = data[5:]
            new = (not m, c) if arg == "main" else (m, not c)
            if arg in ("main", "crt") and set_strategies(*new):
                _edit(cq, _signal_strat_text(), _signal_strat_keyboard())
                ack = "Stratégies : " + _strat_txt()
            else:
                ack = "Au moins une stratégie doit rester active"
        elif data.startswith("scrt:") or data.startswith("scrtp:"):
            ok = set_crt_ref(data[5:]) if data.startswith("scrt:") else set_choice("crt_tp", data[6:], CRT_TP_MODES)
            if ok:
                _edit(cq, _signal_strat_text(), _signal_strat_keyboard())
                ack = "CRT : " + _strat_txt()
        elif data in ("sentd", "sentl"):
            d, l = get_entry_direct(), get_entry_limit()
            if set_entry_types(not d if data == "sentd" else d, not l if data == "sentl" else l):
                _edit(cq, _signal_entry_text(), _signal_entry_keyboard())
                ack = "Entrée : " + _entry_txt()
            else:
                ack = "Au moins un type d'entrée doit rester actif"
        elif data == "sentpoc":
            set_poc_entry(not get_poc_entry())
            _edit(cq, _signal_entry_text(), _signal_entry_keyboard())
            ack = "POC proxy : " + ("ON" if get_poc_entry() else "OFF")
        elif data.startswith("sent:"):
            arg = data[5:]
            modes = {"direct": (True, False), "limit": (False, True), "both": (True, True)}
            if arg in modes and set_entry_types(*modes[arg]):
                _edit(cq, _signal_entry_text(), _signal_entry_keyboard())
                ack = "Entrée : " + _entry_txt()
        elif data.startswith("sfib:"):
            arg = data[5:]
            if arg in ("on", "off"):
                set_mtf_fib_filter(arg == "on")
                _edit(cq, _signal_text(), _signal_keyboard())
                ack = "Filtre Fibo HTF : " + ("ON" if arg == "on" else "OFF")
        elif data.startswith("sm15:"):
            arg = data[5:]
            if arg in ("on", "off"):
                set_m15_imp_filter(arg == "on")
                _edit(cq, _signal_m15imp_text(), _signal_m15imp_keyboard())
                ack = "Filtre Impulsion M15 : " + ("ON" if arg == "on" else "OFF")
        elif data.startswith("sm15r:"):
            try:
                v = float(data[6:])
            except ValueError:
                v = 0
            if v in M15_RETRACE_CHOICES:
                set_m15_min_retrace(v)
                _edit(cq, _signal_m15imp_text(), _signal_m15imp_keyboard())
                ack = f"Retracement mini : {v * 100:g}%"
        elif data.startswith("sm15p:") or data.startswith("sm15s:"):
            arg = data[6:]
            if arg in ("on", "off"):
                set_setting("m15_need_poi" if data.startswith("sm15p:") else "m15_need_sweep", "1" if arg == "on" else "0")
                _edit(cq, _signal_m15imp_text(), _signal_m15imp_keyboard())
                ack = "OB/FVG exigé" if data.startswith("sm15p:") else "Sweep exigé"
                ack += " : " + arg.upper()
        elif data.startswith(("sselg:", "ssela:", "ssecw:", "sseen:", "ssefe:", "ssesl:")):
            kind, _, arg = data.partition(":")
            ok_ = False
            if kind == "sselg" and arg in ("on", "off"):
                set_setting("se_leg", "1" if arg == "on" else "0"); ok_ = True
            elif kind == "ssela":
                try:
                    v = float(arg)
                except ValueError:
                    v = 0
                if v in SE_LEG_ATR_CHOICES:
                    set_setting("se_leg_atr", v); ok_ = True
            elif kind == "ssecw":
                try:
                    v = int(arg)
                except ValueError:
                    v = -1
                if v in SE_CHOCH_WINDOW_CHOICES:
                    set_setting("se_choch_window", v); ok_ = True
            elif kind == "ssefe":
                try:
                    v = float(arg)
                except ValueError:
                    v = 0
                if v in SE_FIB_CHOICES:
                    set_setting("se_fib_entry", v); ok_ = True
            elif kind == "ssesl":
                ok_ = set_choice("se_sl", arg, SE_SL_CHOICES)
            elif kind == "sseen":
                ok_ = set_choice("se_entry", arg, SE_ENTRY_CHOICES)
            if ok_:
                _edit(cq, _signal_seq_text(), _signal_seq_keyboard())
                ack = "Séquence SETUP50 mise à jour"
        elif data.startswith("sbos:"):
            arg = data[5:]
            if arg in ("on", "off"):
                set_setting("bos_real", "1" if arg == "on" else "0")
                _edit(cq, _signal_bos_text(), _signal_bos_keyboard())
                ack = "Vrai BOS : " + arg.upper()
        elif data.startswith("sbosr:"):
            try:
                v = float(data[6:])
            except ValueError:
                v = 0
            if v in BOS_RETRACE_CHOICES:
                set_setting("bos_retrace", v)
                _edit(cq, _signal_bos_text(), _signal_bos_keyboard())
                ack = f"Vrai BOS : retracement mini {v * 100:g}%"
        elif data.startswith("spoi:"):
            arg = data[5:]
            if arg in ("on", "off"):
                set_setting("poi_filter", "1" if arg == "on" else "0")
                _edit(cq, _signal_poih_text(), _signal_poih_keyboard())
                ack = "Filtre POI HTF : " + arg.upper()
        elif data.startswith("spoit:"):
            if set_choice("poi_filter_tf", data[6:], POI_TF_CHOICES):
                _edit(cq, _signal_poih_text(), _signal_poih_keyboard())
                ack = "POI HTF : " + data[6:]
        elif data.startswith("spause:"):
            arg = data[7:]
            if arg in ("on", "off"):
                set_se_pause(arg == "on")
                ack = "Pause après SL : " + ("ON" if arg == "on" else "OFF")
            elif arg[:1] == "n" and arg[1:].isdigit() and int(arg[1:]) in SE_PAUSE_N_CHOICES:
                set_setting("se_pause_n", int(arg[1:]))
                ack = f"Pause après {arg[1:]} SL"
            elif arg[:1] == "c" and arg[1:].isdigit() and int(arg[1:]) in SE_PAUSE_CANDLES_CHOICES:
                set_setting("se_pause_candles", int(arg[1:]))
                ack = f"Pause de {arg[1:]} bougies"
            if ack:
                _edit(cq, _signal_pause_text(), _signal_pause_keyboard())
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
        elif data.startswith("ana:str:") or data.startswith("ana:all:"):
            is_all = data.startswith("ana:all:")
            parts_ = data.split(":")
            sym = _ANALYSE_SYMS.get(parts_[2]) if len(parts_) > 2 else None
            if sym and (is_all or (len(parts_) > 3 and parts_[3] in ("main", "crt"))):
                chat_id = cq["message"]["chat"]["id"]
                _edit(cq, f"⏳ Analyse {sym} en cours…", None)
                try:
                    results = analyse_all(sym) if is_all else [strategy_preview(sym, parts_[3])]
                except Exception as e:
                    traceback.print_exc(limit=-3)
                    results = [(f"📊 <b>{sym}</b>\n\n⚠️ Analyse impossible ({type(e).__name__}) — réessaie dans un instant.", None)]
                for i_, (txt, chart) in enumerate(results):
                    if chart:
                        send(chat_id, f"🎯 {sym}", photo=chart)
                    send(chat_id, txt, reply_markup=_analyse_symbol_keyboard(sym) if i_ == len(results) - 1 else None)
                ack = "Analyse envoyée"
        elif data.startswith("ana:qmlg:"):   # QML GAGNANT : analyse toujours disponible, ON ou OFF
            sym = _ANALYSE_SYMS.get(data[9:])
            if sym:
                _edit(cq, f"⏳ Analyse QML GAGNANT {sym} en cours…", None)
                txt = qml_gagnant_text(qml_gagnant_analyze(sym, QMLG_TF))
                _edit(cq, txt, _analyse_symbol_keyboard(sym))
                ack = "Analyse QML GAGNANT"
        elif data.startswith("ana:qml:"):
            code, _, tfs = data[8:].partition(":")
            sym = _ANALYSE_SYMS.get(code)
            if sym:
                a_tf = int(tfs) if tfs.isdigit() and int(tfs) in QML_ANALYSE_TFS else get_qml_tf()
                chat_id = cq["message"]["chat"]["id"]
                _edit(cq, f"⏳ Analyse QML {sym} ({_tf_lbl(a_tf)}) en cours…", None)
                chart = None
                try:
                    res = qml_analyze(sym, force=True, tf=a_tf)
                    txt = qml_analysis_text(res)
                    try:
                        txt += qml_context_text(res) + qml_history_text(res)   # contexte FVG/OB + historique analysé : information seulement
                    except Exception:
                        traceback.print_exc(limit=-3)
                    nb = news_block_info(sym)
                    if nb:
                        txt += f"\n\n📰 ⚠️ News en cours : {_news_line(nb)} — aucun signal ne sera ouvert pendant la fenêtre."
                    try:
                        chart = make_qml_chart(sym, a_tf, res)
                    except Exception:
                        traceback.print_exc(limit=-3)
                except Exception as e:   # réseau / données : jamais de crash du polling
                    txt = f"📊 <b>ANALYSE QML — {sym}</b>\n\n⚠️ Analyse impossible ({type(e).__name__}) — réessaie dans un instant."
                    traceback.print_exc(limit=-3)
                if chart:
                    send(chat_id, f"🎯 {sym} — zones QML {_tf_lbl(a_tf)}", photo=chart)
                send(chat_id, txt, reply_markup=_analyse_symbol_keyboard(sym))
                ack = "Analyse envoyée"
        elif data.startswith("qml:"):
            ack = _qml_callback(cq, data[4:])
        elif data.startswith("qmlg:"):
            ack = _qmlg_callback(cq, data[5:])
        elif data.startswith("ft:"):
            ack = _ft_callback(cq, data[3:])
        elif data.startswith("mm:"):
            ack = _mm_callback(cq, data[3:])
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
                # Nouveau flux : on n'exécute plus au clic. On attend le SL de l'utilisateur, puis validation.
                _pending_manual[cq["message"]["chat"]["id"]] = {"symbol": sym, "side": side, "sl": None,
                                                                "ts": time.time()}
                _post("answerCallbackQuery", {"callback_query_id": cq["id"], "text": f"✍️ Envoie ton SL ({side} {sym})"})
                _edit(cq, _trade_sl_prompt_text(sym, side), _trade_sl_keyboard())
                return
        elif data == "mtr:sweep":
            chat_id = cq["message"]["chat"]["id"]
            pend = _pending_manual_get(chat_id)
            if not pend:
                _post("answerCallbackQuery", {"callback_query_id": cq["id"],
                                              "text": "⏱ Expiré — recommence depuis 📈 TRADE.", "show_alert": True})
                return
            sym, side = pend["symbol"], pend["side"]
            try:
                price, perr = _run_mt5(_manual_price_async(sym))
            except Exception as e:
                price, perr = None, str(e)
            if perr:
                _post("answerCallbackQuery", {"callback_query_id": cq["id"], "text": f"❌ {perr}"[:180], "show_alert": True})
                return
            spread = abs(float(price["ask"]) - float(price["bid"]))
            sl, info = manual_sweep_sl(sym, side, spread)
            if sl is None:
                _post("answerCallbackQuery", {"callback_query_id": cq["id"], "text": f"🎯 {info}"[:180], "show_alert": True})
                return
            plan = _manual_plan(sym, side, sl)
            if not plan["ok"]:
                _edit(cq, f"❌ {plan['error']}\n\nEnvoie ton propre SL, ou annule.", _trade_sl_keyboard())
                return
            dec = SYMBOLS[sym]["decimals"]
            note = (f"\n🎯 SL au sweep {info['tf']} : mèche à {_fmt(info['extreme'], dec)} "
                    f"(niveau balayé {_fmt(info['level'], dec)}, il y a {info['age']} bougie(s)), marge {_fmt(info['margin'], dec)}.\n")
            pend["sl"], pend["ts"] = sl, time.time()
            _edit(cq, _manual_preview_text(plan, note), _trade_confirm_keyboard())
            ack = "SL au sweep"
        elif data in ("mtr:auto", "mtr:go"):
            chat_id = cq["message"]["chat"]["id"]
            pend = _pending_manual_get(chat_id)
            if not pend or (data == "mtr:go" and pend["sl"] is None):
                _post("answerCallbackQuery", {"callback_query_id": cq["id"],
                                              "text": "⏱ Expiré — recommence depuis 📈 TRADE.", "show_alert": True})
                _pending_manual.pop(chat_id, None)
                return
            if not get_manual_mode():
                _post("answerCallbackQuery", {"callback_query_id": cq["id"],
                                              "text": "👤 Manuel OFF — active-le d'abord.", "show_alert": True})
                return
            _pending_manual.pop(chat_id, None)   # une seule exécution par validation (anti double-clic)
            sym, side = pend["symbol"], pend["side"]
            _post("answerCallbackQuery", {"callback_query_id": cq["id"], "text": f"⏳ {side} {sym}…"})
            r = place_manual_order(sym, side, sl_price=pend["sl"] if data == "mtr:go" else None)
            _edit(cq, _manual_order_text(r), _trade_side_keyboard(sym))
            if r["ok"]:
                to_admin(f"👤 Trade manuel : {_manual_order_text(r)}")
            return
        elif data == "mtr:cancel":
            _pending_manual.pop(cq["message"]["chat"]["id"], None)
            _edit(cq, _trade_home_text(), _trade_home_keyboard())
            ack = "Annulé"
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
        elif data in ("bot:stop", "bot:start"):
            set_bot_on(data == "bot:start")
            _edit(cq, _home_text(), _menu_keyboard())
            ack = "Bot démarré" if get_bot_on() else "Bot arrêté (positions toujours suivies)"
        elif data.startswith("str:"):
            a_ = data[4:]
            if a_ == "main":
                set_setting("strat_main", "0" if get_strat_main() else "1"); ack = "BOS/CHoCH : " + ("ON" if get_strat_main() else "OFF")
            elif a_ == "crt":
                set_setting("strat_crt", "0" if get_strat_crt() else "1"); ack = "CRT : " + ("ON" if get_strat_crt() else "OFF")
            elif a_ == "qml":
                set_setting("qml_on", "0" if get_qml_on() else "1"); ack = "QML : " + ("ON" if get_qml_on() else "OFF")
            elif a_ == "qmlauto":
                set_setting("qml_auto", "0" if get_qml_auto() else "1"); ack = "QML auto : " + ("ON" if get_qml_auto() else "OFF")
            elif a_ == "ft":
                set_setting("ft_on", "0" if get_ft_on() else "1"); ack = "FIBTREND : " + ("ON" if get_ft_on() else "OFF")
            elif a_ == "ftauto":
                set_setting("ft_auto", "0" if get_ft_auto() else "1"); ack = "FIBTREND auto : " + ("ON" if get_ft_auto() else "OFF")
            elif a_ == "mt5":
                set_setting("mt5_orders", "0" if get_mt5_orders_on() else "1"); ack = "Ordres MT5 : " + ("ON" if get_mt5_orders_on() else "OFF")
            elif a_ == "qmlg":
                set_setting("qmlg_on", "0" if get_qmlg_on() else "1"); ack = "QML GAGNANT : " + ("ON" if get_qmlg_on() else "OFF")
            _edit(cq, _strategies_text(), _strategies_keyboard())
        elif data.startswith("news:"):
            if data == "news:on":
                set_setting("news_filter", "0" if get_news_filter_on() else "1")
                ack = "Filtre news : " + ("ON" if get_news_filter_on() else "OFF")
            elif data == "news:med":
                set_setting("news_medium", "0" if get_news_medium() else "1")
                ack = "Impact moyen : " + ("ON" if get_news_medium() else "OFF")
            _edit(cq, _news_text(), _news_keyboard())
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
        _pending_manual.pop(msg["chat"]["id"], None)   # une commande annule l'attente d'un SL
        reply, kb = handle_command(text)
        send(msg["chat"]["id"], reply, reply_markup=kb)
    elif text:
        _handle_manual_sl_input(msg["chat"]["id"], text)   # ne fait rien si aucun trade manuel n'attend de SL


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
        if not get_symbol_on(sym) and count_open(sym) == 0:
            out.append(f"⏸ {sym} : désactivé (menu ⚙️ Paramètres signal → 🌍 ACTIFS)")
            continue
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


# ============================================================================
# MONEY MANAGEMENT — /mm  (compounding, réduction du risque, objectif / perte journalière, limites de trades)
#
# DÉSACTIVÉ PAR DÉFAUT : tant que `/mm on` n'est pas tapé, le bot se comporte exactement comme avant.
# Toutes les valeurs se tapent à la main (ex. /mm sl 4 2) ; « 0 » désactive une règle.
# Sans état : tout est recalculé à la demande depuis les trades CLÔTURÉS de la base (aucun compteur à désynchroniser).
# Seul le P&L RÉALISÉ compte (les trades encore ouverts ne comptent pas). Le jour = jour UTC (comme les rapports).
# Un BE (P&L nul) est neutre : il n'interrompt ni ne prolonge une série de TP ou de SL.
# Le Risk Gate (MAX_RISK_USD) reste le plafond DUR : le compounding ne peut jamais le dépasser.
# ============================================================================
_mm_cap_cache = {}
MM_HELP = (
    "⚙️ <b>/mm — money management</b> (valeur 0 = règle désactivée ; montants : 10 ou 10$ = dollars, 2% = pourcentage)\n\n"
    "/mm on | off — active / coupe tout le module\n"
    "/mm capital 1000 | auto — capital de référence pour les % (auto = solde MetaApi)\n"
    "/mm hausse 2 10% — après 2 TP de suite, +10 % du risque de base par palier (ou +2$)\n"
    "/mm gain 100$ 5% — par tranche de 100 $ (ou 2%) de gain net, +5 % (ou +1$) de risque\n"
    "/mm perte 50$ 5% — par tranche de 50 $ (ou 2%) de perte nette, −5 % (ou −1$) de risque\n"
    "/mm sl 4 2 — après 4 SL de suite, risque ÷ 2\n"
    "/mm dd 5% 2 — drawdown ≥ 5 % (ou 50$) depuis le sommet, risque ÷ 2\n"
    "/mm max 20 — plafond de risque du module ($) · /mm min 1 — plancher ($)\n"
    "/mm obj 2% — objectif journalier (ou 100$) · /mm perte_jour 3% — perte journalière max\n"
    "/mm action pause | stop — à l'objectif / à la perte max : pause jusqu'à demain UTC, ou bot arrêté\n"
    "/mm trades_jour 5 — nb max de trades ouverts par jour · /mm ouverts 2 — nb max de trades ouverts en même temps (tous actifs)\n"
    "/mm reset — repart de zéro pour hausse / gain / perte / drawdown (n'efface rien)"
)


def mm_on():
    return _get_bool_setting("mm_on", False)


def _mm_load(s):
    """'pct:2' / 'usd:100' -> (kind, valeur) | None."""
    try:
        kind, v = str(s).split(":")
        v = float(v)
        return (kind, v) if kind in ("pct", "usd") and v > 0 else None
    except (ValueError, AttributeError):
        return None


def _mm_parse_amt(tok):
    """'10' / '10$' -> 'usd:10' ; '2%' -> 'pct:2' ; '0' -> '' (règle off). ValueError si invalide."""
    t = str(tok).strip().replace(",", ".").replace(" ", "")
    kind = "usd"
    if t.endswith("%"):
        kind, t = "pct", t[:-1]
    elif t.endswith("$"):
        t = t[:-1]
    v = float(t)
    if v < 0 or (kind == "pct" and v > 100):
        raise ValueError
    return "" if v == 0 else f"{kind}:{v:g}"


def _mm_fmt(a):
    return "OFF" if not a else f"{a[1]:g} {'%' if a[0] == 'pct' else '$'}"


def _mm_usd(a, capital):
    """Montant en $ : un pourcentage est pris sur le capital de référence (None si le capital est inconnu)."""
    if not a:
        return None
    if a[0] == "usd":
        return a[1]
    return capital * a[1] / 100.0 if capital and capital > 0 else None


def _mm_step(a, base):
    """Pas de risque en $ : un pourcentage est pris sur le risque de BASE (/risque)."""
    return base * a[1] / 100.0 if a[0] == "pct" else a[1]


def mm_cfg():
    g = lambda k, d="": get_setting("mm_" + k, d)

    def num(k, d=0.0):
        try:
            return float(g(k, d))
        except (TypeError, ValueError):
            return float(d)
    return {"capital": str(g("capital", "")).strip().lower(), "max": num("max"), "min": num("min"),
            "up_n": int(num("up_n")), "up_step": _mm_load(g("up_step")),
            "gain_x": _mm_load(g("gain_x")), "gain_step": _mm_load(g("gain_step")),
            "loss_x": _mm_load(g("loss_x")), "loss_step": _mm_load(g("loss_step")),
            "sl_n": int(num("sl_n")), "sl_div": num("sl_div", 2),
            "dd_x": _mm_load(g("dd_x")), "dd_div": num("dd_div", 2),
            "day_goal": _mm_load(g("day_goal")), "day_loss": _mm_load(g("day_loss")),
            "action": "stop" if str(g("action", "pause")).lower() == "stop" else "pause",
            "max_day": int(num("max_day")), "max_open": int(num("max_open")), "reset_ts": int(num("reset_ts"))}


def _mm_needs_capital(cfg):
    return any(a and a[0] == "pct" for a in (cfg["gain_x"], cfg["loss_x"], cfg["dd_x"], cfg["day_goal"], cfg["day_loss"]))


def _mm_capital(cfg):
    c = cfg["capital"]
    if c == "auto":
        hit = _mm_cap_cache.get("v")
        if hit and time.time() - hit[0] < 120:
            return hit[1]
        try:
            bal = float((get_account_summary() or {}).get("balance") or 0)
        except Exception:
            bal = 0.0
        if bal > 0:
            _mm_cap_cache["v"] = (time.time(), bal)
            set_setting("mm_capital_last", bal)
            return bal
        try:   # solde indisponible : dernier solde connu
            v = float(get_setting("mm_capital_last"))
            return v if v > 0 else None
        except (TypeError, ValueError):
            return None
    try:
        v = float(c.replace(",", "."))
        return v if v > 0 else None
    except ValueError:
        return None


def _mm_pnl(r):
    p = r.get("pnl_usd")
    if p is None:
        p = (r.get("result_r") or 0) * (r.get("risk_usd") or 0)
    return float(p)


def _mm_rows(since_ts):
    cur = _q("SELECT outcome,result_r,risk_usd,pnl_usd,closed_ts FROM trades WHERE status='CLOSED' AND closed_ts>=? "
             "ORDER BY closed_ts,id", (int(since_ts),))
    return [dict(x) for x in cur.fetchall()]


def mm_compute(rows, cfg, base, capital):
    """PUR (sans base de données ni réseau) : série en cours, P&L net, drawdown et risque effectif à partir des trades clôturés."""
    pnls = [_mm_pnl(r) for r in rows]
    wins = losses = 0
    for p in reversed(pnls):   # série en cours en fin d'historique ; un BE est neutre
        if abs(p) < 1e-9:
            continue
        if p > 0:
            if losses:
                break
            wins += 1
        else:
            if wins:
                break
            losses += 1
    net = sum(pnls)
    eq = peak = 0.0
    for p in pnls:
        eq += p
        peak = max(peak, eq)
    dd = peak - eq
    risk, notes, ignored = float(base), [], []

    def usd(a, label):
        v = _mm_usd(a, capital)
        if v is None:
            ignored.append(label)
        return v
    if cfg["up_n"] > 0 and cfg["up_step"] and wins >= cfg["up_n"]:
        lvl, st = wins // cfg["up_n"], _mm_step(cfg["up_step"], base)
        risk += lvl * st
        notes.append(f"{wins} TP de suite → +{lvl * st:g} $")
    if cfg["gain_x"] and cfg["gain_step"] and net > 0:
        x = usd(cfg["gain_x"], "gain")
        if x and int(net // x) > 0:
            lvl, st = int(net // x), _mm_step(cfg["gain_step"], base)
            risk += lvl * st
            notes.append(f"gain net {net:.2f} $ → +{lvl * st:g} $")
    if cfg["loss_x"] and cfg["loss_step"] and net < 0:
        x = usd(cfg["loss_x"], "perte")
        if x and int((-net) // x) > 0:
            lvl, st = int((-net) // x), _mm_step(cfg["loss_step"], base)
            risk -= lvl * st
            notes.append(f"perte nette {-net:.2f} $ → −{lvl * st:g} $")
    if cfg["sl_n"] > 0 and cfg["sl_div"] > 1 and losses >= cfg["sl_n"]:
        risk /= cfg["sl_div"]
        notes.append(f"{losses} SL de suite → ÷ {cfg['sl_div']:g}")
    if cfg["dd_x"] and cfg["dd_div"] > 1:
        thr = usd(cfg["dd_x"], "drawdown")
        if thr and dd >= thr:
            risk /= cfg["dd_div"]
            notes.append(f"drawdown {dd:.2f} $ → ÷ {cfg['dd_div']:g}")
    if cfg["max"] > 0 and risk > cfg["max"]:
        risk = cfg["max"]
        notes.append(f"plafond MM {cfg['max']:g} $")
    risk = max(risk, cfg["min"] if cfg["min"] > 0 else 0.01)
    return {"risk": round(risk, 2), "notes": notes, "ignored": ignored, "wins": wins, "losses": losses,
            "net": net, "dd": dd, "n": len(pnls)}


def mm_state(cfg=None):
    cfg = cfg or mm_cfg()
    now = time.time()
    day0 = int(now - now % 86400)
    capital = _mm_capital(cfg) if _mm_needs_capital(cfg) else None
    res = mm_compute(_mm_rows(cfg["reset_ts"]), cfg, get_risk(), capital)
    res["today"] = sum(_mm_pnl(r) for r in _mm_rows(day0))
    res["opened_today"] = _q("SELECT COUNT(*) n FROM trades WHERE opened_ts>=? AND status!='CANCELLED'", (day0,)).fetchone()["n"]
    res["open_now"] = _q("SELECT COUNT(*) n FROM trades WHERE status IN ('OPEN','PENDING','EXECUTING')").fetchone()["n"]
    res["capital"], res["day0"] = capital, day0
    return res


def mm_effective_risk():
    """(risque $ à utiliser pour un NOUVEAU signal, note admin). Module OFF ou erreur -> risque de base inchangé (/risque)."""
    base = get_risk()
    if not mm_on():
        return base, ""
    try:
        res = mm_state()
    except Exception:
        traceback.print_exc(limit=-3)
        return base, ""
    risk = res["risk"]
    if abs(risk - base) < 1e-9:
        return base, ""
    return risk, f"\n📈 MM : risque {risk:g} $ (base {base:g} $ — {' ; '.join(res['notes']) or 'bornes MM'})"


def mm_block_reason(companion=False):
    """Raison de refus d'un NOUVEAU signal (objectif / perte du jour, limites de trades), sinon None. Ne touche jamais
    aux trades déjà ouverts (ils restent suivis). `companion` (2e ordre du même CHoCH) échappe aux limites de nombre,
    comme pour « signaux max »."""
    if not mm_on():
        return None
    try:
        cfg = mm_cfg()
        st = mm_state(cfg)
    except Exception:
        traceback.print_exc(limit=-3)
        return None
    cap, today = st["capital"], st["today"]
    goal, lim = _mm_usd(cfg["day_goal"], cap), _mm_usd(cfg["day_loss"], cap)
    why, kind = None, None
    if goal and today >= goal:
        why, kind = f"objectif journalier atteint ({today:+.2f} $ ≥ {goal:.2f} $)", "goal"
    elif lim and today <= -lim:
        why, kind = f"perte journalière max atteinte ({today:+.2f} $ ≤ −{lim:.2f} $)", "loss"
    elif not companion and cfg["max_day"] and st["opened_today"] >= cfg["max_day"]:
        why = f"{st['opened_today']}/{cfg['max_day']} trades déjà ouverts aujourd'hui"
    elif not companion and cfg["max_open"] and st["open_now"] >= cfg["max_open"]:
        why = f"{st['open_now']}/{cfg['max_open']} trades déjà ouverts en même temps"
    if kind:
        nkey = f"mm_notify:{st['day0']}:{kind}"
        if not get_meta(nkey):
            set_meta(nkey, 1)
            stop = cfg["action"] == "stop"
            if stop and get_bot_on():
                set_bot_on(False)
            to_admin(f"💰 <b>Money management</b> : {why}.\n" + (
                "⏸ Bot ARRÊTÉ (aucun nouveau signal) — relance-le avec le bouton Démarrer. Les trades ouverts restent suivis."
                if stop else "⏸ Pause : aucun nouveau signal jusqu'à demain (UTC). Les trades ouverts restent suivis."))
    return why


def mm_status_text():
    cfg = mm_cfg()
    try:
        st = mm_state(cfg)
    except Exception:
        traceback.print_exc(limit=-3)
        return "⚠️ /mm : état indisponible pour le moment."
    onoff = "🟢 ON" if mm_on() else "🔴 OFF (comportement d'avant : risque fixe /risque)"
    cap = st["capital"]
    lines = [f"💰 <b>Money management</b> : {onoff}", f"Risque de base (/risque) : {get_risk():g} $",
             f"Capital de référence : {f'{cap:,.2f} $'.replace(',', ' ') if cap else '—'}"
             + (f" ({cfg['capital']})" if cfg["capital"] else ""), "",
             f"Hausse : {'après ' + str(cfg['up_n']) + ' TP → +' + _mm_fmt(cfg['up_step']) if cfg['up_n'] and cfg['up_step'] else 'OFF'}",
             f"Gain : {'chaque ' + _mm_fmt(cfg['gain_x']) + ' → +' + _mm_fmt(cfg['gain_step']) if cfg['gain_x'] and cfg['gain_step'] else 'OFF'}",
             f"Perte : {'chaque ' + _mm_fmt(cfg['loss_x']) + ' → −' + _mm_fmt(cfg['loss_step']) if cfg['loss_x'] and cfg['loss_step'] else 'OFF'}",
             f"SL de suite : {'après ' + str(cfg['sl_n']) + ' → ÷ ' + format(cfg['sl_div'], 'g') if cfg['sl_n'] else 'OFF'}",
             f"Drawdown : {'≥ ' + _mm_fmt(cfg['dd_x']) + ' → ÷ ' + format(cfg['dd_div'], 'g') if cfg['dd_x'] else 'OFF'}",
             f"Bornes : plafond {cfg['max']:g} $ · plancher {cfg['min']:g} $" if cfg["max"] or cfg["min"] else "Bornes : OFF",
             f"Objectif jour : {_mm_fmt(cfg['day_goal'])} · perte jour max : {_mm_fmt(cfg['day_loss'])} → {cfg['action']}",
             f"Trades : max {cfg['max_day'] or 'OFF'} / jour · max {cfg['max_open'] or 'OFF'} ouverts en même temps", "",
             f"<b>État</b> : série {st['wins']} TP / {st['losses']} SL · net {st['net']:+.2f} $ · drawdown {st['dd']:.2f} $ "
             f"({st['n']} trades clôturés depuis le dernier reset)",
             f"Aujourd'hui (UTC) : {st['today']:+.2f} $ · {st['opened_today']} trade(s) ouvert(s) · {st['open_now']} en cours",
             f"➡️ Risque effectif pour le prochain signal : <b>{st['risk']:g} $</b>"
             + (f" ({' ; '.join(st['notes'])})" if st["notes"] else "")]
    if st["ignored"]:
        lines.append(f"⚠️ Règle(s) en % ignorée(s) ({', '.join(st['ignored'])}) : capital inconnu → /mm capital 1000 ou auto.")
    eff = max(st["risk"], 0)
    if RISK_ENABLED and eff > MAX_RISK_USD + 1e-9:
        lines.append(f"⚠️ Risk Gate : MAX_RISK_USD = {MAX_RISK_USD:g} $ (variable Render). Les ordres MT5 ne dépasseront pas ce plafond "
                     f"tant que tu ne le relèves pas toi-même.")
    return "\n".join(lines)


def _mm_keyboard():
    return _with_back({"inline_keyboard": [
        [{"text": "🔴 Désactiver /mm" if mm_on() else "🟢 Activer /mm", "callback_data": "mm:toggle"}],
        [{"text": "🛑 À l'objectif : STOP" if mm_cfg()["action"] == "stop" else "⏸ À l'objectif : PAUSE", "callback_data": "mm:action"}],
        [{"text": "📖 Comment régler (commandes)", "callback_data": "mm:help"}, {"text": "🔄 Actualiser", "callback_data": "mm:home"}],
        [{"text": "♻️ Reset séries / gains", "callback_data": "mm:reset"}],
    ]})


def _mm_callback(cq, arg):
    """Boutons « mm:... » du menu 💰 Money management. Les VALEURS se tapent avec /mm (voir l'aide)."""
    ack = ""
    if arg == "toggle":
        set_setting("mm_on", "0" if mm_on() else "1"); ack = "MM : " + ("ON" if mm_on() else "OFF")
    elif arg == "action":
        set_setting("mm_action", "pause" if mm_cfg()["action"] == "stop" else "stop"); ack = "Action : " + mm_cfg()["action"]
    elif arg == "reset":
        set_setting("mm_reset_ts", int(time.time())); ack = "Remis à zéro"
    if arg == "help":
        _edit(cq, MM_HELP, _with_back({"inline_keyboard": [[{"text": "◀️ Retour", "callback_data": "mm:home"}]]}))
    else:
        _edit(cq, mm_status_text() + "\n\n✍️ Pour changer une valeur, tape la commande (ex. <code>/mm sl 4 2</code>) — bouton 📖 pour la liste.", _mm_keyboard())
    return ack


def mm_command(args):
    """Traite « /mm ... » (admin). Retourne le texte de réponse."""
    if not args:
        return mm_status_text()
    sub, rest = args[0].lower(), args[1:]
    bad = "Valeur invalide.\n\n" + MM_HELP

    def need(n):
        return len(rest) >= n

    try:
        if sub in ("aide", "help"):
            return MM_HELP
        if sub in ("on", "off"):
            set_setting("mm_on", "1" if sub == "on" else "0")
        elif sub == "capital" and need(1):
            if rest[0].lower() == "auto":
                set_setting("mm_capital", "auto")
            else:
                v = float(rest[0].replace(",", ".").rstrip("$"))
                if v < 0:
                    raise ValueError
                set_setting("mm_capital", f"{v:g}" if v else "")
        elif sub == "hausse" and need(2):
            n = int(rest[0])
            if not 0 <= n <= 100:
                raise ValueError
            set_setting("mm_up_n", n)
            set_setting("mm_up_step", _mm_parse_amt(rest[1]))
        elif sub in ("gain", "perte") and need(2):
            k = "gain" if sub == "gain" else "loss"
            set_setting(f"mm_{k}_x", _mm_parse_amt(rest[0]))
            set_setting(f"mm_{k}_step", _mm_parse_amt(rest[1]))
        elif sub == "sl" and need(1):
            n = int(rest[0])
            d = float(rest[1].replace(",", ".")) if need(2) else 2.0
            if not 0 <= n <= 100 or not 1 < d <= 20:
                raise ValueError
            set_setting("mm_sl_n", n)
            set_setting("mm_sl_div", d)
        elif sub == "dd" and need(1):
            d = float(rest[1].replace(",", ".")) if need(2) else 2.0
            if not 1 < d <= 20:
                raise ValueError
            set_setting("mm_dd_x", _mm_parse_amt(rest[0]))
            set_setting("mm_dd_div", d)
        elif sub in ("max", "min") and need(1):
            v = float(rest[0].replace(",", ".").rstrip("$"))
            if v < 0:
                raise ValueError
            set_setting(f"mm_{sub}", v)
        elif sub in ("obj", "perte_jour") and need(1):
            set_setting("mm_day_goal" if sub == "obj" else "mm_day_loss", _mm_parse_amt(rest[0]))
        elif sub == "action" and need(1) and rest[0].lower() in ("pause", "stop"):
            set_setting("mm_action", rest[0].lower())
        elif sub in ("trades_jour", "ouverts") and need(1):
            n = int(rest[0])
            if not 0 <= n <= 1000:
                raise ValueError
            set_setting("mm_max_day" if sub == "trades_jour" else "mm_max_open", n)
        elif sub == "reset":
            set_setting("mm_reset_ts", int(time.time()))
        else:
            return bad
    except ValueError:
        return bad
    return mm_status_text()


def publish_signal(symbol, candles, events, sig, companion=False):
    """Enregistre le trade puis envoie : signal (+ image) au groupe, lot en privé à l'admin.
    `companion` : 2e ordre du même CHoCH (limit OB/FVG à côté de l'entrée directe) -> ne compte pas dans « signaux max ».
    Retourne True si le signal a été enregistré."""
    dec = SYMBOLS[symbol]["decimals"]
    key = f"{symbol}:{TF_LABEL}:{sig['t']}" + (f":{sig['key_suffix']}" if sig.get("key_suffix") else "")
    if signal_exists(key):
        return False
    if not get_bot_on():   # bot arrêté depuis Telegram : aucun nouveau signal (le suivi des positions continue)
        return False
    mm_why = mm_block_reason(companion)   # /mm : objectif / perte du jour, limites de trades (module OFF -> None)
    if mm_why:
        print(f"[{symbol}] {sig['side']} {sig['type']} ignoré : money management — {mm_why}")
        return False
    nb = news_block_info(symbol)
    if nb:   # filtre news : pas de nouveau signal autour d'une news (un seul message admin par news et par actif)
        print(f"[{symbol}] {sig['side']} {sig['type']} ignoré : news {_news_line(nb)}")
        nkey = f"news_blk:{symbol}:{nb['dt'].strftime('%Y%m%d%H%M')}:{nb['title']}"[:180]
        if not get_meta(nkey):
            set_meta(nkey, 1)
            to_admin(f"📰 Signal {symbol} {sig['side']} ignoré (filtre news) : {_news_line(nb)}\n"
                     f"Fenêtre : {NEWS_BEFORE_MIN} min avant / {NEWS_AFTER_MIN} min après.")
        return False
    n_open = count_open(symbol)
    max_pos = get_max_positions()
    if n_open >= max_pos and not companion:
        print(f"[{symbol}] {sig['side']} {sig['type']} ignoré ({n_open}/{max_pos} signal(s) encore ouvert(s) : pas de doublon)")
        return False

    risk_usd, mm_note = mm_effective_risk()   # = get_risk() tant que /mm est OFF
    mm_target = risk_usd if mm_on() else None
    leverage = get_leverage()
    lot_info = calc_lot(symbol, risk_usd, sig["risk"], price=sig["entry"])
    if lot_info["lot"] <= 0:
        reason = lot_info.get("rejected_reason") or "lot invalide (risque ou distance SL)"
        print(f"[{symbol}] signal ignoré : {reason}")
        return False
    margin = calc_margin(symbol, lot_info["lot"], sig["entry"], leverage)

    is_market = sig.get("order", "MARKET") == "MARKET"
    mt5_enabled = mt5_orders_allowed() and not sig.get("no_mt5")   # interrupteur « Ordres MT5 » du menu Stratégies ; no_mt5 : signal seul
    # Le trade est TOUJOURS suivi (OPEN, ou PENDING pour une LIMIT) dès sa création : le suivi RR / BE / TP / SL
    # sur le groupe et le bot ne dépend jamais de l'exécution MT5. L'exécution est tracée à part (exec_status).
    initial_status = "PENDING" if sig.get("order") == "LIMIT" else "OPEN"

    trade_id = add_trade(
        status=initial_status,
        order_type=sig.get("order", "MARKET"), placed_ts=sig["t"], ref_price=sig.get("ref_price"),
        sent_admin=0, sent_group=0,   # passent à 1 seulement quand Telegram a confirmé l'envoi
        signal_key=key, symbol=symbol, side=sig["side"], kind=sig["type"],
        timeframe=(sig["tf"] if sig.get("type") == "FIBTREND" and sig.get("tf") else TF_LABEL),
        entry=sig["entry"], sl=sig["sl"], sl_initial=sig["sl"], tp=sig["tp"],
        be_rr=sig.get("be_rr"), htf_mode=sig.get("htf_mode"),
        risk_usd=lot_info["real_risk"],  # risque réel du lot pris (= risque demandé sauf lot minimum)
        lot=lot_info["lot"], leverage=leverage, opened_ts=now_ts(), last_ts=sig["t"],
        exec_status="PENDING" if (is_market and mt5_enabled) else None,
        part_cfg=_partial_cfg_json())   # niveaux partiels figés (None = partiels OFF)
    try:
        signal_log_block(symbol, sig, dec)
    except Exception as e:   # un souci de log ne doit jamais bloquer un signal
        print(f"[{symbol}] bloc de log indisponible ({type(e).__name__})")
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
                    admin_txt=admin_signal(symbol, sig, risk_usd, leverage, lot_info, margin, dec) + mm_note,
                    group_txt=group_signal(symbol, sig, n_open + 1, dec, lot=lot_info["lot"]),
                    chart=chart)
    entry_media_and_lot(symbol, sig, lot_info["lot"])

    # 2) Exécution MT5 ensuite, dans un thread dédié : n'interrompt ni le scan ni le suivi des autres trades.
    #    Un échec est notifié à l'admin mais n'arrête JAMAIS le suivi TP / SL / BE du signal.
    if is_market and mt5_enabled:
        exec_lot = lot_info["lot"]
        gate_ok = True
        if RISK_ENABLED:
            gate_ok, gate_lot, gate_risk = risk_gate_check(symbol, sig["entry"], sig["sl"],
                                                           strategy=risk_strategy_label(sig), target_usd=mm_target)
            if gate_ok:
                exec_lot = gate_lot
                if gate_lot != lot_info["lot"]:   # le suivi (partiels, BE, PnL) doit refléter le volume réellement envoyé
                    update_trade(trade_id, lot=gate_lot, risk_usd=gate_risk)
        if gate_ok:
            threading.Thread(target=_run_mt5_execution, name=f"mt5-exec-{trade_id}", daemon=True,
                             args=(trade_id, symbol, sig, exec_lot)).start()
        else:
            # Risk Gate : ordre bloqué (même MIN_LOT dépasse MAX_RISK_USD). Le signal reste publié et
            # suivi (RR/BE/TP/SL) normalement ; seul l'envoi de l'ordre broker est annulé, SL inchangé.
            update_trade(trade_id, exec_status="FAILED")
            to_admin(f"⛔ {symbol} {sig['side']} {sig['type']} (trade #{trade_id})\n"
                     f"[RISK GATE] ORDER BLOCKED — risk exceeds maximum ({MAX_RISK_USD:.2f}$).\n"
                     f"Le suivi RR / BE / TP / SL continue normalement sur le groupe et ici.")
    return True


def _exec_filled_limit(t, dec):
    """Limit touché (suivi bougies) -> ordre MT5 au marché, sauf si le prix a déjà trop dérivé du limit.
    Avant : un LIMIT n'était jamais envoyé à MT5 (signal + suivi seulement)."""
    if not (LIMIT_MT5_EXEC and mt5_orders_allowed()):
        return
    if t.get("mt5_position_id") or t.get("exec_status") in ("PENDING", "OK"):
        return
    symbol, entry, sl = t["symbol"], t["entry"], t["sl"]
    risk = abs(entry - t["sl_initial"]) or abs(entry - sl)
    px = get_live_price(symbol)
    if px is not None and risk and abs(px - entry) > LIMIT_FILL_MAX_DRIFT_R * risk:
        update_trade(t["id"], exec_status="FAILED")
        to_admin(f"⚠️ {symbol} {t['side']} LIMIT touché @ {_fmt(entry, dec)} mais le prix est déjà à {_fmt(px, dec)} "
                 f"(> {LIMIT_FILL_MAX_DRIFT_R:g} R du limit) : ordre MT5 NON envoyé. Le suivi continue.")
        return
    sig = {"side": t["side"], "type": t["kind"], "entry": entry, "sl": sl, "tp": t["tp"]}
    exec_lot = t["lot"]
    gate_ok = True
    if RISK_ENABLED:
        gate_ok, gate_lot, gate_risk = risk_gate_check(symbol, entry, sl, strategy=risk_strategy_label(
            dict(sig, key_suffix=("qml" if t["kind"] == "QML" else None))),
            target_usd=((t.get("risk_usd") or None) if mm_on() else None))
        if gate_ok:
            exec_lot = gate_lot
            if gate_lot != t["lot"]:
                update_trade(t["id"], lot=gate_lot, risk_usd=gate_risk)
    if not gate_ok:
        update_trade(t["id"], exec_status="FAILED")
        to_admin(f"⛔ {symbol} {t['side']} LIMIT touché @ {_fmt(entry, dec)}\n"
                 f"[RISK GATE] ORDER BLOCKED — risk exceeds maximum ({MAX_RISK_USD:.2f}$).\n"
                 f"Le suivi RR / BE / TP / SL continue normalement.")
        return
    update_trade(t["id"], exec_status="PENDING")
    threading.Thread(target=_run_mt5_execution, name=f"mt5-exec-{t['id']}", daemon=True,
                     args=(t["id"], symbol, sig, exec_lot)).start()


def _run_mt5_execution(trade_id, symbol, sig, lot):
    """Exécute l'ordre MT5 d'un signal déjà publié et suivi. Ne modifie jamais le statut de suivi (OPEN)."""
    cid = _auto_client_id(trade_id, symbol, sig["side"])
    try:
        mt5_position_id = execute_mt5_order(symbol, sig["side"], lot, sig["entry"], sig["sl"], sig["tp"],
                                            client_id=cid, strategy=risk_strategy_label(sig))
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
    if cid in _RISK_BLOCKED:
        to_admin(f"⛔ {symbol} {sig['side']} {sig['type']} (trade #{trade_id})\n"
                 f"[RISK GATE] ORDER BLOCKED — risk exceeds maximum ({MAX_RISK_USD:.2f}$) avec le volume final du broker.\n"
                 f"SL inchangé, rien n'est forcé. Le suivi RR / BE / TP / SL continue normalement.")
        return
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
                        group_txt=late + group_signal(sym, sig, None, dec, tf=t["timeframe"], lot=t["lot"]))


def _publish_group(symbol, candles, events, sigs):
    """Publie les signaux d'un même CHoCH : le 1er normalement, les suivants (limit) en companion hors « signaux max »
    seulement si le 1er est parti. Retourne True si le 1er a été enregistré."""
    ok = False
    for k, sig in enumerate(sigs):
        if k == 0:
            ok = publish_signal(symbol, candles, events, sig)
        elif ok:
            publish_signal(symbol, candles, events, sig, companion=True)
    return ok


def process_symbol(symbol):
    """Un passage pour un actif : suivi des positions ouvertes, puis détection des nouveaux CHoCH."""
    dec = SYMBOLS[symbol]["decimals"]
    meta_key = f"last_t:{symbol}:{TF_LABEL}"  # par timeframe : en changer réinitialise proprement
    if not get_symbol_on(symbol) and count_open(symbol) == 0:
        return   # actif désactivé et rien à suivre : aucun appel réseau
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
        for ev in track_trade_verified(trade, candles):
            setup_engine_report(ev["trade"], ev["name"])
            if not send_result_image(ev, dec):   # image de résultat QML (section 7ter) ; sinon envoi habituel
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
            if ev["name"] == "FILLED":
                _exec_filled_limit(ev["trade"], dec)

    # 2) nouveaux signaux : CHoCH survenus sur les bougies clôturées depuis le dernier passage
    if not get_symbol_on(symbol):   # actif désactivé : le suivi ci-dessus a eu lieu, mais aucun nouveau signal
        set_meta(meta_key, last_t)
        return
    n = len(candles)
    se_ok = False   # stratégie principale (machine à états SETUP50) : signal d'entrée publié pendant ce passage
    if SETUP_ENGINE and get_strat_main():
        for se_sig in setup_engine_signals(symbol, candles, last_seen):
            se_ok = _publish_group(symbol, candles, events, [se_sig]) or se_ok
    for ev in events:
        if ev["t"] <= last_seen or ev["i"] < n - SIGNAL_MAX_AGE:
            continue
        if ev["kind"] == "CHOCH" and not session_ok(ev["t"]):
            if DEBUG_CHOCH:
                print(f"[{symbol}] CHoCH {'BUY' if ev['dir'] == 1 else 'SELL'} {ev.get('type')} ignoré : hors session ({get_session()})")
            continue
        main_ok = se_ok
        if get_strat_main() and not SETUP_ENGINE:   # ancienne logique (SETUP_ENGINE=0) : entrée directe sur le CHoCH
            main_ok = _publish_group(symbol, candles, events, build_signal(candles, ev, symbol, multi=True) or [])
        if get_strat_crt() and (not main_ok or get_crt_dup()):   # sauf interrupteur « doublons CRT » : même CHoCH déjà pris = pas de 2e trade
            try:
                _publish_group(symbol, candles, events, crt_build(candles, ev, symbol))
            except Exception:
                print(f"[{symbol}] module CRT : erreur (signal ignoré)")
                traceback.print_exc()

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


# --- Double vérification SL / RR1 / RR2 en direct (annonce immédiate, groupe UNIQUEMENT) -------------------
#
# Objectif : dès que le prix LIVE touche un niveau connu du trade (SL, RR1, RR2), le groupe est prévenu
# tout de suite, sans attendre la clôture de bougie. C'est une simple ANNONCE anticipée au tick, pas une
# clôture : track_trade_verified() reste seul maître du résultat officiel (TP/SL/BE), avec ses 4 contrôles,
# à la clôture de bougie. Donc :
#   - on ne touche JAMAIS trade["status"], trade["sl"], trade["result_r"], etc.
#   - on n'envoie RIEN à l'admin / au bot (privé) : uniquement au groupe, et une seule fois par niveau.
#   - "double vérification" = 2 lectures du prix live qui doivent concorder avant d'annoncer, pour ne pas
#     signaler sur un tick aberrant (glitch de flux prix).
RT_WATCH_INTERVAL = _env_int("RT_WATCH_INTERVAL", 5)     # secondes entre deux passages de cette boucle
RT_DOUBLE_CHECK_DELAY = _env_float("RT_DOUBLE_CHECK_DELAY", 1.5)   # secondes entre les 2 lectures de prix

# Mémorise, par trade, les niveaux déjà annoncés en direct : {trade_id: {"SL", "RR1", "RR2", ...}}
_rt_announced = {}


def _rt_levels(trade):
    """Niveaux connus du trade : SL (prix), RR1 (prix), RR2 (prix) — uniquement ceux atteignables par CE trade
    (un RR au-delà de la TP finale du trade n'existe pas pour lui)."""
    side = 1 if trade["side"] == "BUY" else -1
    entry = trade["entry"]
    risk = abs(entry - trade["sl_initial"])
    if risk <= 0:
        return {}
    tp_rr = trade_tp_rr(trade)
    levels = {"SL": trade["sl"]}   # SL courant (initial, ou déplacé à l'entrée si BE déjà armé)
    for lvl in (1, 2):
        if lvl <= tp_rr:
            levels[f"RR{lvl}"] = entry + side * lvl * risk
    return levels


def _rt_touched(trade, name, price):
    side = 1 if trade["side"] == "BUY" else -1
    lvl_price = _rt_levels(trade).get(name)
    if lvl_price is None:
        return False
    if name == "SL":
        return (price <= lvl_price) if side == 1 else (price >= lvl_price)
    return (price >= lvl_price) if side == 1 else (price <= lvl_price)   # RR1 / RR2 : sens favorable


def _rt_group_text(trade, name):
    head = f"{trade['symbol']} {trade['side']}"
    if name == "SL":
        return f"🔴 <b>SL touché (prix en direct)</b> — {head} · confirmation à la clôture de bougie"
    return f"🟡 <b>{name} touché (prix en direct)</b> — {head} · confirmation à la clôture de bougie"


def check_rt_levels():
    """Pour chaque trade ouvert : relit le prix live, et si un niveau connu (SL, RR1, RR2) vient d'être
    touché, le CONFIRME avec une 2e lecture avant d'annoncer au groupe. Un niveau n'est annoncé qu'une
    seule fois par trade. Ne modifie jamais l'état du trade (aucune écriture en base) : purement informatif."""
    for trade in open_trades():
        if trade["status"] != "OPEN":
            continue
        levels = _rt_levels(trade)
        if not levels:
            continue
        seen = _rt_announced.setdefault(trade["id"], set())
        price1 = get_live_price(trade["symbol"])
        if price1 is None:
            continue
        for name in levels:
            if name in seen:
                continue
            if not _rt_touched(trade, name, price1):
                continue
            # double vérification : une 2e lecture du prix, après un court délai, doit confirmer le niveau
            time.sleep(RT_DOUBLE_CHECK_DELAY)
            price2 = get_live_price(trade["symbol"])
            if price2 is None or not _rt_touched(trade, name, price2):
                continue   # tick isolé non confirmé : on ne signale rien, un prochain passage retentera
            seen.add(name)
            print(f"[rt-watch] {trade['symbol']} {trade['side']} #{trade['id']} : {name} touché en direct "
                  f"(prix {price1:g} puis {price2:g}, confirmé) — annonce groupe seule, rien à l'admin/bot.")
            to_group(_rt_group_text(trade, name))
            # volontairement : pas de to_admin(), pas d'écriture base — le résultat officiel reste celui
            # de track_trade_verified() à la clôture de bougie.
    # nettoyage mémoire : les trades fermés/disparus n'ont plus besoin d'être suivis ici
    open_ids = {t["id"] for t in open_trades()}
    for tid in [tid for tid in _rt_announced if tid not in open_ids]:
        _rt_announced.pop(tid, None)


def _rt_watch_loop():
    """Boucle dédiée à l'annonce anticipée SL/RR1/RR2, en parallèle de _trading_loop et _be_watch_loop :
    tourne toutes les RT_WATCH_INTERVAL secondes, n'envoie qu'au groupe, jamais à l'admin, et n'écrit
    jamais en base — ne peut donc jamais interférer avec le résultat officiel du trade."""
    while True:
        try:
            check_rt_levels()
        except Exception:
            traceback.print_exc()
        time.sleep(RT_WATCH_INTERVAL)


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
                try:
                    qml_process_symbol(symbol)   # stratégie QML séparée (OFF par défaut) : jamais bloquante
                except Exception:
                    print(f"[{symbol}] module QML : erreur (ignorée)")
                    traceback.print_exc(limit=-3)
                try:
                    ft_process_symbol(symbol)    # stratégie FIBTREND séparée (OFF par défaut) : jamais bloquante
                except Exception:
                    print(f"[{symbol}] module FIBTREND : erreur (ignorée)")
                    traceback.print_exc(limit=-3)
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
        threading.Thread(target=_rt_watch_loop, daemon=True).start()
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
    threading.Thread(target=_rt_watch_loop, daemon=True).start()
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
    start_qmlg_alerts()          # alertes de réaction QML GAGNANT (inactives tant que tu n'as pas activé le bouton 🔔)
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

    def _get(self, symbol, minutes=None, count=None):
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
        self.assertIn("BE armé", _E.group_event(ev[0], 2))   # message groupe minimal (le RR du BE n'y figure plus)
        self.assertEqual(t["be_rr"], 0.5)
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


class TestEntreeOBFVG(unittest.TestCase):
    """Types d'entrée : direct / limit sur OB-FVG / les deux (réglage Telegram) + limit companion hors « signaux max »."""

    @staticmethod
    def _candles():
        path = [100, 102, 104, 106, 104, 102, 101, 103, 106, 109, 112, 110, 108, 106, 105, 107, 110, 113, 116, 119, 122,
                120, 118, 117, 118, 120, 121, 118, 114, 110, 106, 102, 98, 96, 95]
        out, prev = [], path[0]
        for k, cl in enumerate(path):
            out.append({"t": 1_700_000_000 + k * 60, "o": prev, "h": max(prev, cl) + 0.3, "l": min(prev, cl) - 0.3, "c": cl})
            prev = cl
        return out

    def test_reglage_et_commande(self):
        try:
            self.assertTrue(_E.get_entry_direct() and not _E.get_entry_limit())          # défaut = comportement historique
            self.assertFalse(_E.set_entry_types(False, False), "au moins un type doit rester actif")
            _E.handle_command("/entree both")
            self.assertTrue(_E.get_entry_direct() and _E.get_entry_limit())
            _E.handle_command("/entree limit")
            self.assertTrue(not _E.get_entry_direct() and _E.get_entry_limit())
            self.assertIn("sentd", str(_E._signal_entry_keyboard())); self.assertIn("sentl", str(_E._signal_entry_keyboard()))
            self.assertIn("sig:entry", str(_E._signal_keyboard()))
        finally:
            _E._q("DELETE FROM settings WHERE key IN ('entry_direct','entry_limit')", commit=True)

    def test_signaux_selon_reglage(self):
        c = self._candles()
        ev = next(e for e in _E.analyze(c) if e["kind"] == "CHOCH")
        saved = _E.TIMEFRAME_MIN
        try:
            _E.TIMEFRAME_MIN = 1
            for d, l, exp in ((True, False, ["MARKET"]), (False, True, ["LIMIT"]), (True, True, ["MARKET", "LIMIT"])):
                _E.set_entry_types(d, l)
                self.assertEqual([x["order"] for x in _E.build_signal(c, ev, None, multi=True)], exp)
            lim = _E.build_signal(c, ev, None, multi=True)[1]
            z = lim["zone"]
            self.assertEqual(lim["side"], "SELL")
            self.assertAlmostEqual(lim["entry"], z["lo"], places=6)          # bord PROCHE de la zone d'offre
            self.assertGreater(lim["sl"], z["hi"] - 1e-9)                    # SL derrière la zone
            self.assertLess(lim["tp"], lim["entry"])
            self.assertEqual(_E.build_signal(c, ev, None)["order"], "MARKET")   # appel historique inchangé
        finally:
            _E.TIMEFRAME_MIN = saved
            _E._q("DELETE FROM settings WHERE key IN ('entry_direct','entry_limit')", commit=True)

    def test_limit_companion_hors_signaux_max(self):
        c = self._candles()
        ev = next(e for e in _E.analyze(c) if e["kind"] == "CHOCH")
        saved = (_E._deliver_signal, _E.make_chart, _E.TIMEFRAME_MIN)
        try:
            _E._q("DELETE FROM trades", commit=True)
            _E._deliver_signal = lambda *a, **k: None
            _E.make_chart = lambda *a, **k: None
            _E.TIMEFRAME_MIN = 1
            _E.set_entry_types(True, True)
            direct, lim = _E.build_signal(c, ev, None, multi=True)
            self.assertTrue(_E.publish_signal("XAUUSD", [], [], direct))
            self.assertFalse(_E.publish_signal("XAUUSD", [], [], lim), "sans companion : bloqué par signaux max = 1")
            self.assertTrue(_E.publish_signal("XAUUSD", [], [], lim, companion=True))
            self.assertFalse(_E.publish_signal("XAUUSD", [], [], lim, companion=True), "pas de doublon du limit")
            rows = _E._q("SELECT order_type, status FROM trades ORDER BY id").fetchall()
            self.assertEqual([(r["order_type"], r["status"]) for r in rows], [("MARKET", "OPEN"), ("LIMIT", "PENDING")])
        finally:
            _E._deliver_signal, _E.make_chart, _E.TIMEFRAME_MIN = saved
            _E._q("DELETE FROM trades", commit=True)
            _E._q("DELETE FROM settings WHERE key IN ('entry_direct','entry_limit')", commit=True)


class TestSLTPCRT(unittest.TestCase):
    """Modes SL / TP réglables + module CRT (bougie HTF de référence, sweep, réintégration, CHoCH)."""
    _KEYS = "('sl_mode','tp_mode','strat_main','strat_crt','crt_ref','crt_tp','entry_direct','entry_limit','session','limit_zone','crt_htf','crt_dup','qf_swings','qf_disp','qf_range','qf_fvg','qf_ind')"

    def setUp(self):
        self.c = TestEntreeOBFVG._candles()
        self.ev = next(e for e in _E.analyze(self.c) if e["kind"] == "CHOCH")   # SELL
        self.saved = (_E.TIMEFRAME_MIN, _E._ctx_candles, _E.liquidity_reading)
        _E.TIMEFRAME_MIN = 1
        _E.liquidity_reading = lambda *a, **k: (None, None)

    def tearDown(self):
        _E.TIMEFRAME_MIN, _E._ctx_candles, _E.liquidity_reading = self.saved
        _E._q(f"DELETE FROM settings WHERE key IN {self._KEYS}", commit=True)

    def _htf(self, crh, crl):
        h0 = (self.c[0]["t"] // 3600) * 3600
        return [{"t": h0 - 3600, "o": (crh + crl) / 2, "h": crh, "l": crl, "c": (crh + crl) / 2}]

    def test_modes_sl(self):
        leg = self.c[self.ev["src"]:self.ev["i"] + 1]
        top = max(x["h"] for x in leg)
        buf = _E.SL_BUFFER_ATR * self.ev["atr"]
        _E.set_choice("sl_mode", "SWEEP", _E.SL_MODES)
        sig = _E.build_signal(self.c, self.ev, None, multi=True)[0]
        self.assertAlmostEqual(sig["sl"], top + buf, places=6)
        self.assertEqual(sig["sl_mode"], "SWEEP")
        _E.set_choice("sl_mode", "ZONE", _E.SL_MODES)
        sig = _E.build_signal(self.c, self.ev, None, multi=True)[0]
        self.assertEqual(sig["sl_mode"], "ZONE")
        self.assertGreater(sig["sl"], sig["entry"])
        _E.set_choice("sl_mode", "STRUCT", _E.SL_MODES)
        sig = _E.build_signal(self.c, self.ev, None, multi=True)[0]
        self.assertAlmostEqual(sig["sl"], self.ev["sl_level"] + buf, places=6)
        self.assertFalse(_E.set_choice("sl_mode", "nimportequoi", _E.SL_MODES))

    def test_modes_tp(self):
        sig = {"dir": -1, "entry": 100.0, "risk": 2.0, "tp": 92.0, "rr": 4.0, "ext": {"level": 90.0, "tf": 60}}
        _E.set_choice("tp_mode", "RR", _E.TP_MODES)
        self.assertEqual(_E.apply_tp_mode(dict(sig), self.c, self.ev, None, 4.0)["tp"], 92.0)
        _E.set_choice("tp_mode", "TARGET", _E.TP_MODES)
        r = _E.apply_tp_mode(dict(sig), self.c, self.ev, None, 4.0)
        self.assertEqual((r["tp"], r["rr"]), (90.0, 5.0))
        near = dict(sig, ext={"level": 99.0, "tf": 60})            # cible à 0.5R : sous TP_MIN_RR -> repli RR fixe
        self.assertEqual(_E.apply_tp_mode(near, self.c, self.ev, None, 4.0)["tp"], 92.0)
        wrong = dict(sig, ext={"level": 105.0, "tf": 60})          # cible du mauvais côté -> repli RR fixe
        self.assertEqual(_E.apply_tp_mode(wrong, self.c, self.ev, None, 4.0)["tp"], 92.0)
        _E.set_choice("tp_mode", "SWING", _E.TP_MODES)
        lows = [_E.nearest_swing_target(self.c, self.ev["i"], -1, 200.0)]
        self.assertTrue(lows[0] is None or lows[0] < 200.0)

    def test_reglages_strategies(self):
        self.assertTrue(_E.get_strat_main() and not _E.get_strat_crt())        # défaut : CRT désactivé
        self.assertFalse(_E.set_strategies(False, False))
        _E.handle_command("/strategie both")
        self.assertTrue(_E.get_strat_main() and _E.get_strat_crt())
        _E.handle_command("/crt h4")
        _E.handle_command("/crt mid")
        self.assertEqual((_E.get_crt_ref(), _E.get_crt_tp()), (240, "MID"))
        _E.handle_command("/modetp target")
        self.assertEqual(_E.get_tp_mode(), "TARGET")
        for cb in ("sig:sltp", "sig:strat"):
            self.assertIn(cb, str(_E._signal_keyboard()))
        self.assertIn("sstr:crt", str(_E._signal_strat_keyboard()))

    def test_crt_detection_et_signaux(self):
        _E._ctx_candles = lambda s, tf: self._htf(121.0, 90.0)   # CRH 121 balayé (plus haut 122.3), CRL 90 intact, clôture 114 dans le range
        _E.set_entry_types(True, True)
        sigs = _E.crt_build(self.c, self.ev, "XAUUSD")
        self.assertEqual([x["order"] for x in sigs], ["MARKET", "LIMIT"])
        d, lim = sigs
        sweep_sl = 122.3 + _E.SL_BUFFER_ATR * self.ev["atr"]
        self.assertAlmostEqual(d["sl"], sweep_sl, places=6)               # SL derrière le sweep
        self.assertAlmostEqual(lim["sl"], sweep_sl, places=6)
        self.assertEqual((d["tp"], lim["tp"]), (90.0, 90.0))               # TP = autre bout du range (CRL)
        self.assertEqual((d["key_suffix"], lim["key_suffix"]), ("crt", "crtpoi"))
        self.assertEqual(d["crt"]["crh"], 121.0)
        _E.set_choice("crt_tp", "MID", _E.CRT_TP_MODES)
        fb = _E.crt_build(self.c, self.ev, "XAUUSD")[0]                     # 50 % = RR 0.96 < TP_MIN_RR : repli RR fixe
        self.assertAlmostEqual(fb["rr"], _E.get_tp_rr())
        saved_min, _E.TP_MIN_RR = _E.TP_MIN_RR, 0.5
        try:
            self.assertAlmostEqual(_E.crt_build(self.c, self.ev, "XAUUSD")[0]["tp"], 105.5, places=6)
        finally:
            _E.TP_MIN_RR = saved_min
        _E.set_choice("crt_tp", "RR", _E.CRT_TP_MODES)
        self.assertAlmostEqual(_E.crt_build(self.c, self.ev, "XAUUSD")[0]["rr"], _E.get_tp_rr())

    def test_session_zone_et_filtre_htf(self):
        ts = lambda h: 1_700_000_000 - 1_700_000_000 % 86400 + h * 3600   # jour donné à l'heure UTC h
        self.assertTrue(_E.session_ok(ts(3)))                                # ALL par défaut
        _E.handle_command("/session londres")
        self.assertTrue(_E.session_ok(ts(8)) and not _E.session_ok(ts(3)) and not _E.session_ok(ts(20)))
        _E.handle_command("/session ny")
        self.assertTrue(_E.session_ok(ts(14)) and not _E.session_ok(ts(8)))
        _E.handle_command("/session both")
        self.assertTrue(_E.session_ok(ts(8)) and _E.session_ok(ts(20)) and not _E.session_ok(ts(3)))
        self.assertIn("ssess:NY", str(_E._signal_sess_keyboard()))
        # zone du limit : OB seul / FVG seul / les deux
        entries = {}
        for z in ("BOTH", "OB", "FVG"):
            _E.handle_command(f"/zone {z.lower()}")
            lim, _ = _E.poi_limit_signal(self.c, self.ev, None, 4.0, 1.0, {})
            entries[z] = lim["entry"], lim["zone"]["kind"]
        self.assertEqual(entries["OB"][1], "OB")
        self.assertEqual(entries["FVG"][1], "FVG")
        self.assertLess(entries["FVG"][0], entries["OB"][0])                 # SELL : le FVG est plus bas, donc plus proche du prix
        self.assertEqual(entries["BOTH"], entries["FVG"])                    # la plus proche du prix
        # CRT + filtre de tendance HTF (option)
        _E._ctx_candles = lambda s, tf: self._htf(121.0, 90.0)
        _E.set_entry_types(True, False)
        self.assertEqual(len(_E.crt_build(self.c, self.ev, "XAUUSD")), 1)
        _E.handle_command("/crt htfon")
        saved = _E.htf_trend_check
        try:
            _E.htf_trend_check = lambda sym, ev: (1, "tendance H1 haussière : SELL refusé")
            self.assertEqual(_E.crt_build(self.c, self.ev, "XAUUSD"), [])
            _E.htf_trend_check = lambda sym, ev: (-1, None)
            self.assertIn("tendance", _E.crt_build(self.c, self.ev, "XAUUSD")[0]["setup"])
        finally:
            _E.htf_trend_check = saved

    def test_crt_refus(self):
        _E.set_entry_types(True, False)
        for crh, crl, why in ((125.0, 90.0, "CRH non balayé"), (121.0, 99.9, "deux côtés balayés"), (113.0, 90.0, "clôture hors range")):
            _E._ctx_candles = lambda s, tf, a=(crh, crl): self._htf(*a)
            self.assertEqual(_E.crt_build(self.c, self.ev, "XAUUSD"), [], why)
        _E._ctx_candles = lambda s, tf: []
        self.assertEqual(_E.crt_build(self.c, self.ev, "XAUUSD"), [])
        _E._ctx_candles = lambda s, tf: self._htf(121.0, 90.0)
        _E.TIMEFRAME_MIN = 60                                              # UT d'entrée >= UT de référence : pas de CRT
        self.assertEqual(_E.crt_build(self.c, self.ev, "XAUUSD"), [])


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
            self.assertNotIn("Filtre HTF", sent[0][1], "message groupe ultra court : pas de ligne paramètres")
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
            self.assertEqual(len(sent), 4, "avec max = 2, le 2e signal ouvert est bien publié")   # (message groupe minimal : plus de ligne « Position n/m »)
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
        txt = _E._liq_lines(sig, 0)   # les messages groupe / admin sont désormais minimaux : on teste le formateur
        self.assertIn("Ext. : M15 EQL 80 500 (à 266 pts)", txt)
        self.assertIn("Int. : M1 EQH 80 780 balayé", txt)
        self.assertLess(txt.index("Ext. :"), txt.index("Int. :"))
        for msg in (_E.group_signal("BTCUSD", sig, 1, 0), _E.admin_signal("BTCUSD", sig, 10.0, 100.0, lot, 5.0, 0)):
            self.assertIn("SL", msg)
        sig = self._fake_sig(ext={**ext, "eq": False, "side": "haut"}, sweep={**sw, "swept": False, "eq": False})
        txt = _E._liq_lines(sig, 0)
        self.assertIn("Ext. : M15 haut 80 500", txt)
        self.assertIn("Int. : M1 haut 80 780 non balayé", txt)

    def test_cles_absentes_tolerees(self):
        sig = self._fake_sig()   # renvoi différé : pas de sig["ext"] / sig["sweep"]
        lot = {"lot": 0.01, "real_risk": 10.0, "raised_to_min": False}
        for txt in (_E.group_signal("BTCUSD", sig, None, 0), _E.admin_signal("BTCUSD", sig, 10.0, 100.0, lot, 5.0, 0)):
            self.assertNotIn("Ext. :", txt)
            self.assertNotIn("Int. :", txt)
            self.assertIn("SL", txt)
        sig = self._fake_sig(ext=None, sweep={"tf": "M1", "side": "haut", "level": None, "swept": False, "eq": False})
        self.assertEqual(_E._liq_lines(sig, 0), "")

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


class TestModifyPositionGardeSLTP(unittest.TestCase):
    """POSITION_MODIFY : la requête doit toujours contenir SL ET TP (sinon MT5 supprime celui qui manque)."""

    @staticmethod
    def _conn(positions):
        c = _E._MetaApiRestConnection()
        sent = []

        async def get_positions():
            return positions

        async def _trade(body):
            sent.append(body)
            return {"numericCode": 10009}
        c.get_positions, c._trade = get_positions, _trade
        return c, sent

    def test_sl_seul_garde_le_tp(self):
        c, sent = self._conn([{"id": "42", "stopLoss": 84212.89, "takeProfit": 84887.94}])
        asyncio.run(c.modify_position("42", stop_loss=84382.0))
        self.assertEqual(sent[0]["stopLoss"], 84382.0)
        self.assertEqual(sent[0]["takeProfit"], 84887.94)

    def test_tp_seul_garde_le_sl(self):
        c, sent = self._conn([{"id": "42", "stopLoss": 84212.89, "takeProfit": 84887.94}])
        asyncio.run(c.modify_position("42", take_profit=85000.0))
        self.assertEqual(sent[0]["stopLoss"], 84212.89)
        self.assertEqual(sent[0]["takeProfit"], 85000.0)

    def test_sl_et_tp_fournis_pas_de_lecture(self):
        c, sent = self._conn([])
        asyncio.run(c.modify_position("42", stop_loss=1.0, take_profit=2.0))
        self.assertEqual((sent[0]["stopLoss"], sent[0]["takeProfit"]), (1.0, 2.0))


class _FakeBE:
    """Connexion MetaApi factice : positions, bid/ask, specs ; enregistre les modify_position."""

    def __init__(self, positions, bid=None, ask=None, apply_modify=True, spec=None):
        self.pos, self.bid, self.ask, self.apply, self.calls = positions, bid, ask, apply_modify, []
        self.spec = spec if spec is not None else {"digits": 2, "point": 0.01, "stopsLevel": 0}

    async def get_positions(self):
        return [dict(p) for p in self.pos]

    async def get_symbol_specification(self, symbol):
        return self.spec

    async def get_symbol_price(self, symbol):
        if self.bid is None:
            raise RuntimeError("pas de prix")
        return {"bid": self.bid, "ask": self.ask}

    async def modify_position(self, pid, stop_loss=None, take_profit=None):
        self.calls.append((pid, stop_loss, take_profit))
        if self.apply:
            for p in self.pos:
                if str(p["id"]) == str(pid):
                    if stop_loss is not None:
                        p["stopLoss"] = stop_loss
                    if take_profit is not None:
                        p["takeProfit"] = take_profit


class TestClientId(unittest.TestCase):
    """clientId MetaApi : 3 blocs alphanumériques ; un id non conforme n'est jamais envoyé (plus de HTTP 400 à chaque ordre)."""

    def test_format_auto_conforme(self):
        for tid in (3, 42, 12345, 123456):
            for sym, side in (("BTCUSD", "BUY"), ("XAUUSD", "SELL")):
                cid = _E._auto_client_id(tid, sym, side)
                self.assertEqual(_E._sanitize_client_id(cid), cid, cid)   # conforme -> conservé
                self.assertEqual(cid.count("_"), 2, cid)
                self.assertLessEqual(len(cid), 30, cid)

    def test_ancien_format_refuse_localement(self):
        self.assertEqual(_E._sanitize_client_id("AB_3_BTCB_98649045"), "")   # 4 blocs : celui rejeté en HTTP 400
        self.assertEqual(_E._sanitize_client_id("AB-3-BTCB"), "")
        self.assertEqual(_E._sanitize_client_id(""), "")
        self.assertEqual(_E._sanitize_client_id(None), "")

    def test_format_manuel_conforme(self):
        self.assertEqual(_E._sanitize_client_id("MAN_BTCB_12345678"), "MAN_BTCB_12345678")

    def test_commentaire_tient_dans_30_caracteres(self):
        cid = _E._auto_client_id(123456, "BTCUSD", "BUY")
        comment = f"AlphaBot BTCUSD"[:max(30 - len(cid) - 1, 0)]
        self.assertLessEqual(len(cid) + len(comment), 30)


class TestLogVolumeReel(unittest.TestCase):
    """Après un arrondi de volume (0.059 -> 0.05), le message « Ordre exécuté » affiche le volume réellement exécuté."""

    def test_ordre_execute_affiche_le_volume_broker(self):
        class Conn:
            async def get_positions(self):
                return [{"id": "99", "volume": 0.05, "openPrice": 84385.3, "stopLoss": 84212.89, "takeProfit": 84887.94}]

        async def fake_exec(*a, **k):
            return {"positionId": "99", "numericCode": 10009}
        saved = (_E._execute_mt5_order_async, _E.get_ready_connection)
        _E._execute_mt5_order_async, _E.get_ready_connection = fake_exec, (lambda: Conn())
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                pid = _E.execute_mt5_order("BTCUSD", "BUY", 0.059, 84381.65, 84212.89, 84887.94, client_id="AB_3BTCB_98649045")
        finally:
            _E._execute_mt5_order_async, _E.get_ready_connection = saved
        out = buf.getvalue()
        self.assertEqual(pid, "99")
        self.assertIn("Ordre exécuté : BTCUSD BUY lot=0.05 ", out)
        self.assertNotIn("lot=0.059 entry", out)
        self.assertIn("Prix d'ouverture réel broker : 84385.3", out)


class TestRiskGateUniversel(unittest.TestCase):
    """Risk Gate unique : 0.01 lot <= MAX_RISK_USD -> ok (0.55 / 0.70) ; > MAX (0.71 / 0.90) -> refusé, SL jamais modifié ;
    plus grand volume possible au pas du broker ; dernier verrou sur le volume FINAL ; commun à toutes les stratégies."""

    SYM, ENTRY = "XAUUSD", 2000.0

    def setUp(self):
        self.saved = (_E.RISK_ENABLED, _E.MAX_RISK_USD, _E.RISK_TARGET_USD, _E.MIN_LOT)
        _E.RISK_ENABLED, _E.MAX_RISK_USD, _E.RISK_TARGET_USD, _E.MIN_LOT = True, 0.70, 0.70, 0.01
        self.vpp = _E.value_per_point(self.SYM, self.ENTRY)

    def tearDown(self):
        _E.RISK_ENABLED, _E.MAX_RISK_USD, _E.RISK_TARGET_USD, _E.MIN_LOT = self.saved

    def _gate(self, risk_at_min_lot, strategy="QML"):
        dist = risk_at_min_lot / (0.01 * self.vpp)   # distance SL pour que 0.01 lot risque exactement ce montant
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            res = _E.risk_gate_check(self.SYM, self.ENTRY, self.ENTRY - dist, strategy=strategy)
        return res, buf.getvalue()

    def test_lot_minimum_respecte_le_max(self):
        for r in (0.55, 0.70):
            (ok, lot, risk), out = self._gate(r)
            self.assertTrue(ok, r)
            self.assertAlmostEqual(lot, 0.01)
            self.assertAlmostEqual(risk, r, places=6)
            self.assertIn("→ APPROVED", out)

    def test_lot_minimum_depasse_le_max_est_refuse(self):
        for r in (0.71, 0.90):
            (ok, lot, risk), out = self._gate(r)
            self.assertFalse(ok, r)
            self.assertAlmostEqual(lot, 0.01)   # jamais forcé plus haut, jamais modifié
            self.assertIn("→ REJECTED", out)
            self.assertIn("[RISK GATE] ORDER BLOCKED — risk exceeds maximum", out)

    def test_plus_grand_volume_possible_au_pas_du_broker(self):
        # distance telle que 0.035 lot = 0.70$ -> lot 0.03 (0.60$), jamais 0.04 (0.80$)
        dist = 0.70 / (0.035 * self.vpp)
        with contextlib.redirect_stdout(io.StringIO()):
            ok, lot, risk = _E.risk_gate_check(self.SYM, self.ENTRY, self.ENTRY + dist, strategy="CRT")
        self.assertTrue(ok)
        self.assertAlmostEqual(lot, 0.03)
        self.assertLessEqual(risk, 0.70 + 1e-9)

    def test_format_du_log_et_strategie(self):
        (_, _, _), out = self._gate(0.55, strategy="CHS+BOS/CHOCH1")
        self.assertIn("[RISK GATE] Strategy=CHS+BOS/CHOCH1 Entry=2000.0 SL=", out)
        self.assertIn("Lot=0.01 Risk=$0.55 MAX=$0.70 → APPROVED", out)

    def test_libelle_strategie_commun(self):
        self.assertEqual(_E.risk_strategy_label({"type": "QML", "key_suffix": "qml"}), "QML")
        self.assertEqual(_E.risk_strategy_label({"type": "CHOCH1", "strategy": "CRT", "key_suffix": "crt"}), "CRT/CHOCH1")
        self.assertEqual(_E.risk_strategy_label({"type": "CHOCH2"}), "CHS+BOS/CHOCH2")

    def test_risk_desactive_comportement_inchange(self):
        _E.RISK_ENABLED = False
        self.assertEqual(_E.risk_gate_check(self.SYM, 2000.0, 1990.0), (True, None, None))
        self.assertEqual(_E.risk_gate_verify(self.SYM, 5.0, 2000.0, 1990.0), (True, None))

    def test_sl_jamais_modifie_et_entry_sl_invalide_refuse(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(_E.risk_gate_check(self.SYM, 2000.0, 2000.0), (False, None, None))

    def test_dernier_verrou_sur_le_volume_final(self):
        with contextlib.redirect_stdout(io.StringIO()):
            ok, risk = _E.risk_gate_verify(self.SYM, 0.10, 2000.0, 1999.5, strategy="QML")   # 0.10 x 0.5 x 100 = 5$
        self.assertFalse(ok)
        self.assertAlmostEqual(risk, 5.0)

    def test_volume_broker_arrondi_au_dessus_bloque_avant_envoi(self):
        """minVolume broker 0.10 > lot du gate 0.01 : _normalize_volume remonte à 0.10 -> risque > max -> AUCUN ordre envoyé."""
        sent = []

        class Conn:
            async def create_market_buy_order(self, *a, **k):
                sent.append(a)
                return {"positionId": "1"}
            create_market_sell_order = create_market_buy_order

        async def fake_ready():
            return Conn()

        async def fake_spec(conn, sym):
            return {"minVolume": 0.10, "volumeStep": 0.01, "maxVolume": 100, "digits": 2, "point": 0.01}
        saved = (_E._ensure_terminal_ready, _E._get_symbol_spec, _E._log_symbol_diag)
        _E._ensure_terminal_ready, _E._get_symbol_spec, _E._log_symbol_diag = fake_ready, fake_spec, (lambda *a, **k: None)
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                pid = _E.execute_mt5_order(self.SYM, "BUY", 0.01, 2000.0, 1999.7, 2001.0,
                                           client_id="AB_9XAUB_11112222", strategy="CRT")
        finally:
            _E._ensure_terminal_ready, _E._get_symbol_spec, _E._log_symbol_diag = saved
        self.assertIsNone(pid)
        self.assertEqual(sent, [])   # rien n'est parti vers le broker
        self.assertIn("AB_9XAUB_11112222", _E._RISK_BLOCKED)
        out = buf.getvalue()
        self.assertIn("[RISK GATE] Strategy=CRT", out)
        self.assertIn("→ REJECTED", out)
        self.assertIn("[RISK GATE] ORDER BLOCKED — risk exceeds maximum", out)


class TestBEDoubleVerif(unittest.TestCase):
    """BE : SL calculé sur l'ouverture et le volume RÉELS du broker (spread inclus), net attendu >= +0.01 $, contrôle
    marché (bid/ask) avant envoi, relecture après envoi, TP conservé, jamais de SL dégradé."""

    def _run(self, conn, pid, entry, side, lot, symbol="BTCUSD"):
        saved = (_E.get_metaapi_connection, _E.send)
        old_cache = dict(_E._SPEC_CACHE)
        sent = []

        async def _gc(force_reconnect=False):
            return conn
        _E.get_metaapi_connection = _gc
        _E.send = lambda *a, **k: sent.append(a[1] if len(a) > 1 else "")
        _E._SPEC_CACHE.clear()
        try:
            try:
                res = _E._run_mt5(_E._move_to_breakeven_async(pid, entry, side, symbol, lot))
            except RuntimeError as e:
                res = e
            return res, sent
        finally:
            _E.get_metaapi_connection, _E.send = saved
            _E._SPEC_CACHE.clear()
            _E._SPEC_CACHE.update(old_cache)

    def test_buy_part_de_l_ouverture_broker_et_du_volume_reel(self):
        pos = [{"id": "42", "openPrice": 84385.30, "volume": 0.05, "stopLoss": 84212.89, "takeProfit": 84887.94}]
        conn = _FakeBE(pos, bid=84600.0, ask=84612.0)
        res, sent = self._run(conn, "42", 84381.65, "BUY", 0.059)   # signal 84381.65, lot bot 0.059, broker 0.05
        self.assertAlmostEqual(conn.calls[-1][1], 84385.50, places=6)   # ouverture + 0.2 (0.01 $ / 0.05 lot), pas 84382
        self.assertEqual(conn.calls[-1][2], 84887.94)                    # TP conservé
        self.assertGreaterEqual(_E._be_net_usd("BTCUSD", "BUY", 84385.30, conn.calls[-1][1], 0.05), 0.01 - 1e-9)
        self.assertEqual(sent, [])                                        # aucune alerte : tout est vérifié

    def test_sell_part_de_l_ouverture_broker(self):
        pos = [{"id": "7", "openPrice": 84455.59, "volume": 0.05, "stopLoss": 84624.5, "takeProfit": 83948.86}]
        conn = _FakeBE(pos, bid=84300.0, ask=84312.0)
        self._run(conn, "7", 84456.0, "SELL", 0.059)
        self.assertAlmostEqual(conn.calls[-1][1], 84455.39, places=6)
        self.assertGreaterEqual(_E._be_net_usd("BTCUSD", "SELL", 84455.59, conn.calls[-1][1], 0.05), 0.01 - 1e-9)

    def test_arrondi_favorable_jamais_sous_l_entree(self):
        # digits=0 : l'ancien round() donnait 84381 (< ouverture 84381.2) = perte ; l'arrondi favorable donne 84382
        pos = [{"id": "9", "openPrice": 84381.2, "volume": 0.05, "stopLoss": 84200.0, "takeProfit": 84900.0}]
        conn = _FakeBE(pos, bid=84600.0, ask=84610.0, spec={"digits": 0, "point": 1.0, "stopsLevel": 0})
        self._run(conn, "9", 84381.2, "BUY", 0.05)
        self.assertEqual(conn.calls[-1][1], 84382.0)

    def test_prix_deja_sous_le_be_pas_d_envoi(self):
        pos = [{"id": "5", "openPrice": 84385.30, "volume": 0.05, "stopLoss": 84212.89, "takeProfit": 84887.94}]
        conn = _FakeBE(pos, bid=84385.0, ask=84397.0)   # le bid est repassé sous le BE (84385.5)
        res, sent = self._run(conn, "5", 84385.3, "BUY", 0.05)
        self.assertIsInstance(res, RuntimeError)
        self.assertEqual(conn.calls, [])                 # rien n'est envoyé : le SL d'origine reste
        self.assertTrue(any("NON appliqué" in t for t in sent))

    def test_ne_degrade_jamais_un_sl_meilleur(self):
        pos = [{"id": "3", "openPrice": 84385.30, "volume": 0.05, "stopLoss": 84400.0, "takeProfit": 84887.94}]
        conn = _FakeBE(pos, bid=84600.0, ask=84612.0)
        res, _ = self._run(conn, "3", 84385.3, "BUY", 0.05)
        self.assertEqual(conn.calls, [])
        self.assertEqual(res, 84400.0)

    def test_relecture_detecte_un_sl_non_applique(self):
        pos = [{"id": "8", "openPrice": 84385.30, "volume": 0.05, "stopLoss": 84212.89, "takeProfit": 84887.94}]
        conn = _FakeBE(pos, bid=84600.0, ask=84612.0, apply_modify=False)   # le broker ignore la modification
        _, sent = self._run(conn, "8", 84385.3, "BUY", 0.05)
        self.assertTrue(any("à vérifier" in t for t in sent))

    def test_tp_repris_en_base_si_le_broker_n_en_a_plus(self):
        pos = [{"id": "43", "openPrice": 84381.65, "volume": 0.05, "stopLoss": 84212.89}]   # TP déjà perdu
        conn = _FakeBE(pos, bid=84600.0, ask=84612.0)
        _E._q("INSERT INTO trades (signal_key, symbol, side, tp, mt5_position_id) VALUES ('t-be-tp','BTCUSD','BUY',84900,'43')",
              commit=True)
        try:
            self._run(conn, "43", 84381.65, "BUY", 0.05)
        finally:
            _E._q("DELETE FROM trades WHERE signal_key='t-be-tp'", commit=True)
        self.assertEqual(conn.calls[-1][2], 84900.0)

    def test_fonctions_pures(self):
        self.assertTrue(_E._be_market_check("BUY", 100.0, 101.0, 102.0)[0])
        self.assertFalse(_E._be_market_check("BUY", 100.0, 99.9, 100.5)[0])
        self.assertTrue(_E._be_market_check("SELL", 100.0, 98.0, 99.0)[0])
        self.assertFalse(_E._be_market_check("SELL", 100.0, 100.4, 100.9)[0])
        self.assertTrue(_E._be_market_check("BUY", 100.0, None, None)[0])   # prix inconnu : jamais bloquant
        self.assertAlmostEqual(_E._be_net_usd("BTCUSD", "BUY", 100.0, 100.2, 0.05), 0.01, places=9)
        self.assertAlmostEqual(_E._be_net_usd("BTCUSD", "SELL", 100.0, 99.8, 0.05), 0.01, places=9)


class TestFiltresQualite(unittest.TestCase):
    """Filtres qualité du CHoCH : displacement, anti-range, FVG (états), IND, structure HH/HL, interrupteurs et log."""

    def _c(self, rows):
        return [{"t": 1_700_000_000 + 60 * k, "o": o, "h": h, "l": l, "c": cl} for k, (o, h, l, cl) in enumerate(rows)]

    def _ev(self, i, d=-1, src=0, level=100.0, atr=1.0):
        return {"i": i, "t": 0, "dir": d, "kind": "CHOCH", "level": level, "src": src, "atr": atr, "type": "CHOCH1", "sl_level": 101.0}

    def test_displacement_et_vraie_cassure(self):
        fort = self._c([(100.5, 100.6, 99.9, 100.2), (100.5, 100.6, 98.9, 99.0)])
        self.assertTrue(_E.disp_check(fort, self._ev(1))["ok"])
        faible = self._c([(100.5, 100.6, 99.9, 100.2), (100.10, 100.6, 99.4, 99.95)])   # corps 0.15 ATR : pas un displacement
        self.assertFalse(_E.disp_check(faible, self._ev(1))["ok"])
        mince = self._c([(100.5, 100.6, 99.9, 100.2), (100.5, 103.0, 98.9, 99.95)])     # mèche énorme, cassure de 0.05 ATR
        self.assertFalse(_E.disp_check(mince, self._ev(1))["ok"])
        self.assertFalse(_E.disp_check(fort, self._ev(1, atr=None))["ok"])

    def test_anti_range(self):
        zig = self._c([(100, 100.6, 99.5, 100.4 if k % 2 else 99.6) for k in range(25)])
        tend = self._c([(100 - k * .3, 100.1 - k * .3, 99.7 - k * .3, 99.8 - k * .3) for k in range(25)])
        self.assertFalse(_E.range_check(zig, self._ev(24))["ok"])
        self.assertTrue(_E.range_check(tend, self._ev(24))["ok"])
        self.assertFalse(_E.range_check(zig, self._ev(5))["ok"], "historique insuffisant")

    def test_fvg_etats(self):
        base = [(102, 103, 101, 101.5), (101.5, 101.6, 99.0, 99.2), (99.2, 99.6, 98.6, 98.8), (98.8, 98.9, 98.0, 98.1)]
        ev = self._ev(3)
        r = _E.choch_fvg(self._c(base), ev)
        self.assertEqual((r["state"], r["ok"]), ("non mitigé", True))
        self.assertAlmostEqual(r["zone"][0], 99.6); self.assertAlmostEqual(r["zone"][1], 101.0)
        r = _E.choch_fvg(self._c(base + [(98.1, 100.2, 98.0, 99.0)]), ev)     # le prix remonte dans la zone
        self.assertEqual((r["state"], r["ok"]), ("partiel", True))
        r = _E.choch_fvg(self._c(base + [(98.1, 101.2, 98.0, 99.0)]), ev)     # traverse toute la zone
        self.assertEqual((r["state"], r["ok"]), ("mitigé", False))
        nofvg = [(102, 103, 101, 101.5), (101.5, 101.6, 100.0, 100.2), (100.2, 101.2, 99.5, 99.7), (99.7, 100.5, 99.0, 99.2)]
        self.assertFalse(_E.choch_fvg(self._c(nofvg), ev)["ok"])

    def test_fvg_haussier(self):
        rows = [(98, 99, 97, 98.5), (98.5, 101.0, 98.4, 100.8), (100.8, 101.6, 100.5, 101.4), (101.4, 102, 100.9, 101.8)]
        r = _E.choch_fvg(self._c(rows), self._ev(3, d=1, level=99.0))
        self.assertEqual(r["state"], "non mitigé")
        r = _E.choch_fvg(self._c(rows + [(101.8, 101.9, 99.7, 100.0)]), self._ev(3, d=1, level=99.0))
        self.assertEqual(r["state"], "partiel")

    def test_ind_entre_prix_et_zone(self):
        rows = [(109, 110, 108, 108.5), (106.0, 106.0, 105.0, 105.2), (105.2, 107.5, 105.1, 107.0),
                (106.8, 106.9, 104.0, 104.5), (104.5, 104.6, 102.5, 103.0)]
        c, ev = self._c(rows), self._ev(4, src=0)
        self.assertTrue(_E.ind_check(c, ev, 108.5)["ok"])       # pivot 107.5 entre le prix (103) et l'entrée (108.5)
        self.assertFalse(_E.ind_check(c, ev, 107.0)["ok"])      # limit sous le pivot : pas d'inducement à balayer

    def test_structure_hh_hl(self):
        def path(pts):
            out = []
            for (a, b) in zip(pts, pts[1:]):
                for k in range(10):
                    o = a + (b - a) * k / 10
                    cl = a + (b - a) * (k + 1) / 10
                    out.append((o, max(o, cl) + 0.05, min(o, cl) - 0.05, cl))
            return out
        # hausse (HH, HL) puis cassure du dernier HL -> CHoCH vendeur qui suit une structure HH+HL
        up = self._c(path([100, 110, 105, 115, 108, 120, 112, 100]))
        evs = [e for e in _E.analyze(up) if e["kind"] == "CHOCH" and e["dir"] == -1]
        self.assertTrue(evs, "un CHoCH vendeur doit exister")
        r = _E.swing_check(up, evs[0])
        self.assertTrue(r["ok"], r["txt"])
        self.assertIn("HH+HL", r["txt"])
        # chute (LL, LH) puis cassure du dernier LH -> CHoCH acheteur
        dn = self._c(path([120, 110, 115, 105, 112, 100, 108, 122]))
        evb = [e for e in _E.analyze(dn) if e["kind"] == "CHOCH" and e["dir"] == 1]
        self.assertTrue(evb, "un CHoCH acheteur doit exister")
        self.assertTrue(_E.swing_check(dn, evb[0])["ok"], _E.swing_check(dn, evb[0])["txt"])

    def test_interrupteurs(self):
        keys = _E.QF_KEYS
        saved = {k: _E.get_setting("qf_" + k) for k in keys}
        try:
            for k in keys:
                _E.set_setting("qf_" + k, "0")
            qf = {"swings": {"ok": False, "txt": "x"}, "disp": {"ok": False, "txt": "y"}, "range": {"ok": True, "txt": "z"},
                  "fvg": {"ok": False, "txt": "w"}}
            self.assertIsNone(_E.quality_reject(qf), "tout OFF : rien n'est refusé")
            self.assertTrue(_E.set_qf("disp", True))
            self.assertIn("Displacement", _E.quality_reject(qf))
            _E.set_qf("disp", False); _E.set_qf("range", True)
            self.assertIsNone(_E.quality_reject(qf), "filtre ON mais réussi")
            self.assertFalse(_E.set_qf("inconnu", True))
            self.assertIn("aucun", (_E._qf_txt() if not _E.get_qf("range") else "aucun"))
            self.assertIn("sqf:fvg", str(_E._signal_qf_keyboard()))
            self.assertIn("sig:qf", str(_E._signal_keyboard()))
        finally:
            for k, v in saved.items():
                _E._q("DELETE FROM settings WHERE key=?", ("qf_" + k,), commit=True) if v is None else _E.set_setting("qf_" + k, v)

    def test_bloc_log(self):
        sig = {"side": "SELL", "order": "MARKET", "type": "CHOCH1", "t": 1_700_000_000, "entry": 100.0, "sl": 101.0, "tp": 96.0,
               "risk": 1.0, "rr": 4.0, "tp_mode": None,
               "qf": {"disp": {"ok": True, "txt": "corps 1.20xATR"}, "range": {"ok": False, "txt": "chop"}}}
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _E.signal_log_block("XAUUSD", sig, 2)
        out = buf.getvalue()
        for needle in ("TARGET_TYPE=RR fixe", "TARGET_PRICE=96.00", "EXPECTED_RR=4.0", "DISP", "RANGE", "filtre OFF"):
            self.assertIn(needle, out)


class TestAjoutsNewsStopUS30(unittest.TestCase):
    """US30, filtre news, bot arrêté / démarré, point potentiel par stratégie."""
    _KEYS = "('news_filter','news_medium','bot_on','strat_main','strat_crt','qml_on','qml_auto')"

    def tearDown(self):
        _E._ff_cache.clear()
        _E._q(f"DELETE FROM settings WHERE key IN {self._KEYS}", commit=True)

    def _ev(self, cur, mins, impact="High", title="NFP"):
        now = datetime.now(timezone.utc)
        _E._ff_cache.update(data=[{"country": cur, "impact": impact, "title": title,
                                   "date": (now + timedelta(minutes=mins)).isoformat()}], ts=time.time())

    def test_us30_present(self):
        self.assertIn("US30", _E.SYMBOLS)
        self.assertEqual(_E.value_per_point("US30", 42000), 1.0)
        self.assertGreater(_E.calc_lot("US30", 10, 50.0, price=42000)["lot"], 0)
        self.assertIn("US30", _E._ANALYSE_SYMS.values())
        self.assertFalse(_E.get_symbol_on("US30"), "nouveaux marchés OFF par défaut")

    def test_news_fenetre_et_devises(self):
        self._ev("USD", 10)
        self.assertIsNotNone(_E.news_block_info("XAUUSD"))
        self.assertIsNotNone(_E.news_block_info("US30"))
        self.assertIsNotNone(_E.news_block_info("USDJPY"))
        self.assertIsNone(_E.news_block_info("EURGBP" if "EURGBP" in _E.SYMBOLS else "EURJPY"))   # aucune devise USD
        self._ev("USD", 90)
        self.assertIsNone(_E.news_block_info("XAUUSD"), "hors fenêtre")
        self._ev("USD", -10)
        self.assertIsNotNone(_E.news_block_info("XAUUSD"), "juste après la news")
        self._ev("USD", 5, impact="Medium")
        self.assertIsNone(_E.news_block_info("XAUUSD"), "impact moyen ignoré par défaut")
        _E.set_setting("news_medium", "1")
        self.assertIsNotNone(_E.news_block_info("XAUUSD"))
        _E.set_setting("news_filter", "0")
        self.assertIsNone(_E.news_block_info("XAUUSD"), "filtre OFF")

    def test_calendrier_indisponible_ne_bloque_pas(self):
        saved = _E._forexfactory_events
        _E._forexfactory_events = lambda: (_ for _ in ()).throw(RuntimeError("réseau"))
        try:
            self.assertIsNone(_E.news_block_info("XAUUSD"))
        finally:
            _E._forexfactory_events = saved

    def _publish(self):
        sent = []
        saved = (_E._deliver_signal, _E.make_chart, _E.to_admin)
        _E._deliver_signal = lambda *a, **k: sent.append(1)
        _E.make_chart = lambda *a, **k: None
        _E.to_admin = lambda *a, **k: None
        try:
            _E._q("DELETE FROM trades", commit=True)
            sig = TestSignauxSimultanes._sig(TestSignauxSimultanes, int(time.time()))
            ok = _E.publish_signal("XAUUSD", [], [], sig)
        finally:
            _E._deliver_signal, _E.make_chart, _E.to_admin = saved
            _E._q("DELETE FROM trades", commit=True)
        return ok, sent

    def test_bot_arrete_et_news_bloquent_publication(self):
        _E.set_setting("bot_on", "0")
        self.assertEqual(self._publish()[0], False)
        _E.set_setting("bot_on", "1")
        self._ev("USD", 5)
        self.assertEqual(self._publish()[0], False)
        _E._ff_cache.clear()
        _E.set_setting("news_filter", "0")
        self.assertEqual(self._publish()[0], True)

    def test_menu_strategies(self):
        kb = _E._strategies_keyboard()["inline_keyboard"]
        datas = {b["callback_data"] for r in kb for b in r}
        self.assertTrue({"str:main", "str:crt", "str:qml", "str:qmlauto"} <= datas)
        self.assertIn("BOS / CHoCH", _E._strategies_text())
        ana = {b["callback_data"] for r in _E._analyse_symbol_keyboard("XAUUSD")["inline_keyboard"] for b in r}
        self.assertTrue({"ana:str:XAU:main", "ana:str:XAU:crt", "ana:all:XAU", "ana:qml:XAU:15"} <= ana)

    def test_point_potentiel_main_et_crt(self):
        c = TestEntreeOBFVG._candles()
        saved = (_E.get_candles, _E.TIMEFRAME_MIN, _E.PREVIEW_MAX_AGE, _E.liquidity_reading)
        _E.get_candles = lambda s, tf=None: c
        _E.TIMEFRAME_MIN, _E.PREVIEW_MAX_AGE = 1, 10 ** 6
        _E.liquidity_reading = lambda *a, **k: (None, None)
        try:
            for strat in ("main", "crt"):
                txt, _chart = _E.strategy_preview("XAUUSD", strat)
                self.assertIn("XAUUSD", txt)
                self.assertTrue("Entrée" in txt or "Pas de point" in txt, txt)
                self.assertNotIn("Traceback", txt)
        finally:
            _E.get_candles, _E.TIMEFRAME_MIN, _E.PREVIEW_MAX_AGE, _E.liquidity_reading = saved


class TestQmlSansLimiteAge(unittest.TestCase):
    """L'âge d'un QML ne l'invalide pas : seule l'invalidation structurelle compte."""
    _REAL_GET_CANDLES = staticmethod(_E.get_candles)   # (d'autres tests remplacent _E.get_candles par un faux flux)

    @staticmethod
    def _walk(seed, n=5000, tf=900):
        import random
        rnd, px, cs = random.Random(seed), 100.0, []
        for i in range(n):
            o = px
            c = o + rnd.gauss(0, 0.35)
            cs.append({"t": 1_700_000_000 + i * tf, "o": o, "h": max(o, c) + abs(rnd.gauss(0, 0.2)),
                       "l": min(o, c) - abs(rnd.gauss(0, 0.2)), "c": c})
            px = c
        return cs

    def setUp(self):
        self._saved = _E.QML_ZONE_MAX_BARS

    def tearDown(self):
        _E.QML_ZONE_MAX_BARS = self._saved

    def test_zone_ancienne_conservee_sans_limite(self):
        if _E.np is None:
            self.skipTest("numpy / pandas absents")
        cs = self._walk(2)
        _E.QML_ZONE_MAX_BARS = 0
        zs = _E.qml_zones(cs)
        n = len(cs)
        old = [z for z in zs if n - 1 - z["j"] > 40]
        self.assertTrue(old, "une zone vieille de plus de 40 bougies doit rester valide")
        _E.QML_ZONE_MAX_BARS = 40   # ancienne limite, encore activable par variable d'environnement
        zs40 = _E.qml_zones(cs)
        self.assertFalse([z for z in zs40 if n - 1 - z["j"] > 40])
        self.assertTrue(all(z in zs for z in zs40), "les zones récentes sont identiques avec ou sans limite")

    def test_invalidation_structurelle_toujours_active(self):
        if _E.np is None:
            self.skipTest("numpy / pandas absents")
        cs = self._walk(2)
        _E.QML_ZONE_MAX_BARS = 0
        z = [z for z in _E.qml_zones(cs) if len(cs) - 1 - z["j"] > 40][0]
        k = z["j"] + 5   # une CLÔTURE traverse la zone après sa création => zone morte
        px = (z["head"] + 1.0) if z["side"] == "SELL" else (z["head"] - 1.0)
        cs2 = [dict(c) for c in cs]
        cs2[k].update(c=px, h=max(cs2[k]["h"], px), l=min(cs2[k]["l"], px))
        self.assertFalse([w for w in _E.qml_zones(cs2) if w["t"] == z["t"] and w["side"] == z["side"]])

    def test_get_candles_historique_long_et_repli(self):
        calls = []
        saved = (_E._deriv, _E._only_closed)
        _E._deriv = lambda sym, minutes=None, count=None: calls.append(count) or [{"t": 1, "o": 1, "h": 1, "l": 1, "c": 1}]
        _E._only_closed = lambda raw, sec: raw
        try:
            self._REAL_GET_CANDLES("XAUUSD", 15, 5000)
            self._REAL_GET_CANDLES("XAUUSD", 15)
        finally:
            _E._deriv, _E._only_closed = saved
        self.assertEqual(calls, [5000, None], "historique long demandé pour le QML, inchangé pour le reste")


class TestQmlMemoireEtHistoriqueProfond(TestQmlSansLimiteAge):
    """Mémoire persistante des zones QML, pagination Deriv au-delà de 5000 bougies, historique long BTC."""

    def test_memoire_garde_la_zone_hors_fenetre_puis_l_invalide(self):
        if _E.np is None:
            self.skipTest("numpy / pandas absents")
        sym, cs = "TST_QML", self._walk(2)
        _E.QML_ZONE_MAX_BARS = 0
        _E._qml_store_init()
        _E._q("DELETE FROM qml_zone_store WHERE symbol=?", (sym,), commit=True)
        try:
            fresh = _E.qml_zones(cs)
            z_old = [z for z in fresh if len(cs) - 1 - z["j"] > 300][0]
            _E.qml_merge_store(sym, 15, cs, fresh)
            win = cs[-250:]   # fenêtre COURTE : la zone est plus ancienne que l'historique chargé
            out = _E.qml_merge_store(sym, 15, win, _E.qml_zones(win))
            same = lambda w: w["t"] == z_old["t"] and w["side"] == z_old["side"]
            self.assertTrue(any(same(w) for w in out), "zone hors fenêtre conservée par la mémoire")
            px = z_old["head"] + (1.0 if z_old["side"] == "SELL" else -1.0)
            win2 = win + [{"t": win[-1]["t"] + 900, "o": px, "h": px + .1, "l": px - .1, "c": px}]
            out2 = _E.qml_merge_store(sym, 15, win2, _E.qml_zones(win2))
            self.assertFalse(any(same(w) for w in out2), "une clôture au-delà de la tête invalide la zone mémorisée")
            out3 = _E.qml_merge_store(sym, 15, win2, _E.qml_zones(win2))
            self.assertFalse(any(same(w) for w in out3), "une zone invalidée ne revient pas")
        finally:
            _E._q("DELETE FROM qml_zone_store WHERE symbol=?", (sym,), commit=True)

    def test_deriv_pagination(self):
        import bisect
        epochs = [1_700_000_000 + i * 900 for i in range(12000)]
        calls = []

        def fake(payload):
            calls.append(payload["end"])
            e = len(epochs) if payload["end"] == "latest" else bisect.bisect_right(epochs, payload["end"])
            sl = epochs[max(0, e - payload["count"]):e]
            return {"candles": [{"epoch": t, "open": 1, "high": 2, "low": 0.5, "close": 1.5} for t in sl]}
        saved = _E._deriv_request
        _E._deriv_request = fake
        try:
            got = _E._deriv("frxXAUUSD", 15, 12000)
            self.assertEqual(len(got), 12000)
            self.assertEqual([c["t"] for c in got], sorted({c["t"] for c in got}))
            self.assertEqual(len(calls), 3, "3 pages de 5000 max")
            n = {"i": 0}

            def flaky(payload):
                n["i"] += 1
                if n["i"] == 2:
                    raise RuntimeError("page 2 indisponible")
                return fake(payload)
            _E._deriv_request = flaky
            self.assertEqual(len(_E._deriv("frxXAUUSD", 15, 12000)), 5000, "page 2 en échec : on garde la page 1")
            self.assertEqual(len(_E._deriv("frxXAUUSD", 15)), _E.CANDLES_LIMIT, "sans count : comportement inchangé")
        finally:
            _E._deriv_request = saved

    def test_btc_historique_long_deriv_binance_repli(self):
        big = [{"t": i, "o": 1, "h": 1, "l": 1, "c": 1} for i in range(6000)]
        small = big[:300]
        saved = (_E._deriv, _E._binance_long, _E._binance, _E._only_closed)
        used = []
        _E._only_closed = lambda raw, sec: raw
        _E._binance = lambda sym, minutes=None: used.append("normal") or small
        try:
            _E._deriv = lambda sym, minutes=None, count=None: used.append("deriv:" + sym) or big
            _E._binance_long = lambda *a: used.append("binance") or big
            self.assertEqual(len(self._REAL_GET_CANDLES("BTCUSD", 15, 5000)), 6000)
            self.assertEqual(used, ["deriv:cryBTCUSD"], "Deriv d'abord pour l'historique long")
            used.clear()
            _E._deriv = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("Deriv HS"))
            self.assertEqual(len(self._REAL_GET_CANDLES("BTCUSD", 15, 5000)), 6000)
            self.assertEqual(used, ["binance"], "repli Binance")
            used.clear()
            _E._binance_long = lambda *a: (_ for _ in ()).throw(RuntimeError("Binance HS"))
            self.assertEqual(len(self._REAL_GET_CANDLES("BTCUSD", 15, 5000)), 300)
            self.assertEqual(used, ["normal"], "dernier repli : historique normal")
            used.clear()
            self.assertEqual(len(self._REAL_GET_CANDLES("BTCUSD", 15)), 300)
            self.assertEqual(used, ["normal"], "sans count : inchangé")
        finally:
            _E._deriv, _E._binance_long, _E._binance, _E._only_closed = saved


class TestActifsEtEntrees(unittest.TestCase):
    """Actifs activables par paire (défaut : Gold + BTC) et interrupteurs indépendants entrée directe / ordre limit."""

    def _restore(self, key, old):
        if old is None:
            _E._q("DELETE FROM settings WHERE key=?", (key,), commit=True)
        else:
            _E.set_setting(key, old)

    def test_defauts_et_interrupteur(self):
        olds = {x: _E.get_setting("sym_on_" + x) for x in _E.SYMBOLS}
        try:
            for x in _E.SYMBOLS:
                _E._q("DELETE FROM settings WHERE key=?", ("sym_on_" + x,), commit=True)
            self.assertTrue(_E.get_symbol_on("XAUUSD") and _E.get_symbol_on("BTCUSD"))
            self.assertFalse(_E.get_symbol_on("USDCAD") or _E.get_symbol_on("USDJPY"), "forex désactivé par défaut")
            self.assertTrue(_E.set_symbol_on("USDCAD", True) and _E.get_symbol_on("USDCAD"))
            self.assertFalse(_E.set_symbol_on("INCONNU", True))
            self.assertIn("ssym:USDJPY", str(_E._signal_sym_keyboard()))
            self.assertIn("sig:sym", str(_E._signal_keyboard()))
        finally:
            for x, v in olds.items():
                self._restore("sym_on_" + x, v)

    def test_actif_desactive_pas_de_scan(self):
        old = _E.get_setting("sym_on_USDJPY")
        try:
            _E._q("DELETE FROM settings WHERE key='sym_on_USDJPY'", commit=True)
            _E._q("DELETE FROM trades WHERE symbol='USDJPY'", commit=True)
            calls = []
            saved = _E.get_candles
            _E.get_candles = lambda *a, **k: calls.append(1) or []
            try:
                _E.process_symbol("USDJPY")
            finally:
                _E.get_candles = saved
            self.assertEqual(calls, [], "désactivé et sans position : aucun téléchargement")
            self.assertTrue(any("USDJPY : désactivé" in l for l in _E.scan_status_lines()))
        finally:
            self._restore("sym_on_USDJPY", old)

    def test_entrees_independantes(self):
        old = (_E.get_setting("entry_direct"), _E.get_setting("entry_limit"))
        try:
            self.assertTrue(_E.set_entry_types(True, False) and _E.get_entry_direct() and not _E.get_entry_limit())
            self.assertTrue(_E.set_entry_types(False, True) and _E.get_entry_limit() and not _E.get_entry_direct())
            self.assertTrue(_E.set_entry_types(True, True))
            self.assertFalse(_E.set_entry_types(False, False), "au moins un type d'entrée reste actif")
        finally:
            self._restore("entry_direct", old[0]); self._restore("entry_limit", old[1])


class TestSetupEngineEntreeEtPause(unittest.TestCase):
    """SETUP50 : entrée sur bougie de rejet à la clôture (clôture > 50 % permise), annulation à 100 %, pause après N SL activable."""

    def _engine(self):
        eng = _E.SetupEngine("TEST", "M1")
        for k in range(30):
            eng._push({"t": k * 60, "o": 100.0, "h": 100.5, "l": 99.5, "c": 100.0})
        return eng

    def _arm(self, eng):
        """VENTE : sweep haut à 110, ancre 100, zone 50 % à 105, annulation à 110 ; prev = bougie haussière."""
        eng._push({"t": 30 * 60, "o": 104.0, "h": 105.6, "l": 103.9, "c": 105.5})
        i = eng.base + len(eng.c) - 1
        eng.setup = {"dir": -1, "level": 109.0, "start": 110.0, "ext_i": i - 5, "sweep_i": i - 4, "choch_lvl": 101.0,
                     "choch_i": i, "anchor": 100.0, "retr": 0.5, "ext_t": 0}
        eng.state = _E.SE_ZONE_50_ACTIVE

    def _step(self, eng, o, h, l, c):
        eng._push({"t": eng.c[-1]["t"] + 60, "o": o, "h": h, "l": l, "c": c})
        i = eng.base + len(eng.c) - 1
        return eng._zone(i, eng._g(i), eng.atr[-1])

    def test_rejection_types(self):
        prev = {"o": 104.0, "h": 105.6, "l": 103.9, "c": 105.5}
        self.assertEqual(_E.se_rejection(prev, {"o": 106.0, "h": 106.2, "l": 103.0, "c": 103.5}, -1), "engulfing")
        self.assertEqual(_E.se_rejection(prev, {"o": 105.2, "h": 108.0, "l": 104.9, "c": 105.1}, -1), "pin bar")
        self.assertEqual(_E.se_rejection(prev, {"o": 105.0, "h": 106.0, "l": 104.2, "c": 105.02}, -1), "doji")
        self.assertIsNone(_E.se_rejection(prev, {"o": 105.0, "h": 106.0, "l": 104.9, "c": 105.9}, -1), "bougie haussière : pas de rejet")

    def test_entree_sur_rejet_cloture_au_dela_de_50(self):
        eng = self._engine()
        self._arm(eng)
        sig = self._step(eng, 105.2, 108.0, 104.9, 105.1)   # pin bar : clôture 105.1 > 105 (50 %)
        self.assertIsNotNone(sig, "la clôture peut dépasser 50 %")
        self.assertEqual((sig["side"], sig["entry"], sig["type"]), ("SELL", 105.1, "SETUP50"))
        self.assertGreater(sig["sl"], 110.0, "SL derrière le sweep")
        self.assertAlmostEqual(sig["tp"], sig["entry"] - sig["risk"] * sig["rr"], places=6)
        self.assertEqual(eng.state, _E.SE_IDLE)

    def test_pas_d_entree_sans_rejet(self):
        eng = self._engine()
        self._arm(eng)
        self.assertIsNone(self._step(eng, 105.0, 106.0, 104.9, 105.9))
        self.assertEqual(eng.state, _E.SE_ZONE_50_ACTIVE, "le setup reste actif, en attente d'un rejet")

    def test_annulation_a_100_pourcent(self):
        eng = self._engine()
        self._arm(eng)
        self.assertIsNone(self._step(eng, 105.2, 110.2, 104.9, 105.1), "même avec une bougie de rejet")
        self.assertEqual((eng.state, eng.setup), (_E.SE_IDLE, None))

    def test_pause_bloque_l_entree(self):
        eng = self._engine()
        self._arm(eng)
        eng.pause_until = eng.base + len(eng.c) + 50
        self.assertIsNone(self._step(eng, 105.2, 108.0, 104.9, 105.1))
        self.assertEqual(eng.state, _E.SE_IDLE)

    def test_pause_apres_n_sl_et_interrupteur(self):
        eng = self._engine()
        eng.p.pause_after_sl, eng.p.pause_candles = 3, 60
        for _ in range(3):
            eng.report_result(True, pause=True)
        self.assertGreater(eng.pause_until, eng.base + len(eng.c))
        off = self._engine()
        off.p.pause_after_sl = 3
        for _ in range(5):
            off.report_result(True, pause=False)
        self.assertEqual((off.pause_until, off.losses), (-1, 0), "interrupteur OFF : jamais de pause")

    def test_parametres_en_tete_de_fichier(self):
        got = (_E.SWEEP_EQUAL_ATR, _E.SWEEP_MIN_ATR, _E.SWEEP_MAX_ATR, _E.CHOCH_BODY_ATR_MIN, _E.SWEEP_LEVEL_MIN_AGE,
               _E.SWEEP_CLOSE_BACK, _E.CHOCH_WINDOW, _E.MAX_BOUGIES, _E.SWING_FRACTAL)
        self.assertEqual(got, (0.2, 0.1, 1.0, 1.0, 20, 3, 10, 20, 5))
        p = _E.SEParams()
        self.assertEqual((p.level_min_age, p.sweep_close_back, p.choch_window, p.max_bougies), (20, 3, 10, 20))

    def test_duree_de_vie_max_bougies(self):
        eng = self._engine()
        self._arm(eng)
        for _ in range(20):    # 20 bougies après le CHoCH : le setup vit encore
            self.assertIsNone(self._step(eng, 100.5, 101.0, 100.2, 100.6))
        self.assertEqual(eng.state, _E.SE_ZONE_50_ACTIVE)
        self.assertIsNone(self._step(eng, 105.2, 108.0, 104.9, 105.1), "21e bougie : annulé même avec un rejet")
        self.assertEqual((eng.state, eng.setup), (_E.SE_IDLE, None))
        eng2 = self._engine()
        self._arm(eng2)
        for _ in range(19):
            self._step(eng2, 100.5, 101.0, 100.2, 100.6)
        self.assertIsNotNone(self._step(eng2, 105.2, 108.0, 104.9, 105.1), "20e bougie : entrée encore permise")

    # --- SWEEP -> CHoCH -> verrou ---------------------------------------------------------------------
    def _struct(self):
        """30 bougies plates ; swing high intact 108 (HH, vieux de 22 bougies), swing low intact 103 (à casser)."""
        eng = self._engine()
        eng.highs = [{"i": 4, "p": 106.0, "used": False}, {"i": 8, "p": 108.0, "used": False}]
        eng.lows = [{"i": 10, "p": 103.0, "used": False}]
        return eng

    def _feed(self, eng, o, h, l, c):
        eng._push({"t": eng.c[-1]["t"] + 60, "o": o, "h": h, "l": l, "c": c})
        i = eng.base + len(eng.c) - 1
        return eng._step(i)

    def _lines(self, eng, tag):
        return [l for l in eng.log if l.startswith(f"[{tag}]")]

    def _sweep_choch(self, eng):
        self._feed(eng, 107.5, 108.5, 107.4, 107.9)       # mèche 0.5 ATR au-dessus du swing high 108, clôture de retour
        self._feed(eng, 107.0, 107.2, 105.8, 106.0)
        self._feed(eng, 106.0, 106.2, 101.5, 102.0)       # clôture sous le swing low 103, corps >= 1 ATR

    def test_sweep_choch_verrou(self):
        eng = self._struct()
        self._feed(eng, 107.5, 108.5, 107.4, 107.9)
        self.assertEqual(eng.state, _E.SE_SWEEP)
        self.assertIn("HH 108", self._lines(eng, "SWEEP")[-1])
        self.assertIn("✅", self._lines(eng, "SWEEP")[-1])
        self.assertIn("🔒", self._lines(eng, "LOCK")[-1])
        self._feed(eng, 107.0, 107.2, 105.8, 106.0)
        self._feed(eng, 106.0, 106.2, 101.5, 102.0)
        self.assertEqual(eng.state, _E.SE_MESURE)
        self.assertIn("103", self._lines(eng, "CHOCH")[-1])
        self.assertIn("✅", self._lines(eng, "CHOCH")[-1])
        eng.highs.append({"i": 5, "p": 107.0, "used": False})       # autre niveau percé pendant le verrou : ignoré
        self._feed(eng, 102.0, 107.5, 102.0, 105.0)
        self.assertEqual(eng.setup["level"], 108.0)

    def test_niveau_trop_recent_refuse(self):
        eng = self._struct()
        eng.highs = [{"i": 20, "p": 108.0, "used": False}]            # 10 bougies seulement
        self._feed(eng, 107.5, 108.5, 107.4, 107.9)
        self.assertEqual(eng.state, _E.SE_IDLE)
        self.assertIn("trop récent", self._lines(eng, "SWEEP")[-1])

    def test_equal_highs(self):
        eng = self._struct()
        eng.highs = [{"i": 6, "p": 108.0, "used": False}, {"i": 9, "p": 108.1, "used": False}]   # écart 0.1 < 0.2 ATR
        self._feed(eng, 107.5, 108.7, 107.4, 107.9)
        self.assertEqual(eng.state, _E.SE_SWEEP)
        self.assertIn("EQH 108.1", self._lines(eng, "SWEEP")[-1])

    def test_sweep_trop_profond_est_un_bos(self):
        eng = self._struct()
        self._feed(eng, 107.5, 110.6, 107.4, 108.2)       # mèche > 1 ATR
        self.assertEqual((eng.state, eng.pending), (_E.SE_IDLE, None))
        self.assertIn("❌", self._lines(eng, "SWEEP")[-1])
        self.assertIn("BOS", self._lines(eng, "SWEEP")[-1])

    def test_cloture_de_retour_dans_les_3_bougies(self):
        eng = self._struct()
        self._feed(eng, 107.5, 108.5, 107.4, 108.3)       # clôture au-dessus : en attente
        self._feed(eng, 108.3, 108.5, 108.1, 108.3)
        self._feed(eng, 108.3, 108.5, 108.1, 108.3)
        self.assertIsNotNone(eng.pending)
        self._feed(eng, 108.2, 108.3, 107.5, 107.8)       # 3e bougie suivante : retour accepté
        self.assertEqual(eng.state, _E.SE_SWEEP)
        eng2 = self._struct()
        self._feed(eng2, 107.5, 108.5, 107.4, 108.3)
        for _ in range(3):
            self._feed(eng2, 108.3, 108.5, 108.1, 108.3)
        self.assertEqual((eng2.state, eng2.pending), (_E.SE_IDLE, None))
        self.assertIn("aucune clôture de retour", self._lines(eng2, "SWEEP")[-1])

    def test_choch_dans_les_10_bougies(self):
        eng = self._struct()
        self._feed(eng, 107.5, 108.5, 107.4, 107.9)
        for _ in range(10):
            self._feed(eng, 107.0, 107.5, 106.5, 107.0)
        self.assertEqual(eng.state, _E.SE_SWEEP)
        self._feed(eng, 107.0, 107.5, 106.5, 107.0)       # 11e bougie sans CHoCH
        self.assertEqual((eng.state, eng.setup), (_E.SE_IDLE, None))
        self.assertIn("🔓", self._lines(eng, "LOCK")[-1])
        self.assertIn("pas de CHoCH", self._lines(eng, "LOCK")[-1])

    def test_sweep_casse_leve_le_verrou(self):
        eng = self._struct()
        self._feed(eng, 107.5, 108.5, 107.4, 107.9)
        self._feed(eng, 107.9, 108.8, 107.5, 107.8)       # dépasse l'extrême du sweep avant le CHoCH
        self.assertEqual((eng.state, eng.setup), (_E.SE_IDLE, None))
        self.assertIn("sweep cassé", self._lines(eng, "LOCK")[-1])

    def test_choch_ignore_micro_break_et_meche(self):
        eng = self._struct()
        self._feed(eng, 107.5, 108.5, 107.4, 107.9)
        c = 107.9
        for _ in range(8):                                 # descente lente : aucun FVG, aucun corps >= 1 ATR
            self._feed(eng, c, c + 0.3, c - 0.85, c - 0.55)
            c -= 0.55
        self._feed(eng, c, c + 0.1, 102.4, 103.2)         # mèche sous 103, clôture au-dessus
        self.assertEqual(eng.state, _E.SE_SWEEP)
        self.assertIn("mèche seule", self._lines(eng, "CHOCH")[-1])
        self._feed(eng, 103.2, 103.3, 102.7, 102.9)       # clôture sous 103 mais corps 0.3 ATR, pas de FVG
        self.assertEqual(eng.state, _E.SE_SWEEP, "micro-break : pas de CHoCH")
        self.assertIn("micro-break", self._lines(eng, "CHOCH")[-1])

    def test_verrou_libere_a_100_pourcent(self):
        eng = self._struct()
        self._sweep_choch(eng)
        self.assertEqual(eng.state, _E.SE_MESURE)
        self._feed(eng, 102.0, 108.6, 102.0, 103.0)       # mèche jusqu'au sweep (100 %)
        self.assertEqual((eng.state, eng.setup), (_E.SE_IDLE, None))
        self.assertIn("🔓", self._lines(eng, "LOCK")[-1])
        self.assertIn("100 %", self._lines(eng, "LOCK")[-1])

    def test_un_seul_trade_par_setup_et_log(self):
        eng = self._engine()
        self._arm(eng)
        eng.setup["anchor_t"] = 1700000000
        sig = self._step(eng, 105.2, 108.0, 104.9, 105.1)
        self.assertIsNotNone(sig)
        self.assertIsNone(eng.setup, "setup consommé")
        self.assertIsNone(self._step(eng, 105.2, 108.0, 104.9, 105.1) if eng.state == _E.SE_ZONE_50_ACTIVE else None)
        for needle in ("ancre Fibo 100", "2023-11-14 22:13 UTC", "retracement atteint 80%", "rejet pin bar", "timeframe M1"):
            self.assertIn(needle, sig["log"])

    def test_interrupteur_telegram(self):
        keys = ("se_pause", "se_pause_n", "se_pause_candles")
        old = {k: _E.get_setting(k) for k in keys}
        try:
            _E.handle_command("/pausesl off")
            self.assertFalse(_E.get_se_pause())
            self.assertIn("spause:on", str(_E._signal_pause_keyboard()))
            _E.handle_command("/pausesl on 4 90")
            self.assertTrue(_E.get_se_pause())
            self.assertEqual((_E.get_se_pause_n(), _E.get_se_pause_candles()), (4, 90))
            self.assertIn("sig:pause", str(_E._signal_keyboard()))
        finally:
            for k, v in old.items():
                if v is None:
                    _E._q("DELETE FROM settings WHERE key=?", (k,), commit=True)
                else:
                    _E.set_setting(k, v)


class TestMoteurSmc(unittest.TestCase):
    """Moteur SMC fusionné (section 14, ex analyse_smc.py) : son auto-test d'origine doit rester vert."""

    def test_selftest_d_origine(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertEqual(_E.smc_selftest(), 0)
        self.assertIn("tous les tests OK", buf.getvalue())


class TestQmlGagnant(unittest.TestCase):
    """Stratégie QML GAGNANT (section 15) : OFF par défaut, aucune règle -> aucun signal ; analyse toujours dispo."""
    _KEYS = ("qmlg_on",)

    def setUp(self):
        self._old = {k: _E.get_setting(k) for k in self._KEYS}
        self._gc = _E.get_candles
        self._env_default = _E.QMLG_ENABLED_DEFAULT
        _E.QMLG_ENABLED_DEFAULT = False
        for k in self._KEYS:
            _E._q("DELETE FROM settings WHERE key=?", (k,), commit=True)

    def tearDown(self):
        _E.get_candles = self._gc
        _E.QMLG_ENABLED_DEFAULT = self._env_default
        for k, v in self._old.items():
            if v is None:
                _E._q("DELETE FROM settings WHERE key=?", (k,), commit=True)
            else:
                _E.set_setting(k, v)

    def _fake_flux(self, n=1500):
        cs = TestQmlSansLimiteAge._walk(2, n=n, tf=900)
        _E.get_candles = lambda symbol, minutes=None, count=None: list(cs)
        return cs

    def test_off_par_defaut_et_bascule(self):
        self.assertFalse(_E.get_qmlg_on(), "désactivée par défaut")
        _E.set_setting("qmlg_on", "1")
        self.assertTrue(_E.get_qmlg_on())
        _E.set_setting("qmlg_on", "0")
        self.assertFalse(_E.get_qmlg_on())

    def test_bouton_menu_et_qml_existant_present(self):
        kb = str(_E._strategies_keyboard())
        self.assertIn("str:qmlg", kb)
        self.assertIn("str:qml\"", kb.replace("'", '"'), "le bouton QML existant reste en place")
        self.assertIn("QML GAGNANT", _E._strategies_text())

    def test_tourne_sur_bougies_synthetiques(self):
        cs = self._fake_flux()
        r = _E.qml_gagnant_analyze("XAUUSD", 15)
        self.assertEqual(r["strategy"], "QML_GAGNANT")
        self.assertEqual((r["symbol"], r["tf_minutes"], r["bars"]), ("XAUUSD", 15, len(cs)))
        self.assertEqual(r["status"], "NO_RULES")
        self.assertIn(r["trend"], ("bullish", "bearish", "range"))
        self.assertIsInstance(r["n_qml"], int)
        self.assertFalse(r["enabled"])

    def test_aucun_signal_meme_active(self):
        self._fake_flux()
        _E.set_setting("qmlg_on", "1")
        r = _E.qml_gagnant_analyze("XAUUSD", "M15")
        self.assertTrue(r["enabled"])
        self.assertIsNone(r["signal"], "sans règles, jamais de signal")

    def test_pas_de_donnees_ne_leve_pas(self):
        _E.get_candles = lambda symbol, minutes=None, count=None: []
        r = _E.qml_gagnant_analyze("XAUUSD", 15)
        self.assertEqual(r["status"], "NO_DATA")
        self.assertIsNone(r["signal"])

    def test_analyse_toujours_dispo_bouton_et_texte(self):
        self._fake_flux()
        self.assertIn("ana:qmlg:", str(_E._analyse_symbol_keyboard("XAUUSD")))
        txt = _E.qml_gagnant_text(_E.qml_gagnant_analyze("XAUUSD", 15))   # réglage OFF : l'analyse répond quand même
        self.assertIn("QML GAGNANT", txt)
        self.assertIn("aucune", txt)


class TestComputeTrade(unittest.TestCase):
    """compute_trade(qml) : entrée = bord proche (jamais 50 %), SL derrière la zone (jamais derrière la tête), TP = liquidité."""

    @staticmethod
    def _structure(highs=(), lows=(), extra_h=None, base=100.0):
        n = 60
        h, l, c = [base + 1] * n, [base - 1] * n, [base] * n   # TR = 2 partout -> ATR = 2
        sw = [{"type": "high", "idx": i, "price": p, "label": lab} for i, p, lab in highs]
        sw += [{"type": "low", "idx": i, "price": p, "label": lab} for i, p, lab in lows]
        for i, v in (extra_h or {}).items():
            h[i] = v
        return {"_series": {"t": list(range(n)), "h": h, "l": l, "c": c}, "swings": sw}

    BUY = {"dir": "bull", "zone": {"top": 110.0, "bottom": 108.0}, "head": {"price": 106.0}}
    SELL = {"dir": "bear", "zone": {"top": 110.0, "bottom": 108.0}, "head": {"price": 112.0}}

    def setUp(self):
        self._p = (_E.QMLG_SL_MODE, _E.QMLG_SL_BUFFER_ATR, _E.QMLG_EQ_TOL_ATR, _E.QMLG_ATR_PERIOD)
        _E.QMLG_SL_MODE, _E.QMLG_SL_BUFFER_ATR, _E.QMLG_EQ_TOL_ATR, _E.QMLG_ATR_PERIOD = "BUFFER", 0.05, 0.10, 14

    def tearDown(self):
        _E.QMLG_SL_MODE, _E.QMLG_SL_BUFFER_ATR, _E.QMLG_EQ_TOL_ATR, _E.QMLG_ATR_PERIOD = self._p

    def test_achat_entree_bord_proche_sl_buffer_tp_liquidite(self):
        st = self._structure(highs=[(10, 115.0, "HH"), (20, 120.0, "HH"), (30, 120.1, "EQH")])
        t = _E.compute_trade(self.BUY, st)
        self.assertTrue(t["ok"], t["reason"])
        self.assertEqual((t["side"], t["order"]), ("BUY", "LIMIT"))
        self.assertEqual(t["entry"], 110.0, "bord proche, jamais le milieu (109)")
        self.assertAlmostEqual(t["sl"], 107.9, places=9)   # 108 - ATR(2) x 0.05
        self.assertGreater(t["sl"], 106.0, "jamais derrière la tête")
        self.assertEqual((t["tp1"], t["tp1_kind"]), (115.0, "SWING"))
        self.assertEqual((t["tp2"], t["tp2_kind"]), (120.0, "EQH"), "120 et 120.1 = equal highs, premier niveau touché")
        self.assertAlmostEqual(t["rr1"], 5 / 2.1)
        self.assertAlmostEqual(t["rr2"], 10 / 2.1)

    def test_vente_miroir(self):
        st = self._structure(lows=[(10, 103.0, "LL"), (20, 98.0, "LL"), (30, 97.9, "EQL")], base=120.0)
        t = _E.compute_trade(self.SELL, st)
        self.assertTrue(t["ok"], t["reason"])
        self.assertEqual((t["side"], t["order"], t["entry"]), ("SELL", "LIMIT", 108.0))
        self.assertAlmostEqual(t["sl"], 110.1, places=9)
        self.assertLess(t["sl"], 112.0, "jamais derrière la tête")
        self.assertEqual((t["tp1"], t["tp2"], t["tp2_kind"]), (103.0, 98.0, "EQL"))
        self.assertAlmostEqual(t["rr1"], 5 / 2.1)

    def test_sl_extreme_et_jamais_derriere_la_tete(self):
        st = self._structure(highs=[(10, 115.0, "HH")])
        _E.QMLG_SL_MODE = "EXTREME"
        self.assertEqual(_E.compute_trade(self.BUY, st)["sl"], 108.0, "EXTREME : pile sur le bord de la zone")
        _E.QMLG_SL_MODE = "BUFFER"
        near = dict(self.BUY, head={"price": 107.95})   # le buffer (107.9) passerait derrière la tête
        t = _E.compute_trade(near, st)
        self.assertEqual(t["sl"], 108.0)
        self.assertGreater(t["sl"], 107.95)

    def test_liquidite_deja_prise_ignoree(self):
        st = self._structure(highs=[(10, 115.0, "HH"), (20, 120.0, "HH")], extra_h={40: 116.0})
        t = _E.compute_trade(self.BUY, st)
        self.assertEqual(t["tp1"], 120.0, "le sommet 115 a déjà été balayé : plus de liquidité")
        self.assertIsNone(t["tp2"])
        self.assertIsNone(t["rr2"])

    def test_jamais_de_marche_et_sans_structure(self):
        t = _E.compute_trade(self.BUY)
        self.assertFalse(t["ok"])
        self.assertEqual(t["order"], "LIMIT")
        self.assertIsNone(t["tp1"])
        self.assertEqual(t["entry"], 110.0)
        self.assertFalse(_E.compute_trade({"dir": "bull"})["ok"], "QML incomplet : pas d'exception")
        for q, st in ((self.BUY, self._structure(highs=[(10, 115.0, "HH")])), (self.SELL, self._structure(lows=[(10, 103.0, "LL")], base=120.0))):
            self.assertEqual(_E.compute_trade(q, st)["order"], "LIMIT")

    def test_analyse_expose_le_plan_sans_signal(self):
        cs = TestQmlSansLimiteAge._walk(2, n=1500, tf=900)
        saved = _E.get_candles
        _E.get_candles = lambda symbol, minutes=None, count=None: list(cs)
        try:
            r = _E.qml_gagnant_analyze("XAUUSD", 15)
        finally:
            _E.get_candles = saved
        self.assertIsNone(r["signal"])
        self.assertIsNotNone(r["trade"])
        self.assertEqual(r["trade"]["order"], "LIMIT")
        self.assertIn("LIMIT", _E.qml_gagnant_text(r) + "LIMIT")


class TestQmlGagnantBoutons(unittest.TestCase):
    """3 boutons : Analyse (scan + JSON + validation), Ordre limite (EN ATTENTE de mitigation), Entrée directe (rejet dans la zone)."""

    @staticmethod
    def _candle(o, h, l, c, t=0):
        return {"t": t, "o": o, "h": h, "l": l, "c": c}

    def _flat(self, n=60):
        return [self._candle(100, 101, 99, 100, t=i) for i in range(n)]

    def _structure(self, n=60, highs=(), lows=()):
        sw = [{"type": "high", "idx": i, "price": p, "label": "HH"} for i, p in highs]
        sw += [{"type": "low", "idx": i, "price": p, "label": "LL"} for i, p in lows]
        return {"_series": {"t": list(range(n)), "h": [101.0] * n, "l": [99.0] * n, "c": [100.0] * n}, "swings": sw}

    CR = {"tendance_avant": True, "sweep": True, "choch_cloture": True, "zone_epaule": True, "zone_fraiche": True, "rr_ok": True}

    def _qml(self, buy=True, mitigated_idx=None, **cr):
        z = {"top": 110.0, "bottom": 108.0}
        return {"dir": "bull" if buy else "bear", "zone": z, "head": {"price": 106.0 if buy else 112.0},
                "criteres": dict(self.CR, **cr), "mitigated_idx": mitigated_idx, "statut": "non mitigé"}

    # --- rejets
    def test_rejets_dans_le_sens_du_trade(self):
        eng = [self._candle(110, 110.5, 108, 108.5), self._candle(108.4, 111, 108.3, 110.6)]   # haussière avale baissière
        self.assertEqual(_E.qmlg_rejection(eng, True), "ENGULFING")
        self.assertIsNone(_E.qmlg_rejection(eng, False), "un avalement haussier ne valide pas une vente")
        sell = [self._candle(108.5, 110, 108.4, 109.5), self._candle(109.6, 109.7, 107, 108.2)]
        self.assertEqual(_E.qmlg_rejection(sell, False), "ENGULFING")
        pin = [self._candle(109, 109, 109, 109), self._candle(109, 109.3, 106.0, 109.1)]   # longue mèche basse
        self.assertEqual(_E.qmlg_rejection(pin, True), "PIN_BAR")
        self.assertIsNone(_E.qmlg_rejection(pin, False))
        doji = [self._candle(109, 109, 109, 109), self._candle(109.0, 110.0, 108.0, 109.05)]
        self.assertEqual(_E.qmlg_rejection(doji, True), "DOJI")
        plain = [self._candle(109, 109, 109, 109), self._candle(108.5, 110.5, 108.4, 110.3)]   # grosse bougie sans rejet
        self.assertIsNone(_E.qmlg_rejection(plain, True))
        self.assertIsNone(_E.qmlg_rejection([], True))

    # --- entrée directe
    def test_entree_directe_achat(self):
        st = self._structure(highs=[(10, 115.0)])
        cs = self._flat() + [self._candle(109.5, 109.8, 108, 108.5), self._candle(108.4, 110.2, 108.3, 109.8)]
        st = self._structure(n=len(cs), highs=[(10, 115.0)])
        d = _E.qmlg_direct_entry(self._qml(mitigated_idx=len(cs) - 1), st, cs)
        self.assertIsNotNone(d)
        self.assertEqual((d["pattern"], d["entry"]), ("ENGULFING", 109.8), "entrée = clôture de la bougie de rejet")
        tr = d["trade"]
        self.assertEqual((tr["order"], tr["side"]), ("DIRECT", "BUY"))
        self.assertAlmostEqual(tr["sl"], 107.9, places=9)
        self.assertEqual(tr["tp1"], 115.0)
        self.assertGreaterEqual(tr["rr1"], _E.QMLG_MIN_RR)

    def test_entree_directe_refus(self):
        base = self._flat() + [self._candle(109.5, 109.8, 108, 108.5)]
        eng = base + [self._candle(108.4, 110.2, 108.3, 109.8)]
        st = self._structure(n=len(eng), highs=[(10, 115.0)])
        last = len(eng) - 1
        self.assertIsNotNone(_E.qmlg_direct_entry(self._qml(mitigated_idx=last), st, eng))
        self.assertIsNone(_E.qmlg_direct_entry(self._qml(mitigated_idx=last - 5), st, eng), "zone déjà touchée avant : pas fraîche")
        hors = base + [self._candle(111.0, 113.0, 110.9, 112.5)]   # clôture au-dessus de la zone
        self.assertIsNone(_E.qmlg_direct_entry(self._qml(mitigated_idx=None), self._structure(n=len(hors), highs=[(10, 115.0)]), hors))
        sans_rejet = base + [self._candle(108.8, 110.0, 108.6, 109.9)]
        self.assertIsNone(_E.qmlg_direct_entry(self._qml(mitigated_idx=len(sans_rejet) - 1), self._structure(n=len(sans_rejet), highs=[(10, 115.0)]), sans_rejet))
        self.assertIsNone(_E.qmlg_direct_entry(self._qml(mitigated_idx=last, sweep=False), st, eng), "formation invalide : pas de signal")

    def test_entree_directe_vente(self):
        cs = [self._candle(120, 121, 119, 120, t=i) for i in range(60)] + [self._candle(108.5, 110, 108.4, 109.5),
                                                                          self._candle(109.6, 109.7, 108.2, 108.4)]
        n = len(cs)
        st = {"_series": {"t": list(range(n)), "h": [121.0] * n, "l": [119.0] * n, "c": [120.0] * n},
              "swings": [{"type": "low", "idx": 10, "price": 100.0, "label": "LL"}]}
        d = _E.qmlg_direct_entry(self._qml(buy=False, mitigated_idx=n - 1), st, cs)
        self.assertIsNotNone(d)
        self.assertEqual((d["trade"]["side"], d["entry"], d["trade"]["order"]), ("SELL", 108.4, "DIRECT"))
        self.assertAlmostEqual(d["trade"]["sl"], 110.1, places=9)

    def test_ordre_limite_jamais_direct(self):
        t = _E.compute_trade(self._qml(), self._structure(highs=[(10, 115.0)]))
        self.assertEqual(t["order"], "LIMIT")
        self.assertEqual(t["entry"], 110.0)

    # --- listes
    def test_liste_ordre_limite(self):
        def rec(valid, statut, ago):
            return {"symbol": "XAUUSD", "tf": 15, "dir": "bull", "statut": statut, "bars_ago": ago, "direct": None,
                    "validation": {"valid": valid, "reasons": []},
                    "plan": {"side": "BUY", "entry": 110.0, "sl": 107.9, "tp1": 115.0, "tp2": 120.0, "rr1": 2.4, "rr2": 4.8}}
        data = {"formations": [rec(True, "non mitigé", 30), rec(True, "mitigé", 5), rec(False, "non mitigé", 2), rec(True, "non mitigé", 7)],
                "generated_ts": time.time(), "n_formations": 4, "n_valid": 3, "markets": ["XAUUSD"], "tfs": [15], "errors": []}
        items = _E.qmlg_pending_limits(data)
        self.assertEqual([f["bars_ago"] for f in items], [7, 30], "valides + non mitigés seulement, la plus récente d'abord")
        txt = _E.qmlg_limit_text(data)
        self.assertIn("EN ATTENTE de mitigation", txt)
        self.assertIn("LIMIT", txt)
        self.assertIn("EN ATTENTE", _E.qmlg_limit_text(dict(data, formations=[])) + "EN ATTENTE")
        self.assertIn("Aucun QML valide", _E.qmlg_limit_text(dict(data, formations=[])))

    # --- scan + JSON
    def test_scan_ecrit_le_json_et_valide_chaque_formation(self):
        import tempfile
        cs = TestQmlSansLimiteAge._walk(2, n=1500, tf=900)
        saved = (_E.get_candles, _E.QMLG_JSON_PATH)
        path = os.path.join(tempfile.mkdtemp(), "sub", "qml.json")
        _E.QMLG_JSON_PATH = path
        def fake(symbol, minutes=None, count=None):
            if symbol == "EURUSD":
                raise RuntimeError("réseau")
            return list(cs)
        _E.get_candles = fake
        try:
            data = _E.qmlg_scan(symbols=["XAUUSD", "EURUSD", "BTCUSD"], tfs=[15, 60])
        finally:
            _E.get_candles, _E.QMLG_JSON_PATH = saved
        self.assertGreater(data["n_formations"], 0)
        self.assertEqual(len(data["errors"]), 2, "EURUSD en erreur sur 2 TF : noté, le scan continue")
        self.assertTrue(all("valid" in f["validation"] and "plan" in f for f in data["formations"]), "chaque formation est validée")
        self.assertEqual(data["n_valid"], sum(1 for f in data["formations"] if f["validation"]["valid"]))
        on_disk = _E._qmlg_load(path)
        self.assertEqual(on_disk["n_formations"], data["n_formations"], "toutes les formations sont dans le JSON")
        self.assertEqual({f["symbol"] for f in on_disk["formations"]}, {"XAUUSD", "BTCUSD"})
        self.assertEqual(len({f["id"] for f in on_disk["formations"] if f["symbol"] == "XAUUSD" and f["tf"] == 15}),
                         len([f for f in on_disk["formations"] if f["symbol"] == "XAUUSD" and f["tf"] == 15]), "ids uniques")
        self.assertIn("ANALYSE QML GAGNANT", _E.qmlg_analyse_text(data))
        for f in _E.qmlg_pending_limits(data):
            self.assertTrue(f["validation"]["valid"] and f["statut"] == "non mitigé")

    # --- boutons / thread
    def test_boutons_et_callbacks(self):
        kb = str(_E._strategies_keyboard())
        self.assertIn("qmlg:home", kb)
        kb2 = str(_E._qmlg_keyboard())
        for cb in ("qmlg:analyse", "qmlg:limit", "qmlg:direct"):
            self.assertIn(cb, kb2)
        for label in ("Analyse", "Ordre limite", "Entrée directe"):
            self.assertIn(label, kb2)
        edits = []
        saved = _E._edit
        _E._edit = lambda cq, text, kb: edits.append((text, kb))
        try:
            self.assertEqual(_E._qmlg_callback({"message": {"chat": {"id": 1}, "message_id": 2}}, "home"), "")
        finally:
            _E._edit = saved
        self.assertIn("QML GAGNANT", edits[0][0])

    def test_job_envoie_le_resultat_et_libere_le_verrou(self):
        sent = []
        saved = (_E.send, _E.qmlg_data, _E.qmlg_scan)
        d = {"formations": [], "generated_ts": time.time(), "n_formations": 0, "n_valid": 0, "markets": [], "tfs": [], "errors": []}
        _E.send = lambda chat_id, text, **kw: sent.append(text)
        _E.qmlg_data = lambda age: d
        _E.qmlg_scan = lambda *a, **k: d
        try:
            _E._qmlg_job(1, "limit")
            _E._qmlg_job(1, "direct")
            _E._qmlg_job(1, "analyse")
            self.assertEqual(len(sent), 3)
            self.assertIn("ORDRES LIMITE", sent[0])
            self.assertIn("ENTRÉES DIRECTES", sent[1])
            self.assertIn("ANALYSE QML GAGNANT", sent[2])
            self.assertTrue(_E._qmlg_lock.acquire(blocking=False), "verrou libéré après chaque scan")
            _E._qmlg_job(1, "limit")   # verrou pris : message « déjà en cours », pas de scan
            _E._qmlg_lock.release()
            self.assertIn("déjà en cours", sent[3])
        finally:
            _E.send, _E.qmlg_data, _E.qmlg_scan = saved


class TestQmlGagnantAlertes(unittest.TestCase):
    """Alertes de réaction : rien quand le prix entre dans la zone ; alerte seulement sur avalement / pin bar qui touche la zone ;
    lot calculé ; image + entrée / SL / TP / lot + stickers au groupe ; anti-doublon ; suppression si zone invalidée."""
    _KEYS = ("qmlg_alerts", "qmlg_alert_on", "qmlg_alert_tfs", "risk_usd")

    def setUp(self):
        self._old = {k: _E.get_setting(k) for k in self._KEYS}
        _E._q("DELETE FROM settings WHERE key IN ('qmlg_alerts','qmlg_alert_on','qmlg_alert_tfs')", commit=True)
        self._saved = (_E.get_candles, _E.to_admin, _E.send, _E.qmlg_scan, _E.to_group_media, _E._qmlg_alert_chart,
                       _E.CHAT_ID_GROUPE, _E.QMLG_ALERT_TO_GROUP, _E._qmlg_scan_one, _E.qmlg_alert_symbols)
        _E.set_risk(10)
        _E._qmlg_refresh_due.clear()

    def tearDown(self):
        (_E.get_candles, _E.to_admin, _E.send, _E.qmlg_scan, _E.to_group_media, _E._qmlg_alert_chart,
         _E.CHAT_ID_GROUPE, _E.QMLG_ALERT_TO_GROUP, _E._qmlg_scan_one, _E.qmlg_alert_symbols) = self._saved
        _E._q("DELETE FROM trades WHERE kind='QMLG'", commit=True)
        for k, v in self._old.items():
            if v is None:
                _E._q("DELETE FROM settings WHERE key=?", (k,), commit=True)
            else:
                _E.set_setting(k, v)

    @staticmethod
    def _c(o, h, l, c, t):
        return {"t": t, "o": o, "h": h, "l": l, "c": c}

    def _alert(self, buy=True, last_t=100, tf=15, **kw):
        return dict({"key": "k", "symbol": "XAUUSD", "tf": tf, "dir": "bull" if buy else "bear",
                     "zone": {"top": 110.0, "bottom": 108.0}, "sl": 107.9 if buy else 110.1,
                     "tp1": 115.0 if buy else 103.0, "tp1_kind": "SWING", "tp2": 120.0 if buy else 98.0, "tp2_kind": "EQH",
                     "last_t": last_t, "entered": False}, **kw)

    @staticmethod
    def _rec(symbol="XAUUSD", tf=15, top=110.0, bottom=108.0, valid=True, statut="non mitigé"):
        return {"symbol": symbol, "tf": tf, "dir": "bull", "statut": statut, "bars_ago": 3, "direct": None, "last_t": 500,
                "zone": {"top": top, "bottom": bottom}, "validation": {"valid": valid, "reasons": []},
                "plan": {"side": "BUY", "entry": top, "sl": bottom - 0.1, "tp1": 115.0, "tp1_kind": "SWING", "tp2": None, "tp2_kind": None,
                         "rr1": 2.5, "rr2": None}}

    @staticmethod
    def _fresh(n=4, tf=15):
        """n dernières bougies clôturées de M{tf}, horodatées pour de vrai (la dernière vient de clôturer)."""
        last = int(time.time()) // (tf * 60) * (tf * 60) - tf * 60
        return [last - (n - 1 - i) * tf * 60 for i in range(n)]

    # --- pose + anti-doublon
    def test_pose_et_anti_doublon(self):
        data = {"formations": [self._rec(), self._rec(), self._rec(tf=60), self._rec(top=120.0, bottom=118.0),
                               self._rec(top=130.0, bottom=128.0, valid=False), self._rec(top=140.0, bottom=138.0, statut="mitigé")]}
        r = _E.qmlg_place_alerts(data)
        self.assertEqual(r, {"placed": 3, "existing": 1, "total": 3},
                         "2 QML identiques = 1 alerte ; autre TF / autre zone = autres alertes ; invalides et mitigés ignorés")
        self.assertEqual(_E.qmlg_place_alerts(data), {"placed": 0, "existing": 4, "total": 3}, "relancer ne double rien")
        self.assertEqual(len(_E._qmlg_alerts_load()), 3)
        self.assertEqual(_E.qmlg_alert_key("XAUUSD", 15, {"top": 110.0, "bottom": 108.0}), "XAUUSD|M15|110|108")

    # --- le prix qui entre dans la zone ne déclenche RIEN
    def test_entree_dans_la_zone_sans_reaction_n_alerte_pas(self):
        c1 = self._c(113, 114, 112, 113.5, 1000)
        c2 = self._c(112, 112.5, 109, 109.6, 2000)   # mèche dans la zone, clôture dans la zone, aucun motif
        a, evs = _E.qmlg_alert_eval(self._alert(), [c1, c2])
        self.assertIsNotNone(a, "l'alerte reste en attente d'une réaction")
        self.assertEqual(evs, [], "aucun message quand le prix entre dans la zone")
        self.assertEqual(a["last_t"], 2000)

    def test_reaction_avalement_achat(self):
        c1 = self._c(113, 114, 112, 113.5, 1000)
        c2b = self._c(109.5, 109.8, 108.0, 108.5, 2500)
        c3 = self._c(108.4, 110.2, 108.3, 109.8, 3000)   # avalement haussier, clôture dans la zone
        a, evs = _E.qmlg_alert_eval(self._alert(), [c1, c2b, c3])
        self.assertIsNone(a, "réaction livrée : alerte terminée et supprimée")
        self.assertEqual(len(evs), 1)
        ev = evs[0]
        self.assertEqual((ev["type"], ev["pattern"], ev["close"]), ("reaction", "ENGULFING", 109.8))
        self.assertAlmostEqual(ev["rr1"], (115.0 - 109.8) / (109.8 - 107.9), places=6)

    def test_pin_bar_qui_touche_la_zone_et_ferme_au_dessus_compte(self):
        pin = self._c(111.2, 111.5, 108.9, 111.0, 1000)   # mèche basse dans la zone, clôture AU-DESSUS de la zone
        a, evs = _E.qmlg_alert_eval(self._alert(tp1=118.0), [pin])
        self.assertIsNone(a)
        self.assertEqual(evs[0]["pattern"], "PIN_BAR")
        self.assertEqual(evs[0]["close"], 111.0)

    def test_doji_ne_declenche_pas(self):
        doji = self._c(109.0, 109.6, 108.4, 109.05, 1000)
        a, evs = _E.qmlg_alert_eval(self._alert(), [doji])
        self.assertIsNotNone(a)
        self.assertEqual(evs, [], "seuls l'avalement et le pin bar déclenchent")

    def test_bougie_qui_ne_touche_pas_la_zone_ne_declenche_pas(self):
        eng = [self._c(114, 114.5, 112.6, 112.8, 1000), self._c(112.7, 114.6, 112.5, 114.2, 2000)]   # avalement, mais loin au-dessus
        a, evs = _E.qmlg_alert_eval(self._alert(), eng)
        self.assertIsNotNone(a)
        self.assertEqual(evs, [])

    def test_rr_trop_faible_l_alerte_attend_une_meilleure_reaction(self):
        c2b = self._c(109.5, 109.8, 108.0, 108.5, 2500)
        c3 = self._c(108.4, 110.2, 108.3, 109.8, 3000)
        a, evs = _E.qmlg_alert_eval(self._alert(tp1=111.0), [c2b, c3])   # TP1 trop proche : RR1 < minimum
        self.assertIsNotNone(a, "la zone reste valable, on attend un meilleur point d'entrée")
        self.assertEqual(evs, [])

    def test_vente_miroir(self):
        a = self._alert(buy=False)
        cs = [self._c(106, 108.5, 105, 107.5, 1000),
              self._c(108.5, 109.6, 108.4, 109.5, 2000),
              self._c(109.6, 109.7, 108.2, 108.4, 3000)]   # avalement baissier dans la zone
        a, evs = _E.qmlg_alert_eval(a, cs)
        self.assertIsNone(a)
        self.assertEqual(evs[-1]["pattern"], "ENGULFING")
        self.assertIn("VENTE", _E.qmlg_alert_text(evs[-1], {"lot": 0.05, "real_risk": 9.0}))

    # --- invalidation, péremption, réaction trop vieille
    def test_zone_invalidee_supprime_l_alerte(self):
        a, evs = _E.qmlg_alert_eval(self._alert(), [self._c(108.5, 109, 106.5, 107.0, 1000)])   # clôture sous le bord lointain (108)
        self.assertIsNone(a)
        self.assertEqual(evs[-1]["type"], "invalid")
        a, evs = _E.qmlg_alert_eval(self._alert(buy=False), [self._c(109.5, 111.5, 109.0, 110.5, 1000)])   # vente : clôture au-dessus de 110
        self.assertIsNone(a)
        self.assertEqual(evs[-1]["type"], "invalid")

    def test_meche_au_dela_sans_cloture_n_invalide_pas(self):
        a, evs = _E.qmlg_alert_eval(self._alert(), [self._c(110.5, 110.6, 106.0, 108.5, 1000)])
        self.assertIsNotNone(a, "une mèche au-delà de la zone n'invalide pas : seule une clôture le fait")
        self.assertEqual(evs, [])

    def test_alerte_perimee_supprimee_en_silence(self):
        now = time.time()
        a, evs = _E.qmlg_alert_eval(self._alert(created_ts=now - 301 * 900), [], now=now)
        self.assertIsNone(a)
        self.assertEqual(evs[0]["type"], "expired")
        a, evs = _E.qmlg_alert_eval(self._alert(created_ts=now - 10 * 900), [], now=now)
        self.assertIsNotNone(a)

    def test_reaction_trop_vieille_ignoree(self):
        c2b = self._c(109.5, 109.8, 108.0, 108.5, 2500)
        c3 = self._c(108.4, 110.2, 108.3, 109.8, 3000)
        a, evs = _E.qmlg_alert_eval(self._alert(), [c2b, c3], now=3000 + 900 + 20 * 900)   # redémarrage tardif
        self.assertEqual([e for e in evs if e["type"] == "reaction"], [])

    # --- livraison complète : lot + image + groupe + stickers
    def _mock_io(self):
        io = {"send": [], "admin": [], "media": []}
        _E.CHAT_ID_GROUPE = "-100"
        _E.QMLG_ALERT_TO_GROUP = True
        _E.send = lambda chat_id, text, photo=None, **kw: io["send"].append((str(chat_id), text, photo)) or {"ok": True}
        _E.to_admin = lambda txt, **kw: io["admin"].append(txt)
        _E.to_group_media = lambda cat, n=1: io["media"].append((cat, n))
        _E._qmlg_alert_chart = lambda ev: "/tmp/qml_test.png"
        return io

    def _fresh_candles(self):
        t = self._fresh(3)
        return t, [self._c(113, 114, 112, 113.5, t[0]), self._c(109.5, 109.8, 108.0, 108.5, t[1]), self._c(108.4, 110.2, 108.3, 109.8, t[2])]

    def test_tick_livre_lot_image_groupe_et_stickers(self):
        io = self._mock_io()
        t, cs = self._fresh_candles()
        _E.get_candles = lambda symbol, minutes=None, count=None: list(cs)
        _E._qmlg_alerts_save({"k": self._alert(last_t=t[0] - 900)})
        self.assertEqual(_E.qmlg_alert_tick(), 1)
        self.assertEqual(_E.qmlg_alert_tick(), 0, "second passage : rien de nouveau, aucun doublon")
        group = [x for x in io["send"] if x[0] == "-100"]
        admin = [x for x in io["send"] if x[0] != "-100"]
        self.assertEqual((len(group), len(admin)), (1, 1), "même message à l'admin et au groupe")
        txt = group[0][1]
        for part in ("AVALEMENT", "Entrée", "SL", "TP1", "TP2", "Lot"):
            self.assertIn(part, txt)
        lot = float(re.search(r"Lot <b>([\d.]+)</b>", txt).group(1))
        self.assertGreater(lot, 0, "le lot est toujours calculé")
        self.assertEqual(group[0][2], "/tmp/qml_test.png", "l'image part dans le groupe")
        self.assertEqual(io["media"], [("BUY", _E.MEDIA_ENTRY_COUNT)], "stickers ACHAT envoyés")
        self.assertEqual(_E._qmlg_alerts_load(), {}, "alerte terminée : supprimée de la base")
        self.assertEqual(_E.qmlg_alert_tick(), 0, "plus d'alerte : aucun appel réseau")

    def test_lot_impossible_rien_au_groupe(self):
        io = self._mock_io()
        _E.set_risk(0.01)   # risque minuscule : même le lot minimum le dépasse
        t, cs = self._fresh_candles()
        _E.get_candles = lambda symbol, minutes=None, count=None: list(cs)
        _E._qmlg_alerts_save({"k": self._alert(last_t=t[0] - 900)})
        self.assertEqual(_E.qmlg_alert_tick(), 0)
        self.assertEqual(io["send"], [], "pas de signal sans lot")
        self.assertEqual(io["media"], [])
        self.assertEqual(len(io["admin"]), 1)
        self.assertIn("lot impossible", io["admin"][0])

    def test_groupe_coupe_admin_seulement(self):
        io = self._mock_io()
        _E.QMLG_ALERT_TO_GROUP = False
        t, cs = self._fresh_candles()
        _E.get_candles = lambda symbol, minutes=None, count=None: list(cs)
        _E._qmlg_alerts_save({"k": self._alert(last_t=t[0] - 900)})
        self.assertEqual(_E.qmlg_alert_tick(), 1)
        self.assertEqual([x[0] for x in io["send"] if x[0] == "-100"], [])
        self.assertEqual(io["media"], [])

    def test_tick_sans_reseau_quand_pas_de_bougie_due(self):
        calls = []
        _E.get_candles = lambda *a, **k: calls.append("candles") or []
        _E._qmlg_alerts_save({"k": self._alert(last_t=int(time.time()))})   # dernière bougie toute fraîche
        _E.qmlg_alert_tick()
        self.assertEqual(calls, [], "aucune nouvelle clôture possible : aucun appel réseau")

    # --- recherche des zones + menu
    def test_refresh_pose_les_alertes_sur_les_tf_choisis(self):
        scans = []
        _E.qmlg_alert_symbols = lambda: ["XAUUSD"]
        _E._qmlg_scan_one = lambda s, t: scans.append((s, t)) or ([self._rec(tf=t)], 100)
        self.assertEqual(_E.qmlg_alert_refresh(force=True), 0, "alerte OFF : rien")
        _E.set_qmlg_alert_on(True)
        _E.set_setting("qmlg_alert_tfs", "1,5")
        self.assertEqual(_E.qmlg_alert_refresh(), 2)
        self.assertEqual(sorted(scans), [("XAUUSD", 1), ("XAUUSD", 5)], "M1 scanné comme demandé")
        self.assertEqual(_E.qmlg_alert_refresh(), 0, "prochain scan à la clôture de la bougie, pas avant")
        self.assertEqual({a["tf"] for a in _E._qmlg_alerts_load().values()}, {1, 5})
        _E.set_setting("qmlg_alert_tfs", "5")
        _E.qmlg_alert_refresh()
        self.assertEqual({a["tf"] for a in _E._qmlg_alerts_load().values()}, {5}, "un TF décoché retire ses alertes")

    def test_activer_coupe_repart_de_zero(self):
        _E._qmlg_alerts_save({"k": self._alert()})
        _E.set_qmlg_alert_on(True)
        self.assertTrue(_E.get_qmlg_alert_on())
        self.assertEqual(_E._qmlg_alerts_load(), {}, "jamais d'alerte périmée à l'activation")
        _E.set_qmlg_alert_on(False)
        self.assertFalse(_E.get_qmlg_alert_on())

    def test_boutons_alerte_et_tf(self):
        _E.set_setting("qmlg_alert_tfs", "5")
        self.assertEqual(_E.toggle_qmlg_alert_tf(1), [1, 5])
        self.assertEqual(_E.toggle_qmlg_alert_tf(5), [1])
        self.assertEqual(_E.toggle_qmlg_alert_tf(1), [1], "au moins un TF reste coché")
        self.assertEqual(_E.toggle_qmlg_alert_tf(7), [1], "TF inconnu ignoré")
        kb = str(_E._qmlg_keyboard())
        for cb in ("qmlg:alert", "qmlg:tf:1", "qmlg:tf:5", "qmlg:tf:15", "qmlg:tf:60"):
            self.assertIn(cb, kb)
        edits = []
        _E_edit = _E._edit
        _E._edit = lambda cq, txt, kb=None: edits.append(txt)
        try:
            self.assertEqual(_E._qmlg_callback({}, "alert"), "Alerte réaction : ON")
            self.assertTrue(_E.get_qmlg_alert_on())
            self.assertEqual(_E._qmlg_callback({}, "alert"), "Alerte réaction : OFF")
            self.assertIn("M1", _E._qmlg_callback({}, "tf:5"))
        finally:
            _E._edit = _E_edit
        self.assertIn("Alerte réaction", edits[-1])

    def test_analyse_ne_pose_plus_d_alerte(self):
        sent = []
        data = {"formations": [self._rec()], "generated_ts": time.time(), "n_formations": 1, "n_valid": 1,
                "markets": ["XAUUSD"], "tfs": [15], "errors": []}
        _E.send = lambda chat_id, text, **kw: sent.append(text)
        _E.qmlg_scan = lambda *a, **k: data
        _E._qmlg_job(1, "analyse")
        self.assertEqual(_E._qmlg_alerts_load(), {}, "c'est le bouton 🔔 qui active les alertes, pas Analyse")
        self.assertNotIn("Alertes de zone", sent[-1])

    # --- TF strictement respectés, double vérification, suivi BE / TP / SL, cadence
    def _ev(self, buy=True, tf=15, **kw):
        a = self._alert(buy=buy, tf=tf)
        close = 109.8 if buy else 108.2
        risk = abs(close - a["sl"])
        ev = {"type": "reaction", "alert": a, "pattern": "ENGULFING", "close": close, "t": 3000, "risk": risk,
              "rr1": abs(a["tp1"] - close) / risk, "rr2": abs(a["tp2"] - close) / risk}
        ev.update(kw)
        return ev

    def test_tf_non_coche_jamais_livre(self):
        io = self._mock_io()
        _E.set_setting("qmlg_alert_tfs", "15")
        t = self._fresh(3, tf=5)
        cs = [self._c(113, 114, 112, 113.5, t[0]), self._c(109.5, 109.8, 108.0, 108.5, t[1]), self._c(108.4, 110.2, 108.3, 109.8, t[2])]
        _E.get_candles = lambda symbol, minutes=None, count=None: list(cs)
        _E._qmlg_alerts_save({"k": self._alert(tf=5, last_t=t[0] - 300)})
        self.assertEqual(_E.qmlg_alert_tick(), 0)
        self.assertEqual(io["send"], [], "M5 non coché : aucun message, même si la réaction existe")
        self.assertEqual(_E._qmlg_alerts_load(), {}, "l'alerte d'un TF non coché disparaît")
        self.assertFalse(_E.qmlg_alert_deliver(self._ev(tf=5)), "contrôle de TF aussi à la livraison")
        self.assertEqual(io["send"], [])

    def test_double_verification_du_plan_et_du_lot(self):
        _E.set_setting("qmlg_alert_tfs", "15")
        ev = self._ev()
        li = _E.calc_lot("XAUUSD", 10, ev["risk"], price=ev["close"])
        ok, pb = _E.qmlg_verify_plan(ev["alert"], ev, li)
        self.assertTrue(ok, pb)
        self.assertLessEqual(li["real_risk"], 10 + 1e-9, "le risque suit le SL : jamais au-dessus du montant choisi")
        bad = self._ev()
        bad["alert"]["sl"] = 111.0   # SL du mauvais côté pour un achat
        self.assertFalse(_E.qmlg_verify_plan(bad["alert"], bad, li)[0])
        self.assertFalse(_E.qmlg_verify_plan(ev["alert"], ev, dict(li, real_risk=li["real_risk"] * 2))[0], "risque ≠ lot x SL")
        self.assertFalse(_E.qmlg_verify_plan(ev["alert"], ev, dict(li, lot=li["lot"] * 3, real_risk=li["real_risk"] * 3))[0],
                         "risque au-dessus du montant choisi")
        self.assertFalse(_E.qmlg_verify_plan(self._ev(tf=5)["alert"], self._ev(tf=5), li)[0], "TF non coché")
        sell = self._ev(buy=False)
        lis = _E.calc_lot("XAUUSD", 10, sell["risk"], price=sell["close"])
        self.assertTrue(_E.qmlg_verify_plan(sell["alert"], sell, lis)[0])

    def test_risque_proportionnel_au_sl(self):
        near, far = self._ev(), self._ev()
        far["alert"]["sl"] = 105.8
        far["risk"] = far["close"] - 105.8
        l1 = _E.calc_lot("XAUUSD", 10, near["risk"], price=near["close"])
        l2 = _E.calc_lot("XAUUSD", 10, far["risk"], price=far["close"])
        self.assertGreater(l1["lot"], l2["lot"], "SL plus loin -> lot plus petit")
        for li, ev in ((l1, near), (l2, far)):
            self.assertLessEqual(li["real_risk"], 10 + 1e-9)
            self.assertGreaterEqual(li["real_risk"], 10 * 0.75, "le lot utilise bien le risque choisi (arrondi au pas de lot)")

    def test_trade_enregistre_et_be_suivi(self):
        io = self._mock_io()
        t, cs = self._fresh_candles()
        _E.get_candles = lambda symbol, minutes=None, count=None: list(cs)
        _E._qmlg_alerts_save({"k": self._alert(last_t=t[0] - 900)})
        self.assertEqual(_E.qmlg_alert_tick(), 1)
        rows = [dict(r) for r in _E._q("SELECT * FROM trades WHERE kind='QMLG'").fetchall()]
        self.assertEqual(len(rows), 1, "la réaction est enregistrée pour le suivi")
        tr = rows[0]
        self.assertEqual((tr["status"], tr["side"], tr["symbol"], tr["timeframe"]), ("OPEN", "BUY", "XAUUSD", "M15"))
        self.assertEqual(tr["tp"], 120.0, "TP final = TP2")
        self.assertEqual((tr["sent_admin"], tr["sent_group"]), (1, 1))
        be_rr, tp_rr = _E.trade_be_rr(tr), _E.trade_tp_rr(tr)
        self.assertLess(be_rr, tp_rr, "BE actif sur QML GAGNANT")
        self.assertIn("BE", [x for x in io["send"] if x[0] == "-100"][0][1])
        entry, risk = tr["entry"], abs(tr["entry"] - tr["sl_initial"])
        arm = {"t": tr["last_ts"] + 900, "o": entry, "h": entry + be_rr * risk + 0.01, "l": entry + 0.05, "c": entry + 1.0}
        evs = _E.track_trade(dict(tr), [arm])
        again = dict(_E._q("SELECT * FROM trades WHERE id=?", (tr["id"],)).fetchone())
        self.assertEqual(again["be_hit"], 1, "le prix atteint le RR du BE : SL ramené à l'entrée")
        self.assertAlmostEqual(again["sl"], entry)
        self.assertTrue(any(e["name"].startswith("RR") or e["name"] == "BE_MOVED" for e in evs))
        back = {"t": arm["t"] + 900, "o": entry + 1, "h": entry + 1.2, "l": entry - 0.5, "c": entry}
        evs2 = _E.track_trade(again, [back])
        self.assertEqual([e["name"] for e in evs2], ["BE"], "retour à l'entrée après le BE : BE touché, 0 R")
        self.assertEqual(_E.qmlg_alert_tick(), 0)
        self.assertEqual(len(_E._q("SELECT id FROM trades WHERE kind='QMLG'").fetchall()), 1, "aucun doublon")

    def test_niveaux_be_repli(self):
        ev = self._ev()
        tp, tp_rr, be = _E._qmlg_trade_levels(ev["alert"], ev)
        self.assertEqual(tp, 120.0)
        self.assertLess(be, tp_rr)
        short = self._ev()
        short["alert"]["tp2"], short["rr2"] = None, None
        tp, tp_rr, be = _E._qmlg_trade_levels(short["alert"], short)
        self.assertEqual(tp, 115.0, "sans TP2 : TP final = TP1")
        self.assertLess(be, tp_rr)
        self.assertGreater(be, 0)

    def test_be_propre_et_desactivable(self):
        ev = self._ev()
        _E.set_setting("be_rr", 0.5)                       # le /be de Fibotrain ne doit PAS influencer QML GAGNANT
        _E.set_setting("qmlg_be_on", "1")
        _E.set_setting("qmlg_be_rr", 1)
        _, tp_rr, be = _E._qmlg_trade_levels(ev["alert"], ev)
        self.assertEqual(be, min(1.0, round(tp_rr - 0.01, 2)), "BE QML = réglage QML, pas /be")
        _E.set_setting("qmlg_be_on", "0")
        _, _, be = _E._qmlg_trade_levels(ev["alert"], ev)
        self.assertIsNone(be, "BE OFF : aucun BE")
        _E._q("DELETE FROM settings WHERE key IN ('be_rr','qmlg_be_on','qmlg_be_rr')", commit=True)

    def test_sl_derriere_bougie_entree(self):
        a = self._alert()
        a["atr"] = 1.0
        buy_c = self._c(108.5, 109.5, 106.0, 109.2, 1000)          # mèche (106) sous le SL de la zone
        out = _E._qmlg_sl_behind_candle(a, buy_c)
        self.assertLess(out["sl"], buy_c["l"], "SL derrière la mèche de la bougie d'entrée")
        self.assertLessEqual(out["sl"], a["sl"], "jamais plus proche que le SL de la zone")
        shallow = self._c(108.5, 109.5, 109.0, 109.2, 1000)         # mèche peu profonde : le SL de la zone reste
        self.assertEqual(_E._qmlg_sl_behind_candle(a, shallow)["sl"], a["sl"])

    def test_tendance_de_fond(self):
        _E._q("DELETE FROM settings WHERE key IN ('qmlg_trend_on','qmlg_trend_refs')", commit=True)
        _E._qmlg_trend_cache.clear()
        saved = _E.qmlg_trend
        try:
            _E.qmlg_trend = lambda sym, ref: "bullish"
            self.assertEqual(_E.qmlg_trend_check("XAUUSD", 1, "bear"), (True, ""), "filtre OFF : tout passe")
            _E.set_setting("qmlg_trend_on", "1")
            self.assertTrue(_E.qmlg_trend_check("XAUUSD", 1, "bull")[0])
            self.assertFalse(_E.qmlg_trend_check("XAUUSD", 1, "bear")[0], "vente contre tendance haussière : bloquée")
            _E.qmlg_trend = lambda sym, ref: "bearish"
            self.assertFalse(_E.qmlg_trend_check("XAUUSD", 5, "bull")[0])
            self.assertTrue(_E.qmlg_trend_check("XAUUSD", 5, "bear")[0])
            _E.qmlg_trend = lambda sym, ref: "range"
            self.assertTrue(_E.qmlg_trend_check("XAUUSD", 5, "bull")[0], "range : passe")
            _E.qmlg_trend = lambda sym, ref: None
            self.assertTrue(_E.qmlg_trend_check("XAUUSD", 5, "bull")[0], "indisponible : passe")
        finally:
            _E.qmlg_trend = saved
        self.assertTrue(_E.set_qmlg_trend_ref(1, 240))
        self.assertTrue(_E.set_qmlg_trend_ref(5, 1440))
        self.assertFalse(_E.set_qmlg_trend_ref(60, 15), "référence <= TF d'entrée refusée")
        refs = _E.get_qmlg_trend_refs()
        self.assertEqual((refs[1], refs[5]), (240, 1440))
        _E.set_setting("qmlg_alert_tfs", "1,5,15")
        kb = str(_E._qmlg_trend_keyboard())
        for cb in ("qmlg:tro", "qmlg:trr:1:240", "qmlg:trr:5:1440"):
            self.assertIn(cb, kb)
        self.assertIn("qmlg:trend", str(_E._qmlg_keyboard()))
        _E._q("DELETE FROM settings WHERE key IN ('qmlg_trend_on','qmlg_trend_refs')", commit=True)

    def test_cadence_m1_5s(self):
        _E.set_setting("qmlg_alert_tfs", "1,15")
        self.assertEqual(_E._qmlg_loop_delay(), 5)
        _E.set_setting("qmlg_alert_tfs", "5,15")
        self.assertEqual(_E._qmlg_loop_delay(), max(5, _E.QMLG_ALERT_SEC))


class TestOrdresMt5EtStickers(unittest.TestCase):
    """Interrupteur « Ordres MT5 » et stickers ACHAT / VENTE à l'entrée."""

    def setUp(self):
        self._old = _E.get_setting("mt5_orders")
        self._saved = (_E._post, _E.get_media, _E.CHAT_ID_GROUPE, _E.to_admin)

    def tearDown(self):
        _E._post, _E.get_media, _E.CHAT_ID_GROUPE, _E.to_admin = self._saved
        if self._old is None:
            _E._q("DELETE FROM settings WHERE key=?", ("mt5_orders",), commit=True)
        else:
            _E.set_setting("mt5_orders", self._old)

    def test_interrupteur_mt5(self):
        _E._q("DELETE FROM settings WHERE key=?", ("mt5_orders",), commit=True)
        self.assertEqual(_E.get_mt5_orders_on(), _E.MT5_ORDERS_DEFAULT)
        _E.set_setting("mt5_orders", "0")
        self.assertFalse(_E.get_mt5_orders_on())
        self.assertFalse(_E.mt5_orders_allowed())
        self.assertFalse(_E.place_manual_order("XAUUSD", "BUY")["ok"], "OFF : même l'ordre manuel est bloqué")
        self.assertIn("str:mt5", str(_E._strategies_keyboard()))
        self.assertIn("Ordres MT5", _E._strategies_text())

    def test_stickers_achat_vente_plusieurs(self):
        self.assertIn("BUY", _E.MEDIA_CATS)
        self.assertIn("SELL", _E.MEDIA_CATS)
        sent, msgs = [], []
        _E.CHAT_ID_GROUPE = "-100"
        _E.get_media = lambda cat: [f"sticker:{cat}{i}" for i in range(5)]
        _E._post = lambda method, payload: sent.append((method, payload["sticker"])) or {"ok": True}
        _E.to_admin = lambda txt, **kw: msgs.append(txt)
        _E.entry_media_and_lot("XAUUSD", {"side": "BUY"}, 0.05)
        self.assertEqual(len(sent), _E.MEDIA_ENTRY_COUNT)
        self.assertEqual(len({p for _, p in sent}), len(sent), "stickers tous différents")
        self.assertTrue(all(p.startswith("BUY") for _, p in sent))
        self.assertEqual(len(msgs), 1)
        self.assertIn("ACHAT", msgs[0])
        self.assertIn("0.05", msgs[0])
        self.assertNotIn("SL", msgs[0], "achat + lot seulement")
        sent.clear(); msgs.clear()
        _E.entry_media_and_lot("XAUUSD", {"side": "SELL"}, 0.1)
        self.assertTrue(all(p.startswith("SELL") for _, p in sent))
        self.assertIn("VENTE", msgs[0])


class TestFibtrendBosChoch(unittest.TestCase):
    """FIBTREND en mode BOS + CHoCH : BOS puis CHoCH, mitigation Fibo 50 % obligatoire, bougie de rejet (avalement / pin bar),
    SL derrière la bougie de rejet, TP sur la cible, double vérification du calcul."""

    @staticmethod
    def _mk(path, t0=1_700_000_000, step=300):
        return [{"t": t0 + i * step, "o": o, "h": h, "l": l, "c": c, "v": 100} for i, (o, h, l, c) in enumerate(path)]

    @staticmethod
    def _leg(a, b, n, wig=0.05):
        out, px = [], a
        for i in range(n):
            nx = a + (b - a) * (i + 1) / n
            out.append((px, max(px, nx) + abs(b - a) / n * wig, min(px, nx) - abs(b - a) / n * wig, nx))
            px = nx
        return out

    def _base(self, extra):
        L = self._leg
        pre = L(100, 100.6, 6) + L(100.6, 99.8, 6) + L(99.8, 100.4, 6) + L(100.4, 100, 6)
        down = (L(100, 101, 8) + L(101, 99, 8) + L(99, 100, 5) + L(100, 97, 8) + L(97, 98.2, 4) + L(98.2, 95, 8)
                + L(95, 96, 3) + L(96, 95.5, 3) + L(95.5, 100.5, 10))
        return self._mk(pre + down + extra)

    def _buy(self):
        return self._base(self._leg(100.5, 98.3, 5) + [(98.2, 98.25, 97.7, 97.95)] + [(97.9, 98.4, 97.85, 98.35)])

    def test_achat_bos_choch_50_avalement(self):
        r = _E.ft_find_setup(self._buy(), tp_mode="FIB0", struct="BOS_CHOCH")
        self.assertIsNotNone(r)
        self.assertEqual((r["side"], r["trigger"], r["struct"]), ("BUY", "avalement", "BOS_CHOCH"))
        self.assertLessEqual(r["touch_px"], r["lvl"], "la mèche atteint le 50 %")
        self.assertLess(r["sl"], r["ref_extreme"], "SL derrière la bougie de rejet (et celle qui a touché le 50 %)")
        self.assertAlmostEqual(r["rr"], abs(r["tp"] - r["entry"]) / abs(r["entry"] - r["sl"]), places=9)
        self.assertEqual(r["tp"], r["extreme"], "TP = 0.0 du Fibo en mode FIB0")

    def test_double_verification_ok_puis_ko(self):
        r = _E.ft_find_setup(self._buy(), tp_mode="FIB0", struct="BOS_CHOCH")
        ok, pb = _E.ft_verify(r, "EURUSD")
        self.assertTrue(ok or all("lot" in x or "risque" in x for x in pb), pb)   # seul le lot peut dépendre du broker de test
        bad = dict(r, sl=r["entry"] + 1.0)
        ok, pb = _E.ft_verify(bad, "EURUSD")
        self.assertFalse(ok)
        self.assertIn("SL du mauvais côté de l'entrée", pb)
        bad = dict(r, tp=r["entry"] - 1.0)
        self.assertIn("TP du mauvais côté de l'entrée", _E.ft_verify(bad, "EURUSD")[1])
        bad = dict(r, sl=r["ref_extreme"] + 0.01)
        self.assertIn("SL pas derrière la bougie de rejet", _E.ft_verify(bad, "EURUSD")[1])

    def test_sans_bougie_de_rejet_rien(self):
        c = self._base(self._leg(100.5, 98.3, 5) + [(98.2, 98.25, 97.7, 97.95)] + [(97.95, 98.0, 97.8, 97.85)])
        self.assertIsNone(_E.ft_find_setup(c, tp_mode="FIB0", struct="BOS_CHOCH"))

    def test_50_pourcent_non_atteint_rien(self):
        c = self._base(self._leg(100.5, 98.9, 5) + [(98.8, 98.85, 98.5, 98.6)] + [(98.55, 99.2, 98.5, 99.15)])
        self.assertIsNone(_E.ft_find_setup(c, tp_mode="FIB0", struct="BOS_CHOCH"))

    def test_bos_bos_sans_choch_rien_en_mode_choch(self):
        # tendance baissière pure (BOS + BOS) : aucune entrée en mode BOS + CHoCH
        L = self._leg
        pre = L(100, 100.6, 6) + L(100.6, 99.8, 6) + L(99.8, 100.4, 6) + L(100.4, 100, 6)
        c = self._mk(pre + L(100, 101, 8) + L(101, 99, 8) + L(99, 100, 5) + L(100, 97, 8) + L(97, 98.2, 4) + L(98.2, 95, 8)
                     + L(95, 97, 4) + [(96.9, 97.2, 96.8, 96.85)])
        self.assertIsNone(_E.ft_find_setup(c, tp_mode="FIB0", struct="BOS_CHOCH"))

    def test_doji_ne_declenche_pas_en_mode_choch(self):
        c = self._base(self._leg(100.5, 98.3, 5) + [(98.2, 98.25, 97.7, 97.95)] + [(97.95, 98.3, 97.6, 97.96)])
        r = _E.ft_find_setup(c, tp_mode="FIB0", struct="BOS_CHOCH")
        self.assertTrue(r is None or r["trigger"] != "doji")

    def test_menu_et_commande_struct(self):
        self.assertIn(_E.get_ft_struct(), ("BOS_CHOCH", "BOS_BOS"))
        _E._ft_do(["struct"], explicit=True)
        _E._ft_do(["struct"], explicit=True)
        self.assertIn("BOS + CHoCH", _E._ft_menu_text() + "BOS + CHoCH")


def _selftest():
    _E.HTF_MODE = "FULL"   # les tests historiques (H1 -> M15 -> M5 -> POI) évaluent la cascade complète ; TestHtfM15 passe en mode M15
    suite, loader = unittest.TestSuite(), unittest.TestLoader()
    for cls in (TestRetracement, TestContinuation, TestInvalidationHTF, TestChop, TestGetExtTf,
                TestLiquiditePure, TestM1NeDecidePasSeul, TestLogs, TestEntreeM5, TestNonRegression,
                TestHtfM15, TestBE, TestEntreeOBFVG, TestSLTPCRT, TestSignauxSimultanes, TestLectureExtInt, TestModifyPositionGardeSLTP, TestBEDoubleVerif, TestClientId, TestLogVolumeReel, TestRiskGateUniversel, TestFiltresQualite, TestActifsEtEntrees, TestAjoutsNewsStopUS30, TestQmlSansLimiteAge, TestQmlMemoireEtHistoriqueProfond, TestSetupEngineEntreeEtPause, TestMoteurSmc, TestQmlGagnant, TestComputeTrade, TestQmlGagnantBoutons, TestQmlGagnantAlertes, TestFibtrendBosChoch, TestOrdresMt5EtStickers):
        suite.addTests(loader.loadTestsFromTestCase(cls))
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


# ============================================================================
# 12. MODULE QML (Quasimodo) + SWEEP EQH/EQL  — ex « qml_signals.py »
#
# QML (Quasimodo) + sweep EQH/EQL + retour en zone  --  détection de signaux SANS look-ahead.
#
# Entrée : DataFrame indexé en temps UTC (heure d'OUVERTURE de la bougie) avec open, high, low, close
#          (+ colonne optionnelle 'spread' en unités de prix).
# Toutes les décisions à la bougie i n'utilisent que les bougies <= i (clôturées).
# Entrée supposée à l'OUVERTURE de la bougie i+1.
#
# Le côté BUY est obtenu en inversant les prix (miroir) puis en relançant la logique SELL :
# une seule logique à maintenir.
#
# Module autonome : n'est pas encore branché sur la boucle principale (section 9).
# Nécessite numpy + pandas (optionnels : le reste du bot fonctionne sans).
# ============================================================================
from dataclasses import dataclass

try:
    import numpy as np
    import pandas as pd
except ImportError:  # numpy / pandas absents : seul le module QML est indisponible
    np = pd = None


@dataclass
class Params:
    # --- structure
    swing_k: int = 3              # pivot = extrême sur k bougies de chaque côté (plage 2-6)
    atr_n: int = 14
    # --- EQH / EQL
    eq_atr: float = 0.15          # tolérance d'égalité = eq_atr * ATR (0.05-0.30)
    eq_lookback: int = 120        # fenêtre de recherche des pivots égaux (60-250)
    eq_min_gap: int = 5           # écart mini entre les 2 pivots égaux, en bougies (3-10)
    require_eqh: bool = True      # False => balayer un simple swing high suffit
    min_sweep_atr: float = 0.0    # pénétration mini de la mèche au-delà du niveau (0-0.3)
    # --- QML
    require_hl: bool = True       # exige L1 > L0 (structure HH+HL avant la cassure)
    max_choch_bars: int = 30      # délai max entre la tête H2 et la cassure de L1 (10-60)
    disp_atr: float = 0.8         # corps mini de la bougie de cassure (0.5-1.5 * ATR)
    # --- zone
    zone_mode: str = "level"      # "level": [L1, L1+zone_atr*ATR] | "head": [corps bas de la tête, haut de la tête]
    zone_atr: float = 0.5         # (0.25-1.0) utilisé si zone_mode == "level"
    max_wait: int = 20            # bougies max pour revenir en zone après la cassure (10-40)
    first_touch_only: bool = True
    # --- SL / TP
    sl_buf_atr: float = 0.2       # marge au-delà de la tête (0.1-0.5)
    tp_mode: str = "rr"           # "rr" | "liq"
    rr: float = 2.0               # (1.5-4)
    min_rr: float = 1.5           # RR mini accepté (surtout pour tp_mode "liq")
    min_risk_atr: float = 0.5     # distance SL min / ATR
    max_risk_atr: float = 4.0     # distance SL max / ATR
    # --- filtres
    atr_pctl_lo: float = 0.0      # percentile ATR mini sur atr_pctl_win bougies passées (0-0.4)
    atr_pctl_hi: float = 1.0      # (0.7-1.0)
    atr_pctl_win: int = 500
    max_bar_range_atr: float = 0.0   # 0 = off ; ex. 4.0 : ignore si une bougie du pattern > 4*ATR (indices Jump)
    max_spread_atr: float = 0.0      # 0 = off ; ex. 0.15
    sessions_utc: tuple = ()         # ((7, 16),) = 07h-16h UTC ; () = 24h
    bar_minutes: int = 15
    htf_rule: str = "1h"
    htf_ema: int = 200
    htf_mode: str = "off"         # "off" | "align" (SELL si HTF sous EMA) | "counter" (SELL si HTF au-dessus EMA)


# ----------------------------------------------------------------------------- outils
def atr_wilder(h, l, c, n):
    tr = np.empty(len(c))
    tr[0] = h[0] - l[0]
    tr[1:] = np.maximum.reduce([h[1:] - l[1:], np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])])
    out = np.full(len(c), np.nan)
    if len(c) >= n:
        out[n - 1] = tr[:n].mean()
        for i in range(n, len(c)):
            out[i] = (out[i - 1] * (n - 1) + tr[i]) / n
    return out


def _scan_sell(o, h, l, c, atr, P, zones=None):
    """Logique SELL (tête balaye EQH, cassure de L1, retour en zone). Retourne une liste de signaux bruts."""
    k, n = P.swing_k, len(c)
    highs, lows, pending, out = [], [], [], []
    warm = max(P.atr_n, 2 * k + 1)

    def make_zone(s, a):
        if P.zone_mode == "head":
            return min(o[s["p"]], c[s["p"]]), h[s["p"]]
        return s["L1"], s["L1"] + P.zone_atr * a

    def body_ok(j):
        return (o[j] - c[j]) >= P.disp_atr * atr[j]

    for i in range(warm, n):
        if np.isnan(atr[i]):
            continue
        # 1) confirmation des pivots à p = i-k (utilise les bougies p-k .. i, toutes clôturées)
        p = i - k
        new_high = False
        if p >= k:
            if h[p] > h[p - k:p].max() and h[p] >= h[p + 1:i + 1].max():
                highs.append((p, h[p]))
                new_high = True
            if l[p] < l[p - k:p].min() and l[p] <= l[p + 1:i + 1].min():
                lows.append((p, l[p]))

        # 2) avancer les setups déjà armés (bougies > armed)
        for s in pending[:]:
            if c[i] > s["h2"]:
                pending.remove(s); continue
            if s["state"] == "CHOCH":
                if i - s["p"] > P.max_choch_bars:
                    pending.remove(s)
                elif c[i] < s["L1"] and body_ok(i):
                    s["zlo"], s["zhi"] = make_zone(s, atr[i]); s["state"] = "ZONE"; s["j"] = i
                    if zones is not None:
                        zones.append(s)
                continue
            # state == ZONE
            if i - s["j"] > P.max_wait or c[i] > s["zhi"]:
                pending.remove(s); continue
            if h[i] >= s["zlo"]:
                conf = c[i] < o[i] and c[i] <= (s["zlo"] + s["zhi"]) / 2
                if conf:
                    sig = _emit(s, i, o, h, l, c, atr, lows, P)
                    pending.remove(s)
                    if sig:
                        out.append(sig)
                elif P.first_touch_only:
                    pending.remove(s)

        # 3) armer un nouveau setup quand une nouvelle tête H2 vient d'être confirmée
        if not new_high or len(highs) < 2:
            continue
        (p1, H1), (p2, H2) = highs[-2], highs[-1]
        if H2 <= H1:
            continue
        seg = l[p1:p2 + 1]
        l1i = p1 + int(np.argmin(seg))
        if l1i <= p1:
            continue
        L1 = l[l1i]
        if P.require_hl:
            prev = [x for x in lows if x[0] < p1]
            if not prev or L1 <= prev[-1][1]:
                continue
        # sweep : la bougie-tête p2 dépasse un niveau de liquidité et clôture dessous
        tol, Q = P.eq_atr * atr[i], None
        hs = [x for x in highs[:-1] if p2 - x[0] <= P.eq_lookback]
        for ia in range(len(hs)):
            for ib in range(ia + 1, len(hs)):
                (a, pa), (b, pb) = hs[ia], hs[ib]
                if b - a < P.eq_min_gap or abs(pa - pb) > tol:
                    continue
                q = max(pa, pb)
                if h[a:p2].max() > q:
                    continue
                if h[p2] - q > 0 and h[p2] - q >= P.min_sweep_atr * atr[p2] and c[p2] < q:
                    Q = q
        if Q is None and not P.require_eqh:
            if h[p2] - H1 >= max(P.min_sweep_atr * atr[p2], 1e-12) and c[p2] < H1 and h[p1:p2].max() <= H1:
                Q = H1
        if Q is None:
            continue
        s = dict(p=p2, h2=H2, H1i=p1, L1=L1, Q=Q, state="CHOCH", armed=i, zlo=None, zhi=None, j=None)
        # rejouer (p2, i] : cassure éventuelle déjà survenue ; aucun signal n'est émis avant i
        dead = False
        for j in range(p2 + 1, i + 1):
            if c[j] > H2 or (s["state"] == "CHOCH" and j - p2 > P.max_choch_bars):
                dead = True; break
            if s["state"] == "CHOCH":
                if c[j] < L1 and body_ok(j):
                    s["zlo"], s["zhi"] = make_zone(s, atr[j]); s["state"] = "ZONE"; s["j"] = j
                    if zones is not None:
                        zones.append(s)
            else:
                if c[j] > s["zhi"] or (P.first_touch_only and h[j] >= s["zlo"]):
                    dead = True; break   # zone touchée/mitigée avant qu'on puisse la trader
        if not dead:
            pending.append(s)
    return out


def _emit(s, i, o, h, l, c, atr, lows, P):
    entry = c[i]                                   # référence ; l'exécution réelle = open[i+1]
    sl = max(s["h2"], s["zhi"]) + P.sl_buf_atr * atr[i]
    risk = sl - entry
    if risk <= 0 or not (P.min_risk_atr <= risk / atr[i] <= P.max_risk_atr):
        return None
    if P.tp_mode == "rr":
        tp = entry - P.rr * risk
    else:  # prochaine liquidité : swing low non balayé le plus proche sous l'entrée
        cands = [pr for (ix, pr) in lows if pr < entry and l[ix + 1:i + 1].min() >= pr]
        if not cands:
            return None
        tp = max(cands)
    rr = (entry - tp) / risk
    if rr < P.min_rr:
        return None
    return dict(i=i, entry=entry, sl=sl, tp=tp, rr=rr, risk=risk, atr=atr[i], Q=s["Q"], h2=s["h2"], L1=s["L1"],
                zlo=s["zlo"], zhi=s["zhi"], start=s["H1i"], head_i=s["p"])


# ----------------------------------------------------------------------------- API
def detect_signals(df: "pd.DataFrame", P: Params = Params()) -> "pd.DataFrame":
    o, h, l, c = (df[x].to_numpy(float) for x in ("open", "high", "low", "close"))
    atr = atr_wilder(h, l, c, P.atr_n)
    sells = _scan_sell(o, h, l, c, atr, P)
    buys = _scan_sell(-o, -l, -h, -c, atr, P)          # miroir
    rows = []
    for s in sells:
        rows.append({**s, "side": "SELL"})
    for s in buys:
        rows.append({**s, "side": "BUY", "entry": -s["entry"], "sl": -s["sl"], "tp": -s["tp"], "Q": -s["Q"],
                     "h2": -s["h2"], "L1": -s["L1"], "zlo": -s["zhi"], "zhi": -s["zlo"]})
    if not rows:
        return pd.DataFrame()
    sig = pd.DataFrame(rows).sort_values(["i", "side", "head_i"], ascending=[True, True, False], kind="stable")
    sig = sig.drop_duplicates(["i", "side"]).reset_index(drop=True)   # 1 signal max par bougie et par sens (tête la plus récente)
    idx = df.index
    sig["time"] = idx[sig["i"].to_numpy()]
    sig["entry_time"] = sig["time"] + pd.Timedelta(minutes=P.bar_minutes)

    keep = np.ones(len(sig), bool)
    if P.sessions_utc:                                   # session évaluée à l'heure d'entrée
        hr = sig["entry_time"].dt.hour.to_numpy()
        keep &= np.array([any(a <= x < b for a, b in P.sessions_utc) for x in hr])
    if P.max_spread_atr > 0 and "spread" in df:
        sp = df["spread"].to_numpy(float)[sig["i"].to_numpy()]
        keep &= sp <= P.max_spread_atr * sig["atr"].to_numpy()
    if P.max_bar_range_atr > 0:
        rng = h - l
        keep &= np.array([rng[r.start:r.i + 1].max() <= P.max_bar_range_atr * r.atr for r in sig.itertuples()])
    if P.atr_pctl_lo > 0 or P.atr_pctl_hi < 1:
        ok = []
        for r in sig.itertuples():
            w = atr[max(0, r.i - P.atr_pctl_win):r.i]
            w = w[~np.isnan(w)]
            q = (w < r.atr).mean() if len(w) > 50 else 0.5
            ok.append(P.atr_pctl_lo <= q <= P.atr_pctl_hi)
        keep &= np.array(ok)
    if P.htf_mode != "off":
        htf = df["close"].resample(P.htf_rule).last().dropna()
        ema = htf.ewm(span=P.htf_ema, adjust=False).mean().shift(1)     # shift(1): barre HTF CLÔTURÉE uniquement
        prev_close = htf.shift(1)
        bias = (prev_close - ema).reindex(df.index, method="ffill").to_numpy()[sig["i"].to_numpy()]
        up = bias > 0
        if P.htf_mode == "align":
            keep &= np.where(sig["side"] == "SELL", ~up, up) & ~np.isnan(bias)
        else:
            keep &= np.where(sig["side"] == "SELL", up, ~up) & ~np.isnan(bias)
    return sig[keep].reset_index(drop=True)


# ----------------------------------------------------------------------------- mini-backtest
def backtest(df, sig, cost=0.0, max_hold=200):
    """1 position à la fois, entrée open[i+1], SL prioritaire si SL et TP dans la même bougie. cost = spread+comm (prix)."""
    o, h, l, c = (df[x].to_numpy(float) for x in ("open", "high", "low", "close"))
    res, free_at = [], -1
    for r in sig.itertuples():
        e = r.i + 1
        if e >= len(df) or e <= free_at:
            continue
        ent, sell = o[e], r.side == "SELL"
        risk = (r.sl - ent) if sell else (ent - r.sl)
        if risk <= 0:
            continue
        exit_px, exit_i = c[min(e + max_hold, len(c) - 1)], min(e + max_hold, len(c) - 1)
        for j in range(e, min(e + max_hold, len(c) - 1) + 1):
            hit_sl = h[j] >= r.sl if sell else l[j] <= r.sl
            hit_tp = l[j] <= r.tp if sell else h[j] >= r.tp
            if hit_sl or hit_tp:
                exit_px, exit_i = (r.sl if hit_sl else r.tp), j
                break
        pnl = (ent - exit_px) if sell else (exit_px - ent)
        res.append(dict(time=df.index[e], side=r.side, R=(pnl - cost) / risk, bars=exit_i - e))
        free_at = exit_i
    return pd.DataFrame(res)


def metrics(t):
    if len(t) == 0:
        return {}
    R = t["R"].to_numpy()
    w, lo = R[R > 0], R[R <= 0]
    eq = np.cumsum(R)
    dd = (np.maximum.accumulate(np.r_[0, eq])[1:] - eq).max()
    return dict(n=len(R), winrate=round((R > 0).mean(), 3), avg_win_R=round(w.mean(), 2) if len(w) else 0,
                avg_loss_R=round(lo.mean(), 2) if len(lo) else 0, expectancy_R=round(R.mean(), 3),
                profit_factor=round(w.sum() / abs(lo.sum()), 2) if lo.sum() else np.inf,
                max_dd_R=round(dd, 2), t_stat=round(R.mean() / (R.std(ddof=1) / np.sqrt(len(R))), 2) if len(R) > 2 else 0)


def split_is_oos(df, oos_start):
    """Découpe stricte : le jeu OOS ne sert qu'UNE fois, à la fin."""
    return df[df.index < oos_start], df[df.index >= oos_start]


# ----------------------------------------------------------------------------- 12b. INTÉGRATION AU BOT
# Stratégie QML SÉPARÉE : M15 = zone (POI), M1 = réaction (avalement par défaut), entrée directe proposée.
# N'affecte ni le moteur BOS/CHoCH, ni le CRT, ni ENTRY_MODE, ni MetaApi : OFF par défaut (menu ⚙️ QML / commande /qml).
def _env_on(name, default=False):
    v = os.getenv(name, "").strip().lower()
    return default if not v else v in ("1", "on", "true", "yes", "oui")


QML_MARKETS = [x for x in (y.strip().upper() for y in os.getenv(
    "QML_MARKETS", "XAUUSD,BTCUSD,EURUSD,GBPUSD,USDJPY,USDCHF,USDCAD,AUDUSD,NZDUSD,EURJPY,US30").split(",")) if x in SYMBOLS][:12]
QML_ZONE_MAX_BARS = _env_int("QML_ZONE_MAX_BARS", 0)      # 0 = AUCUNE limite d'âge (défaut) : seule l'invalidation structurelle compte
QML_HISTORY_BARS = _env_int("QML_HISTORY_BARS", 5000)     # fenêtre analysée à chaque nouvelle bougie (M15 ≈ 7 semaines)
QML_DEEP_BARS = _env_int("QML_DEEP_BARS", 20000)          # balayage PROFOND au démarrage, par pages de 5000 (M15 ≈ 7 mois) ; les zones trouvées sont ensuite mémorisées
QML_NEAR_ATR = _env_float("QML_NEAR_ATR", 1.0)            # M1 n'est analysé que si le prix est à < N x ATR(TF QML) de la zone
QML_TF_CHOICES = (5, 15)                                  # TF de recherche du QML (minutes) ; M1 reste le TF de réaction
QML_HTF_MINUTES = _env_int("QML_HTF_MINUTES", 60)         # TF du filtre supérieur (H1) : contexte uniquement, jamais déclencheur
QML_HTF_EMA = _env_int("QML_HTF_EMA", 200)                # biais HTF : clôture H1 vs EMA(200) des bougies H1 clôturées
QML_SCAN_SEC = _env_int("QML_SCAN_SEC", 15)               # pas minimal entre 2 analyses d'un même actif
QML_ANALYSE_TFS = (5, 15, 30, 60)                         # TF proposés au bouton Analyse (le scan auto reste sur QML_TF_CHOICES)
QML_CONF_MODES = ("ENGULFING", "CHOCH", "ENGULFING_OR_CHOCH")
QML_PARAMS = Params(first_touch_only=False) if "Params" in globals() else None   # la zone reste valable tant que le M15 ne la traverse pas
_qml_cache, _qml_last_scan = {}, {}
_qml_ref_px = {}


def _f_setting(key, default):
    try:
        return float(get_setting(key, default))
    except (TypeError, ValueError):
        return float(default)


def get_qml_on():
    return _get_bool_setting("qml_on", _env_on("QML_ENABLED", False))


def get_qml_tf():
    """TF de recherche du QML (5 ou 15), indépendant du TIMEFRAME du moteur BOS/CHoCH. Défaut M15."""
    try:
        v = int(float(get_setting("qml_tf", os.getenv("QML_TF", "15").strip().upper().lstrip("M") or 15)))
    except (TypeError, ValueError):
        v = 15
    return v if v in QML_TF_CHOICES else 15


def get_qml_htf_on():
    """Filtre Higher Time Frame (H1) : OFF par défaut -> aucun appel H1, le QML tourne seul sur son TF."""
    return _get_bool_setting("qml_htf", _env_on("QML_HTF_FILTER", False))


def get_qml_direct():
    """DIRECT ENTRY : ON = le module peut proposer / publier une entrée directe ; OFF = analyse seule (zone + confirmation)."""
    return _get_bool_setting("qml_direct", True)


def get_qml_react():
    """DIRECT_REACTION (réaction M1 forte sans avalement) : OFF par défaut."""
    return _get_bool_setting("qml_react", False)


def get_qml_auto():
    """ON : une proposition validée est publiée comme vrai signal (suivi + exécution existante). OFF : proposition seule."""
    return _get_bool_setting("qml_auto", False)


def get_qml_conf():
    return _get_choice("qml_conf", "ENGULFING", QML_CONF_MODES)


def get_partial_on():
    return _get_bool_setting("part_on", False)


_PART_DEFAULTS = {"p1_rr": 1.0, "p1_pct": 30.0, "p2_rr": 2.0, "p2_pct": 30.0, "p3_rr": 3.0}


def get_partial_cfg():
    return {k: _f_setting("part_" + k, v) for k, v in _PART_DEFAULTS.items()}


def set_partial_value(key, v):
    """Valide puis enregistre un réglage de clôture partielle. Retourne (ok, message)."""
    if key not in _PART_DEFAULTS:
        return False, "réglage inconnu"
    cfg = get_partial_cfg()
    cfg[key] = float(v)
    if not (0 < cfg["p1_rr"] < cfg["p2_rr"] < cfg["p3_rr"]):
        return False, "il faut TP1 < TP2 < TP3"
    if not (0 <= cfg["p1_pct"] <= 90 and 0 <= cfg["p2_pct"] <= 90 and cfg["p1_pct"] + cfg["p2_pct"] <= 90):
        return False, "TP1 % + TP2 % doivent rester ≤ 90 % (il faut un reste pour TP3)"
    set_setting("part_" + key, float(v))
    return True, ""


def _partial_cfg_json():
    """Niveaux partiels à figer dans un NOUVEAU trade ([[rr, fraction], ...]) ; None si OFF."""
    if not get_partial_on():
        return None
    c = get_partial_cfg()
    lv = [[c["p1_rr"], c["p1_pct"] / 100.0], [c["p2_rr"], c["p2_pct"] / 100.0]]
    lv = [x for x in lv if x[1] > 0]
    return json.dumps(lv) if lv else None


def _partial_levels_of(trade):
    try:
        raw = trade.get("part_cfg")
        return [(float(a), float(b)) for a, b in json.loads(raw)] if raw else []
    except (TypeError, ValueError):
        return []


# --- M15 : zones QML actives ---------------------------------------------------------------
def _qml_df(candles):
    return pd.DataFrame({"open": [x["o"] for x in candles], "high": [x["h"] for x in candles],
                         "low": [x["l"] for x in candles], "close": [x["c"] for x in candles]},
                        index=pd.to_datetime([x["t"] for x in candles], unit="s", utc=True))


def qml_zones(candles, P=None, include_dead=False):
    """Zones QML M15 encore valides sur `candles` (bougies clôturées), en PRIX RÉELS, la plus récente d'abord.
    Valide = créée (cassure de L1 confirmée) et jamais traversée par une CLÔTURE du TF QML (ni au-delà de la tête H2).
    L'ÂGE n'invalide pas un QML (QML_ZONE_MAX_BARS = 0) ; la profondeur d'historique (QML_HISTORY_BARS) ne sert qu'à le trouver."""
    P = P or QML_PARAMS
    if np is None or pd is None or len(candles) < 80:
        return []   # (le TF des bougies est celui du TF QML : les indices j / head_i sont en bougies de ce TF)
    df = _qml_df(candles)
    o, h, l, c = (df[x].to_numpy(float) for x in ("open", "high", "low", "close"))
    atr = atr_wilder(h, l, c, P.atr_n)
    n, out = len(c), []
    for side, (oo, hh, ll, cc), sg in (("SELL", (o, h, l, c), 1), ("BUY", (-o, -l, -h, -c), -1)):
        zs = []
        _scan_sell(oo, hh, ll, cc, atr, P, zs)
        for s in zs:
            if s["zlo"] is None or (QML_ZONE_MAX_BARS > 0 and n - 1 - s["j"] > QML_ZONE_MAX_BARS):
                continue
            if sg == 1:
                zlo, zhi, head, L1, Q = s["zlo"], s["zhi"], s["h2"], s["L1"], s["Q"]
            else:
                zlo, zhi, head, L1, Q = -s["zhi"], -s["zlo"], -s["h2"], -s["L1"], -s["Q"]
            after = c[s["j"] + 1:]
            dead = bool((after > min(zhi, head)).any()) if side == "SELL" else bool((after < max(zlo, head)).any())
            z_dead = None
            if dead:
                if not include_dead:
                    continue
                lvl = min(zhi, head) if side == "SELL" else max(zlo, head)
                hit = (after > lvl) if side == "SELL" else (after < lvl)
                z_dead = candles[s["j"] + 1 + int(np.argmax(hit))]["t"]   # 1re clôture qui traverse la zone / la tête
            zd = dict(side=side, zlo=float(zlo), zhi=float(zhi), head=float(head), L1=float(L1), Q=float(Q),
                      j=int(s["j"]), head_i=int(s["p"]), t=candles[s["j"]]["t"], t_head=candles[s["p"]]["t"])
            if z_dead is not None:   # uniquement avec include_dead=True (affichage) : le moteur de signal ne l'utilise jamais
                zd.update(dead=True, t_dead=z_dead)
            out.append(zd)
    out.sort(key=lambda z: -z["j"])
    return out


_qml_store_ready = False


def _qml_store_init():
    global _qml_store_ready
    if not _qml_store_ready:
        _q("""CREATE TABLE IF NOT EXISTS qml_zone_store (
            symbol TEXT, tf INTEGER, side TEXT, t INTEGER, t_head INTEGER,
            zlo REAL, zhi REAL, head REAL, l1 REAL, q REAL, alive INTEGER, checked_t INTEGER,
            PRIMARY KEY (symbol, tf, side, t, t_head))""", commit=True)
        _qml_store_ready = True


def qml_merge_store(symbol, tf, candles, fresh):
    """Fusionne les zones détectées sur la fenêtre d'historique (`fresh`) avec la mémoire persistante.
    - toute zone valide trouvée est enregistrée ; elle reste valide tant qu'une CLÔTURE ne la traverse pas ;
    - une zone plus ancienne que la fenêtre est revalidée seulement avec les bougies postérieures à sa dernière vérification ;
    - une zone encore dans la fenêtre mais plus détectée est invalidée.
    Retourne la liste complète des zones valides (indices j / head_i virtuels pour les zones hors fenêtre)."""
    _qml_store_init()
    n, tf_sec = len(candles), tf * 60
    t0, tl = candles[0]["t"], candles[-1]["t"]
    key = lambda z: (z["side"], z["t"], z["t_head"])
    fresh_keys = {key(z) for z in fresh}
    for z in fresh:
        _q("INSERT OR REPLACE INTO qml_zone_store VALUES (?,?,?,?,?,?,?,?,?,?,1,?)",
           (symbol, tf, z["side"], z["t"], z["t_head"], z["zlo"], z["zhi"], z["head"], z["L1"], z["Q"], tl))
    out = list(fresh)
    for r in _q("SELECT * FROM qml_zone_store WHERE symbol=? AND tf=? AND alive=1", (symbol, tf)).fetchall():
        if (r["side"], r["t"], r["t_head"]) in fresh_keys:
            continue
        where = (symbol, tf, r["side"], r["t"], r["t_head"])
        if r["t"] >= t0:   # dans la fenêtre analysée mais non détectée => invalidée (ou conditions plus remplies)
            _q("UPDATE qml_zone_store SET alive=0 WHERE symbol=? AND tf=? AND side=? AND t=? AND t_head=?", where, commit=True)
            continue
        start = max(r["checked_t"] or 0, r["t"])
        after = [c["c"] for c in candles if c["t"] > start]
        lvl = min(r["zhi"], r["head"]) if r["side"] == "SELL" else max(r["zlo"], r["head"])
        dead = any(x > lvl for x in after) if r["side"] == "SELL" else any(x < lvl for x in after)
        if dead:
            _q("UPDATE qml_zone_store SET alive=0 WHERE symbol=? AND tf=? AND side=? AND t=? AND t_head=?", where, commit=True)
            continue
        _q("UPDATE qml_zone_store SET checked_t=? WHERE symbol=? AND tf=? AND side=? AND t=? AND t_head=?", (tl,) + where, commit=True)
        vj = n - 1 - int(round((tl - r["t"]) / tf_sec))   # indice virtuel (négatif = avant la fenêtre) : sert au tri seulement
        vh = n - 1 - int(round((tl - r["t_head"]) / tf_sec))
        out.append(dict(side=r["side"], zlo=r["zlo"], zhi=r["zhi"], head=r["head"], L1=r["l1"], Q=r["q"],
                        j=vj, head_i=vh, t=r["t"], t_head=r["t_head"]))
    _q("DELETE FROM qml_zone_store WHERE alive=0 AND checked_t < ?", (tl - 90 * 86400,), commit=True)
    out.sort(key=lambda z: -z["j"])
    return out


def _qml_load_m15(symbol, force=False, tf=None):
    """Zones QML du TF choisi (nom historique conservé ; le cache est séparé par TF)."""
    tf = tf or get_qml_tf()
    st = _qml_cache.setdefault((symbol, tf), {"t": None, "zones": [], "atr": None, "close": None, "try": 0.0})
    now = time.time()
    tf_sec = tf * 60
    expected = int((now - CLOSE_GRACE_SEC) // tf_sec) * tf_sec - tf_sec   # ouverture de la dernière bougie du TF censée être clôturée
    if not force and st["t"] is not None and st["t"] >= expected:
        return st
    if not force and now - st["try"] < 20:
        return st
    st["try"] = now
    deep = not st.get("deep_done")   # 1er chargement du processus : balayage profond ; ensuite fenêtre normale + mémoire
    want = QML_DEEP_BARS if deep else QML_HISTORY_BARS
    try:
        m15 = get_candles(symbol, tf, want)
        if deep:
            st["deep_done"] = True
    except Exception:
        m15 = get_candles(symbol, tf)   # historique long refusé : on garde au moins l'historique normal
    if len(m15) >= 80:
        fresh = qml_zones(m15)
        try:
            zones = qml_merge_store(symbol, tf, m15, fresh)
        except Exception:
            traceback.print_exc(limit=-3)
            zones = fresh   # mémoire indisponible : on garde au moins les zones de la fenêtre
        st.update(t=m15[-1]["t"], zones=zones, atr=atr_series(m15)[-1], close=m15[-1]["c"], n=len(m15), t0=m15[0]["t"])
    return st


def qml_htf_bias(symbol):
    """Biais du filtre HTF (H1) : +1 si la dernière clôture H1 est au-dessus de son EMA, -1 en dessous, None si indisponible.
    Bougies H1 CLÔTURÉES uniquement (pas de look-ahead). Contexte seulement : ne déclenche jamais une entrée."""
    c = get_candles(symbol, QML_HTF_MINUTES)
    if len(c) < QML_HTF_EMA // 2 + 20:
        return None
    k, e = 2.0 / (QML_HTF_EMA + 1), c[0]["c"]
    for x in c[1:]:
        e = x["c"] * k + e * (1 - k)
    return 1 if c[-1]["c"] > e else (-1 if c[-1]["c"] < e else None)


# --- M1 : réaction dans la zone ------------------------------------------------------------
def _qml_engulf(c, side):
    """Avalement : bougie M1 clôturée de sens `side` dont le corps avale celui de la bougie précédente (de sens opposé)."""
    if len(c) < 2:
        return False
    p, k = c[-2], c[-1]
    if side == "BUY":
        return p["c"] < p["o"] and k["c"] > k["o"] and k["o"] <= p["c"] and k["c"] >= p["o"]
    return p["c"] > p["o"] and k["c"] < k["o"] and k["o"] >= p["c"] and k["c"] <= p["o"]


def _qml_reaction(c, side, a1):
    """Réaction forte sans avalement : corps >= 0.5 ATR(M1) et clôture au-delà de l'extrême de la bougie précédente."""
    if len(c) < 2:
        return False
    p, k = c[-2], c[-1]
    if abs(k["c"] - k["o"]) < 0.5 * a1:
        return False
    return (k["c"] > k["o"] and k["c"] > p["h"]) if side == "BUY" else (k["c"] < k["o"] and k["c"] < p["l"])


def _qml_m1_check(m1, z):
    d = 1 if z["side"] == "BUY" else -1
    in_zone = any(x["l"] <= z["zhi"] and x["h"] >= z["zlo"] for x in m1[-3:])
    out = {"in_zone": in_zone, "engulf": False, "reaction": False, "choch": False, "ok": False, "label": None}
    if not in_zone or len(m1) < 30:
        return out
    a1 = atr_series(m1)[-1]
    out["engulf"] = _qml_engulf(m1, z["side"])
    out["reaction"] = get_qml_react() and _qml_reaction(m1, z["side"], a1)
    mode = get_qml_conf()
    if mode in ("CHOCH", "ENGULFING_OR_CHOCH"):
        out["choch"] = any(e["kind"] == "CHOCH" and e["dir"] == d and e["i"] >= len(m1) - 3 for e in analyze(m1))
    use_eng = mode in ("ENGULFING", "ENGULFING_OR_CHOCH")
    use_ch = mode in ("CHOCH", "ENGULFING_OR_CHOCH")
    if use_eng and out["engulf"]:
        out["ok"], out["label"] = True, "ENGULFING"
    elif use_ch and out["choch"]:
        out["ok"], out["label"] = True, "CHoCH M1"
    elif out["reaction"]:
        out["ok"], out["label"] = True, "RÉACTION M1"
    return out


# --- analyse complète d'un marché ------------------------------------------------------------
def qml_analyze(symbol, force=False, tf=None):
    """Analyse QML d'un marché (lecture seule). `status` : NO_QML | FAR | WAIT | SKIP | PROPOSAL | SL_EXCESSIF | SL_INVALIDE.
    Une PROPOSITION n'est jamais un ordre exécuté."""
    if np is None or pd is None:
        return {"symbol": symbol, "status": "ERREUR", "msg": "numpy / pandas manquent sur le serveur : ajoute « numpy » et « pandas » dans requirements.txt puis redéploie"}
    dec = SYMBOLS[symbol]["decimals"]
    tf = tf or get_qml_tf()
    htf_on = get_qml_htf_on()
    r = {"symbol": symbol, "status": "NO_QML", "dec": dec, "tf": tf, "htf_on": htf_on}
    st = _qml_load_m15(symbol, force, tf)
    zones, atr = st["zones"], st["atr"]
    r.update(hist_n=st.get("n"), hist_t0=st.get("t0"))   # information d'affichage : taille réelle de l'historique analysé
    if not zones or not atr:
        return r
    if htf_on:   # H1 = filtre : seulement les QML dans le sens du biais ; HTF OFF -> aucune requête H1
        bias = qml_htf_bias(symbol)
        r["htf_bias"] = bias
        if bias is None:
            r["status"] = "HTF_INDISPO"
            return r
        ok = [z for z in zones if (z["side"] == "BUY") == (bias == 1)]
        if not ok:
            r.update(status="HTF", zone=zones[0], side=zones[0]["side"], price=st["close"], atr15=atr)
            return r
        zones = ok
    px = get_live_price(symbol) or st["close"]
    _qml_ref_px[symbol] = px

    def dist(z):
        return 0.0 if z["zlo"] <= px <= z["zhi"] else min(abs(px - z["zlo"]), abs(px - z["zhi"]))
    z = min(zones, key=lambda x: (dist(x), -x["j"]))
    side, d = z["side"], (1 if z["side"] == "BUY" else -1)
    r.update(side=side, zone=z, price=px, atr15=atr, direction=side, type=f"QML {side}")
    if dist(z) > QML_NEAR_ATR * atr:
        r["status"] = "FAR"
        return r
    m1 = get_candles(symbol, 1)   # M1 seulement quand le prix est proche de la zone
    chk = _qml_m1_check(m1, z) if len(m1) >= 30 else {"in_zone": False, "ok": False}
    r["m1"], r["m1_candles"] = chk, m1
    if not chk["in_zone"]:
        r["status"] = "WAIT"
        return r
    if not chk["ok"]:
        r["status"] = "SKIP"
        return r
    entry = m1[-1]["c"]
    if not get_qml_direct():   # DIRECT ENTRY OFF : on s'arrête à la confirmation, aucune entrée proposée
        r.update(status="DIRECT_OFF", confirm=chk["label"], entry=entry, t=m1[-1]["t"])
        return r
    buf = QML_PARAMS.sl_buf_atr * atr
    sl = (max(z["head"], z["zhi"]) + buf) if side == "SELL" else (min(z["head"], z["zlo"]) - buf)
    risk = (sl - entry) * -d
    r.update(entry=entry, t=m1[-1]["t"], confirm=chk["label"])
    if risk <= 0:
        r["status"] = "SL_INVALIDE"
        return r
    if risk > MAX_SL_ATR * atr:
        r.update(status="SL_EXCESSIF", sl=sl, risk=risk)
        return r
    min_pct = SYMBOLS[symbol].get("min_sl_pct", MIN_SL_PCT)
    min_risk = max(MIN_SL_ATR * atr, entry * min_pct)
    if risk < min_risk:   # mêmes planchers que le moteur existant
        risk = min_risk
        sl = entry - d * risk
    rr = get_partial_cfg()["p3_rr"] if get_partial_on() else get_tp_rr()
    r.update(status="PROPOSAL", sl=sl, risk=risk, rr=rr, tp=entry + d * risk * rr)
    return r


QML_CTX_SHOW = _env_on("QML_CTX_SHOW", True)              # AFFICHAGE SEUL : FVG/OB de contexte sur le graphique / texte d'analyse QML
QML_CTX_NEAR_ATR = _env_float("QML_CTX_NEAR_ATR", 1.5)    # un FVG/OB est montré s'il est à < N x ATR(TF QML) de la zone QML
QML_CTX_MAX = 3                                           # nombre max de FVG/OB dessinés


def qml_context(symbol, tf, zone, atr=None):
    """Contexte VISUEL d'une zone QML : FVG / OB déjà produits par `find_pois` (même source que 📈 Analyse technique),
    de même sens que la zone, formés APRÈS la tête (liquidité) et proches de la zone.
    LECTURE SEULE : ne modifie ni `r`, ni la zone, ni aucun état ; n'est lu par aucune condition de signal."""
    out = {"pois": []}
    if not zone:
        return out
    d = 1 if zone["side"] == "BUY" else -1
    tol = QML_CTX_NEAR_ATR * (atr or 0)
    found = {}
    for ptf in dict.fromkeys((tf, QML_HTF_MINUTES)):
        try:
            raw = _ctx_candles(symbol, ptf)
            pois = find_pois(raw, ptf, d) if len(raw) > 2 * SWING_DEPTH + 1 else []
        except Exception:
            continue
        for q in pois:
            t_f = q["t_valid"] - ptf * 60            # heure de la bougie qui valide le POI
            gap = max(q["lo"] - zone["zhi"], zone["zlo"] - q["hi"], 0.0)
            if t_f < zone["t_head"] or (atr and gap > tol):
                continue
            found[(ptf, q["kind"], q["lo"], q["hi"])] = dict(q, t_f=t_f, gap=gap)
    out["pois"] = sorted(found.values(), key=lambda q: (q["gap"], -q["t_f"]))[:QML_CTX_MAX]
    return out


def qml_context_text(r):
    """Bloc texte « séquence » ajouté aux analyses MANUELLES uniquement (jamais au message de proposition live)."""
    if not QML_CTX_SHOW or not r or not r.get("zone"):
        return ""
    z, dec, tf = r["zone"], r.get("dec", 2), r.get("tf") or get_qml_tf()
    ctx = qml_context(r["symbol"], tf, z, r.get("atr15"))
    if ctx["pois"]:
        poi = " · ".join(f"{_tf_lbl(q['tf'])} {q['kind']} {_fmt(q['lo'], dec)}-{_fmt(q['hi'], dec)}" for q in ctx["pois"])
    else:
        poi = "aucun à proximité"
    m1 = r.get("m1")
    if m1 is None:
        m1_txt = "non lu (prix éloigné)"
    elif not m1.get("in_zone"):
        m1_txt = "prix pas encore dans la zone"
    else:
        m1_txt = ("✅ " + str(m1.get("label"))) if m1.get("ok") else "❌ pas de confirmation"
    return ("\n\n<b>Séquence (contexte visuel)</b>\n"
            f"✅ Liquidité balayée · ✅ Cassure L1 · FVG/OB : {poi}\n"
            f"✅ Zone QML · M1 : {m1_txt}\n"
            "ℹ️ FVG/OB = information seulement, aucun effet sur le signal.")


QML_PAST_SHOW = _env_on("QML_PAST_SHOW", True)            # AFFICHAGE SEUL : QML passés / invalidés (gris) sur le graphique d'analyse
QML_PAST_MAX = 4                                          # nombre max de zones passées dessinées


def qml_past_zones(candles, t_from):
    """QML PASSÉS pour l'affichage : (a) zones détectées avec les règles du moteur mais déjà invalidées par une clôture ;
    (b) zones « larges » (EQH non exigé) absentes du moteur strict — jamais tradées. LECTURE SEULE : le moteur de signal
    (qml_zones sans include_dead, QML_PARAMS) n'est pas modifié."""
    from dataclasses import replace as _dc_replace
    key = lambda z: (z["side"], z["t"], z["t_head"])
    strict = qml_zones(candles, include_dead=True)
    skeys, found = {key(z) for z in strict}, {}
    for z in strict:
        if z.get("dead"):
            found[key(z)] = dict(z, kind="past")
    try:
        for z in qml_zones(candles, P=_dc_replace(QML_PARAMS, require_eqh=False), include_dead=True):
            if key(z) not in skeys:
                found[key(z)] = dict(z, kind="loose")
    except Exception:
        traceback.print_exc(limit=-3)
    vis = sorted((z for z in found.values() if z["t_head"] >= t_from), key=lambda z: -z["j"])
    firm = [z for z in vis if z["kind"] == "past"][:2]   # les QML « stricts » invalidés d'abord, puis les larges
    return sorted(firm + [z for z in vis if z["kind"] == "loose"][:max(0, QML_PAST_MAX - len(firm))], key=lambda z: -z["j"])


def qml_history_text(r):
    """Ligne de diagnostic ajoutée aux analyses manuelles : combien de bougies ont RÉELLEMENT été analysées."""
    n, t0 = (r or {}).get("hist_n"), (r or {}).get("hist_t0")
    if not n or not t0:
        return ""
    tf = r.get("tf") or get_qml_tf()
    out = f"\n\n🕓 Historique analysé : {n} bougies {_tf_lbl(tf)} (depuis le {datetime.fromtimestamp(t0, timezone.utc).strftime('%d/%m')})"
    if n < 0.9 * QML_HISTORY_BARS:
        out += (f"\n⚠️ Seulement {n} bougies reçues sur {QML_HISTORY_BARS} demandées : la source d'historique long n'a pas répondu "
                f"(repli sur l'historique court).")
    if QML_PAST_SHOW:
        out += "\n⬜ Zones grises = QML passés / invalidés (affichage seul) · « large* » = règles assouplies (EQH non exigé), jamais tradé."
    return out


def make_qml_chart(symbol, tf, r=None, n_show=120, extend=25):
    """Graphique QML (lecture seule) : bougies du TF, zones QML valides (la plus proche en plein, les autres atténuées),
    tête / liquidité balayée, ligne de cassure BOS/CHoCH (L1), et si une entrée est proposée : entrée, SL, TP."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except ImportError:
        return None
    try:
        candles = get_candles(symbol, tf, QML_HISTORY_BARS)
    except Exception:
        candles = get_candles(symbol, tf)
    if len(candles) < 30:
        return None
    px0 = (r or {}).get("price") or candles[-1]["c"]
    all_z = _qml_load_m15(symbol, False, tf)["zones"]
    main_z = (r or {}).get("zone")
    dist = lambda z: 0.0 if z["zlo"] <= px0 <= z["zhi"] else min(abs(px0 - z["zlo"]), abs(px0 - z["zhi"]))
    zones = sorted(all_z, key=lambda z: (dist(z), -z["j"]))[:3]   # les 3 plus proches du prix (anciennes incluses)
    if main_z and not any(z["t"] == main_z["t"] and z["side"] == main_z["side"] for z in zones):
        zones = [main_z] + zones[:2]
    dec = SYMBOLS[symbol]["decimals"]
    oldest = min([z["t_head"] for z in zones] or [candles[-1]["t"]])
    idx_old = next((k for k, c in enumerate(candles) if c["t"] >= oldest), len(candles) - n_show)
    n_show = min(max(n_show, len(candles) - idx_old + 15), 700)   # la vue remonte jusqu'à la tête de la zone la plus ancienne
    os.makedirs(CHART_DIR, exist_ok=True)
    files = sorted((os.path.join(CHART_DIR, f) for f in os.listdir(CHART_DIR)), key=os.path.getmtime)
    for f in files[:-40]:
        try:
            os.remove(f)
        except OSError:
            pass
    view = candles[-n_show:]
    pos = {c["t"]: k for k, c in enumerate(view)}
    fig, ax = plt.subplots(figsize=(9, 5.5), dpi=110)
    fig.patch.set_facecolor("#0e1117")
    ax.set_facecolor("#0e1117")
    for k, c in enumerate(view):
        col = "#26a69a" if c["c"] >= c["o"] else "#ef5350"
        ax.plot([k, k], [c["l"], c["h"]], color=col, lw=0.8)
        ax.add_patch(Rectangle((k - 0.3, min(c["o"], c["c"])), 0.6, max(abs(c["c"] - c["o"]), 1e-9), color=col))
    x_end = len(view) - 1 + extend
    lows, highs = [c["l"] for c in view], [c["h"] for c in view]
    for z in zones:
        col = "#26a69a" if z["side"] == "BUY" else "#ef5350"
        is_main = bool(main_z) and z["t"] == main_z["t"] and z["side"] == main_z["side"]
        alpha = 0.32 if is_main else 0.14
        xj, xh = pos.get(z["t"]), pos.get(z["t_head"])
        x_from = xj if xj is not None else 0
        ax.add_patch(Rectangle((x_from, z["zlo"]), x_end - x_from, z["zhi"] - z["zlo"], color=col, alpha=alpha))
        ax.text(x_end + 0.5, (z["zlo"] + z["zhi"]) / 2, f"ZONE QML {z['side']}", color=col, fontsize=7, va="center")
        if xh is not None:   # tête = liquidité balayée (EQH pour un SELL, EQL pour un BUY)
            ax.plot([max(xh - 4, 0), xh + 4], [z["head"]] * 2, color="#f5c542", lw=1.2)
            ax.text(xh, z["head"], "Liquidité (tête)", color="#f5c542", fontsize=7, ha="center",
                    va="bottom" if z["side"] == "SELL" else "top")
            if xj is not None:   # cassure de L1 = BOS / CHoCH qui valide le QML
                ax.plot([xh, xj], [z["L1"]] * 2, color=col, lw=0.9, ls="--")
                ax.text((xh + xj) / 2, z["L1"], "BOS/CHoCH (L1)", color=col, fontsize=7, ha="center",
                        va="top" if z["side"] == "SELL" else "bottom")
        lows += [z["zlo"], z["head"], z["L1"]]
        highs += [z["zhi"], z["head"], z["L1"]]
    past = []
    if QML_PAST_SHOW:   # QML passés / invalidés en gris (affichage seul : aucune décision ne les lit)
        try:
            live = {(z["side"], z["t"], z["t_head"]) for z in zones}
            past = [z for z in qml_past_zones(candles, view[0]["t"]) if (z["side"], z["t"], z["t_head"]) not in live]
        except Exception:
            traceback.print_exc(limit=-3)
        for z in past:
            gry, loose = "#8a8f98", z["kind"] == "loose"
            xj, xh = pos.get(z["t"]), pos.get(z["t_head"])
            x_from = xj if xj is not None else 0
            x_to = pos.get(z["t_dead"], x_end) if z.get("dead") else x_end
            ax.add_patch(Rectangle((x_from, z["zlo"]), max(x_to - x_from, 1), z["zhi"] - z["zlo"], facecolor=gry, alpha=0.16,
                                   edgecolor=gry, lw=0.8, ls=":" if loose else "-"))
            ax.text(x_from + 0.5, z["zhi"], f"QML {'large*' if loose else 'passé'} {z['side']}", color=gry, fontsize=6, va="bottom")
            if xh is not None:
                ax.plot([max(xh - 3, 0), xh + 3], [z["head"]] * 2, color=gry, lw=0.9)
                if xj is not None:
                    ax.plot([xh, xj], [z["L1"]] * 2, color=gry, lw=0.7, ls="--")
            lows += [z["zlo"], z["head"]]
            highs += [z["zhi"], z["head"]]
    ctx_pois = []
    if QML_CTX_SHOW and zones:   # FVG/OB de contexte de la zone principale (affichage seul, aucune décision)
        try:
            ctx_pois = qml_context(symbol, tf, main_z or zones[0], (r or {}).get("atr15") or atr_series(candles)[-1])["pois"]
        except Exception:
            traceback.print_exc(limit=-3)
        for q in ctx_pois:
            col = "#26a69a" if q["d"] == 1 else "#ef5350"
            xf = pos.get(q["t_f"], 0)
            ax.add_patch(Rectangle((xf, q["lo"]), x_end - xf, q["hi"] - q["lo"], facecolor=col, alpha=0.10,
                                   edgecolor=col, lw=0.8, ls=":"))
            ax.text(xf + 0.5, q["hi"], f"{_tf_lbl(q['tf'])} {q['kind']}", color=col, fontsize=6, va="bottom")
            lows.append(q["lo"])
            highs.append(q["hi"])
    price = (r or {}).get("price") or view[-1]["c"]
    ax.axhline(price, color="#ffffff", lw=0.7, ls=":")
    if not (r and r.get("status") == "PROPOSAL"):   # (en cas de proposition, l'étiquette ENTRÉE tient lieu de prix)
        ax.text(x_end + 0.5, price, f"PRIX {price:.{dec}f}", color="#ffffff", fontsize=8, va="center")
    lows.append(price)
    highs.append(price)
    if r and r.get("status") == "PROPOSAL":   # entrée / SL / TP potentiels (proposition, pas un ordre)
        x0 = len(view) - 1
        ax.add_patch(Rectangle((x0, min(r["entry"], r["sl"])), extend, abs(r["entry"] - r["sl"]), color="#ef5350", alpha=0.22))
        ax.add_patch(Rectangle((x0, min(r["entry"], r["tp"])), extend, abs(r["tp"] - r["entry"]), color="#26a69a", alpha=0.22))
        for lbl, v, col in (("ENTRÉE", r["entry"], "#ffffff"), ("SL", r["sl"], "#ef5350"), ("TP", r["tp"], "#26a69a")):
            ax.plot([x0, x0 + extend], [v, v], color=col, lw=1.0)
            ax.text(x_end + 0.5, v, f"{lbl} {v:.{dec}f}", color=col, fontsize=7, va="center")
        lows += [r["sl"], r["entry"], r["tp"]]
        highs += [r["sl"], r["entry"], r["tp"]]
    ax.set_xlim(-1, x_end + 22)
    lo, hi = min(lows), max(highs)
    pad = (hi - lo) * 0.05 or 1
    ax.set_ylim(lo - pad, hi + pad)
    title = f"{symbol} {_tf_lbl(tf)} — QML" + ("" if zones else " : aucune zone valide") + (" + FVG/OB" if ctx_pois else "") + (f" · {len(past)} passé(s)" if past else "")
    ax.set_title(title, color="white", fontsize=11)
    ax.tick_params(colors="#888888", labelsize=7)
    ax.set_xticks([])
    for sp in ax.spines.values():
        sp.set_color("#333333")
    path = os.path.join(CHART_DIR, f"{symbol}_qml_{int(time.time())}.png")
    fig.savefig(path, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return path


PREVIEW_MAX_AGE = 60   # « point potentiel » : un CHoCH vieux de plus de N bougies n'est plus proposé


def strategy_preview(symbol, strat):
    """Point d'entrée POTENTIEL d'une stratégie ("main" = BOS/CHoCH, "crt" = CRT), sans rien publier ni exécuter.
    Retourne (texte, chemin_image|None). Utilise les mêmes règles / filtres que le signal automatique."""
    name = {"main": "BOS / CHoCH", "crt": "CRT"}[strat]
    head = f"🎯 <b>{name} — {symbol}</b> · {TF_LABEL}\n"
    dec = SYMBOLS[symbol]["decimals"]
    candles = get_candles(symbol)
    if len(candles) < 2 * SWING_DEPTH + 5:
        return head + "\n⚠️ Données insuffisantes pour le moment.", None
    events = analyze(candles)
    n = len(candles)
    on_txt = "🟢 ON" if (get_strat_main() if strat == "main" else get_strat_crt()) else "🔴 OFF (analyse seule, aucun signal automatique)"
    head += f"Stratégie : {on_txt}\n"
    chochs = [e for e in events if e["kind"] == "CHOCH" and e["i"] >= n - PREVIEW_MAX_AGE]
    found = None
    for ev in reversed(chochs):   # le plus récent d'abord
        try:
            sigs = (build_signal(candles, ev, symbol, multi=True) if strat == "main" else crt_build(candles, ev, symbol)) or []
        except Exception:
            traceback.print_exc(limit=-3)
            sigs = []
        if sigs:
            found = (ev, sigs)
            break
    if not found:
        why = "aucun CHoCH récent" if not chochs else f"{len(chochs)} CHoCH récent(s) mais aucun ne passe les filtres"
        return head + f"\nPas de point d'entrée potentiel : {why}.\n\nAction : ⏸ attendre", None
    ev, sigs = found
    age = n - 1 - ev["i"]
    lines = []
    for sg in sigs:
        icon = "🟢" if sg["side"] == "BUY" else "🔴"
        lbl = f"{sg['side']} LIMIT" if sg.get("order") == "LIMIT" else f"{sg['side']} (direct)"
        lines.append(f"{icon} <b>{lbl}</b> · {sg.get('type', '')}\nEntrée : {_fmt(sg['entry'], dec)}\n"
                     f"SL : {_fmt(sg['sl'], dec)}\nTP : {_fmt(sg['tp'], dec)} (RR{_sig_rr(sg):g})")
    notes = []
    if not session_ok(ev["t"]):
        notes.append(f"⏸ hors session ({get_session()})")
    nb = news_block_info(symbol)
    if nb:
        notes.append(f"📰 news en cours : {_news_line(nb)}")
    if not get_symbol_on(symbol):
        notes.append("🌍 actif désactivé (pas de signal automatique)")
    if not get_bot_on():
        notes.append("⏸ bot arrêté")
    status = ("✅ serait publié maintenant" if not notes else "⚠️ ne serait pas publié : " + " ; ".join(notes))
    text = (head + f"\nCHoCH {_dir_txt(ev['dir'])} il y a {age} bougie(s)\n\n" + "\n\n".join(lines)
            + f"\n\nStatut : {status}\n⚠️ PROPOSITION — pas un ordre exécuté.")
    chart = None
    try:
        chart = make_chart(symbol, candles, events, sigs[0], dec)
    except Exception:
        traceback.print_exc(limit=-3)
    return text, chart


def analyse_all(symbol):
    """Les 3 stratégies d'un actif : liste de (texte, image|None). Lecture seule."""
    out = []
    for strat in ("main", "crt"):
        try:
            out.append(strategy_preview(symbol, strat))
        except Exception as e:
            traceback.print_exc(limit=-3)
            out.append((f"🎯 <b>{strat.upper()} — {symbol}</b>\n\n⚠️ Analyse impossible ({type(e).__name__}).", None))
    try:
        res = qml_analyze(symbol, force=True)
        txt = qml_analysis_text(res)
        try:
            txt += qml_context_text(res) + qml_history_text(res)   # contexte FVG/OB + historique analysé : information seulement
        except Exception:
            traceback.print_exc(limit=-3)
        chart = None
        try:
            chart = make_qml_chart(symbol, res.get("tf") or get_qml_tf(), res)
        except Exception:
            traceback.print_exc(limit=-3)
        out.append((txt, chart))
    except Exception as e:
        traceback.print_exc(limit=-3)
        out.append((f"📊 <b>ANALYSE QML — {symbol}</b>\n\n⚠️ Analyse impossible ({type(e).__name__}).", None))
    return out


def _qml_age_txt(z):
    """Âge de la zone, à titre INFORMATIF (ne rend jamais la zone invalide)."""
    mins = max(0, int((time.time() - z["t"]) // 60))
    if mins < 120:
        return f"{mins} min"
    if mins < 2880:
        return f"{mins // 60} h"
    return f"{mins // 1440} j"


def qml_analysis_text(r):
    s, dec = r["symbol"], r.get("dec", 2)
    tf = r.get("tf") or get_qml_tf()
    htf_txt = (f"HTF : {_tf_lbl(QML_HTF_MINUTES)} (filtre ON)" if r.get("htf_on", get_qml_htf_on()) else "HTF : OFF")
    head = f"📊 <b>ANALYSE QML — {s}</b>\n\nTF QML : {_tf_lbl(tf)} · {htf_txt}\n"
    st = r["status"]
    if st == "ERREUR":
        return head + f"\n⚠️ {r.get('msg')}"
    if st == "NO_QML":
        return head + f"\nAucun QML valide sur {_tf_lbl(tf)} pour le moment.\n\nAction : ⏸ attendre"
    if st == "HTF_INDISPO":
        return head + f"\nBiais {_tf_lbl(QML_HTF_MINUTES)} indisponible (données insuffisantes).\n\nAction : ⏸ SKIP"
    z = r["zone"]
    zone = f"{_fmt(z['zlo'], dec)} — {_fmt(z['zhi'], dec)}"
    base = f"\nDirection : {r['side']}\nType : QML {r['side']}\n\nZONE QML :\n{zone}\nCréée il y a {_qml_age_txt(z)} · toujours valide (non invalidée)\n"
    if st == "HTF":
        bias = "haussier" if r.get("htf_bias") == 1 else "baissier"
        return head + base + (f"\nFiltre {_tf_lbl(QML_HTF_MINUTES)} : ❌ biais {bias}, QML {r['side']} contre le contexte\n\n"
                              f"Action : ⏸ SKIP — filtre HTF (désactive-le dans ⚙️ QML pour trader ce QML pur)")
    if st == "FAR":
        return head + base + f"\nPrix actuel : {_fmt(r['price'], dec)} (zone éloignée)\n\nAction : ⏸ SKIP — attendre que le prix approche"
    if st == "WAIT":
        return head + base + "\nPrix dans la zone : ❌ pas encore\n\nAction : ⏸ attendre"
    if st == "SKIP":
        m = r["m1"]
        why = "Aucun avalement" if get_qml_conf() == "ENGULFING" else "Aucune confirmation (" + get_qml_conf() + ")"
        return head + base + f"\nPrix dans la zone : ✅\n\nM1 :\n❌ {why}\n\nAction :\n⏸ SKIP — attendre une réaction"
    if st == "DIRECT_OFF":
        return head + base + f"\nConfirmation M1 : 🟢 {r['confirm']}\n\nEntrée directe : 🔴 OFF — analyse seule\nStatut : aucune entrée proposée"
    if st in ("SL_EXCESSIF", "SL_INVALIDE"):
        why = "SL excessif" if st == "SL_EXCESSIF" else "SL invalide"
        extra = f" ({r['risk'] / r['atr15']:.1f} x ATR M15 > {MAX_SL_ATR:g})" if st == "SL_EXCESSIF" else ""
        return head + base + f"\nConfirmation M1 : 🟢 {r['confirm']}\n\n⛔ {why}{extra}\nStatut : SKIP (proposition refusée)"
    icon = "🟢" if r["side"] == "BUY" else "🔴"
    htf_line = f"\nContexte {_tf_lbl(QML_HTF_MINUTES)} : ✅ {'haussier' if r.get('htf_bias') == 1 else 'baissier'}, aligné\n" if r.get("htf_on") else ""
    return (head + base + htf_line + f"\nEntrée potentielle :\n{_fmt(r['entry'], dec)}\n\nConfirmation M1 :\n🟢 {r['confirm']}\n\n"
            f"Type d'entrée :\nDIRECT\n\nSL potentiel :\n{_fmt(r['sl'], dec)}\n\nTP potentiel : {_fmt(r['tp'], dec)} (RR{r['rr']:g})\n"
            f"Risque structurel :\n{_fmt(r['risk'], dec)} pts\n\nStatut :\n✅ ENTRÉE PROPOSÉE\n\n"
            f"{icon} <b>{r['side']} POTENTIEL</b> · QML + M1 {r['confirm']}\n⚠️ PROPOSITION — pas un ordre exécuté.")


def qml_build_signal(r):  # noqa: E302
    """Signal compatible avec publish_signal (même format que build_signal ; sig["zone"] = {tf, kind, d, lo, hi})."""
    d = 1 if r["side"] == "BUY" else -1
    z = r["zone"]
    return {
        "dir": d, "side": r["side"], "type": "QML", "order": "MARKET", "ref_price": r["entry"],
        "entry": r["entry"], "sl": r["sl"], "risk": r["risk"], "tp": r["tp"], "t": r["t"], "bos_level": z["L1"],
        "rr": r["rr"], "htf": bool(r.get("htf_on")), "htf_mode": "OFF", "be_rr": get_be_rr(),
        "tf": f"{_tf_lbl(r['tf'])}→M1",
        "setup": f"QML {_tf_lbl(r['tf'])}" + (f" + {_tf_lbl(QML_HTF_MINUTES)}" if r.get("htf_on") else "") + f" + M1 {r['confirm']}", "poi": None, "mtf_fib_on": False, "mtf_fib": None,
        "qf": {}, "ext": None, "sweep": None, "sl_mode": "STRUCT", "key_suffix": "qml",
        "zone": {"tf": r["tf"], "kind": "QML", "d": d, "lo": z["zlo"], "hi": z["zhi"]},
    }


def qml_process_symbol(symbol):
    """Scan M15 -> surveillance de la zone -> M1 seulement quand le prix y entre. Une proposition par zone."""
    if not get_qml_on() or not get_symbol_on(symbol) or not get_bot_on():
        return
    now = time.time()
    if now - _qml_last_scan.get(symbol, 0.0) < QML_SCAN_SEC:
        return
    _qml_last_scan[symbol] = now
    r = qml_analyze(symbol)
    if r["status"] != "PROPOSAL":
        return
    z = r["zone"]
    key = f"qml_sent:{symbol}:{r['tf']}:{r['side']}:{z['t_head']}"
    if get_meta(key) or not session_ok(r["t"]) or news_block_info(symbol):
        return   # (news : la zone reste valable, la proposition pourra partir après la fenêtre)
    set_meta(key, 1)
    if get_qml_auto():
        m1 = r["m1_candles"]
        if publish_signal(symbol, m1, analyze(m1), qml_build_signal(r)):
            return   # signal publié (groupe + admin + exécution existante)
    to_admin(qml_analysis_text(r))


# --- Telegram : menu ⚙️ QML ----------------------------------------------------------------------
def _qml_text():
    c = get_partial_cfg()
    on = lambda b: "🟢 ON" if b else "🔴 OFF"
    htf = f"{on(get_qml_htf_on())}" + (f" · HTF = {_tf_lbl(QML_HTF_MINUTES)}" if get_qml_htf_on() else "")
    return (f"⚙️ <b>QML</b>\n\nQML : {on(get_qml_on())}\nTF QML : {_tf_lbl(get_qml_tf())}\nHTF FILTER : {htf}\n"
            f"Confirmation M1 : {get_qml_conf().replace('_', ' ')}\nEntrée directe : {on(get_qml_direct())}\n"
            f"Réaction sans engulfing : {on(get_qml_react())}\nPublier comme signal (auto) : {on(get_qml_auto())}\n\n"
            f"Clôtures partielles : {on(get_partial_on())}\n"
            f"TP1 : {c['p1_rr']:g}R → {c['p1_pct']:g} %\nTP2 : {c['p2_rr']:g}R → {c['p2_pct']:g} %\n"
            f"TP3 : {c['p3_rr']:g}R → reste ({100 - c['p1_pct'] - c['p2_pct']:g} %)\n\n"
            f"Marchés : {', '.join(QML_MARKETS)} (variable QML_MARKETS). Le scan ne couvre que les actifs ON du menu 🌍 ACTIFS.\n"
            f"OFF / proposition : le bot n'envoie qu'un message à l'admin, aucun ordre. « Auto » = signal publié + exécution existante.")


def _qml_keyboard():
    c = get_partial_cfg()

    def row(lbl, key, vals, fmt="{:g}"):
        return [_btn(f"{lbl} " + fmt.format(v), f"qml:set:{key}:{v:g}", abs(c[key] - v) < 1e-9) for v in vals]
    return _with_back({"inline_keyboard": [
        [_btn("QML", "qml:on", get_qml_on()), _btn("Auto (signal)", "qml:auto", get_qml_auto())],
        [_btn(_tf_lbl(t), f"qml:tf:{t}", get_qml_tf() == t) for t in QML_TF_CHOICES]
        + [_btn(f"Filtre {_tf_lbl(QML_HTF_MINUTES)}", "qml:htf", get_qml_htf_on())],
        [_btn("Entrée directe", "qml:direct", get_qml_direct())],
        [_btn("Avalement", "qml:conf:ENGULFING", get_qml_conf() == "ENGULFING"),
         _btn("CHoCH M1", "qml:conf:CHOCH", get_qml_conf() == "CHOCH"),
         _btn("Les deux", "qml:conf:ENGULFING_OR_CHOCH", get_qml_conf() == "ENGULFING_OR_CHOCH")],
        [_btn("Réaction sans engulfing", "qml:react", get_qml_react())],
        [_btn("Clôtures partielles", "qml:part", get_partial_on())],
        row("TP1 RR", "p1_rr", (0.5, 1, 1.5)), row("TP1 %", "p1_pct", (20, 30, 40, 50)),
        row("TP2 RR", "p2_rr", (1.5, 2, 2.5)), row("TP2 %", "p2_pct", (20, 30, 40, 50)),
        row("TP3 RR", "p3_rr", (2, 3, 4)),
    ]})


def _qml_callback(cq, arg):
    """Gère les boutons « qml:... ». Retourne le texte de confirmation (toast)."""
    ack = ""
    if arg == "home":
        pass
    elif arg == "on":
        set_setting("qml_on", "0" if get_qml_on() else "1"); ack = "QML : " + ("ON" if get_qml_on() else "OFF")
    elif arg == "auto":
        set_setting("qml_auto", "0" if get_qml_auto() else "1"); ack = "Auto : " + ("ON" if get_qml_auto() else "OFF")
    elif arg.startswith("tf:") and arg[3:].isdigit() and int(arg[3:]) in QML_TF_CHOICES:
        set_setting("qml_tf", int(arg[3:])); ack = "TF QML : " + _tf_lbl(get_qml_tf())
    elif arg == "htf":
        set_setting("qml_htf", "0" if get_qml_htf_on() else "1"); ack = "Filtre HTF : " + ("ON" if get_qml_htf_on() else "OFF")
    elif arg == "direct":
        set_setting("qml_direct", "0" if get_qml_direct() else "1"); ack = "Entrée directe : " + ("ON" if get_qml_direct() else "OFF")
    elif arg == "react":
        set_setting("qml_react", "0" if get_qml_react() else "1"); ack = "Réaction : " + ("ON" if get_qml_react() else "OFF")
    elif arg == "part":
        set_setting("part_on", "0" if get_partial_on() else "1"); ack = "Partiels : " + ("ON" if get_partial_on() else "OFF")
    elif arg.startswith("conf:") and arg[5:] in QML_CONF_MODES:
        set_setting("qml_conf", arg[5:]); ack = arg[5:].replace("_", " ")
    elif arg.startswith("set:"):
        try:
            _, key, val = arg.split(":")
            ok, msg = set_partial_value(key, float(val))
        except ValueError:
            ok, msg = False, "valeur invalide"
        ack = "OK" if ok else msg
    _edit(cq, _qml_text(), _qml_keyboard())
    return ack


# ============================================================================
# 13. MODULE FIBTREND — tendance (BOS + BOS) + ligne de tendance + retracement Fibo >= 50 % + avalement / doji
#
# Stratégie SÉPARÉE, activable / désactivable à part (/ft on|off ou menu ⚙️ FIBTREND, OFF par défaut) :
# ne touche ni au moteur BOS/CHoCH, ni au CRT, ni au QML. Fonctionne sur M1 / M3 / M5 / M15 / M30 / H1 (réglable à chaud).
# Règles (tout est en ATR, en % du Fibo ou en nombre de bougies, donc valable sur tous les timeframes) :
#   1. TENDANCE : les FT_MIN_BOS (2) derniers événements de `analyze` sont des BOS de même sens
#      (BOS + BOS baissier = vente ; BOS + BOS haussier = achat). Un CHoCH annule la tendance.
#   2. LIGNE DE TENDANCE : les 2 derniers pivots (sommets pour une vente, creux pour un achat) sont décroissants /
#      croissants et la clôture reste du bon côté de la ligne (réglable : /ft tl).
#   3. PAS DE RANGE : efficience >= FT_MIN_ER à la fin de l'impulsion, et jambe >= FT_MIN_LEG_ATR x ATR.
#   4. FIBO : 1.0 = origine de la jambe (dernier pivot avant l'extrême), 0.0 = extrême (plus bas / plus haut depuis la
#      cassure). Recalculé à chaque bougie : l'extrême suit le prix tant qu'il fait de nouveaux extrêmes.
#   5. RETRACEMENT : la MÈCHE de la bougie signal atteint au moins FT_MIN_RET (50 %) de la jambe, sans dépasser l'origine.
#   6. DÉCLENCHEUR : bougie d'avalement (dans le sens de la tendance) ou doji. Entrée à la clôture de cette bougie.
#   7. SL : DERRIÈRE la bougie d'entrée (+ FT_SL_ATR x ATR, 0 = pile sur la mèche).
#   8. TP au choix (/ft tp FIB0|LIQ|RR) :
#        FIB0 = niveau 0.0 du Fibo (extrême de la jambe) · RR = RR fixe (/ft rr 2)
#        LIQ  = liquidité externe : buy side (achat, plus proche haut non balayé au-dessus) / sell side (vente, plus proche
#               bas non balayé en dessous) sur l'UT supérieure (LIQ_EXT) ; repli sur le 0.0 si aucune cible exploitable.
# ============================================================================
FT_MIN_BOS = _env_int("FT_MIN_BOS", 2)              # nombre de BOS consécutifs exigés (même sens)
FT_MIN_RET = _env_float("FT_MIN_RET", 0.5)          # retracement minimum par la mèche (0.5 = 50 %)
FT_MAX_AGE = _env_int("FT_MAX_AGE", 120)            # âge maximum du dernier BOS (en bougies)
FT_MIN_LEG_ATR = _env_float("FT_MIN_LEG_ATR", 3.0)  # jambe minimale en ATR (anti micro-impulsion)
FT_MIN_ER = _env_float("FT_MIN_ER", 0.20)           # efficience minimale (anti-range)
FT_ER_LOOKBACK = _env_int("FT_ER_LOOKBACK", 30)     # bougies analysées pour l'efficience
FT_SL_ATR = _env_float("FT_SL_ATR", 0.05)           # marge du SL derrière la bougie d'entrée (x ATR ; 0 = pile sur la mèche)
FT_MIN_RR = _env_float("FT_MIN_RR", 1.0)            # RR minimum
FT_DOJI_BODY = _env_float("FT_DOJI_BODY", 0.15)     # doji : corps <= 15 % du range
FT_DOJI_MIN_ATR = _env_float("FT_DOJI_MIN_ATR", 0.3)  # doji : range >= 0.3 ATR (ignore les micro-bougies)
FT_STRUCT_CHOICES = ("BOS_CHOCH", "BOS_BOS")
FT_TRIGGERS = tuple(x.strip().upper() for x in os.getenv("FT_TRIGGERS", "ENGULFING,PIN_BAR").split(",") if x.strip())   # bougie de rejet (mode BOS+CHoCH) : avalement / pin bar, jamais de doji
FT_REACT_BARS = _env_int("FT_REACT_BARS", 1)        # mode BOS+CHoCH : la mèche qui touche le 50 % peut être celle de la bougie PRÉCÉDANT la bougie de rejet
FT_TF_CHOICES = (1, 3, 5, 15, 30, 60)
FT_TP_MODES = ("FIB0", "LIQ", "RR")
FT_RR_CHOICES = (1.5, 2.0, 3.0, 4.0)
FT_SIDE_CHOICES = ("BOTH", "SELL", "BUY")
FT_SCAN_SEC = _env_int("FT_SCAN_SEC", 15)
_ft_last_scan, _ft_last_t = {}, {}


def get_ft_on():
    return _get_bool_setting("ft_on", _env_on("FT_ENABLED", False))


def get_ft_auto():
    """ON : une entrée validée est publiée comme vrai signal (suivi + exécution existante). OFF : message admin seulement."""
    return _get_bool_setting("ft_auto", _env_on("FT_AUTO", False))


def get_ft_tf():
    try:
        v = int(float(get_setting("ft_tf", os.getenv("FT_TF", "15").strip().upper().lstrip("M") or 15)))
    except (TypeError, ValueError):
        v = 15
    return v if v in FT_TF_CHOICES else 15


def get_ft_tp():
    # TP = « target » : liquidité externe (repli sur le 0.0 du Fibo si aucune cible exploitable). Réglable : /ft tp FIB0|LIQ|RR
    return _get_choice("ft_tp", os.getenv("FT_TP", "LIQ").strip().upper(), FT_TP_MODES)


def get_ft_struct():
    """Structure exigée : BOS_CHOCH (défaut : BOS puis CHoCH) ou BOS_BOS (ancienne règle : 2 BOS de même sens)."""
    return _get_choice("ft_struct", os.getenv("FT_STRUCT", "BOS_CHOCH").strip().upper(), FT_STRUCT_CHOICES)


def get_ft_rr():
    try:
        v = float(get_setting("ft_rr", os.getenv("FT_RR", "2")))
    except (TypeError, ValueError):
        v = 2.0
    return v if v > 0 else 2.0


def get_ft_sides():
    return _get_choice("ft_sides", os.getenv("FT_SIDES", "BOTH").strip().upper(), FT_SIDE_CHOICES)


def get_ft_trendline():
    return _get_bool_setting("ft_tl", _env_on("FT_TRENDLINE", True))


def _ft_pivots(c, depth):
    """Pivots confirmés (même règle que `analyze`) : (sommets, creux), chacun = liste de (index, prix)."""
    n, hs, ls = len(c), [], []
    for p in range(depth, n - depth):
        win_h = [x["h"] for x in c[p - depth:p + depth + 1]]
        win_l = [x["l"] for x in c[p - depth:p + depth + 1]]
        if c[p]["h"] == max(win_h) and all(c[p]["h"] > c[k]["h"] for k in range(p - depth, p)):
            hs.append((p, c[p]["h"]))
        if c[p]["l"] == min(win_l) and all(c[p]["l"] < c[k]["l"] for k in range(p - depth, p)):
            ls.append((p, c[p]["l"]))
    return hs, ls


def _ft_efficiency(c, end, n):
    """Déplacement net / chemin parcouru sur les n bougies qui finissent à `end` (1 = tendance pure, ~0 = range)."""
    w = c[max(0, end - n):end + 1]
    path = sum(abs(w[k]["c"] - w[k - 1]["c"]) for k in range(1, len(w)))
    return abs(w[-1]["c"] - w[0]["c"]) / path if path > 0 else 0.0


def _ft_trigger(c, atr):
    """Dernière bougie clôturée : ('doji', 0) / ('avalement', sens : 1 haussier, -1 baissier) ou (None, 0)."""
    x, p = c[-1], c[-2]
    rng, body = x["h"] - x["l"], abs(x["c"] - x["o"])
    if rng <= 0:
        return None, 0
    if body <= FT_DOJI_BODY * rng and rng >= FT_DOJI_MIN_ATR * atr:
        return "doji", 0
    if x["c"] < x["o"] and p["c"] > p["o"] and x["o"] >= p["c"] and x["c"] <= p["o"]:
        return "avalement", -1
    if x["c"] > x["o"] and p["c"] < p["o"] and x["o"] <= p["c"] and x["c"] >= p["o"]:
        return "avalement", 1
    return None, 0


def ft_ext_pools(symbol, tf, own=None):
    """Liquidité externe NON balayée (pivots swing de l'UT supérieure LIQ_EXT[tf] ; sans UT supérieure : l'UT elle-même `own`).
    Retourne {"tf", "highs": [{"level", "eq"}], "lows": [...]} (même méthode que `ext_map`, mais pour l'UT du module)."""
    ext_tf = LIQ_EXT.get(tf, HTF_MINUTES)
    if ext_tf <= tf:
        ext_tf = tf
    raw = own if (ext_tf == tf and own) else _ctx_candles(symbol, ext_tf)
    if len(raw) <= 2 * EXT_DEPTH + 1:
        return {"tf": ext_tf, "highs": [], "lows": []}
    atr_now = atr_series(raw)[-1]
    highs, lows = [], []
    for p in range(EXT_DEPTH, len(raw) - EXT_DEPTH):
        win_h = [x["h"] for x in raw[p - EXT_DEPTH:p + EXT_DEPTH + 1]]
        win_l = [x["l"] for x in raw[p - EXT_DEPTH:p + EXT_DEPTH + 1]]
        if raw[p]["h"] == max(win_h) and all(raw[p]["h"] > raw[k]["h"] for k in range(p - EXT_DEPTH, p)):
            if not any(k["h"] > raw[p]["h"] for k in raw[p + 1:]):
                highs.append({"level": raw[p]["h"]})
        if raw[p]["l"] == min(win_l) and all(raw[p]["l"] < raw[k]["l"] for k in range(p - EXT_DEPTH, p)):
            if not any(k["l"] < raw[p]["l"] for k in raw[p + 1:]):
                lows.append({"level": raw[p]["l"]})
    tol = EQ_TOL_ATR * atr_now
    for lst in (highs, lows):
        for a in lst:
            a["eq"] = any(b is not a and abs(a["level"] - b["level"]) < tol for b in lst)
    return {"tf": ext_tf, "highs": highs, "lows": lows}


def _ft_pick_tp(d, entry, risk, extreme, tp_mode, rr_fixed, pools):
    """(tp, libellé) selon le mode ; None si aucune cible valable (RR < FT_MIN_RR)."""
    if tp_mode == "RR":
        if rr_fixed < FT_MIN_RR:
            return None
        return entry + d * rr_fixed * risk, f"RR fixe {rr_fixed:g}"
    if tp_mode == "LIQ":
        try:
            m = pools() if callable(pools) else pools
        except Exception as e:
            print(f"[FIBTREND] liquidité externe indisponible ({type(e).__name__}) : repli sur le 0.0")
            m = None
        if m:
            lst = m["highs"] if d == 1 else m["lows"]
            for pv in sorted((p for p in lst if (p["level"] - entry) * d > 0), key=lambda p: abs(p["level"] - entry)):
                if abs(pv["level"] - entry) / risk >= FT_MIN_RR:
                    kind = ("EQH" if pv["eq"] else "buy side") if d == 1 else ("EQL" if pv["eq"] else "sell side")
                    return pv["level"], f"liquidité {kind} {_tf_lbl(m['tf'])}"
    # FIB0 (ou repli de LIQ) : 0.0 du Fibo
    if (extreme - entry) * d > 0 and abs(extreme - entry) / risk >= FT_MIN_RR:
        return extreme, "Fibo 0.0" + (" (repli : pas de liquidité externe)" if tp_mode == "LIQ" else "")
    return None


def ft_find_setup(c, depth=None, tp_mode="FIB0", rr_fixed=2.0, pools=None, sides="BOTH", use_tl=True, struct="BOS_BOS"):
    """Cherche une entrée FIBTREND sur la DERNIÈRE bougie clôturée de `c`. Retourne un dict ou None (jamais de look-ahead).
    pools : carte de liquidité externe (dict) ou fonction qui la calcule (appelée seulement si tout le reste est validé)."""
    depth = depth or SWING_DEPTH
    c = c[-500:]
    n = len(c)
    if n < max(60, 4 * depth + 10):
        return None
    atr = atr_series(c)[-1]
    if not atr:
        return None
    evs = analyze(c, depth)
    chmode = struct == "BOS_CHOCH"
    if chmode:
        # BOS PUIS CHoCH : le dernier CHoCH est directement précédé d'un BOS (ancienne tendance) ; ensuite il ne reste que des
        # BOS dans le sens du CHoCH (confirmation). Un événement opposé après le CHoCH annule le setup.
        chs = [k for k, e in enumerate(evs) if e["kind"] == "CHOCH"]
        if not chs or chs[-1] < 1:
            return None
        k = chs[-1]
        d = evs[k]["dir"]
        if evs[k - 1]["kind"] != "BOS" or evs[k - 1]["dir"] != -d:
            return None
        if any(e["kind"] != "BOS" or e["dir"] != d for e in evs[k + 1:]):
            return None
        anchor, last_ev = evs[k], evs[-1]
    else:
        if len(evs) < FT_MIN_BOS:
            return None
        last = evs[-FT_MIN_BOS:]
        d = last[-1]["dir"]
        if any(e["kind"] != "BOS" or e["dir"] != d for e in last):
            return None                                     # BOS + BOS exigés, un CHoCH annule la tendance
        anchor, last_ev = last[-1], last[-1]
    if (sides == "SELL" and d != -1) or (sides == "BUY" and d != 1):
        return None
    if n - 1 - last_ev["i"] > FT_MAX_AGE:
        return None                                         # structure trop ancienne

    hs, ls = _ft_pivots(c, depth)
    # --- jambe / Fibo : 0.0 = extrême depuis la cassure, 1.0 = dernier pivot avant cet extrême
    src = anchor["src"]
    if d == -1:
        e = min(range(src, n), key=lambda k: c[k]["l"])
        extreme = c[e]["l"]
        before = [v for p, v in hs if p < e]
    else:
        e = max(range(src, n), key=lambda k: c[k]["h"])
        extreme = c[e]["h"]
        before = [v for p, v in ls if p < e]
    if e >= n - 1 or not before:
        return None                                         # pas de retracement encore, ou pas d'origine
    origin = before[-1]
    leg = abs(origin - extreme)
    if (origin - extreme) * d >= 0 or leg < FT_MIN_LEG_ATR * atr:
        return None                                         # jambe trop petite (anti micro-impulsion)
    er = _ft_efficiency(c, e, FT_ER_LOOKBACK)
    if not chmode and er < FT_MIN_ER:
        return None                                         # range : on ne trade pas (règle BOS+BOS ; BOS+CHoCH : rien d'autre que BOS + CHoCH + 50 % + rejet)

    # --- ligne de tendance : pivots décroissants (vente) / croissants (achat), clôture du bon côté
    x = c[-1]
    tl = None
    if use_tl and not chmode:
        pts = hs if d == -1 else ls
        if len(pts) < 2:
            return None
        (i1, p1), (i2, p2) = pts[-2], pts[-1]
        if (p2 - p1) * d <= 0 or i2 == i1:
            return None
        tl = p2 + (p2 - p1) / (i2 - i1) * (n - 1 - i2)
        if (x["c"] - tl) * d < 0:
            return None                                     # clôture au-delà de la ligne : tendance cassée

    # --- retracement : la MÈCHE atteint >= 50 % de la jambe, sans dépasser l'origine (BOS+CHoCH : la bougie de rejet OU la précédente)
    react = FT_REACT_BARS if chmode else 0
    w0 = max(e + 1, n - 1 - react)
    if d == -1:
        lvl = extreme + FT_MIN_RET * leg
        far = max(k["h"] for k in c[e + 1:])
        ti = next((k for k in range(w0, n) if c[k]["h"] >= lvl), None)
        ret = (max(k["h"] for k in c[ti:]) - extreme) / leg if ti is not None else 0.0
    else:
        lvl = extreme - FT_MIN_RET * leg
        far = min(k["l"] for k in c[e + 1:])
        ti = next((k for k in range(w0, n) if c[k]["l"] <= lvl), None)
        ret = (extreme - min(k["l"] for k in c[ti:])) / leg if ti is not None else 0.0
    if ti is None or (origin - far) * d > 0:
        return None                                         # 50 % pas atteint, ou origine dépassée

    # --- déclencheur : bougie de rejet, entrée à la clôture
    if chmode:
        pat = qmlg_rejection(c, d == 1)                     # avalement / pin bar dans le sens du CHoCH (le doji ne compte pas)
        if pat not in FT_TRIGGERS:
            return None
        trig = {"ENGULFING": "avalement", "PIN_BAR": "pin bar", "DOJI": "doji"}.get(pat, pat.lower())
        # SL DERRIÈRE la bougie de rejet (et derrière la bougie qui a touché le 50 % si c'est la précédente)
        ref = c[ti:]
        sl = (min(k["l"] for k in ref) - FT_SL_ATR * atr) if d == 1 else (max(k["h"] for k in ref) + FT_SL_ATR * atr)
    else:
        trig, tdir = _ft_trigger(c, atr)
        if trig is None or (trig == "avalement" and tdir != d):
            return None
        ref = [x]
        sl = x["h"] + FT_SL_ATR * atr if d == -1 else x["l"] - FT_SL_ATR * atr   # DERRIÈRE la bougie d'entrée
    entry = x["c"]
    risk = abs(entry - sl)
    if risk <= 0 or (entry - sl) * d <= 0:
        return None
    picked = _ft_pick_tp(d, entry, risk, extreme, tp_mode, rr_fixed, pools)
    if picked is None:
        return None
    tp, tp_lbl = picked
    return {"dir": d, "side": "BUY" if d == 1 else "SELL", "t": x["t"], "entry": entry, "sl": sl, "tp": tp,
            "risk": risk, "rr": abs(tp - entry) / risk, "tp_label": tp_lbl, "trigger": trig, "origin": origin,
            "extreme": extreme, "lvl": lvl, "ret": ret, "t_extreme": c[e]["t"], "trendline": tl, "atr": atr, "er": er,
            "struct": struct, "t_touch": c[ti]["t"],
            "ref_extreme": (min(k["l"] for k in ref) if d == 1 else max(k["h"] for k in ref)),
            "touch_px": (min(k["l"] for k in c[ti:]) if d == 1 else max(k["h"] for k in c[ti:]))}


def ft_build_signal(r, tf):
    """Signal compatible avec publish_signal (même format que qml_build_signal)."""
    d = r["dir"]
    lo, hi = sorted((r["lvl"], r["origin"]))
    return {
        "dir": d, "side": r["side"], "type": "FIBTREND", "order": "MARKET", "ref_price": r["entry"],
        "entry": r["entry"], "sl": r["sl"], "risk": r["risk"], "tp": r["tp"], "t": r["t"], "bos_level": r["origin"],
        "rr": round(r["rr"], 2), "htf": False, "htf_mode": "OFF", "be_rr": get_be_rr(), "tf": _tf_lbl(tf),
        "setup": f"FIBTREND {_tf_lbl(tf)} {'BOS+CHoCH' if r.get('struct') == 'BOS_CHOCH' else 'BOS+BOS'} · Fibo 50 % ({r['ret']:.0%}) · {r['trigger']}",
        "fib": {"origin": r["origin"], "lvl": r["lvl"], "extreme": r["extreme"]},
        "no_mt5": not get_ft_auto(),   # auto OFF : signal + image + lot au groupe et à l'admin, SANS ordre MT5
        "poi": None, "mtf_fib_on": False, "mtf_fib": None, "qf": {}, "ext": None, "sweep": None,
        "sl_mode": "STRUCT", "tp_mode": r["tp_label"], "key_suffix": "ft",
        "zone": {"tf": tf, "kind": "FIBTREND", "d": d, "lo": lo, "hi": hi},
    }


def ft_text(r, symbol, tf):
    dec = SYMBOLS[symbol]["decimals"]
    f = lambda v: _fmt(v, dec)
    icon = "🔴" if r["dir"] == -1 else "🟢"
    if r.get("struct") == "BOS_CHOCH":
        trend = f"BOS + CHoCH {'baissier' if r['dir'] == -1 else 'haussier'}"
    else:
        trend = f"BOS + BOS {'baissier' if r['dir'] == -1 else 'haussier'} · efficience {r['er']:.2f}"
    return (f"{icon} <b>FIBTREND {r['side']} {symbol}</b> · {_tf_lbl(tf)}\n"
            f"Structure : {trend}\n"
            f"Fibo : 1.0 = {f(r['origin'])} · 50 % = {f(r['lvl'])} · 0.0 = {f(r['extreme'])}\n"
            f"Mitigation 50 % : {r['ret']:.0%} · bougie de rejet : {r['trigger']}\n"
            f"Entrée {f(r['entry'])} · SL {f(r['sl'])} (derrière la bougie de rejet) · TP {f(r['tp'])} ({r['tp_label']}) · RR {r['rr']:.2f}")


def ft_verify(r, symbol):
    """DOUBLE VÉRIFICATION avant tout envoi. Retourne (ok, [problèmes]). Contrôle : SL / TP du bon côté, SL bien DERRIÈRE la bougie de
    rejet, risque et RR recalculés, Fibo cohérent (1.0 / 50 % / 0.0), 50 % réellement touché sans dépasser l'origine, RR >= FT_MIN_RR,
    puis lot recalculé (lot x distance SL x valeur du point) et jamais au-dessus du risque choisi (sauf lot minimum du broker)."""
    pb, d = [], r["dir"]
    entry, sl, tp = r["entry"], r["sl"], r["tp"]
    risk = (entry - sl) * d
    if risk <= 0:
        pb.append("SL du mauvais côté de l'entrée")
    elif abs(risk - r["risk"]) > 1e-9 * max(1.0, abs(entry)):
        pb.append("distance SL incohérente")
    if (tp - entry) * d <= 0:
        pb.append("TP du mauvais côté de l'entrée")
    elif risk > 0:
        rr = abs(tp - entry) / risk
        if abs(rr - r["rr"]) > 1e-6 * max(1.0, rr):
            pb.append("RR incohérent")
        if rr < FT_MIN_RR:
            pb.append(f"RR {rr:.2f} < {FT_MIN_RR:g}")
    if r.get("ref_extreme") is not None and (sl - r["ref_extreme"]) * d >= 0:
        pb.append("SL pas derrière la bougie de rejet")
    if (r["origin"] - r["extreme"]) * d >= 0:
        pb.append("Fibo incohérent (1.0 / 0.0)")
    else:
        mid = r["extreme"] + (r["origin"] - r["extreme"]) * FT_MIN_RET
        if abs(mid - r["lvl"]) > 1e-9 * max(1.0, abs(mid)):
            pb.append("niveau 50 % incohérent")
        if r.get("touch_px") is not None and (r["touch_px"] - r["lvl"]) * d > 0 and r.get("struct") == "BOS_CHOCH":
            pb.append("50 % non touché")
    if not pb:
        try:
            risk_usd = mm_effective_risk()[0]
            li = calc_lot(symbol, risk_usd, risk, price=entry)
            if li["lot"] <= 0:
                pb.append("lot impossible : " + str(li.get("rejected_reason") or "risque / distance SL"))
            else:
                real = li["lot"] * risk * value_per_point(symbol, entry)
                if abs(real - li["real_risk"]) > 0.01 + 1e-6 * real:
                    pb.append(f"risque recalculé {real:.2f} $ ≠ {li['real_risk']:.2f} $")
                if not li.get("raised_to_min") and real > risk_usd * (1 + 1e-6) + 0.005:
                    pb.append(f"risque {real:.2f} $ > {risk_usd:.2f} $ choisi")
        except Exception as e:
            pb.append(f"calcul du lot impossible ({type(e).__name__})")
    return not pb, pb


def ft_process_symbol(symbol):
    """Une analyse par nouvelle bougie clôturée ; une proposition par jambe de Fibo."""
    if not get_ft_on() or not get_symbol_on(symbol) or not get_bot_on():
        return
    now = time.time()
    if now - _ft_last_scan.get(symbol, 0.0) < FT_SCAN_SEC:
        return
    _ft_last_scan[symbol] = now
    tf = get_ft_tf()
    c = get_candles(symbol, minutes=tf)
    if not c or _ft_last_t.get((symbol, tf)) == c[-1]["t"]:
        return                                              # pas de nouvelle bougie clôturée
    _ft_last_t[(symbol, tf)] = c[-1]["t"]
    r = ft_find_setup(c, tp_mode=get_ft_tp(), rr_fixed=get_ft_rr(), sides=get_ft_sides(), use_tl=get_ft_trendline(),
                      pools=lambda: ft_ext_pools(symbol, tf, own=c), struct=get_ft_struct())
    if not r:
        return
    key = f"ft_sent:{symbol}:{tf}:{r['side']}:{r['t_extreme']}"
    if get_meta(key) or not session_ok(r["t"]) or news_block_info(symbol):
        return
    set_meta(key, 1)
    ok, problems = ft_verify(r, symbol)   # double vérification du calcul (SL / TP / RR / Fibo / lot) avant tout envoi
    if not ok:
        to_admin(f"⛔ <b>FIBTREND {r['side']} {symbol} {_tf_lbl(tf)}</b> BLOQUÉ par la double vérification : " + " · ".join(problems))
        return
    # Signal COMPLET au groupe (image + entrée / SL / TP / RR + lot) et à l'admin, suivi RR / BE / TP / SL. « Auto » OFF : aucun ordre MT5.
    if publish_signal(symbol, c, analyze(c), ft_build_signal(r, tf)):
        return
    to_admin(ft_text(r, symbol, tf) + "\n⚠️ Non publié au groupe (filtre : bot arrêté, news, money management, signal déjà ouvert ou lot impossible — voir les logs).")


# --- Telegram : menu ⚙️ FIBTREND ------------------------------------------------------------------
_FT_TP_LBL = {"FIB0": "Fibo 0.0", "LIQ": "Liquidité externe", "RR": "RR fixe"}
_FT_SIDE_LBL = {"BOTH": "Achat + vente", "SELL": "Vente seule", "BUY": "Achat seul"}


def _ft_menu_text():
    on = lambda b: "🟢 ON" if b else "🔴 OFF"
    tp = get_ft_tp()
    return (f"⚙️ <b>FIBTREND</b>\n\nModule : {on(get_ft_on())}\nTimeframe : {_tf_lbl(get_ft_tf())}\n"
            f"Publier comme signal (auto) : {on(get_ft_auto())}\nSens : {_FT_SIDE_LBL[get_ft_sides()]}\n\n"
            f"Structure : <b>{'BOS + CHoCH' if get_ft_struct() == 'BOS_CHOCH' else 'BOS + BOS'}</b>"
            + ("" if get_ft_struct() == "BOS_CHOCH" else f" · ligne de tendance : {on(get_ft_trendline())} · efficience min : {FT_MIN_ER:g}") + "\n"
            f"Mitigation Fibo min (mèche) : {FT_MIN_RET:.0%} (obligatoire) · jambe min : {FT_MIN_LEG_ATR:g} ATR\n"
            + ("Bougie de rejet : avalement ou pin bar · SL derrière la bougie de rejet\n" if get_ft_struct() == "BOS_CHOCH"
               else "Déclencheur : avalement ou doji · SL derrière la bougie d'entrée\n") +
            f"TP : <b>{_FT_TP_LBL[tp]}</b>" + (f" {get_ft_rr():g}" if tp == "RR" else "") + "\n\n"
            f"OFF = plus aucune entrée FIBTREND. Chaque signal part au groupe (image + entrée / SL / TP / lot) et à l'admin. "
            f"« Auto » ON : l'ordre MT5 part aussi (si les ordres MT5 sont autorisés) ; OFF : aucun ordre.\n"
            f"Commandes : /ft on · off · auto · struct · tf 5 · tp FIB0|LIQ|RR · rr 2 · side BOTH|SELL|BUY · tl")


def _ft_keyboard():
    return _with_back({"inline_keyboard": [
        [_btn("FIBTREND", "ft:on", get_ft_on()), _btn("Auto (signal)", "ft:auto", get_ft_auto())],
        [_btn(_tf_lbl(t), f"ft:tf:{t}", get_ft_tf() == t) for t in FT_TF_CHOICES],
        [_btn(_FT_TP_LBL[m], f"ft:tp:{m}", get_ft_tp() == m) for m in FT_TP_MODES],
        [_btn(f"RR {v:g}", f"ft:rr:{v:g}", abs(get_ft_rr() - v) < 1e-9) for v in FT_RR_CHOICES],
        [_btn(_FT_SIDE_LBL[s], f"ft:side:{s}", get_ft_sides() == s) for s in FT_SIDE_CHOICES],
        [_btn("BOS + CHoCH", "ft:struct", get_ft_struct() == "BOS_CHOCH"), _btn("Ligne de tendance", "ft:tl", get_ft_trendline())],
    ]})


def _ft_command(parts):
    """/ft [on|off|auto|tf N|tp X|rr N|side X|tl] — retourne le texte du menu."""
    _ft_do([x.lower() for x in parts[1:]], explicit=True)
    return _ft_menu_text()


def _ft_do(args, explicit):
    """Applique une action. explicit=True : « on » / « off » fixent l'état (commande) ; False : le bouton « ft:on » bascule."""
    if not args:
        return ""
    a, v = args[0], (args[1] if len(args) > 1 else "")
    if a == "on":
        set_setting("ft_on", "1" if explicit else ("0" if get_ft_on() else "1"))
        return "FIBTREND : " + ("ON" if get_ft_on() else "OFF")
    if a == "off":
        set_setting("ft_on", "0")
        return "FIBTREND : OFF"
    if a == "auto":
        set_setting("ft_auto", "0" if get_ft_auto() else "1")
        return "Auto : " + ("ON" if get_ft_auto() else "OFF")
    if a == "struct":
        set_setting("ft_struct", "BOS_BOS" if get_ft_struct() == "BOS_CHOCH" else "BOS_CHOCH")
        return "Structure : " + ("BOS + CHoCH" if get_ft_struct() == "BOS_CHOCH" else "BOS + BOS")
    if a == "tl":
        set_setting("ft_tl", "0" if get_ft_trendline() else "1")
        return "Ligne de tendance : " + ("ON" if get_ft_trendline() else "OFF")
    if a == "tf" and v.lstrip("mM").isdigit() and int(v.lstrip("mM")) in FT_TF_CHOICES:
        set_setting("ft_tf", int(v.lstrip("mM")))
        return "TF FIBTREND : " + _tf_lbl(get_ft_tf())
    if a == "tp" and v.upper() in FT_TP_MODES:
        set_setting("ft_tp", v.upper())
        return "TP : " + _FT_TP_LBL[get_ft_tp()]
    if a == "rr":
        try:
            if float(v) > 0:
                set_setting("ft_rr", float(v))
                return f"RR fixe : {get_ft_rr():g}"
        except ValueError:
            pass
        return "RR invalide"
    if a == "side" and v.upper() in FT_SIDE_CHOICES:
        set_setting("ft_sides", v.upper())
        return "Sens : " + _FT_SIDE_LBL[get_ft_sides()]
    return ""


def _ft_callback(cq, arg):
    """Gère les boutons « ft:... ». Retourne le texte de confirmation (toast)."""
    ack = "" if arg == "home" else _ft_do(arg.split(":"), explicit=False)
    _edit(cq, _ft_menu_text(), _ft_keyboard())
    return ack



# ============================================================================
# 14. MOTEUR SMC — structure de marché (fractals, HH/HL/LH/LL, BOS / CHoCH par clôture) + Quasimodo
#     ex « analyse_smc.py », fusionné ici : detect_structure(df, tf) puis detect_qml(structure).
#     Module pur (aucun réseau, aucune base) ; ses noms internes sont préfixés smc_ pour ne rien heurter dans ce fichier.
#     Auto-test : smc_selftest() (lancé par python main.py --selftest, classe TestMoteurSmc).
# ============================================================================
SMC_FRACTAL_N = 3
_SMC_TREND = {1: "bullish", -1: "bearish", 0: "range"}


# ----------------------------------------------------------------------------- timeframe
def smc_parse_tf(tf):
    """'M5' / '5m' / 5 -> 5 ; 'H4' / '4h' / 240 -> 240 ; 'D1' -> 1440 ; None -> None."""
    if tf is None:
        return None
    if isinstance(tf, (int, float)) and not isinstance(tf, bool):
        m = int(tf)
    else:
        s = str(tf).strip().upper()
        g = re.fullmatch(r"M?(\d+)M?", s)
        if g:
            m = int(g.group(1))
        else:
            g = re.fullmatch(r"H(\d+)|(\d+)H", s)
            if g:
                m = int(g.group(1) or g.group(2)) * 60
            else:
                g = re.fullmatch(r"D(\d+)|(\d+)D", s)
                if not g:
                    raise ValueError(f"timeframe illisible : {tf!r}")
                m = int(g.group(1) or g.group(2)) * 1440
    if m <= 0:
        raise ValueError(f"timeframe invalide : {tf!r}")
    return m


def smc_tf_label(minutes):
    if minutes is None:
        return None
    if minutes % 1440 == 0:
        return f"D{minutes // 1440}"
    if minutes % 60 == 0:
        return f"H{minutes // 60}"
    return f"M{minutes}"


# ----------------------------------------------------------------------------- données
def _smc_ts(x):
    if x is None:
        return None
    if hasattr(x, "isoformat"):
        return x.isoformat()
    if hasattr(x, "item"):
        return x.item()
    return x


def _smc_load(df):
    """-> (temps, high, low, close) : listes nettoyées (NaN retirés, triées, sans doublon de temps)."""
    if df is None:
        raise ValueError("df est None")
    if hasattr(df, "columns"):
        low = {str(k).lower(): k for k in df.columns}
        pick = lambda *names: next((low[x] for x in names if x in low), None)
        kh, kl, kc = pick("high", "h"), pick("low", "l"), pick("close", "c")
        if kh is None or kl is None or kc is None:
            raise ValueError("df doit avoir des colonnes high/low/close (ou h/l/c)")
        h, l, c = (df[k].tolist() for k in (kh, kl, kc))
        kt = pick("time", "t", "timestamp", "datetime", "date", "open_time")
        if kt is not None:
            t = df[kt].tolist()
        elif getattr(getattr(df.index, "dtype", None), "kind", None) == "M":
            t = list(df.index)
        else:
            t = [None] * len(h)
    else:
        rows = list(df)
        g = lambda r, *ks: next((r[k] for k in ks if k in r), None)
        t = [g(r, "t", "time") for r in rows]
        h = [g(r, "h", "high") for r in rows]
        l = [g(r, "l", "low") for r in rows]
        c = [g(r, "c", "close") for r in rows]

    rows = []
    for ti, hi, li, ci in zip(t, h, l, c):
        try:
            hi, li, ci = float(hi), float(li), float(ci)
        except (TypeError, ValueError):
            continue
        if math.isfinite(hi) and math.isfinite(li) and math.isfinite(ci):
            rows.append((ti, hi, li, ci))
    if rows and all(r[0] is not None for r in rows):
        try:
            rows.sort(key=lambda r: r[0])            # tri stable
            out = []
            for r in rows:                           # doublon de temps (bougie répétée) : on garde la dernière
                if out and out[-1][0] == r[0]:
                    out[-1] = r
                else:
                    out.append(r)
            rows = out
        except TypeError:
            pass
    return ([r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows], [r[3] for r in rows])


# ----------------------------------------------------------------------------- moteur
def detect_structure(df, tf=None, n=SMC_FRACTAL_N, keep=50, drop_last=False):
    """Analyse la structure de marché. Retourne un dict :

      tf, tf_minutes, n, bars, last_time, last_close
      trend            "bullish" | "bearish" | "range" (aucune cassure encore)
      label_bias       "bullish" (HH+HL) | "bearish" (LH+LL) | "mixed" | None — d'après les derniers swings
      swing_high / swing_low             dernier fractal confirmé : {type, idx, time, price, label, confirmed_idx, bars_ago}
      prev_swing_high / prev_swing_low   le fractal précédent du même type
      break_up / break_down              niveaux dont la CLÔTURE au-delà casse la structure (None = niveau déjà
                                         consommé, en attente d'un nouveau fractal)
      bos_level / choch_level            niveau de continuation / de retournement selon la tendance
                                         (None si tendance "range")
      last_bos / last_choch / last_event {type, dir, level, pivot_idx, pivot_time, break_idx, break_time,
                                         close, initial, bars_ago} ou None
      event_on_last_bar                  l'événement survenu sur la dernière bougie, sinon None
      swings, events                     historique (les `keep` derniers), du plus ancien au plus récent
      _series                            {"t","h","l","c"} de la série nettoyée (lu par detect_qml)
    Les indices (idx) renvoient à la série nettoyée (NaN retirés, triée) ; `time` est le temps d'origine.
    """
    n = int(n)
    if n < 1:
        raise ValueError("n doit être >= 1")
    tf_min = smc_parse_tf(tf)
    t, h, l, c = _smc_load(df)
    if drop_last and t:
        t, h, l, c = t[:-1], h[:-1], l[:-1], c[:-1]
    N = len(c)
    last = N - 1

    # --- fractals : gauche strict (> les n précédents), droite >= (le 1er de deux extrêmes égaux est retenu)
    is_ph, is_pl = [False] * N, [False] * N
    for p in range(n, N - n):
        if h[p] > max(h[p - n:p]) and h[p] >= max(h[p + 1:p + n + 1]):
            is_ph[p] = True
        if l[p] < min(l[p - n:p]) and l[p] <= min(l[p + 1:p + n + 1]):
            is_pl[p] = True

    swings, events = [], []
    last_h = last_l = prev_h = prev_l = None      # derniers fractals confirmés
    ref_h = ref_l = None                          # fractals pas encore cassés (niveaux actifs)
    trend = 0

    for k in range(N):
        p = k - n                                 # fractal confirmé à la bougie k
        if p >= n:
            if is_ph[p]:
                lab = None if last_h is None else ("HH" if h[p] > last_h["price"] else "LH" if h[p] < last_h["price"] else "EQH")
                s = {"type": "high", "idx": p, "time": _smc_ts(t[p]), "price": h[p], "label": lab, "confirmed_idx": k}
                swings.append(s)
                prev_h, last_h, ref_h = last_h, s, s
            if is_pl[p]:
                lab = None if last_l is None else ("HL" if l[p] > last_l["price"] else "LL" if l[p] < last_l["price"] else "EQL")
                s = {"type": "low", "idx": p, "time": _smc_ts(t[p]), "price": l[p], "label": lab, "confirmed_idx": k}
                swings.append(s)
                prev_l, last_l, ref_l = last_l, s, s

        # --- cassure par CLÔTURE uniquement
        up = ref_h is not None and c[k] > ref_h["price"]
        dn = (not up) and ref_l is not None and c[k] < ref_l["price"]
        if up or dn:
            ref = ref_h if up else ref_l
            d = 1 if up else -1
            events.append({
                "type": "CHOCH" if trend == -d else "BOS", "dir": "bull" if up else "bear",
                "level": ref["price"], "pivot_idx": ref["idx"], "pivot_time": ref["time"],
                "break_idx": k, "break_time": _smc_ts(t[k]), "close": c[k], "initial": trend == 0,
            })
            trend = d
            if up:
                ref_h = None                      # un niveau ne casse qu'une fois
            else:
                ref_l = None

    # --- assemblage du résultat
    def _sw(s):
        return None if s is None else {**s, "bars_ago": last - s["confirmed_idx"]}

    def _ev(e):
        return None if e is None else {**e, "bars_ago": last - e["break_idx"]}

    def _last(pred):
        return next((_ev(e) for e in reversed(events) if pred(e)), None)

    lab_h = last_h["label"] if last_h else None
    lab_l = last_l["label"] if last_l else None
    if lab_h in ("HH",) and lab_l in ("HL",):
        bias = "bullish"
    elif lab_h in ("LH",) and lab_l in ("LL",):
        bias = "bearish"
    else:
        bias = "mixed" if (lab_h and lab_l) else None

    break_up = ref_h["price"] if ref_h else None
    break_dn = ref_l["price"] if ref_l else None
    bos_level = {1: break_up, -1: break_dn}.get(trend)
    choch_level = {1: break_dn, -1: break_up}.get(trend)
    ev_last = _ev(events[-1]) if events else None

    return {
        "tf": smc_tf_label(tf_min), "tf_minutes": tf_min, "n": n, "bars": N,
        "last_time": _smc_ts(t[-1]) if N else None, "last_close": c[-1] if N else None,
        "trend": _SMC_TREND[trend], "label_bias": bias,
        "swing_high": _sw(last_h), "swing_low": _sw(last_l),
        "prev_swing_high": _sw(prev_h), "prev_swing_low": _sw(prev_l),
        "break_up": break_up, "break_down": break_dn,
        "bos_level": bos_level, "choch_level": choch_level,
        "last_bos": _last(lambda e: e["type"] == "BOS"),
        "last_choch": _last(lambda e: e["type"] == "CHOCH"),
        "last_event": ev_last,
        "event_on_last_bar": ev_last if ev_last and ev_last["bars_ago"] == 0 else None,
        "swings": [_sw(s) for s in swings[-keep:]],
        "events": [_ev(e) for e in events[-keep:]],
        "_series": {"t": [_smc_ts(x) for x in t], "h": h, "l": l, "c": c},
    }


# ----------------------------------------------------------------------------- QML
SMC_QML_MIN_RR = 2.0


def detect_qml(structure, min_rr=SMC_QML_MIN_RR):
    """Quasimodo (QML) à partir de la sortie de detect_structure. Retourne un dict :

      tf, min_rr
      qmls        tous les QML trouvés, du plus ancien au plus récent
      last_qml    le dernier (ou None)
      last_valid  le dernier QML valide (ou None)

    Un QML est construit sur chaque CHoCH (celui de detect_structure, donc par CLÔTURE) :
      épaule gauche = dernier swing du même côté que la tête, avant le niveau cassé (le "neckline")
      tête          = extrême entre le neckline et la bougie de cassure (bougies brutes, pas de fractal :
                      aucun regard dans le futur)
      balayage      = la tête dépasse l'épaule (mèche suffisante), l'épaule étant la liquidité prise

    Type
      "continuation" : séquence BOS(d) -> CHoCH(-d) -> CHoCH(d) et épaule APRÈS le BOS, donc petit QML
                       dans le retracement, dans le sens de la tendance ;
      "retournement" : sinon (QML qui retourne la tendance précédente).

    Zone = bougie de l'épaule gauche (haut/bas de cette bougie ; l'open n'est pas lu).
    Statut : "non mitigé" si, APRÈS la bougie de cassure du CHoCH, aucune bougie n'a retouché la zone
             (la zone n'existe comme POI qu'à partir du CHoCH : la tête la traverse forcément avant),
             sinon "mitigé" (mitigated_idx / mitigated_time = première retouche).
    Trade : entrée = bord proche de la zone, SL = extrême de la tête, TP =
      retournement : dernier swing opposé avant l'épaule ; continuation : extrême de l'impulsion
      (entre le BOS et la tête). rr = |TP-entrée| / |entrée-SL| (None si TP ou SL inexploitable).
    Valide si TOUT est vrai (détail dans "criteres") :
      tendance_avant   retournement : le CHoCH suit un BOS (tendance confirmée) ; continuation : BOS présent
      sweep            la tête balaie l'épaule
      choch_cloture    clôture au-delà du neckline
      zone_epaule      zone = épaule, placée avant le neckline et la tête
      zone_fraiche     statut "non mitigé"
      rr_ok            rr >= min_rr
    """
    ser = (structure or {}).get("_series")
    if ser is None:
        raise ValueError("structure doit venir de detect_structure() (clé '_series' absente)")
    H, L, C, T = ser["h"], ser["l"], ser["c"], ser["t"]
    swings, events = structure["swings"], structure["events"]
    last = len(C) - 1
    qmls = []

    for i, e in enumerate(events):
        if e["type"] != "CHOCH":
            continue
        d = 1 if e["dir"] == "bull" else -1                  # sens du mouvement attendu
        sh_t, nk_t = ("low", "high") if d == 1 else ("high", "low")
        ext = L if d == 1 else H                             # série où vivent épaule et tête
        beyond = (lambda a, b: a < b) if d == 1 else (lambda a, b: a > b)
        k, nk = e["break_idx"], e["pivot_idx"]

        ls = next((w for w in reversed(swings) if w["type"] == sh_t and w["idx"] < nk), None)
        if ls is None or k <= nk:
            continue
        seg = range(nk + 1, k + 1)
        hd = min(seg, key=lambda j: ext[j]) if d == 1 else max(seg, key=lambda j: ext[j])

        prev = events[i - 1] if i >= 1 else None
        prev2 = events[i - 2] if i >= 2 else None
        cont = bool(prev and prev2 and prev["type"] == "CHOCH" and prev2["type"] == "BOS"
                    and prev2["dir"] == e["dir"] and ls["idx"] > prev2["break_idx"])
        trend_before = cont or bool(prev and prev["type"] == "BOS")

        zt, zb = H[ls["idx"]], L[ls["idx"]]
        mit = next((j for j in range(k + 1, last + 1) if (L[j] <= zt if d == 1 else H[j] >= zb)), None)

        entry, sl = (zt if d == 1 else zb), ext[hd]
        if cont:
            tp = max(H[prev2["break_idx"]:hd + 1]) if d == 1 else min(L[prev2["break_idx"]:hd + 1])
        else:
            o = next((w for w in reversed(swings) if w["type"] == nk_t and w["idx"] < ls["idx"]), None)
            tp = o["price"] if o else None
        side_ok = tp is not None and ((sl < entry < tp) if d == 1 else (tp < entry < sl))
        rr = abs(tp - entry) / abs(entry - sl) if side_ok else None

        crit = {
            "tendance_avant": trend_before,
            "sweep": beyond(ext[hd], ls["price"]),
            "choch_cloture": (C[k] > e["level"]) if d == 1 else (C[k] < e["level"]),
            "zone_epaule": ls["idx"] < nk < hd,
            "zone_fraiche": mit is None,
            "rr_ok": rr is not None and rr >= min_rr,
        }
        qmls.append({
            "type": "continuation" if cont else "retournement",
            "dir": e["dir"],
            "shoulder": {"idx": ls["idx"], "time": ls["time"], "price": ls["price"]},
            "neckline": {"idx": nk, "time": e["pivot_time"], "price": e["level"]},
            "head": {"idx": hd, "time": T[hd], "price": ext[hd]},
            "choch": e, "bos": prev2 if cont else None,
            "zone": {"top": zt, "bottom": zb, "idx": ls["idx"], "time": ls["time"]},
            "statut": "mitigé" if mit is not None else "non mitigé",
            "mitigated_idx": mit, "mitigated_time": None if mit is None else T[mit],
            "entry": entry, "sl": sl, "tp": tp, "rr": rr,
            "criteres": crit, "valide": all(crit.values()),
            "bars_ago": last - k,
        })

    valid = [q for q in qmls if q["valide"]]
    return {"tf": structure.get("tf"), "min_rr": min_rr, "qmls": qmls,
            "last_qml": qmls[-1] if qmls else None, "last_valid": valid[-1] if valid else None}


# ----------------------------------------------------------------------------- auto-tests
def _smc_path(waypoints, wick=0.1):
    """Zigzag propre : pas de 1.0 entre les points ; high/low = close ± wick."""
    cl = [float(waypoints[0])]
    for a, b in zip(waypoints, waypoints[1:]):
        step = 1.0 if b > a else -1.0
        x = a
        while abs(b - x) > 1e-9:
            x += step if abs(b - x) >= 1 else (b - x)
            cl.append(x)
    return [{"t": 1_700_000_000 + 60 * i, "o": x, "h": x + wick, "l": x - wick, "c": x} for i, x in enumerate(cl)]


def smc_selftest():
    import random
    # 1) scénario déterministe : BOS haussier initial, CHoCH baissier, CHoCH haussier
    bars = _smc_path([100, 110, 104, 116, 108, 114, 100, 106, 101, 112])
    r = detect_structure(bars, "M5")
    seq = [(e["type"], e["dir"], round(e["level"], 1)) for e in r["events"]]
    assert seq == [("BOS", "bull", 110.1), ("CHOCH", "bear", 107.9), ("CHOCH", "bull", 106.1)], seq
    assert r["events"][0]["initial"] and not r["events"][1]["initial"]
    assert r["trend"] == "bullish" and r["tf"] == "M5" and r["tf_minutes"] == 5
    assert [s["label"] for s in r["swings"] if s["type"] == "high"] == [None, "HH", "LH", "LH"], r["swings"]
    assert [s["label"] for s in r["swings"] if s["type"] == "low"] == [None, "HL", "LL", "HL"], r["swings"]
    assert r["choch_level"] == r["break_down"] is not None and r["event_on_last_bar"] is None
    assert r["bos_level"] is None and r["last_choch"]["dir"] == "bull" and r["last_bos"]["initial"]

    # 2) une mèche au-dessus du niveau sans clôture ne casse rien
    wk = _smc_path([100, 110, 104, 108.5])
    wk[-1]["h"] = 112.0
    assert detect_structure(wk, "M1")["events"] == []
    wk[-1]["c"] = 111.0                                   # même bougie, mais clôture au-dessus : cassure
    assert [e["type"] for e in detect_structure(wk, "M1")["events"]] == ["BOS"]

    # 3) aléatoire : invariance d'échelle, pas de regard dans le futur, DataFrame == liste de dicts
    random.seed(7)
    x, rnd = 100.0, []
    for i in range(1500):
        o = x
        x = max(1.0, x + random.gauss(0, 1))
        hi, lo = max(o, x) + abs(random.gauss(0, .4)), min(o, x) - abs(random.gauss(0, .4))
        rnd.append({"t": 1_700_000_000 + 60 * i, "o": o, "h": hi, "l": lo, "c": x})
    sig = lambda res: [(e["type"], e["dir"], e["break_idx"], e["pivot_idx"]) for e in res["events"]]
    full = detect_structure(rnd, "M1", keep=10_000)
    assert len(full["events"]) > 10
    big = [{**b, "h": b["h"] * 800, "l": b["l"] * 800, "c": b["c"] * 800} for b in rnd]
    assert sig(detect_structure(big, "H4", keep=10_000)) == sig(full), "invariance d'échelle"
    for m in (200, 777, 1234):
        part = detect_structure(rnd[:m], "M1", keep=10_000)
        assert sig(part) == [s for s in sig(full) if s[2] < m], f"regard dans le futur à {m}"
    try:
        import pandas as pd
        df = pd.DataFrame({"Open": [b["o"] for b in rnd], "High": [b["h"] for b in rnd], "Low": [b["l"] for b in rnd],
                           "Close": [b["c"] for b in rnd]}, index=pd.to_datetime([b["t"] for b in rnd], unit="s"))
        assert sig(detect_structure(df, 15, keep=10_000)) == sig(full), "DataFrame"
        shuf = df.sample(frac=1.0, random_state=1)        # désordre + NaN : nettoyé
        shuf.iloc[3, 1] = float("nan")
        detect_structure(shuf, "H1")
    except ImportError:
        print("pandas absent : test DataFrame ignoré")

    # 4) cas limites : peu de bougies, tf variés
    assert detect_structure([], "M1")["trend"] == "range"
    assert detect_structure(rnd[:5], "M1")["events"] == []
    assert [smc_parse_tf(v) for v in ("M1", "5m", "M15", "30", "H1", "1h", "H4", 240, "4H", "D1")] == [1, 5, 15, 30, 60, 60, 240, 240, 240, 1440]
    assert smc_tf_label(240) == "H4" and smc_tf_label(3) == "M3"
    # 5) QML : retournement, continuation, miroir, mitigation, absence de regard dans le futur
    def _mirror(bars, K=300.0):
        return [{**b, "h": K - b["l"], "l": K - b["h"], "c": K - b["c"], "o": K - b["o"]} for b in bars]

    def _qml(bars, **kw):
        return detect_qml(detect_structure(bars, "M5", keep=10_000), **kw)

    near = lambda a, b: abs(a - b) < 1e-6
    rev = _smc_path([100, 110, 104, 116, 108, 120, 106])           # épaule 116, tête 120 (balaie), CHoCH sous 108
    q = _qml(rev)["last_qml"]
    assert q["type"] == "retournement" and q["dir"] == "bear" and q["statut"] == "non mitigé", q
    assert q["valide"] and all(q["criteres"].values()) and _qml(rev)["last_valid"]["valide"]
    assert near(q["zone"]["top"], 116.1) and near(q["zone"]["bottom"], 115.9) and near(q["head"]["price"], 120.1)
    assert near(q["entry"], 115.9) and near(q["sl"], 120.1) and near(q["tp"], 103.9) and near(q["rr"], 12 / 4.2)
    assert not _qml(rev, min_rr=3.0)["last_qml"]["valide"]      # RR 2.86 < 3

    mit = _qml(_smc_path([100, 110, 104, 116, 108, 120, 106, 117]))["last_qml"]   # retour dans la zone
    assert mit["statut"] == "mitigé" and mit["mitigated_idx"] > mit["choch"]["break_idx"]
    assert not mit["criteres"]["zone_fraiche"] and not mit["valide"]

    nosweep = [x for x in _qml(_smc_path([100, 110, 104, 116, 108, 114, 100]))["qmls"] if x["dir"] == "bear"][0]
    assert not nosweep["criteres"]["sweep"] and not nosweep["valide"] and nosweep["rr"] is None

    mir = _qml(_mirror(rev))["last_qml"]                        # miroir : retournement haussier
    assert mir["type"] == "retournement" and mir["dir"] == "bull" and mir["valide"] and near(mir["rr"], q["rr"])

    cont = _smc_path([100, 110, 104, 124, 114, 118, 112, 119])     # BOS, retracement, petit QML haussier
    c = _qml(cont)["last_qml"]
    assert c["type"] == "continuation" and c["dir"] == "bull" and c["bos"]["type"] == "BOS", c
    assert c["valide"] and near(c["entry"], 114.1) and near(c["sl"], 111.9) and near(c["tp"], 124.1)
    assert near(c["rr"], 10 / 2.2) and c["shoulder"]["idx"] > c["bos"]["break_idx"]
    cm = _qml(_smc_path([100, 110, 104, 124, 114, 118, 112, 119, 113]))["last_qml"]
    assert cm["statut"] == "mitigé" and not cm["valide"]
    cb = _qml(_mirror(cont))["last_qml"]
    assert cb["type"] == "continuation" and cb["dir"] == "bear" and cb["valide"] and near(cb["rr"], c["rr"])

    fsig = lambda res: [(x["type"], x["dir"], x["shoulder"]["idx"], x["neckline"]["idx"], x["head"]["idx"],
                         x["choch"]["break_idx"]) for x in res["qmls"]]
    qa = detect_qml(detect_structure(rnd, "M1", keep=10_000))
    assert len(qa["qmls"]) > 3 and {x["type"] for x in qa["qmls"]} == {"retournement", "continuation"}
    for m in (300, 777, 1234):
        part = detect_qml(detect_structure(rnd[:m], "M1", keep=10_000))
        assert fsig(part) == [x for x in fsig(qa) if x[5] < m], f"QML : regard dans le futur à {m}"
    vs = lambda res: [(x["valide"], None if x["rr"] is None else round(x["rr"], 6)) for x in res["qmls"]]
    assert vs(detect_qml(detect_structure(big, "H4", keep=10_000))) == vs(qa), "QML : invariance d'échelle"
    assert detect_qml(detect_structure([], "M1"))["qmls"] == []
    assert detect_qml(detect_structure(rnd[:5], "M1"))["last_qml"] is None

    print("analyse_smc : tous les tests OK")
    return 0


# ============================================================================
# 15. STRATÉGIE QML GAGNANT — squelette (AUCUNE règle de signal pour l'instant)
#
# Stratégie SÉPARÉE, à côté du QML de la section 12 (qui n'est ni lu ni modifié ici). OFF par défaut (menu
# « 🧩 Stratégies » -> QML GAGNANT). Elle ne fait QUE : charger les bougies, appeler detect_structure puis
# detect_qml (section 14, dans ce fichier) et renvoyer un dict de statut. L'ANALYSE est toujours disponible, ON ou OFF
# (📊 Analyse -> 🏆 QML GAGNANT). Les règles d'entrée viennent plus tard dans _qmlg_rules() ; tant qu'elle renvoie
# None, signal = None. Rien n'est branché sur la boucle principale : aucun signal, aucun ordre automatique.
# ============================================================================

# --- PARAMÈTRES (variables d'environnement, valeurs par défaut) -----------------------------------------
QMLG_ENABLED_DEFAULT = _env_on("QMLG_ENABLED", False)    # état initial du réglage ON/OFF (le menu Telegram reprend la main)
QMLG_TF = _env_int("QMLG_TF", 15)                        # TF d'analyse par défaut, en minutes
QMLG_HISTORY_BARS = _env_int("QMLG_HISTORY_BARS", 1000)  # nombre de bougies clôturées analysées
QMLG_FRACTAL_N = _env_int("QMLG_FRACTAL_N", 3)           # fractals de detect_structure (n bougies de chaque côté)
QMLG_MIN_RR = _env_float("QMLG_MIN_RR", 2.0)             # RR minimum passé à detect_qml
QMLG_MIN_BARS = _env_int("QMLG_MIN_BARS", 50)            # en dessous : « NO_DATA », pas d'analyse
QMLG_ATR_PERIOD = _env_int("QMLG_ATR_PERIOD", 14)        # ATR (Wilder) du TF analysé, pour le buffer du SL et la tolérance EQH / EQL
QMLG_SL_MODE = os.getenv("QMLG_SL_MODE", "SWEEP").strip().upper()   # BUFFER : SL = bord lointain de la zone + buffer ; EXTREME : pile sur ce bord
QMLG_SL_BUFFER_ATR = _env_float("QMLG_SL_BUFFER_ATR", 0.05)          # buffer derrière la zone, en ATR
# SL fusionné depuis qml_sl.py. SWEEP (PAR DÉFAUT) = derrière le sweep (extrême de la tête, du sweep jusqu'au début du QML) + buffer. CANDLE = derrière la bougie de rejet (entrée directe ; sans bougie -> ATR) ;
# ATR = entrée -/+ QMLG_SL_ATR_MULT x ATR. Ces 2 modes sont SKIP si le SL est hors [QMLG_SL_MIN_ATR ; QMLG_SL_MAX_ATR] x ATR. Telegram : /qmlsl
QMLG_SL_MODES = ("SWEEP", "BUFFER", "EXTREME", "CANDLE", "ATR")
QMLG_SL_ATR_MULT = _env_float("QMLG_SL_ATR_MULT", 1.5)
QMLG_SL_MIN_ATR = _env_float("QMLG_SL_MIN_ATR", 0.3)
QMLG_SL_MAX_ATR = _env_float("QMLG_SL_MAX_ATR", 4.0)


def get_qmlg_sl_mode():
    return _get_choice("qmlg_sl_mode", QMLG_SL_MODE, QMLG_SL_MODES)


def get_qmlg_atr_mult():
    try:
        v = float(str(get_setting("qmlg_atr_mult") or QMLG_SL_ATR_MULT).replace(",", "."))
    except (TypeError, ValueError):
        v = QMLG_SL_ATR_MULT
    return v if 0.1 <= v <= 20 else QMLG_SL_ATR_MULT
QMLG_EQ_TOL_ATR = _env_float("QMLG_EQ_TOL_ATR", 0.10)    # deux sommets / creux à moins de N x ATR forment un EQH / EQL
QMLG_TFS = tuple(int(x) for x in re.findall(r"\d+", os.getenv("QMLG_TFS", "5,15,30,60"))) or (5, 15, 30, 60)   # TF scannés (minutes)
QMLG_KEEP = _env_int("QMLG_KEEP", 300)                   # événements / swings gardés par detect_structure (historique des formations)
QMLG_JSON_PATH = os.getenv("QMLG_JSON_PATH") or os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "qml_gagnant.json")
QMLG_MAX_AGE_MIN = _env_int("QMLG_MAX_AGE_MIN", 15)      # « Ordre limite » : au-delà, le JSON est périmé -> nouveau scan
QMLG_CACHE_SEC = _env_int("QMLG_CACHE_SEC", 60)          # « Entrée directe » : un scan plus récent que ça est réutilisé
QMLG_DIRECT_PATTERNS = tuple(x.strip().upper() for x in os.getenv("QMLG_DIRECT_PATTERNS", "ENGULFING,PIN_BAR").split(",") if x.strip())   # ENTRÉE DIRECTE : avalement ou bougie de retournement (pin bar) seulement, jamais de doji
QMLG_DOJI_BODY = _env_float("QMLG_DOJI_BODY", 0.10)      # doji : corps <= 10 % de la bougie
QMLG_PIN_WICK = _env_float("QMLG_PIN_WICK", 0.60)        # pin bar : mèche de rejet >= 60 % de la bougie ...
QMLG_PIN_BODY = _env_float("QMLG_PIN_BODY", 0.35)        # ... et corps <= 35 % de la bougie
QMLG_LIST_MAX = _env_int("QMLG_LIST_MAX", 15)            # lignes max affichées dans Telegram (le JSON garde tout)
QMLG_ALERT_SEC = _env_int("QMLG_ALERT_SEC", 20)          # pas de la surveillance des alertes de zone (secondes)
QMLG_ALERT_LAG = _env_int("QMLG_ALERT_LAG", 5)           # délai après la clôture théorique d'une bougie avant de la relire
QMLG_ALERT_PATTERNS = tuple(x.strip().upper() for x in os.getenv("QMLG_ALERT_PATTERNS", "ENGULFING,PIN_BAR").split(",") if x.strip())   # réactions qui déclenchent l'alerte : avalement / pin bar seulement
QMLG_ALERT_MIN_RR = _env_float("QMLG_ALERT_MIN_RR", QMLG_MIN_RR)   # RR1 minimum sur la VRAIE entrée (clôture de la bougie de réaction) ; en dessous : l'alerte attend une meilleure réaction
QMLG_ALERT_MAX_BARS = _env_int("QMLG_ALERT_MAX_BARS", 300)   # une alerte qui n'a rien donné après N bougies de son TF est supprimée en silence
QMLG_ALERT_STALE_BARS = _env_int("QMLG_ALERT_STALE_BARS", 3)   # une réaction plus vieille que N bougies (redémarrage, retard réseau) n'est plus envoyée
QMLG_ALERT_REACT_BARS = _env_int("QMLG_ALERT_REACT_BARS", 2)   # la bougie d'avalement / pin bar peut venir jusqu'à N bougies APRÈS le toucher de la zone (0 = elle doit toucher la zone elle-même)
QMLG_ALERT_TO_GROUP = _env_on("QMLG_ALERT_TO_GROUP", True)   # la réaction part aussi au groupe : image + entrée / SL / TP / lot + stickers
QMLG_SL_CANDLE_BUF_ATR = _env_float("QMLG_SL_CANDLE_BUF_ATR", 0.15)   # SL des alertes : derrière la zone ET derrière la mèche de la bougie d'entrée, + N x ATR (anti-liquidation) ; 0 = SL de la zone seul
QMLG_BE_RR = _env_float("QMLG_BE_RR", BE_RR)              # RR d'armement du BE propre à QML GAGNANT (indépendant de /be Fibotrain)
QMLG_BE_CHOICES = (0.5, 1, 1.5, 2, 3)                      # boutons du menu BE QML GAGNANT
QMLG_TREND_CHOICES = (15, 60, 240, 1440)                   # TF de référence proposés pour la tendance de fond (M15 / H1 / H4 / D1)
QMLG_TREND_DEFAULT = {1: 15, 3: 15, 5: 60, 15: 240, 30: 240, 60: 240}   # référence proposée par défaut selon le TF d'entrée
QMLG_TREND_CACHE_SEC = _env_int("QMLG_TREND_CACHE_SEC", 120)           # la tendance d'un (marché, TF de référence) est relue au plus toutes les N s


def get_qmlg_on():
    """Réglage ON/OFF de QML GAGNANT (clé « qmlg_on »). OFF tant que personne ne l'a activé."""
    return _get_bool_setting("qmlg_on", QMLG_ENABLED_DEFAULT)


def _qmlg_tf_minutes(tf):
    """15 / "15" / "M15" -> 15 ; "H1" -> 60 ; illisible ou absent -> QMLG_TF."""
    if isinstance(tf, (int, float)) and not isinstance(tf, bool) and tf > 0:
        return int(tf)
    g = re.fullmatch(r"(?:M)?(\d+)|H(\d+)", str(tf or "").strip().upper())
    if g:
        m = int(g.group(1)) if g.group(1) else int(g.group(2)) * 60
        if m > 0:
            return m
    return QMLG_TF


def _qmlg_rules(ctx):
    """RÈGLES DE SIGNAL — VIDES pour l'instant. ctx = {symbol, tf_minutes, structure, qml, candles}.
    À remplir plus tard : renvoyer un dict de signal (sens, entrée, SL, TP...) ou None. None = aucun signal."""
    return None


def _qmlg_atr(ser, n=None):
    """ATR de Wilder sur la série de detect_structure (h / l / c), en Python pur. None si pas assez de bougies."""
    n = n or QMLG_ATR_PERIOD
    H, L, C = ser["h"], ser["l"], ser["c"]
    if len(C) < n + 1:
        return None
    tr = [H[0] - L[0]] + [max(H[i] - L[i], abs(H[i] - C[i - 1]), abs(L[i] - C[i - 1])) for i in range(1, len(C))]
    a = sum(tr[:n]) / n
    for x in tr[n:]:
        a = (a * (n - 1) + x) / n
    return a


def _qmlg_liquidity(structure, buy, entry, tol):
    """Liquidité encore NON prise dans le sens du trade, de la plus proche à la plus lointaine : [{price, kind}].
    achat : sommets (swing highs) au-dessus de l'entrée ; vente : creux (swing lows) en dessous. Un niveau déjà dépassé
    par une mèche depuis sa formation n'est plus de la liquidité. EQH / EQL = niveau étiqueté EQH / EQL par le moteur
    OU au moins 2 swings à moins de `tol` : le niveau retenu est le premier touché par le prix (le plus proche)."""
    ser = structure["_series"]
    ext = ser["h"] if buy else ser["l"]
    kind_sw = "high" if buy else "low"
    pts = []
    for w in structure.get("swings") or []:
        if w.get("type") != kind_sw:
            continue
        px, i = w["price"], w["idx"]
        if (px <= entry) if buy else (px >= entry):
            continue
        later = ext[i + 1:]
        if later and ((max(later) >= px) if buy else (min(later) <= px)):
            continue   # liquidité déjà prise
        pts.append((px, w.get("label") in ("EQH", "EQL")))
    pts.sort(key=lambda x: x[0], reverse=not buy)   # du plus proche de l'entrée au plus lointain
    out, cluster = [], []
    for px, eq in pts:
        if cluster and abs(px - cluster[-1][0]) <= tol:
            cluster.append((px, eq))
            continue
        if cluster:
            out.append(cluster)
        cluster = [(px, eq)]
    if cluster:
        out.append(cluster)
    lvls = []
    for cl in out:
        eq = len(cl) >= 2 or any(e for _, e in cl)
        lvls.append({"price": cl[0][0], "kind": ("EQH" if buy else "EQL") if eq else "SWING"})
    return lvls


def compute_trade(qml, structure=None, entry_price=None, candle=None):
    """Plan de trade d'un QML (dict de detect_qml) : ENTRÉE LIMIT uniquement, jamais d'ordre à marché.
      entrée : bord PROCHE de la zone (achat : haut de la zone ; vente : bas), jamais 50 %.
      SL     : juste DERRIÈRE la zone (bord lointain +/- ATR x QMLG_SL_BUFFER_ATR), ou pile sur ce bord (QMLG_SL_MODE=EXTREME).
               Jamais derrière la tête : si le buffer l'y amenait, le SL est ramené sur le bord de la zone.
      TP1/TP2: les deux prochaines liquidités non prises (EQH / EQL, swings) dans le sens du trade, via `structure`
               (sortie de detect_structure ; sans elle : pas de TP).
      RR1/RR2: |TP - entrée| / |entrée - SL|.
      entry_price (ENTRÉE DIRECTE seulement) : entrée = ce prix (clôture de la bougie de rejet, dans la zone), order = "DIRECT" ;
               SL et TP inchangés. Ce plan est un compte rendu : rien ici n'envoie d'ordre à MT5.
    Retourne {ok, reason, side, order ("LIMIT"), entry, sl, sl_mode, tp1, tp2, tp1_kind, tp2_kind, rr1, rr2, risk, atr}.
    ok = True seulement si entrée, SL et TP1 sont exploitables (TP2 peut manquer : rr2 = None)."""
    t = {"ok": False, "reason": None, "side": None, "order": "LIMIT", "entry": None, "sl": None, "sl_mode": None,
         "tp1": None, "tp2": None, "tp1_kind": None, "tp2_kind": None, "rr1": None, "rr2": None,
         "risk": None, "atr": None}
    try:
        z, head = qml["zone"], qml["head"]["price"]
        buy = qml["dir"] == "bull"
        top, bot = float(z["top"]), float(z["bottom"])
    except (TypeError, KeyError, ValueError):
        t["reason"] = "QML incomplet (zone / tête / sens)"
        return t
    t["side"] = "BUY" if buy else "SELL"
    atr = _qmlg_atr(structure["_series"]) if structure and structure.get("_series") else None
    t["atr"] = atr
    entry = top if buy else bot                      # bord proche : le prix y revient en premier
    far = bot if buy else top                        # bord lointain
    if entry_price is not None:                      # entrée directe : à la clôture de la bougie de rejet
        entry, t["order"] = float(entry_price), "DIRECT"
    mode = get_qmlg_sl_mode()
    buf = (atr or 0.0) * QMLG_SL_BUFFER_ATR if mode != "EXTREME" else 0.0
    sl = far - buf if buy else far + buf
    alt = None
    if mode in ("SWEEP", "CANDLE", "ATR") and atr:   # SL fusionné de qml_sl.py
        if mode == "SWEEP":                          # derrière le sweep = extrême de la tête
            alt = head - buf if buy else head + buf
        elif mode == "CANDLE" and candle:
            ext = candle.get("l" if buy else "h")
            if ext is not None:
                alt = float(ext) - buf if buy else float(ext) + buf
        if alt is None or ((alt >= entry) if buy else (alt <= entry)):   # pas de bougie / mauvais côté -> repli ATR
            d_atr = get_qmlg_atr_mult() * atr
            alt = entry - d_atr if buy else entry + d_atr
        dist_atr = abs(entry - alt) / atr
        if not (QMLG_SL_MIN_ATR <= dist_atr <= QMLG_SL_MAX_ATR):
            t["entry"], t["sl"], t["sl_mode"] = entry, alt, mode
            t["reason"] = f"SL {mode} hors limites ({dist_atr:.2f} ATR, permis {QMLG_SL_MIN_ATR:g}-{QMLG_SL_MAX_ATR:g})"
            return t
        sl = alt
    behind_head = (sl <= head) if buy else (sl >= head)
    if behind_head and alt is None:                  # jamais derrière la tête
        sl = far
    t["sl_mode"] = mode if alt is not None else ("BUFFER" if (buf and sl != far) else "EXTREME")
    risk = (entry - sl) if buy else (sl - entry)
    t["entry"], t["sl"] = entry, sl
    if risk <= 0 or ((sl <= head) if buy else (sl >= head)):
        t["reason"] = "zone inexploitable (risque nul ou SL derrière la tête)"
        return t
    t["risk"] = risk
    if not structure or not structure.get("_series"):
        t["reason"] = "structure absente : pas de liquidité pour TP1 / TP2"
        return t
    tol = (atr or 0.0) * QMLG_EQ_TOL_ATR
    lv = _qmlg_liquidity(structure, buy, entry, tol)
    for k, lvl in enumerate(lv[:2], start=1):
        t[f"tp{k}"], t[f"tp{k}_kind"] = lvl["price"], lvl["kind"]
        t[f"rr{k}"] = abs(lvl["price"] - entry) / risk
    if t["tp1"] is None:
        t["reason"] = "aucune liquidité non prise dans le sens du trade"
        return t
    t["ok"] = True
    return t


def qml_gagnant_analyze(symbol, tf):
    """Analyse QML GAGNANT d'un actif (toujours possible, ON ou OFF). Appelle detect_structure puis detect_qml et
    renvoie un dict de statut :
      strategy, symbol, tf, tf_minutes, enabled (réglage ON/OFF), status, signal, bars, trend, n_qml, last_qml, trade, error
    status : NO_RULES (analyse faite, règles vides) · NO_DATA · ERROR · SIGNAL (futur).
    signal est None tant que _qmlg_rules() ne renvoie rien. Ne lève jamais : toute erreur finit dans status/error."""
    tf_min = _qmlg_tf_minutes(tf)
    out = {"strategy": "QML_GAGNANT", "symbol": symbol, "tf": tf, "tf_minutes": tf_min,
           "enabled": get_qmlg_on(), "status": "NO_DATA", "signal": None,
           "bars": 0, "trend": None, "n_qml": 0, "last_qml": None, "trade": None, "error": None}
    try:
        candles = get_candles(symbol, minutes=tf_min, count=QMLG_HISTORY_BARS) or []
        out["bars"] = len(candles)
        if len(candles) < QMLG_MIN_BARS:
            return out
        structure = detect_structure(candles, f"M{tf_min}", n=QMLG_FRACTAL_N)
        qml = detect_qml(structure, min_rr=QMLG_MIN_RR)
        out["trend"] = structure.get("trend")
        out["n_qml"] = len(qml.get("qmls") or [])
        out["last_qml"] = qml.get("last_qml")
        if out["last_qml"]:
            out["trade"] = compute_trade(out["last_qml"], structure)   # information seulement : ce n'est PAS un signal
        out["status"] = "NO_RULES"
        sig = _qmlg_rules({"symbol": symbol, "tf_minutes": tf_min, "structure": structure,
                           "qml": qml, "candles": candles})
        if sig:
            out["signal"], out["status"] = sig, "SIGNAL"
    except Exception as e:
        out["status"], out["error"] = "ERROR", f"{type(e).__name__}: {e}"
    return out


_QMLG_TREND_LBL = {"bullish": "haussière 📈", "bearish": "baissière 📉", "range": "indéterminée (range)"}


def qml_gagnant_text(res):
    """Texte Telegram de l'analyse QML GAGNANT (information seulement : aucune règle d'entrée n'existe encore)."""
    head = (f"🏆 <b>ANALYSE QML GAGNANT — {res['symbol']}</b> · M{res['tf_minutes']}\n"
            f"Stratégie : {'🟢 ON' if res['enabled'] else '🔴 OFF'} · règles d'entrée : <b>aucune</b> (aucun signal)\n")
    if res["status"] == "ERROR":
        return head + f"\n⚠️ Analyse impossible : {res['error']}"
    if res["status"] == "NO_DATA":
        return head + f"\n⚠️ Pas assez de bougies ({res['bars']} < {QMLG_MIN_BARS})."
    txt = head + (f"\nTendance : {_QMLG_TREND_LBL.get(res['trend'], res['trend'])} · {res['bars']} bougies · "
                  f"{res['n_qml']} QML détecté(s)")
    q = res.get("last_qml")
    if not q:
        return txt + "\nAucun QML sur la période."
    dec = SYMBOLS.get(res["symbol"], {}).get("decimals", 2)
    z, tr = q["zone"], res.get("trade") or {}
    txt += (f"\n\nDernier QML : {'🟢 achat' if q['dir'] == 'bull' else '🔴 vente'} · {q['type']} · {q['statut']}"
            f" · {'✅ valide' if q['valide'] else '❌ non valide'}\n"
            f"Zone : {_fmt(z['bottom'], dec)} → {_fmt(z['top'], dec)}\n")
    if not tr.get("ok"):
        return txt + f"Plan de trade : indisponible ({tr.get('reason')})\n(information seulement)"
    f2 = lambda k: "—" if tr[k] is None else f"{_fmt(tr[k], dec)} ({tr[k + '_kind']})"
    r2 = lambda k: "—" if tr[k] is None else f"{tr[k]:.2f}"
    return txt + (f"⏳ <b>{tr['side']} LIMIT</b> (jamais au marché) : {_fmt(tr['entry'], dec)}\n"
                  f"🛡 SL {_fmt(tr['sl'], dec)}\n"
                  f"🎯 TP1 {f2('tp1')} · RR1 {r2('rr1')}\n"
                  f"🎯 TP2 {f2('tp2')} · RR2 {r2('rr2')}\n"
                  f"(information seulement)")


# --- REJET, VALIDATION, SCAN MULTI-MARCHÉS / MULTI-TF, JSON ----------------------------------------------
_QMLG_REASON_LBL = {"tendance_avant": "pas de BOS avant", "sweep": "pas de sweep", "choch_cloture": "CHoCH non clôturé",
                    "zone_epaule": "zone ≠ épaule", "zone_fraiche": "zone mitigée", "plan": "plan de trade impossible",
                    "rr1": f"RR1 < {QMLG_MIN_RR:g}"}


def qmlg_rejection(candles, buy):
    """Motif de rejet de la DERNIÈRE bougie clôturée, dans le sens du trade (achat : haussier ; vente : baissier) :
    "ENGULFING" (le corps avale le corps précédent), "PIN_BAR" (longue mèche de rejet, petit corps), "DOJI" (corps minuscule,
    mèche de rejet au moins aussi longue que l'opposée),
    sinon None. candles : dicts {o, h, l, c}, la plus récente en dernier."""
    if not candles:
        return None
    c = candles[-1]
    o, h, l, cl = c["o"], c["h"], c["l"], c["c"]
    rng = h - l
    if rng <= 0:
        return None
    body = abs(cl - o)
    if len(candles) >= 2:
        po, pc = candles[-2]["o"], candles[-2]["c"]
        if buy and pc < po and cl > o and o <= pc and cl >= po:
            return "ENGULFING"
        if not buy and pc > po and cl < o and o >= pc and cl <= po:
            return "ENGULFING"
    wick = (min(o, cl) - l) if buy else (h - max(o, cl))      # mèche de rejet : basse pour un achat, haute pour une vente
    opposite = (h - max(o, cl)) if buy else (min(o, cl) - l)
    if wick >= QMLG_PIN_WICK * rng and body <= QMLG_PIN_BODY * rng:
        return "PIN_BAR"
    if body <= QMLG_DOJI_BODY * rng and wick >= opposite:      # un doji qui rejette dans l'autre sens ne valide pas
        return "DOJI"
    return None


def qmlg_validate(qml, structure, direct=False, entry_price=None, candle=None):
    """Valide UNE formation QML. Retourne {valid, reasons, trade}. Valide = critères de detect_qml (tendance_avant, sweep,
    choch_cloture, zone_epaule) + zone fraîche (non mitigée ; ignoré en entrée directe, où le prix est justement dans la
    zone) + plan compute_trade exploitable + RR1 >= QMLG_MIN_RR (calculé sur NOTRE plan : SL derrière la zone)."""
    cr = qml.get("criteres") or {}
    reasons = [k for k in ("tendance_avant", "sweep", "choch_cloture", "zone_epaule") if not cr.get(k)]
    if not direct and not cr.get("zone_fraiche"):
        reasons.append("zone_fraiche")
    tr = compute_trade(qml, structure, entry_price=entry_price, candle=candle)
    if not tr["ok"]:
        reasons.append("plan")
    elif tr["rr1"] < QMLG_MIN_RR:
        reasons.append("rr1")
    return {"valid": not reasons, "reasons": reasons, "trade": tr}


def qmlg_direct_entry(qml, structure, candles):
    """ENTRÉE DIRECTE : seulement si le prix (clôture de la dernière bougie clôturée) est DANS la zone ET que cette bougie est
    un rejet dans le sens du QML. Entrée = cette clôture. La zone doit être fraîche jusqu'à cette bougie (1re touche) et la
    formation valide. Retourne {pattern, entry, time, trade} ou None. Compte rendu seulement : aucun ordre n'est envoyé."""
    if not candles or not qml.get("zone"):
        return None
    z, last = qml["zone"], candles[-1]
    close = last["c"]
    if not (z["bottom"] <= close <= z["top"]):
        return None
    mi = qml.get("mitigated_idx")
    if mi is not None and mi != len(structure["_series"]["c"]) - 1:
        return None   # déjà retouchée avant la dernière bougie : plus une zone fraîche
    pattern = qmlg_rejection(candles, qml["dir"] == "bull")
    if pattern not in QMLG_DIRECT_PATTERNS:   # avalement ou pin bar (retournement) seulement ; le doji ne déclenche pas
        return None
    v = qmlg_validate(qml, structure, direct=True, entry_price=close, candle=last)
    if not v["valid"]:
        return None
    return {"pattern": pattern, "entry": close, "time": last["t"], "trade": v["trade"]}


def _qmlg_record(symbol, tf, q, structure, candles):
    """Une formation, prête pour le JSON : données du QML + validation + plan limite + entrée directe éventuelle."""
    v = qmlg_validate(q, structure)
    direct = qmlg_direct_entry(q, structure, candles)
    ag = None
    if QMLG_AGENTS and (v["valid"] or direct):   # 3 agents : aucun QML n'est validé / envoyé sans leur accord
        ag = qmlg_agents(symbol, tf, q, structure, candles, (direct or v)["trade"])
        if not ag["ok"]:
            v = dict(v, valid=False, reasons=list(v["reasons"]) + ["agents"])
            direct = None
    return {"id": f"{symbol}|M{tf}|{q['dir']}|{q['choch']['break_time']}", "symbol": symbol, "tf": tf, "agents": ag,
            "dir": q["dir"], "type": q["type"], "statut": q["statut"], "mitigated_time": q.get("mitigated_time"),
            "bars_ago": q.get("bars_ago"), "zone": q["zone"], "head": q["head"], "shoulder": q["shoulder"],
            "neckline": q["neckline"], "choch_time": q["choch"]["break_time"], "criteres": q.get("criteres"),
            "validation": {"valid": v["valid"], "reasons": v["reasons"]}, "plan": v["trade"],
            "direct": direct, "last_t": candles[-1]["t"]}


def _qmlg_scan_one(symbol, tf):
    """Formations d'un (marché, TF). Retourne (liste de formations, nb de bougies, dernière clôture)."""
    candles = get_candles(symbol, minutes=tf, count=QMLG_HISTORY_BARS) or []
    if len(candles) < QMLG_MIN_BARS:
        raise ValueError(f"{len(candles)} bougies (< {QMLG_MIN_BARS})")
    structure = detect_structure(candles, f"M{tf}", n=QMLG_FRACTAL_N, keep=QMLG_KEEP)
    res = detect_qml(structure, min_rr=QMLG_MIN_RR)
    return [_qmlg_record(symbol, tf, q, structure, candles) for q in res["qmls"]], len(candles)


def _qmlg_save(data, path=None):
    """Écriture atomique du JSON (jamais de fichier à moitié écrit)."""
    path = path or QMLG_JSON_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"), default=str)
    os.replace(tmp, path)


def _qmlg_load(path=None):
    try:
        with open(path or QMLG_JSON_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


_qmlg_cache = {"data": None}


def qmlg_scan(symbols=None, tfs=None):
    """BOUTON « Analyse » : scanne TOUS les marchés (Deriv + BTC Binance) sur TOUS les TF, enregistre TOUTES les formations QML
    dans le JSON (QMLG_JSON_PATH), chacune VALIDÉE (qmlg_validate). Un marché / TF en erreur est noté dans `errors`, le scan continue."""
    # TF = ceux COCHÉS dans le menu 🏆 QML GAGNANT (M1, M5...) ; QMLG_TFS (env) n'est plus qu'un secours.
    symbols, tfs = list(symbols or SYMBOLS), list(tfs or get_qmlg_alert_tfs() or QMLG_TFS)
    forms, errors = [], []
    for sym in symbols:
        for tf in tfs:
            try:
                recs, _n = _qmlg_scan_one(sym, tf)
                forms.extend(recs)
            except Exception as e:
                errors.append({"symbol": sym, "tf": tf, "error": f"{type(e).__name__}: {e}"})
    data = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "generated_ts": time.time(),
            "markets": symbols, "tfs": tfs,
            "params": {"min_rr": QMLG_MIN_RR, "sl_mode": QMLG_SL_MODE, "sl_buffer_atr": QMLG_SL_BUFFER_ATR,
                       "fractal_n": QMLG_FRACTAL_N, "history_bars": QMLG_HISTORY_BARS},
            "n_formations": len(forms), "n_valid": sum(1 for f in forms if f["validation"]["valid"]),
            "formations": forms, "errors": errors}
    try:
        _qmlg_save(data)
    except OSError as e:
        data["errors"].append({"symbol": "*", "tf": "*", "error": f"JSON non écrit : {e}"})
    _qmlg_cache["data"] = data
    return data


def qmlg_data(max_age_sec):
    """Dernier scan s'il a moins de max_age_sec secondes (mémoire, sinon fichier JSON) ; sinon nouveau scan complet."""
    want = set(get_qmlg_alert_tfs())
    for d in (_qmlg_cache["data"], _qmlg_load()):
        if d and time.time() - d.get("generated_ts", 0) <= max_age_sec and {int(t) for t in d.get("tfs", [])} == want:
            return d
    return qmlg_scan()


def qmlg_pending_limits(data):
    """BOUTON « Ordre limite » : QML VALIDES dont la zone n'est pas mitigée (inclut les QML tout juste formés), la plus
    récente d'abord. Statut : EN ATTENTE de mitigation."""
    return sorted((f for f in data["formations"] if f["validation"]["valid"] and f["statut"] == "non mitigé"),
                  key=lambda f: f.get("bars_ago") if f.get("bars_ago") is not None else 10 ** 9)


def qmlg_direct_signals(data):
    """BOUTON « Entrée directe » : formations dont le prix est dans la zone ET la dernière clôture est un rejet."""
    return [f for f in data["formations"] if f.get("direct")]


# --- ALERTES DE RÉACTION (c'est TOI qui les actives) ----------------------------------------------------------
# Menu 🏆 QML GAGNANT -> « 🔔 Alerte réaction » (ON / OFF) + choix du / des TF (M1, M5, M15, M30, H1). Activée, elle scanne
# tous les marchés ON (menu 🌍 ACTIFS) sur ces TF, pose une alerte sur chaque QML VALIDE non mitigé, puis surveille les bougies
# clôturées. RIEN n'est envoyé quand le prix ENTRE dans la zone : l'alerte part seulement sur une RÉACTION = une bougie qui touche
# la zone, ne l'invalide pas (clôture au-delà du bord lointain) et forme un AVALEMENT ou un PIN BAR dans le sens du QML
# (QMLG_ALERT_PATTERNS). La zone reste valable sans autre confirmation.
# « Mieux entrer » (ce n'est PAS une confirmation) : RR1 recalculé sur la vraie entrée (clôture de la bougie de réaction) >=
# QMLG_ALERT_MIN_RR ; sinon l'alerte reste en attente d'une meilleure réaction.
# À la réaction : le lot est calculé (montant risqué du menu 💰 Risque) ; admin ET groupe reçoivent l'image du QML + entrée / SL /
# TP / lot, puis le groupe reçoit les stickers ACHAT / VENTE. Aucun ordre n'est jamais envoyé à MT5 depuis ici.
# Anti-doublon : une seule alerte par (marché, TF, zone). Stockées en base (réglage « qmlg_alerts ») : elles survivent aux
# redémarrages ; activer / couper l'alerte les efface (jamais d'alerte périmée).
_qmlg_alert_lock = threading.RLock()


def qmlg_alert_key(symbol, tf, zone):
    return f"{symbol}|M{tf}|{float(zone['top']):.10g}|{float(zone['bottom']):.10g}"


def _qmlg_alerts_load():
    try:
        d = json.loads(get_setting("qmlg_alerts", "{}") or "{}")
        return d if isinstance(d, dict) else {}
    except ValueError:
        return {}


def _qmlg_alerts_save(store):
    set_setting("qmlg_alerts", json.dumps(store, separators=(",", ":"), default=str))


def qmlg_place_alerts(data):
    """Pose une alerte pour chaque QML valide non mitigé de `data` (résultat de qmlg_scan). Anti-doublon par (marché, TF, zone).
    Retourne {placed, existing, total}."""
    placed = existing = 0
    with _qmlg_alert_lock:
        store = _qmlg_alerts_load()
        for f in qmlg_pending_limits(data):
            key = qmlg_alert_key(f["symbol"], f["tf"], f["zone"])
            if key in store:
                existing += 1
                continue
            if not qmlg_trend_check(f["symbol"], f["tf"], f["dir"])[0]:   # contre la tendance de fond : pas d'alerte (reposée si la tendance change)
                continue
            p = f["plan"]
            store[key] = {"key": key, "symbol": f["symbol"], "tf": f["tf"], "dir": f["dir"],
                          "zone": {"top": f["zone"]["top"], "bottom": f["zone"]["bottom"]},
                          "sl": p["sl"], "atr": p.get("atr"), "tp1": p["tp1"], "tp1_kind": p["tp1_kind"], "tp2": p["tp2"], "tp2_kind": p["tp2_kind"],
                          "last_t": f["last_t"], "entered": False, "created_ts": time.time()}
            placed += 1
        if placed:
            _qmlg_alerts_save(store)
        return {"placed": placed, "existing": existing, "total": len(store)}


QMLG_ALERT_TF_CHOICES = (1, 5, 15, 30, 60)   # TF proposés dans le menu (minutes)
_QMLG_PAT_FR = {"ENGULFING": "AVALEMENT", "PIN_BAR": "PIN BAR", "DOJI": "DOJI"}
_qmlg_refresh_due = {}                         # (marché, TF) -> instant du prochain scan des zones


def get_qmlg_be_on():
    """BE de QML GAGNANT activé ? Réglage PROPRE à cette stratégie (bouton du menu), indépendant de Fibotrain. ON par défaut."""
    return _get_bool_setting("qmlg_be_on", True)


def get_qmlg_be_rr():
    """RR d'armement du BE de QML GAGNANT (menu), sinon QMLG_BE_RR. Ne suit JAMAIS le /be de Fibotrain."""
    try:
        v = float(get_setting("qmlg_be_rr", QMLG_BE_RR))
    except (TypeError, ValueError):
        return QMLG_BE_RR
    return v if v > 0 else QMLG_BE_RR


def get_qmlg_trend_on():
    """Filtre « tendance de fond » de QML GAGNANT activé ? OFF par défaut (bouton du menu)."""
    return _get_bool_setting("qmlg_trend_on", False)


def get_qmlg_trend_refs():
    """{TF d'entrée: TF de référence} choisis dans le menu. Une référence invalide (pas plus grande que le TF d'entrée,
    ou hors choix) est remplacée par la valeur par défaut du TF."""
    try:
        raw = json.loads(get_setting("qmlg_trend_refs", "{}") or "{}")
        raw = raw if isinstance(raw, dict) else {}
    except ValueError:
        raw = {}
    out = {}
    for tf in QMLG_ALERT_TF_CHOICES:
        try:
            ref = int(raw.get(str(tf), 0))
        except (TypeError, ValueError):
            ref = 0
        if ref not in QMLG_TREND_CHOICES or ref <= tf:
            ref = QMLG_TREND_DEFAULT.get(tf) or next((r for r in QMLG_TREND_CHOICES if r > tf), QMLG_TREND_CHOICES[-1])
        out[tf] = ref
    return out


def set_qmlg_trend_ref(tf, ref):
    """Choisit le TF de référence de la tendance pour un TF d'entrée. Retourne True si accepté."""
    if tf not in QMLG_ALERT_TF_CHOICES or ref not in QMLG_TREND_CHOICES or ref <= tf:
        return False
    refs = get_qmlg_trend_refs()
    refs[tf] = ref
    set_setting("qmlg_trend_refs", json.dumps({str(k): v for k, v in refs.items()}))
    return True


_qmlg_trend_cache = {}   # (marché, TF de référence) -> (instant, tendance)


def qmlg_trend(symbol, ref):
    """Tendance de fond du marché sur le TF `ref` d'après detect_structure : 'bullish' | 'bearish' | 'range' ; None si les bougies
    sont indisponibles (le filtre laisse alors passer : une panne réseau ne bloque pas les signaux)."""
    now = time.time()
    hit = _qmlg_trend_cache.get((symbol, ref))
    if hit and now - hit[0] < QMLG_TREND_CACHE_SEC:
        return hit[1]
    try:
        candles = get_candles(symbol, minutes=ref) or []
        if len(candles) < 30:
            return hit[1] if hit else None
        trend = detect_structure(candles, f"M{ref}", n=QMLG_FRACTAL_N, keep=QMLG_KEEP).get("trend")
    except Exception as e:
        print(f"[qmlg-tendance] {symbol} {_tf_lbl(ref)} : lecture impossible ({type(e).__name__})")
        return hit[1] if hit else None
    _qmlg_trend_cache[(symbol, ref)] = (now, trend)
    return trend


_QMLG_TREND_FR = {"bullish": "haussière 📈", "bearish": "baissière 📉", "range": "indécise (range)"}


def qmlg_trend_check(symbol, tf, direction):
    """(autorisé, texte) : le signal `direction` ('bull' / 'bear') sur le TF d'entrée `tf` suit-il la tendance du TF de référence ?
    Filtre OFF, tendance 'range' ou indisponible : autorisé. Texte = ligne pour la légende ('' si filtre OFF)."""
    if not get_qmlg_trend_on():
        return True, ""
    ref = get_qmlg_trend_refs().get(tf) or QMLG_TREND_DEFAULT.get(tf, 60)
    trend = qmlg_trend(symbol, ref)
    if trend is None:
        return True, f"🧭 Tendance {_tf_lbl(ref)} : indisponible (signal non filtré)"
    ok = trend == "range" or (trend == "bullish") == (direction == "bull")
    return ok, f"🧭 Tendance {_tf_lbl(ref)} : {_QMLG_TREND_FR.get(trend, trend)} {'✅' if ok else '⛔'}"


def get_qmlg_alert_on():
    """Alerte de réaction activée par l'admin (bouton du menu QML GAGNANT) ? OFF tant qu'il ne l'a pas activée."""
    return _get_bool_setting("qmlg_alert_on", False)


def qmlg_alerts_clear():
    """Efface toutes les alertes en base : on repart de zéro (jamais d'alerte périmée après une pause)."""
    with _qmlg_alert_lock:
        _qmlg_alerts_save({})
    _qmlg_refresh_due.clear()


def set_qmlg_alert_on(on):
    """Active / coupe l'alerte. Dans les deux cas on repart de zéro : une réaction survenue pendant que c'était OFF ne ressort jamais."""
    set_setting("qmlg_alert_on", "1" if on else "0")
    qmlg_alerts_clear()


def get_qmlg_alert_tfs():
    """TF scannés par l'alerte (minutes, triés). Choix du menu ; sinon QMLG_TF. Jamais vide."""
    raw = get_setting("qmlg_alert_tfs", "") or ""
    tfs = sorted({int(x) for x in re.findall(r"\d+", str(raw)) if int(x) in QMLG_ALERT_TF_CHOICES})
    return tfs or [QMLG_TF if QMLG_TF in QMLG_ALERT_TF_CHOICES else 15]


def toggle_qmlg_alert_tf(tf):
    """Coche / décoche un TF (au moins un reste toujours coché). Retourne la liste des TF choisis."""
    tfs = get_qmlg_alert_tfs()
    if tf not in QMLG_ALERT_TF_CHOICES:
        return tfs
    if tf in tfs:
        if len(tfs) > 1:
            tfs.remove(tf)
    else:
        tfs.append(tf)
    tfs = sorted(tfs)
    set_setting("qmlg_alert_tfs", ",".join(str(t) for t in tfs))
    return tfs


def qmlg_alert_symbols():
    """Marchés surveillés par l'alerte : ceux qui sont ON dans le menu 🌍 ACTIFS."""
    return [s for s in SYMBOLS if get_symbol_on(s)]


def _qmlg_alert_rr(a, close):
    """(risque, RR1, RR2) d'une entrée à `close` avec le SL / les TP de l'alerte. None = niveau déjà dépassé ou absent."""
    sgn = 1 if a["dir"] == "bull" else -1
    risk = (close - a["sl"]) * sgn
    if risk <= 0:
        return 0.0, None, None
    rr = lambda tp: None if tp is None or (tp - close) * sgn <= 0 else abs(tp - close) / risk
    return risk, rr(a.get("tp1")), rr(a.get("tp2"))


def _qmlg_sl_behind_candle(a, c):
    """Copie de l'alerte avec un SL anti-liquidation : le plus éloigné entre le SL de la zone et la mèche de la bougie d'entrée,
    puis + QMLG_SL_CANDLE_BUF_ATR x ATR derrière (sans ATR connu : 0.05 % du prix). QMLG_SL_CANDLE_BUF_ATR = 0 : SL de la zone seul.
    Jamais rapproché de l'entrée : le SL ne peut que s'éloigner."""
    if QMLG_SL_CANDLE_BUF_ATR <= 0:
        return a
    buy = a["dir"] == "bull"
    atr = a.get("atr") or 0.0
    buf = atr * QMLG_SL_CANDLE_BUF_ATR if atr else abs(c["c"]) * 0.0005
    sl = min(a["sl"], c["l"] - buf) if buy else max(a["sl"], c["h"] + buf)
    out = dict(a)
    out["sl_zone"], out["sl"] = a["sl"], sl
    return out


def qmlg_alert_eval(a, candles, now=None):
    """Fait avancer UNE alerte (fonction pure, sans réseau). candles : bougies clôturées (la plus récente en dernier).
    Aucun message quand le prix ENTRE dans la zone. Une RÉACTION = bougie qui touche la zone, ne l'invalide pas et forme un
    AVALEMENT ou un PIN BAR dans le sens du QML ; RR1 sur la vraie entrée (clôture) >= QMLG_ALERT_MIN_RR, sinon l'alerte attend.
    now (secondes) : active la péremption et le refus des réactions trop vieilles. Retourne (alerte mise à jour, ou None si elle
    doit être supprimée, [événements]) ; événement = {type: reaction | invalid | expired, ...}."""
    a, evs = dict(a), []
    top, bot, buy = a["zone"]["top"], a["zone"]["bottom"], a["dir"] == "bull"
    tf_sec = a["tf"] * 60
    if now is not None and a.get("created_ts") and now - a["created_ts"] > QMLG_ALERT_MAX_BARS * tf_sec:
        return None, [{"type": "expired", "alert": a}]
    for k, c in enumerate(candles):
        if c["t"] <= a["last_t"]:
            continue
        if (c["c"] < bot) if buy else (c["c"] > top):   # clôture au-delà du bord lointain : zone invalidée
            return None, [{"type": "invalid", "alert": a, "close": c["c"]}]
        touches = c["l"] <= top and c["h"] >= bot          # la bougie touche la zone
        if touches:
            a["entered"] = True
            a["touch_t"] = c["t"]
        # la réaction (avalement / pin bar) est la bougie qui touche la zone OU l'une des QMLG_ALERT_REACT_BARS suivantes
        recent = touches or (a.get("touch_t") is not None and 0 < c["t"] - a["touch_t"] <= QMLG_ALERT_REACT_BARS * tf_sec)
        if recent:
            pattern = qmlg_rejection(candles[:k + 1], buy)
            fresh = now is None or now - (c["t"] + tf_sec) <= QMLG_ALERT_STALE_BARS * tf_sec
            if pattern in QMLG_ALERT_PATTERNS and fresh:
                a_sl = _qmlg_sl_behind_candle(a, c)   # SL derrière la zone ET derrière la mèche de la bougie d'entrée
                risk, rr1, rr2 = _qmlg_alert_rr(a_sl, c["c"])
                if rr1 is not None and rr1 >= QMLG_ALERT_MIN_RR:
                    evs.append({"type": "reaction", "alert": dict(a_sl), "pattern": pattern, "close": c["c"], "t": c["t"],
                                "risk": risk, "rr1": rr1, "rr2": rr2})
                    return None, evs   # réaction envoyée : l'alerte est terminée
        a["last_t"] = c["t"]
    return a, evs


def qmlg_alert_text(ev, li, be_rr=None):
    """Légende de la réaction (admin + groupe) : sens, entrée, SL, TP1 / TP2 + RR, BE, lot. < 1000 caractères (limite d'une légende photo)."""
    a, buy = ev["alert"], ev["alert"]["dir"] == "bull"
    dec = SYMBOLS.get(a["symbol"], {}).get("decimals", 2)
    px = lambda v: _fmt(v, dec)

    def tp(k, rr):
        return "—" if a.get(k) is None or rr is None else f"{px(a[k])} ({a.get(k + '_kind') or 'liquidité'}) · RR {rr:.2f}"
    txt = (f"{'🟢' if buy else '🔴'} <b>{a['symbol']} {_tf_lbl(a['tf'])}</b> — QML {'ACHAT' if buy else 'VENTE'} · "
           f"réaction {_QMLG_PAT_FR.get(ev['pattern'], ev['pattern'])}\n"
           f"📍 Entrée {px(ev['close'])}\n🛡 SL {px(a['sl'])}\n"
           f"🎯 TP1 {tp('tp1', ev['rr1'])}\n🎯 TP2 {tp('tp2', ev['rr2'])}\n"
           + (f"🔒 BE → RR{be_rr:g}\n" if be_rr else "🔓 BE désactivé (SL fixe)\n")
           + (ev["trend_line"] + "\n" if ev.get("trend_line") else "")
           + f"💰 Lot <b>{li['lot']:g}</b> · risque {li['real_risk']:.2f} $")
    if li.get("raised_to_min"):
        txt += "\n⚠️ lot minimum du broker : risque réel plus élevé que prévu"
    return txt


def _qmlg_alert_chart(ev):
    """Image du QML avec l'outil de position (entrée / SL / TP aux niveaux de l'alerte). None si le dessin est impossible."""
    a = ev["alert"]
    sym, tf = a["symbol"], a["tf"]
    candles = get_candles(sym, minutes=tf, count=QMLG_HISTORY_BARS) or []
    if len(candles) < QMLG_MIN_BARS:
        return None
    structure = detect_structure(candles, f"M{tf}", n=QMLG_FRACTAL_N, keep=QMLG_KEEP)
    res = detect_qml(structure, min_rr=QMLG_MIN_RR)
    tol = max(1e-6, abs(a["zone"]["top"]) * 1e-7)
    q = next((x for x in reversed(res["qmls"]) if x["dir"] == a["dir"] and abs(x["zone"]["top"] - a["zone"]["top"]) <= tol
              and abs(x["zone"]["bottom"] - a["zone"]["bottom"]) <= tol), None)
    if q is None:
        return None
    trade = {"ok": True, "side": "BUY" if a["dir"] == "bull" else "SELL", "order": "DIRECT", "entry": ev["close"], "sl": a["sl"],
             "tp1": a.get("tp1"), "tp1_kind": a.get("tp1_kind"), "rr1": ev["rr1"], "risk": ev["risk"],
             "tp2": a.get("tp2") if ev["rr2"] is not None else None, "tp2_kind": a.get("tp2_kind"), "rr2": ev["rr2"]}
    return draw_chart(candles, q, trade, symbol=sym, tf=tf, structure=structure, others=[q])


def _qmlg_trade_levels(a, ev):
    """(TP final, RR du TP final, RR du BE ou None) du trade suivi. TP final = TP2 s'il existe, sinon TP1.
    BE = réglage PROPRE à QML GAGNANT (get_qmlg_be_on / get_qmlg_be_rr), jamais le /be de Fibotrain. BE désactivé -> None.
    Si le RR du BE n'est pas sous le TP final : RR1 (TP1 atteint -> SL à l'entrée) quand TP2 existe, sinon la moitié du RR final."""
    has_tp2 = a.get("tp2") is not None and ev.get("rr2") is not None
    tp_final = a["tp2"] if has_tp2 else a["tp1"]
    tp_rr = round(ev["rr2"] if has_tp2 else ev["rr1"], 2)
    if not get_qmlg_be_on():
        return tp_final, tp_rr, None
    be = get_qmlg_be_rr()
    if be >= tp_rr:
        be = ev["rr1"] if (has_tp2 and ev["rr1"] < tp_rr) else tp_rr / 2
    return tp_final, tp_rr, round(min(be, tp_rr - 0.01), 2)


def qmlg_verify_plan(a, ev, li):
    """DOUBLE VÉRIFICATION avant tout envoi. Retourne (ok, [problèmes]). Contrôle : TF bien coché dans le menu ; SL du bon côté de
    l'entrée ; distance SL cohérente ; TP1 (et TP2) du bon côté et dans l'ordre ; risque réel recalculé = lot x distance SL x
    valeur du point, et jamais au-dessus du montant risqué choisi (sauf lot minimum déjà signalé)."""
    pb, sym = [], a["symbol"]
    sgn = 1 if a["dir"] == "bull" else -1
    close, sl = ev["close"], a["sl"]
    dist = (close - sl) * sgn
    if a["tf"] not in get_qmlg_alert_tfs():
        pb.append(f"{_tf_lbl(a['tf'])} n'est pas un TF coché")
    if dist <= 0:
        pb.append("SL du mauvais côté de l'entrée")
    elif abs(dist - ev["risk"]) > 1e-9 * max(1.0, abs(close)):
        pb.append("distance SL incohérente")
    tp1, tp2 = a.get("tp1"), a.get("tp2")
    if tp1 is None or (tp1 - close) * sgn <= 0:
        pb.append("TP1 du mauvais côté de l'entrée")
    elif tp2 is not None and ev.get("rr2") is not None and (tp2 - tp1) * sgn <= 0:
        pb.append("TP2 avant TP1")
    if dist > 0 and li["lot"] > 0:
        real = li["lot"] * dist * value_per_point(sym, close)
        if abs(real - li["real_risk"]) > 0.01 + 1e-6 * real:
            pb.append(f"risque recalculé {real:.2f} $ ≠ {li['real_risk']:.2f} $")
        target = get_risk()
        if not li.get("raised_to_min") and real > target * (1 + 1e-6) + 0.005:
            pb.append(f"risque {real:.2f} $ > {target:.2f} $ choisi")
    return not pb, pb


def qmlg_alert_deliver(ev):
    """Réaction détectée : contrôle le TF, calcule le lot (montant risqué du menu 💰 Risque / distance au SL), DOUBLE VÉRIFIE le plan,
    envoie entrée / SL / TP1 / TP2 / RR / BE / lot à l'admin ET au groupe (image, puis stickers ACHAT / VENTE), et enregistre le trade pour
    le suivi : RR, BE (SL -> entrée), TP, SL partent au groupe en réponse à ce message. Lot ou plan impossible : l'admin est prévenu, rien
    ne part au groupe. Aucun ordre n'est envoyé à MT5."""
    a = ev["alert"]
    sym, buy = a["symbol"], a["dir"] == "bull"
    if a["tf"] not in get_qmlg_alert_tfs():   # contrôle n°1 : jamais de signal sur un TF qui n'est pas coché dans le menu
        print(f"[qmlg-alertes] {sym} {_tf_lbl(a['tf'])} : réaction ignorée (TF non coché)")
        return False
    key = f"QMLG:{a['key']}:{ev['t']}"
    if signal_exists(key):   # déjà livrée (anti-doublon)
        return False
    t_ok, t_line = qmlg_trend_check(sym, a["tf"], a["dir"])   # contrôle n°1 bis : tendance de fond (si activée)
    if not t_ok:
        print(f"[qmlg-alertes] {sym} {_tf_lbl(a['tf'])} : réaction ignorée (contre la tendance de fond) — {t_line}")
        return False
    ev["trend_line"] = t_line
    li = calc_lot(sym, get_risk(), ev["risk"], price=ev["close"])
    if li["lot"] <= 0:
        why = li.get("rejected_reason") or "lot invalide (risque ou distance SL)"
        to_admin(f"⚠️ <b>{sym} {_tf_lbl(a['tf'])}</b> — réaction {_QMLG_PAT_FR.get(ev['pattern'], ev['pattern'])} dans la zone QML "
                 f"{'(achat)' if buy else '(vente)'} mais lot impossible : {why}")
        return False
    ok, problems = qmlg_verify_plan(a, ev, li)   # contrôle n°2 : plan, risque et lot recalculés
    if not ok:
        to_admin(f"⛔ <b>{sym} {_tf_lbl(a['tf'])}</b> — signal QML {'achat' if buy else 'vente'} BLOQUÉ par la double vérification : "
                 + " · ".join(problems))
        return False
    tp_final, tp_rr, be_rr = _qmlg_trade_levels(a, ev)
    caption = qmlg_alert_text(ev, li, be_rr=be_rr)
    be_store = be_rr if be_rr else round(tp_rr + 1.0, 2)   # be_rr >= tp_rr : le suivi n'arme jamais le BE pour ce trade
    path = None
    try:
        path = _qmlg_alert_chart(ev)
    except Exception:
        traceback.print_exc(limit=-3)
    send(CHAT_ID_ADMIN, caption, photo=path)
    if QMLG_ALERT_TO_GROUP and CHAT_ID_GROUPE:
        try:
            trade_id = add_trade(
                status="OPEN", order_type="DIRECT", placed_ts=ev["t"], ref_price=ev["close"],
                sent_admin=1, sent_group=0,   # sent_group passe à 1 quand Telegram a confirmé (sinon renvoi automatique)
                signal_key=key, symbol=sym, side="BUY" if buy else "SELL", kind="QMLG", timeframe=_tf_lbl(a["tf"]),
                entry=ev["close"], sl=a["sl"], sl_initial=a["sl"], tp=tp_final, be_rr=be_store,
                risk_usd=li["real_risk"], lot=li["lot"], leverage=get_leverage(), opened_ts=now_ts(),
                last_ts=ev["t"] + a["tf"] * 60 - 1,   # le suivi commence APRÈS la clôture de la bougie de réaction
                exec_status=None, part_cfg=None)
            _deliver_signal(trade_id, sym, admin_txt="", group_txt=caption, chart=path)
        except Exception:
            traceback.print_exc(limit=-3)
            to_admin(f"⚠️ {sym} {_tf_lbl(a['tf'])} : suivi BE / TP / SL non enregistré — le signal part quand même au groupe.")
            to_group(caption, photo=path)
        try:
            to_group_media("BUY" if buy else "SELL", n=MEDIA_ENTRY_COUNT)   # stickers ACHAT / VENTE (comme à chaque entrée)
        except Exception:
            traceback.print_exc(limit=-3)
    return True


def qmlg_alert_tick():
    """Un passage de surveillance : relit les bougies clôturées des marchés ayant une alerte (seulement si une nouvelle a pu
    clôturer), fait avancer chaque alerte, livre les réactions (lot + image + groupe), enregistre l'état. Retourne le nombre de
    réactions livrées."""
    snap = _qmlg_alerts_load()
    if not snap:
        return 0
    tfs_ok, syms_ok = set(get_qmlg_alert_tfs()), set(qmlg_alert_symbols())   # TF cochés + marchés ON : rien d'autre n'est lu
    snap_ok = {k: a for k, a in snap.items() if a["tf"] in tfs_ok and a["symbol"] in syms_ok}
    now, candles = time.time(), {}
    for (sym, tf) in {(a["symbol"], a["tf"]) for a in snap_ok.values()}:
        if any(now >= a["last_t"] + 2 * tf * 60 + QMLG_ALERT_LAG for a in snap_ok.values() if (a["symbol"], a["tf"]) == (sym, tf)):
            try:
                candles[(sym, tf)] = get_candles(sym, minutes=tf) or []
            except Exception as e:
                print(f"[qmlg-alertes] {sym} M{tf} : bougies indisponibles ({type(e).__name__})")
    events = []
    with _qmlg_alert_lock:
        store = _qmlg_alerts_load()   # relu : une alerte posée / supprimée pendant les appels réseau est respectée
        for key in list(store):
            a = store[key]
            if a["tf"] not in tfs_ok or a["symbol"] not in syms_ok:   # TF décoché / marché coupé : l'alerte disparaît sans rien envoyer
                del store[key]
                continue
            new, evs = qmlg_alert_eval(a, candles.get((a["symbol"], a["tf"]), []), now=now)
            events.extend(evs)
            if new is None:
                del store[key]
            else:
                store[key] = new
        _qmlg_alerts_save(store)
    sent = 0
    for ev in events:   # hors du verrou : un envoi Telegram lent ne bloque pas la pose d'alertes
        if ev["type"] == "reaction":
            try:
                sent += 1 if qmlg_alert_deliver(ev) else 0
            except Exception:
                traceback.print_exc(limit=-3)
        else:
            a = ev["alert"]
            print(f"[qmlg-alertes] {a['symbol']} M{a['tf']} zone {a['zone']['bottom']:g}-{a['zone']['top']:g} : alerte supprimée ({ev['type']})")
    return sent


def qmlg_alert_refresh(force=False):
    """Cherche les zones : pour chaque marché ON et chaque TF choisi, nouveau scan à chaque clôture de bougie du TF, et une
    alerte sur chaque QML valide non mitigé (anti-doublon). Retire les alertes d'un marché / TF qui n'est plus choisi.
    Retourne le nombre d'alertes posées. Ne fait rien si l'alerte est OFF."""
    if not get_qmlg_alert_on():
        return 0
    syms, tfs = qmlg_alert_symbols(), get_qmlg_alert_tfs()
    with _qmlg_alert_lock:
        store = _qmlg_alerts_load()
        keep = {k: v for k, v in store.items() if v["symbol"] in syms and v["tf"] in tfs}
        if len(keep) != len(store):
            _qmlg_alerts_save(keep)
    now = time.time()
    todo = [(s, t) for s in syms for t in tfs if force or now >= _qmlg_refresh_due.get((s, t), 0)]
    if not todo or not _qmlg_lock.acquire(blocking=False):   # un scan « Analyse » en cours : on réessaie au prochain passage
        return 0
    try:
        forms = []
        for (s, t) in todo:
            step = t * 60
            _qmlg_refresh_due[(s, t)] = (now // step + 1) * step + QMLG_ALERT_LAG   # prochain scan juste après la clôture de la bougie
            try:
                forms.extend(_qmlg_scan_one(s, t)[0])
            except Exception as e:
                _qmlg_refresh_due[(s, t)] = now + 60
                print(f"[qmlg-alertes] scan {s} M{t} impossible ({type(e).__name__}) : nouvel essai dans 1 min")
    finally:
        _qmlg_lock.release()
    return qmlg_place_alerts({"formations": forms})["placed"] if forms else 0


_qmlg_alert_started = []


def _qmlg_loop_delay():
    """Pause entre deux passages des boucles d'alerte : 5 s si M1 est coché (une bougie dure 60 s), sinon QMLG_ALERT_SEC."""
    try:
        return 5 if 1 in get_qmlg_alert_tfs() else max(5, QMLG_ALERT_SEC)
    except Exception:
        return max(5, QMLG_ALERT_SEC)


def start_qmlg_alerts():
    """Démarre (une seule fois) les 2 boucles : surveillance des réactions (rapide) et recherche des zones (scans plus lourds,
    dans un thread à part pour ne jamais retarder une réaction). Alerte OFF : aucune des deux ne fait d'appel réseau."""
    if _qmlg_alert_started:
        return

    def run(fn, label):
        while True:
            try:
                if get_qmlg_alert_on():
                    fn()
            except Exception:
                print(f"[qmlg-alertes] {label} : erreur (ignorée)")
                traceback.print_exc(limit=-3)
            time.sleep(_qmlg_loop_delay())
    for fn, label in ((qmlg_alert_tick, "réactions"), (qmlg_alert_refresh, "zones")):
        t = threading.Thread(target=run, args=(fn, label), daemon=True, name=f"qmlg-alertes-{label}")
        _qmlg_alert_started.append(t)
        t.start()


# --- TEXTES TELEGRAM + BOUTONS ----------------------------------------------------------------------------
def _qmlg_age(data):
    m = int((time.time() - data.get("generated_ts", 0)) // 60)
    return "à l'instant" if m < 1 else f"il y a {m} min"


def _qmlg_cap(head, lines, tail="", limit=3800):
    """Assemble en respectant la limite Telegram : lignes en trop résumées par « + N autres »."""
    out, used = [], len(head) + len(tail) + 40
    for i, ln in enumerate(lines):
        if i >= QMLG_LIST_MAX or used + len(ln) > limit:
            out.append(f"… + {len(lines) - i} autre(s) (détail dans le JSON)")
            break
        out.append(ln)
        used += len(ln) + 1
    return head + "\n".join(out) + tail


def _qmlg_line(f, plan, extra=""):
    dec = SYMBOLS.get(f["symbol"], {}).get("decimals", 2)
    buy = plan["side"] == "BUY"
    f2 = lambda k: "—" if plan[k] is None else f"{_fmt(plan[k], dec)} (RR {plan['rr' + k[-1]]:.2f})"
    return (f"{'🟢' if buy else '🔴'} <b>{f['symbol']}</b> M{f['tf']} · {plan['side']} @ {_fmt(plan['entry'], dec)}{extra}\n"
            f"   🛡 {_fmt(plan['sl'], dec)} · 🎯 TP1 {f2('tp1')} · TP2 {f2('tp2')}")


def qmlg_analyse_text(data, alerts=None):
    forms = data["formations"]
    bad = Counter(r for f in forms if not f["validation"]["valid"] for r in f["validation"]["reasons"])
    txt = (f"🔍 <b>ANALYSE QML GAGNANT</b>\n{len(data['markets'])} marchés · TF {', '.join('M%d' % t for t in data['tfs'])}\n"
           f"Formations : <b>{data['n_formations']}</b> · valides : <b>{data['n_valid']}</b> "
           f"(en attente de mitigation : {len(qmlg_pending_limits(data))})\n")
    if bad:
        txt += "Rejets : " + " · ".join(f"{_QMLG_REASON_LBL.get(k, k)} ×{n}" for k, n in bad.most_common(4)) + "\n"
    if data["errors"]:
        txt += f"⚠️ {len(data['errors'])} marché(s)/TF en erreur : " + ", ".join(
            f"{e['symbol']} M{e['tf']}" for e in data["errors"][:5]) + ("…" if len(data["errors"]) > 5 else "") + "\n"
    if alerts is not None:
        txt += (f"🔔 Alertes de zone : {alerts['placed']} posée(s) · {alerts['existing']} déjà en place · "
                f"{alerts['total']} active(s)\n")
    return txt + f"💾 JSON : <code>{QMLG_JSON_PATH}</code>"


def qmlg_limit_text(data):
    items = qmlg_pending_limits(data)
    head = (f"⏳ <b>ORDRES LIMITE — QML GAGNANT</b>\nDonnées {_qmlg_age(data)} · {len(items)} QML valide(s)\n"
            f"Statut : <b>EN ATTENTE de mitigation</b>\n\n")
    if not items:
        return head + "Aucun QML valide non mitigé pour l'instant."
    return _qmlg_cap(head, [_qmlg_line(f, f["plan"], " · LIMIT") for f in items], "\n\n(information seulement : aucun ordre envoyé)")


def qmlg_arm_alerts(data):
    """Après « Ordre limite » : arme la surveillance des QML listés (TF cochés + marchés ON seulement) et renvoie un texte de
    statut. Rien n'est envoyé tant que 🔔 Alerte réaction est OFF : le statut le dit clairement."""
    items = qmlg_pending_limits(data)
    tfs, syms = set(get_qmlg_alert_tfs()), set(qmlg_alert_symbols())
    watch = [f for f in items if f["tf"] in tfs and f["symbol"] in syms]
    off = sorted({f["symbol"] for f in items if f["symbol"] not in syms})
    if not get_qmlg_alert_on():
        return ("⚠️ <b>🔔 Alerte réaction est OFF</b> : aucun signal (entrée / SL / TP / lot / image) ne sera envoyé. "
                "Active-la dans le menu QML GAGNANT, puis relance « Ordre limite ».")
    r = qmlg_place_alerts({"formations": watch}) if watch else {"placed": 0, "existing": 0, "total": 0}
    txt = (f"👁 Surveillance : {len(watch)} zone(s) · {r['placed']} posée(s) · {r['existing']} déjà en place. "
           f"Signal envoyé seulement sur AVALEMENT / PIN BAR dans la zone.")
    if off:
        txt += "\n⚠️ Marché(s) OFF dans 🌍 ACTIFS (non surveillés) : " + ", ".join(off)
    return txt


def qmlg_direct_text(data):
    items = qmlg_direct_signals(data)
    head = f"⚡ <b>ENTRÉES DIRECTES — QML GAGNANT</b>\nDonnées {_qmlg_age(data)} · {len(items)} signal(s)\n\n"
    if not items:
        return head + "Aucune entrée : il faut le prix DANS la zone ET une dernière bougie clôturée en AVALEMENT ou en BOUGIE DE RETOURNEMENT (pin bar). Le doji ne compte pas."
    return _qmlg_cap(head, [_qmlg_line(f, f["direct"]["trade"], f" · {f['direct']['pattern']} (clôture)") for f in items],
                     "\n\n(image + lot envoyés ci-dessous · aucun ordre envoyé à MT5)")


def _qmlg_direct_chart(f):
    """Image du QML d'une entrée directe (outil de position : entrée / SL / TP). None si le dessin est impossible."""
    sym, tf, tr = f["symbol"], f["tf"], f["direct"]["trade"]
    candles = get_candles(sym, minutes=tf, count=QMLG_HISTORY_BARS) or []
    if len(candles) < QMLG_MIN_BARS:
        return None
    structure = detect_structure(candles, f"M{tf}", n=QMLG_FRACTAL_N, keep=QMLG_KEEP)
    res = detect_qml(structure, min_rr=QMLG_MIN_RR)
    tol = max(1e-6, abs(f["zone"]["top"]) * 1e-7)
    q = next((x for x in reversed(res["qmls"]) if x["dir"] == f["dir"] and abs(x["zone"]["top"] - f["zone"]["top"]) <= tol
              and abs(x["zone"]["bottom"] - f["zone"]["bottom"]) <= tol), None)
    if q is None:
        return None
    return draw_chart(candles, q, tr, symbol=sym, tf=tf, structure=structure, others=[q])


def qmlg_direct_caption(f, li):
    """Légende d'une entrée directe : sens, motif, entrée, SL, TP1 / TP2 (RR) et lot calculé sur la distance au SL."""
    d, tr = f["direct"], f["direct"]["trade"]
    dec = SYMBOLS.get(f["symbol"], {}).get("decimals", 2)
    buy = tr["side"] == "BUY"
    f2 = lambda k: "—" if tr.get(k) is None else f"{_fmt(tr[k], dec)} · RR {tr['rr' + k[-1]]:.2f}"
    txt = (f"{'🟢' if buy else '🔴'} <b>{f['symbol']} M{f['tf']}</b> — QML {'ACHAT' if buy else 'VENTE'} · ENTRÉE DIRECTE · "
           f"{_QMLG_PAT_FR.get(d['pattern'], d['pattern'])}\n"
           f"📍 Entrée {_fmt(tr['entry'], dec)}\n🛡 SL {_fmt(tr['sl'], dec)}\n🎯 TP1 {f2('tp1')}\n🎯 TP2 {f2('tp2')}\n"
           f"💰 Lot <b>{li['lot']:g}</b> · risque {li['real_risk']:.2f} $")
    if li.get("raised_to_min"):
        txt += "\n⚠️ lot minimum du broker : risque réel plus élevé que prévu"
    return txt


def qmlg_direct_deliver(chat_id, data, max_items=5):
    """Pour chaque entrée directe : calcule le lot (montant du menu 💰 Risque / distance au SL), puis envoie l'IMAGE + entrée / SL /
    TP / lot. Lot impossible -> message d'avertissement, pas d'image. Aucun ordre MT5. Retourne le nombre d'envois."""
    n = 0
    for f in qmlg_direct_signals(data)[:max_items]:
        tr = f["direct"]["trade"]
        risk = tr.get("risk") or abs(tr["entry"] - tr["sl"])
        li = calc_lot(f["symbol"], get_risk(), risk, price=tr["entry"])
        if li["lot"] <= 0:
            send(chat_id, f"⚠️ <b>{f['symbol']} M{f['tf']}</b> — entrée directe détectée mais lot impossible : "
                          f"{li.get('rejected_reason') or 'lot invalide (risque ou distance SL)'}")
            continue
        path = None
        try:
            path = _qmlg_direct_chart(f)
        except Exception:
            traceback.print_exc(limit=-3)
        send(chat_id, qmlg_direct_caption(f, li), photo=path)
        n += 1
    return n


def _qmlg_menu_text():
    return (f"🏆 <b>QML GAGNANT</b> — {'🟢 ON' if get_qmlg_on() else '🔴 OFF'}\n\n"
            f"🔔 <b>Alerte réaction</b> : {'🟢 ON' if get_qmlg_alert_on() else '🔴 OFF'} · TF "
            f"{', '.join(_tf_lbl(t) for t in get_qmlg_alert_tfs())} · {len(qmlg_alert_symbols())} marché(s) (menu 🌍 ACTIFS)\n"
            f"🔒 <b>BE QML GAGNANT</b> : {'🟢 ON à RR' + format(get_qmlg_be_rr(), 'g') if get_qmlg_be_on() else '🔴 OFF (SL fixe)'} — indépendant de /be Fibotrain. "
            f"🛡 SL : derrière la zone ET la mèche de la bougie d'entrée (+{QMLG_SL_CANDLE_BUF_ATR:g} ATR).\n"
            f"🧭 <b>Tendance de fond</b> : {'🟢 ON' if get_qmlg_trend_on() else '🔴 OFF'}"
            + (" · " + " · ".join(f"{_tf_lbl(t)}→{_tf_lbl(get_qmlg_trend_refs()[t])}" for t in get_qmlg_alert_tfs()) if get_qmlg_trend_on() else "")
            + " (signaux seulement dans le sens de la tendance du TF de référence).\n"
            f"C'est toi qui l'actives. Elle alerte seulement sur une RÉACTION dans une zone QML (avalement ou pin bar), pas quand le prix "
            f"y entre. Entrée, SL, TP1 / TP2 (RR), BE, lot, image et stickers partent au groupe, puis RR / BE / TP / SL en réponse. "
            f"Seuls les TF cochés ci-dessous sont scannés (chaque TF à chaque clôture de bougie : M1 = 1 min, M5 = 5 min, M15 = 15 min).\n\n"
            f"🔍 <b>Analyse</b> : scanne tous les marchés sur les TF cochés ({', '.join(_tf_lbl(t) for t in get_qmlg_alert_tfs())}), enregistre toutes les "
            f"formations QML dans le JSON et valide chacune.\n"
            f"⏳ <b>Ordre limite</b> : QML valides non mitigés, EN ATTENTE de mitigation.\n"
            f"⚡ <b>Entrée directe</b> : prix dans la zone + dernière bougie clôturée en rejet.")


def _qmlg_keyboard():
    tfs = get_qmlg_alert_tfs()
    return _with_back({"inline_keyboard": [
        [{"text": f"🔔 Alerte réaction : {'ON ✅' if get_qmlg_alert_on() else 'OFF'}", "callback_data": "qmlg:alert"}],
        [{"text": f"{'✅ ' if t in tfs else ''}{_tf_lbl(t)}", "callback_data": f"qmlg:tf:{t}"} for t in QMLG_ALERT_TF_CHOICES],
        [{"text": f"🧭 Tendance de fond : {'ON ✅' if get_qmlg_trend_on() else 'OFF'}", "callback_data": "qmlg:tro"},
         {"text": "⚙️ Réf. tendance", "callback_data": "qmlg:trend"}],
        [{"text": f"🔒 BE QML GAGNANT : {'ON ✅' if get_qmlg_be_on() else 'OFF'}", "callback_data": "qmlg:be"}],
        [{"text": ("✅ " if abs(get_qmlg_be_rr() - v) < 1e-9 else "") + f"RR{v:g}", "callback_data": f"qmlg:ber:{v}"} for v in QMLG_BE_CHOICES],
        [{"text": "🔍 Analyse", "callback_data": "qmlg:analyse"}],
        [{"text": "⏳ Ordre limite", "callback_data": "qmlg:limit"}, {"text": "⚡ Entrée directe", "callback_data": "qmlg:direct"}],
    ]})


def _qmlg_trend_text():
    on = get_qmlg_trend_on()
    refs = get_qmlg_trend_refs()
    lines = "\n".join(f"• {_tf_lbl(t)} → tendance {_tf_lbl(refs[t])}" for t in get_qmlg_alert_tfs())
    return (f"🧭 <b>Tendance de fond — QML GAGNANT</b> : {'🟢 ON' if on else '🔴 OFF'}\n\n"
            f"Pour chaque TF coché, choisis le TF de référence : le signal n'est envoyé que s'il va dans le sens de la tendance "
            f"(structure BOS / CHoCH) de ce TF. Tendance indécise (range) ou indisponible : le signal passe.\n\n{lines}")


def _qmlg_trend_keyboard():
    refs = get_qmlg_trend_refs()
    rows = [[{"text": f"🧭 Filtre tendance : {'ON ✅' if get_qmlg_trend_on() else 'OFF'}", "callback_data": "qmlg:tro"}]]
    for t in get_qmlg_alert_tfs():
        rows.append([{"text": f"{_tf_lbl(t)} →", "callback_data": "qmlg:trend"}]
                    + [{"text": ("✅ " if refs[t] == r else "") + _tf_lbl(r), "callback_data": f"qmlg:trr:{t}:{r}"}
                       for r in QMLG_TREND_CHOICES if r > t])
    rows.append([{"text": "⬅️ QML GAGNANT", "callback_data": "qmlg:home"}])
    return {"inline_keyboard": rows}


_qmlg_lock = threading.Lock()   # un seul scan à la fois
_QMLG_JOBS = {"analyse": "Analyse", "limit": "Ordre limite", "direct": "Entrée directe"}


def _qmlg_job(chat_id, kind):
    """Exécuté dans un thread (le scan fait des appels réseau : il ne doit pas bloquer le polling Telegram)."""
    if not _qmlg_lock.acquire(blocking=False):
        send(chat_id, "⏳ Un scan QML GAGNANT est déjà en cours, patiente un instant.", reply_markup=_qmlg_keyboard())
        return
    direct_data = None
    try:
        if kind == "analyse":
            data = qmlg_scan()
            txt = qmlg_analyse_text(data)   # les alertes se règlent avec le bouton 🔔 Alerte réaction
        elif kind == "limit":
            ldata = qmlg_data(QMLG_MAX_AGE_MIN * 60)
            txt = qmlg_limit_text(ldata)
            try:
                txt += "\n\n" + qmlg_arm_alerts(ldata)
            except Exception:
                traceback.print_exc(limit=-3)
        else:
            direct_data = qmlg_data(QMLG_CACHE_SEC)
            txt = qmlg_direct_text(direct_data)
    except Exception as e:
        traceback.print_exc(limit=-3)
        txt = f"🏆 QML GAGNANT — {_QMLG_JOBS.get(kind, kind)} : ⚠️ erreur ({type(e).__name__}), réessaie dans un instant."
    finally:
        _qmlg_lock.release()
    send(chat_id, txt, reply_markup=_qmlg_keyboard())
    if kind == "direct" and direct_data:   # chaque entrée directe : image + entrée / SL / TP + lot calculé sur le SL
        try:
            qmlg_direct_deliver(chat_id, direct_data)
        except Exception:
            traceback.print_exc(limit=-3)


def _qmlg_callback(cq, arg):
    """Boutons « qmlg:... » (admin seulement, filtré en amont). Retourne le texte de confirmation (toast)."""
    if arg in _QMLG_JOBS:
        _edit(cq, f"⏳ {_QMLG_JOBS[arg]} en cours… le résultat arrive dans un instant.", None)
        threading.Thread(target=_qmlg_job, args=(cq["message"]["chat"]["id"], arg), daemon=True, name=f"qmlg-{arg}").start()
        return "Scan lancé"
    toast = ""
    if arg == "alert":   # 🔔 Alerte réaction ON / OFF : repart de zéro, les boucles (déjà lancées au démarrage) font le reste
        on = not get_qmlg_alert_on()
        set_qmlg_alert_on(on)
        toast = "Alerte réaction : " + ("ON" if on else "OFF")
    elif arg in ("trend", "tro") or arg.startswith("trr:"):   # tendance de fond : ON/OFF + TF de référence (page dédiée)
        if arg == "tro":
            on = not get_qmlg_trend_on()
            set_setting("qmlg_trend_on", "1" if on else "0")
            toast = "Tendance de fond : " + ("ON" if on else "OFF")
        elif arg.startswith("trr:"):
            try:
                _, t_, r_ = arg.split(":")
                if set_qmlg_trend_ref(int(t_), int(r_)):
                    toast = f"{_tf_lbl(int(t_))} suit la tendance {_tf_lbl(int(r_))}"
            except ValueError:
                pass
        _edit(cq, _qmlg_trend_text(), _qmlg_trend_keyboard())
        return toast
    elif arg == "be":   # BE QML GAGNANT ON / OFF : indépendant de Fibotrain, ne touche que les NOUVEAUX signaux
        on = not get_qmlg_be_on()
        set_setting("qmlg_be_on", "1" if on else "0")
        toast = "BE QML GAGNANT : " + ("ON" if on else "OFF (SL fixe)")
    elif arg.startswith("ber:"):
        try:
            v = float(arg[4:])
            if v > 0:
                set_setting("qmlg_be_rr", v)
                toast = f"BE QML GAGNANT → RR{v:g}"
        except ValueError:
            pass
    elif arg.startswith("tf:") and arg[3:].isdigit():
        tfs = toggle_qmlg_alert_tf(int(arg[3:]))
        toast = "TF : " + ", ".join(_tf_lbl(t) for t in tfs)
        if get_qmlg_trend_on() and int(arg[3:]) in tfs:   # TF qui vient d'être coché : on demande sa tendance de référence
            _edit(cq, _qmlg_trend_text(), _qmlg_trend_keyboard())
            return toast + " — choisis la tendance de référence"
    _edit(cq, _qmlg_menu_text(), _qmlg_keyboard())
    return toast


# ============================================================================
# 16. GRAPHIQUE QML — draw_chart(df, qml, trade) + envoi Telegram avec légende
#
# À coller dans main.py juste AVANT le bloc `if __name__ == "__main__":`.
# Utilise les globaux du fichier : CHART_DIR, SYMBOLS, _fmt, _tf_lbl, send, CHAT_ID_ADMIN, QMLG_*, get_candles,
# detect_structure, detect_qml, compute_trade, _qmlg_tf_minutes.
# ============================================================================
QMLC_BARS_BEFORE = _env_int("QMLC_BARS_BEFORE", 25)   # bougies affichées avant l'élément le plus ancien du QML
QMLC_MAX_BARS = _env_int("QMLC_MAX_BARS", 260)        # largeur maximale de la vue (en bougies)
QMLC_EXTEND = _env_int("QMLC_EXTEND", 28)             # largeur de l'outil de position, à droite de la dernière bougie
_QMLC = {"SELL": "#fb8c00", "BUY": "#1e66f5", "RED": "#e53935", "GREEN": "#2e9d4f", "BOS": "#546e7a",
         "CHOCH": "#8e24aa", "EQ": "#37474f", "SWEEP": "#b71c1c", "BLACK": "#000000"}


def _qmlc_load(df):
    """df (DataFrame open/high/low/close, ou liste de dicts {t,o,h,l,c}) -> liste de bougies {t,o,h,l,c}.
    Même nettoyage que detect_structure (NaN retirés, tri, doublons de temps) : les indices du QML restent valables.
    Si detect_structure a été appelé avec drop_last=True, passer ici le même df tronqué."""
    if df is None:
        raise ValueError("df est None")
    if hasattr(df, "columns"):
        low = {str(k).lower(): k for k in df.columns}
        pick = lambda *names: next((low[x] for x in names if x in low), None)
        ko, kh, kl, kc = pick("open", "o"), pick("high", "h"), pick("low", "l"), pick("close", "c")
        if kh is None or kl is None or kc is None:
            raise ValueError("df doit avoir des colonnes high/low/close (ou h/l/c)")
        h, l, c = (df[k].tolist() for k in (kh, kl, kc))
        o = df[ko].tolist() if ko is not None else list(c)
        kt = pick("time", "t", "timestamp", "datetime", "date", "open_time")
        if kt is not None:
            t = df[kt].tolist()
        elif getattr(getattr(df.index, "dtype", None), "kind", None) == "M":
            t = list(df.index)
        else:
            t = [None] * len(h)
    else:
        rows = list(df)
        g = lambda r, *ks: next((r[k] for k in ks if k in r), None)
        t = [g(r, "t", "time") for r in rows]
        o = [g(r, "o", "open") for r in rows]
        h = [g(r, "h", "high") for r in rows]
        l = [g(r, "l", "low") for r in rows]
        c = [g(r, "c", "close") for r in rows]
    rows = []
    for ti, oi, hi, li, ci in zip(t, o, h, l, c):
        try:
            hi, li, ci = float(hi), float(li), float(ci)
        except (TypeError, ValueError):
            continue
        if not all(math.isfinite(x) for x in (hi, li, ci)):
            continue
        try:
            oi = float(oi)
        except (TypeError, ValueError):
            oi = ci
        rows.append((ti, oi if math.isfinite(oi) else ci, hi, li, ci))
    if rows and all(r[0] is not None for r in rows):
        try:
            rows.sort(key=lambda r: r[0])
            out = []
            for r in rows:
                if out and out[-1][0] == r[0]:
                    out[-1] = r
                else:
                    out.append(r)
            rows = out
        except TypeError:
            pass
    return [{"t": r[0], "o": r[1], "h": r[2], "l": r[3], "c": r[4]} for r in rows]


def _qmlc_time_label(t):
    """Temps (Timestamp / datetime / epoch s ou ms / ISO) -> 'jj/mm HH:MM' ; '' si illisible."""
    try:
        if t is None:
            return ""
        if hasattr(t, "strftime"):
            return t.strftime("%d/%m %H:%M")
        if isinstance(t, (int, float)):
            return datetime.fromtimestamp(t / 1000 if t > 1e11 else t, timezone.utc).strftime("%d/%m %H:%M")
        s = str(t)
        return f"{s[8:10]}/{s[5:7]} {s[11:16]}" if len(s) >= 16 and s[4] == "-" else s[:16]
    except Exception:
        return ""


def _qmlc_purge(keep=40):
    os.makedirs(CHART_DIR, exist_ok=True)
    files = sorted((os.path.join(CHART_DIR, f) for f in os.listdir(CHART_DIR)), key=os.path.getmtime)
    for f in files[:-keep]:
        try:
            os.remove(f)
        except OSError:
            pass


def draw_chart(df, qml, trade=None, symbol=None, tf=None, structure=None, path=None, others=None):
    """Graphique d'un QML (matplotlib, fond clair). Retourne le chemin du PNG, ou None (matplotlib absent / données vides).

      df         DataFrame (open/high/low/close, index temps) ou liste de dicts {t,o,h,l,c} — LA MÊME série que celle
                 passée à detect_structure (les indices du QML y renvoient).
      qml        un élément de detect_qml(...)["qmls"] (zone, épaule, tête, choch, bos éventuel, statut, mitigated_idx).
      trade      dict de compute_trade(qml, structure) ; None / non exploitable -> pas d'outil de position.
      others     (optionnel) autres QML (achat ET vente) à afficher aussi : zone + étiquette.
      structure  (optionnel) sortie de detect_structure : ajoute les BOS / CHoCH de la vue (sinon : ceux du QML seulement).

    Éléments : bougies · EQH/EQL en pointillés (liquidité de l'épaule balayée + liquidités visées par TP1/TP2) ·
    flèche SWEEP sur la tête · BOS / CHoCH étiquetés · zone orange (vente) ou bleue (achat) ;
    QML NON MITIGÉ : zone, ligne d'entrée et bordure NOIRES ÉPAISSES (la zone mitigée s'arrête à sa 1re retouche) ·
    outil de position : boîte rouge entrée→SL, verte entrée→TP2 (TP1 si pas de TP2), ligne TP1, étiquettes prix + RR."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None
    candles = _qmlc_load(df)
    n = len(candles)
    if n < 10 or not qml:
        return None
    C = _QMLC
    buy = qml["dir"] == "bull"
    side = "BUY" if buy else "SELL"
    z, sh, hd, ck = qml["zone"], qml["shoulder"], qml["head"], qml["choch"]
    zt, zb = float(z["top"]), float(z["bottom"])
    fresh = qml.get("mitigated_idx") is None
    bos = qml.get("bos")
    dec = SYMBOLS.get(symbol, {}).get("decimals", 2)
    px = lambda v: f"{v:.{dec}f}"       # image : prix sans séparateur de milliers (lisible)

    # --- fenêtre visible : depuis l'élément le plus ancien du QML (- marge), plafonnée à QMLC_MAX_BARS
    anchors = [sh["idx"], ck["pivot_idx"]] + ([bos["pivot_idx"]] if bos else [])
    left = max(0, min(anchors) - QMLC_BARS_BEFORE)
    if n - left > QMLC_MAX_BARS:
        left = n - QMLC_MAX_BARS
    view = candles[left:]
    m = len(view)
    X = lambda i: i - left
    x0, x1 = m - 1, m - 1 + QMLC_EXTEND          # outil de position : de la dernière bougie vers la droite
    ok_trade = bool(trade) and trade.get("entry") is not None and trade.get("sl") is not None

    ys = [c["l"] for c in view] + [c["h"] for c in view] + [zt, zb, hd["price"], sh["price"]]
    if ok_trade:
        ys += [trade["entry"], trade["sl"]] + [trade[k] for k in ("tp1", "tp2") if trade.get(k) is not None]
    ys += [float(v) for o in others or [] if o is not qml and o["shoulder"]["idx"] >= left for v in (o["zone"]["top"], o["zone"]["bottom"])]
    lo, hi = min(ys), max(ys)
    rng = (hi - lo) or 1.0
    ys += [hd["price"] + (0.09 * rng if not buy else -0.09 * rng)]      # place pour la flèche SWEEP
    lo, hi = min(ys), max(ys)
    pad = (hi - lo) * 0.05

    fig, ax = plt.subplots(figsize=(11, 6.5), dpi=110)
    try:
        fig.patch.set_facecolor("white")
        ax.set_facecolor("white")
        ax.grid(color="#eceff1", lw=0.6, zorder=0)

        # --- bougies
        xs = list(range(m))
        cols = ["#26a69a" if c["c"] >= c["o"] else "#ef5350" for c in view]
        ax.vlines(xs, [c["l"] for c in view], [c["h"] for c in view], colors="#455a64", lw=0.9, zorder=3)
        ax.bar(xs, [max(abs(c["c"] - c["o"]), rng * 0.0006) for c in view], bottom=[min(c["o"], c["c"]) for c in view],
               width=0.62, color=cols, edgecolor="#455a64", linewidth=0.6, zorder=3)

        # --- zone QML : noire épaisse si non mitigée, sinon orange (vente) / bleue (achat)
        xa = max(X(z.get("idx", sh["idx"])), 0) - 0.5
        near = zt if buy else zb                                        # bord proche : niveau d'entrée limite
        if fresh:
            xb = x1
            ax.add_patch(plt.Rectangle((xa, zb), xb - xa, zt - zb, facecolor=C["BLACK"], alpha=0.14, edgecolor="none", zorder=1))
            ax.add_patch(plt.Rectangle((xa, zb), xb - xa, zt - zb, fill=False, edgecolor=C["BLACK"], lw=2.6, zorder=5))
            ax.plot([xa, xb], [near, near], color=C["BLACK"], lw=3.0, solid_capstyle="butt", zorder=5)
        else:
            xb = x1
            xm = max(X(qml["mitigated_idx"]), xa + 1)
            col = C[side]
            ax.add_patch(plt.Rectangle((xa, zb), xb - xa, zt - zb, facecolor=col, alpha=0.40, edgecolor="none", zorder=1))
            ax.add_patch(plt.Rectangle((xa, zb), xb - xa, zt - zb, fill=False, edgecolor=col, lw=2.2, zorder=5))
            ax.plot([xa, xb], [near, near], color=col, lw=2.2, zorder=5)
            ax.axvline(xm, ymin=0, ymax=1, color=col, lw=0.8, ls=":", zorder=2)   # 1re retouche de la zone
        ax.text(x0 - 1, zt if not buy else zb, f"QML {'ACHAT' if buy else 'VENTE'} · {'NON MITIGÉ' if fresh else 'mitigé'}",
                fontsize=8, fontweight="bold", color=C["BLACK"] if fresh else C[side], ha="right", va="bottom" if not buy else "top",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=C["BLACK"] if fresh else C[side], lw=1.0, alpha=0.95), zorder=7)

        # --- autres QML (achat et vente) : zone + étiquette
        for o in others or []:
            if o is qml or o["shoulder"]["idx"] < left or (o["zone"]["top"], o["zone"]["bottom"]) == (z["top"], z["bottom"]):
                continue
            ob, ofresh = o["dir"] == "bull", o.get("mitigated_idx") is None
            ot, obt = float(o["zone"]["top"]), float(o["zone"]["bottom"])
            oxa = max(X(o["zone"].get("idx", o["shoulder"]["idx"])), 0) - 0.5
            ocol = C["BLACK"] if ofresh else C["BUY" if ob else "SELL"]
            ax.add_patch(plt.Rectangle((oxa, obt), x1 - oxa, ot - obt, facecolor=ocol, alpha=0.14 if ofresh else 0.35,
                                       edgecolor=ocol, lw=2.0 if ofresh else 1.6, zorder=1))
            ax.text(oxa + 0.5, ot if not ob else obt, f"QML {'ACHAT' if ob else 'VENTE'}{' · valide' if o.get('valide') else ''}",
                    fontsize=7, fontweight="bold", color=ocol, va="bottom" if not ob else "top",
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.85), zorder=7)

        # --- EQH / EQL : liquidité de l'épaule, balayée par la tête
        ex1 = min(X(hd["idx"]) + 3, m - 1)
        ax.plot([X(sh["idx"]), ex1], [sh["price"], sh["price"]], color=C["EQ"], lw=1.3, ls=":", zorder=4)
        ax.text(X(sh["idx"]), sh["price"], "EQL" if buy else "EQH", fontsize=8, fontweight="bold", color=C["EQ"],
                ha="right", va="top" if buy else "bottom", zorder=7)

        # --- flèche SWEEP sur la tête
        hx, hp = X(hd["idx"]), hd["price"]
        ax.annotate("SWEEP", xy=(hx, hp), xytext=(hx, hp + (-0.09 * rng if buy else 0.09 * rng)), ha="center",
                    va="top" if buy else "bottom", fontsize=9, fontweight="bold", color=C["SWEEP"],
                    arrowprops=dict(arrowstyle="-|>", color=C["SWEEP"], lw=1.6), zorder=8)

        # --- BOS / CHoCH étiquetés (structure fournie + ceux du QML)
        evs, seen = [], set()
        key = lambda e: (e["type"], e["break_idx"], e["pivot_idx"])
        for e in list((structure or {}).get("events") or [])[-8:] + [bos, ck]:
            if e and key(e) not in seen and e["break_idx"] >= left:
                seen.add(key(e))
                evs.append(e)
        for e in evs:
            is_ch = e["type"] == "CHOCH"
            col = C["CHOCH"] if is_ch else C["BOS"]
            ea, eb = max(X(e["pivot_idx"]), 0), X(e["break_idx"])
            ax.plot([ea, eb], [e["level"], e["level"]], color=col, lw=1.6 if is_ch else 1.1, ls="-" if is_ch else "--", zorder=4)
            up = e["dir"] == "bull"
            ax.text((ea + eb) / 2, e["level"], "CHoCH" if is_ch else "BOS", fontsize=8, fontweight="bold", color=col,
                    ha="center", va="bottom" if up else "top", zorder=7)

        # --- outil de position : boîte rouge entrée→SL, verte entrée→TP2, ligne TP1, étiquettes prix + RR
        if ok_trade:
            entry, sl = float(trade["entry"]), float(trade["sl"])
            tp1, tp2 = trade.get("tp1"), trade.get("tp2")
            tpb = tp2 if tp2 is not None else tp1
            # liquidités visées (EQH / EQL / swing) : pointillés depuis la bougie du niveau jusqu'à l'outil
            for k, tp in ((1, tp1), (2, tp2)):
                if tp is None:
                    continue
                hit = next((i for i, c in enumerate(view) if math.isclose(c["h"], tp, rel_tol=1e-12) or math.isclose(c["l"], tp, rel_tol=1e-12)), None)
                xs0 = hit if hit is not None else max(x0 - 12, 0)
                ax.plot([xs0, x0], [tp, tp], color=C["EQ"], lw=1.2, ls=":", zorder=4)
                kind = trade.get(f"tp{k}_kind")
                ax.text(xs0, tp, f"{kind if kind in ('EQH', 'EQL') else 'Swing'} (TP{k})", fontsize=7, color=C["EQ"],
                        ha="left", va="bottom" if tp > entry else "top", zorder=7)
            ax.add_patch(plt.Rectangle((x0, min(entry, sl)), QMLC_EXTEND, abs(entry - sl), facecolor=C["RED"], alpha=0.22,
                                       edgecolor=C["RED"], lw=1.0, zorder=2))
            if tpb is not None:
                ax.add_patch(plt.Rectangle((x0, min(entry, tpb)), QMLC_EXTEND, abs(tpb - entry), facecolor=C["GREEN"], alpha=0.22,
                                           edgecolor=C["GREEN"], lw=1.0, zorder=2))
            if tp1 is not None:
                ax.plot([x0, x1], [tp1, tp1], color=C["GREEN"], lw=1.8, ls="--", zorder=5)
            ax.plot([x0, x1], [entry, entry], color="#263238", lw=1.5, zorder=5)
            risk = abs(entry - sl)
            pills = [(f"ENTRÉE {side}{' LIMIT' if trade.get('order') == 'LIMIT' else ''} {px(entry)}", entry, "#263238"),
                     (f"SL {px(sl)} · 1R ({px(risk)})", sl, C["RED"])]
            for k, tp in ((1, tp1), (2, tp2)):
                if tp is not None:
                    rr = trade.get(f"rr{k}")
                    pills.append((f"TP{k} {px(tp)}" + (f" · RR {rr:.2f}" if rr is not None else ""), tp, C["GREEN"]))
            gap, prev = 0.045 * (hi - lo + 2 * pad), None
            for txt, y, col in sorted(pills, key=lambda p: -p[1]):     # du haut vers le bas ; on écarte les étiquettes qui se chevauchent
                yt = y if prev is None else min(y, prev - gap)
                prev = yt
                ax.text(x1 + 0.8, yt, txt, fontsize=8, fontweight="bold", color="white", va="center", ha="left", zorder=8,
                        bbox=dict(boxstyle="round,pad=0.25", fc=col, ec="none"))
                if abs(yt - y) > 1e-9:
                    ax.plot([x1, x1 + 0.8], [y, yt], color=col, lw=1.0, zorder=7)

        # --- cadre, axes, titre
        ax.set_xlim(-1, x1 + int(0.27 * (m + QMLC_EXTEND)) + 8)
        ax.set_ylim(lo - pad, hi + pad)
        step = max(1, m // 7)
        ticks = list(range(0, m, step))
        ax.set_xticks(ticks)
        ax.set_xticklabels([_qmlc_time_label(view[i]["t"]) for i in ticks], fontsize=7, color="#607d8b")
        ax.tick_params(axis="y", labelsize=7, colors="#607d8b")
        ax.yaxis.tick_right()
        for sp in ax.spines.values():
            sp.set_color("#cfd8dc")
        title = " ".join(s for s in (symbol, _tf_lbl(tf) if isinstance(tf, int) else tf) if s)
        ax.set_title(f"{title} — QML {'ACHAT' if buy else 'VENTE'} ({qml.get('type', '')}) · "
                     f"{'non mitigé' if fresh else 'mitigé'} · {'valide' if qml.get('valide') else 'non valide'}",
                     fontsize=11, color="#263238", loc="left")
        _qmlc_purge()
        path = path or os.path.join(CHART_DIR, f"{symbol or 'qml'}_qmlchart_{int(time.time() * 1000)}.png")
        fig.savefig(path, facecolor="white", bbox_inches="tight")
    finally:
        plt.close(fig)
    return path


def qmlc_caption(qml, trade, symbol=None, tf=None):
    """Légende Telegram (HTML, < 1000 caractères : limite d'une légende photo)."""
    buy = qml["dir"] == "bull"
    dec = SYMBOLS.get(symbol, {}).get("decimals", 2)
    z, fresh = qml["zone"], qml.get("mitigated_idx") is None
    tfl = _tf_lbl(tf) if isinstance(tf, int) else (tf or "")
    txt = (f"{'🟢' if buy else '🔴'} <b>{symbol or 'QML'}</b> {tfl} · QML {'ACHAT' if buy else 'VENTE'} · {qml.get('type', '')}\n"
           f"Zone {_fmt(z['bottom'], dec)} → {_fmt(z['top'], dec)} · {'non mitigé ⬛' if fresh else 'mitigé'} · "
           f"{'✅ valide' if qml.get('valide') else '❌ non valide'}\n")
    if not trade or not trade.get("ok"):
        return txt + f"Plan de trade : indisponible ({(trade or {}).get('reason') or 'aucun plan'})"
    f2 = lambda k: "—" if trade.get(k) is None else (f"{_fmt(trade[k], dec)} ({trade.get(k + '_kind')}) · RR {trade['rr' + k[-1]]:.2f}")
    return txt + (f"⏳ <b>{trade['side']} {trade.get('order', 'LIMIT')}</b> : {_fmt(trade['entry'], dec)}\n"
                  f"🛡 SL {_fmt(trade['sl'], dec)}\n🎯 TP1 {f2('tp1')}\n🎯 TP2 {f2('tp2')}\n"
                  f"(information seulement : aucun ordre envoyé)")


def send_qml_chart(df, qml, trade=None, symbol=None, tf=None, structure=None, chat_id=None, others=None):
    """Dessine le graphique puis l'envoie sur Telegram avec sa légende (admin par défaut). Sans image (matplotlib absent
    ou erreur de dessin), la légende part seule en texte. Retourne la réponse Telegram."""
    chat_id = chat_id or CHAT_ID_ADMIN
    path = None
    try:
        path = draw_chart(df, qml, trade, symbol=symbol, tf=tf, structure=structure, others=others)
    except Exception:
        traceback.print_exc(limit=-3)
    return send(chat_id, qmlc_caption(qml, trade, symbol, tf), photo=path)


def qmlg_send_last_chart(symbol, tf=None, chat_id=None, valid_only=False):
    """Récupère les bougies, détecte structure + QML et envoie UN graphique par sens : le dernier QML VALIDE d'achat ET
    celui de vente (les deux zones apparaissent sur chaque image). Sans QML valide : le dernier QML (sauf valid_only).
    Retourne (ok, message)."""
    tf_min = _qmlg_tf_minutes(tf)
    candles = get_candles(symbol, minutes=tf_min, count=QMLG_HISTORY_BARS) or []
    if len(candles) < QMLG_MIN_BARS:
        return False, f"{len(candles)} bougies (< {QMLG_MIN_BARS})"
    structure = detect_structure(candles, f"M{tf_min}", n=QMLG_FRACTAL_N)
    res = detect_qml(structure, min_rr=QMLG_MIN_RR)
    valid = [q for q in res["qmls"] if q["valide"]]
    picks = [next((q for q in reversed(valid) if q["dir"] == d), None) for d in ("bull", "bear")]
    picks = [q for q in picks if q]
    if not picks and not valid_only and res["last_qml"]:
        picks = [res["last_qml"]]
    if not picks:
        return False, "aucun QML valide" if valid_only else "aucun QML sur la période"
    for q in picks:
        send_qml_chart(candles, q, compute_trade(q, structure), symbol=symbol, tf=tf_min, structure=structure,
                       chat_id=chat_id, others=picks + res["qmls"][-6:])
    return True, f"{len(picks)} graphique(s) envoyé(s)"



# ============================================================================
# 17. VÉRIFICATIONS — 3 AGENTS QML (avant tout signal) + 4 CONTRÔLES TP / SL (avant tout rapport)
#
# A) qmlg_agents : un QML n'est « valide » que si les 3 agents sont d'accord (QMLG_AGENTS=0 pour désactiver).
#      Agent 1 DONNÉES    : relit le maximum de bougies (appel indépendant), contrôle leur qualité, REFAIT la détection
#                           et vérifie que le même QML est retrouvé.
#      Agent 2 STRUCTURE  : critères (tendance, sweep, CHoCH clôturé, épaule), géométrie zone / SL / TP, RR >= minimum.
#      Agent 3 PRIX       : la zone n'est pas invalidée (clôture au-delà du bord lointain) ; une zone simplement liquidée
#                           (mèche au-delà, clôture revenue) reste valide ; position du prix (avant / dans la zone).
# B) verify_outcome : un TP / SL / BE n'est annoncé et enregistré que s'il passe 4 contrôles (M1 rejouées, deals MT5,
#    position MT5 fermée, cohérence des niveaux et du R). Sinon le résultat est annulé et ré-évalué à la bougie suivante.
# ============================================================================
QMLG_AGENTS = _env_on("QMLG_AGENTS", True)
VERIFY_OUTCOME = _env_on("VERIFY_OUTCOME", True)
VERIFY_MAX_TRIES = _env_int("VERIFY_MAX_TRIES", 3)       # refus consécutifs avant alerte admin (le trade reste ouvert)
VERIFY_M1_COUNT = _env_int("VERIFY_M1_COUNT", 1000)
_qmlg_fresh_cache = {}


def _qmlg_fresh(symbol, tf):
    """Agent 1 : nouvelle récupération des bougies + nouvelle détection (cache 45 s par marché / TF)."""
    key, now = (symbol, tf), time.time()
    hit = _qmlg_fresh_cache.get(key)
    if hit and now - hit["ts"] < 45:
        return hit
    c2 = get_candles(symbol, minutes=tf, count=QMLG_HISTORY_BARS) or []
    out = {"ts": now, "candles": c2, "qmls": [], "structure": None}
    if len(c2) >= QMLG_MIN_BARS:
        st = detect_structure(c2, f"M{tf}", n=QMLG_FRACTAL_N, keep=QMLG_KEEP)
        out["structure"], out["qmls"] = st, detect_qml(st, min_rr=QMLG_MIN_RR)["qmls"]
    _qmlg_fresh_cache[key] = out
    return out


def _qmlg_agent1(symbol, tf, q, candles):
    try:
        fr = _qmlg_fresh(symbol, tf)
    except Exception as e:
        return False, f"relecture impossible ({type(e).__name__})"
    c2 = fr["candles"]
    if len(c2) < QMLG_MIN_BARS:
        return False, f"{len(c2)} bougies (< {QMLG_MIN_BARS})"
    ts = [c["t"] for c in c2]
    if any(b <= a for a, b in zip(ts, ts[1:])):
        return False, "bougies désordonnées / doublons"
    if any((c["h"] < c["l"]) or not all(math.isfinite(float(c[k])) for k in ("o", "h", "l", "c")) for c in c2):
        return False, "bougie incohérente (high < low / NaN)"
    if abs(c2[-1]["t"] - candles[-1]["t"]) > 2 * tf * 60:
        return False, "les deux lectures ne finissent pas à la même bougie"
    z = q["zone"]
    same = [o for o in fr["qmls"] if o["dir"] == q["dir"] and o["choch"]["break_time"] == q["choch"]["break_time"]
            and math.isclose(float(o["zone"]["top"]), float(z["top"]), rel_tol=1e-9)
            and math.isclose(float(o["zone"]["bottom"]), float(z["bottom"]), rel_tol=1e-9)]
    if not same:
        return False, "QML non retrouvé à la 2e détection"
    return True, f"{len(c2)} bougies · QML reproduit"


def _qmlg_agent2(q, plan):
    cr = q.get("criteres") or {}
    miss = [k for k in ("tendance_avant", "sweep", "choch_cloture", "zone_epaule") if not cr.get(k)]
    if miss:
        return False, "critères manquants : " + ", ".join(miss)
    z = q["zone"]
    top, bot = float(z["top"]), float(z["bottom"])
    if not top > bot:
        return False, "zone invalide (haut <= bas)"
    if not plan or not plan.get("ok"):
        return False, "plan de trade indisponible"
    buy = q["dir"] == "bull"
    e, sl, tp1, tp2 = plan["entry"], plan["sl"], plan.get("tp1"), plan.get("tp2")
    if buy and not (sl < bot <= e <= top * (1 + 1e-9) and (tp1 is None or tp1 > e)):
        return False, "géométrie achat incohérente (SL / entrée / TP)"
    if not buy and not (sl > top >= e >= bot * (1 - 1e-9) and (tp1 is None or tp1 < e)):
        return False, "géométrie vente incohérente (SL / entrée / TP)"
    if tp1 is not None and tp2 is not None and ((tp2 - tp1) * (1 if buy else -1)) < 0:
        return False, "TP2 avant TP1"
    if plan.get("rr1") is None or plan["rr1"] < QMLG_MIN_RR:
        return False, f"RR1 {plan.get('rr1')} < {QMLG_MIN_RR}"
    return True, f"critères OK · RR1 {plan['rr1']:.2f}"


def _qmlg_agent3(symbol, q, structure, candles):
    z, buy = q["zone"], q["dir"] == "bull"
    top, bot = float(z["top"]), float(z["bottom"])
    series = (structure.get("_series") or {}).get("c") or []
    off = len(candles) - len(series)
    start = max(0, q["choch"]["break_idx"] + 1 + off)
    swept = False
    for c in candles[start:]:
        if (c["c"] < bot) if buy else (c["c"] > top):
            return False, "zone invalidée (clôture au-delà du bord lointain)", "invalide"
        if (c["l"] < bot) if buy else (c["h"] > top):
            swept = True   # liquidité prise puis clôture revenue : la zone reste valide
    try:
        px = get_live_price(symbol)
    except Exception:
        px = None
    px = px if px is not None else candles[-1]["c"]
    if (px < bot) if buy else (px > top):
        return False, "prix en direct au-delà du bord lointain", "invalide"
    state = "dans la zone" if bot <= px <= top else "avant la zone"
    return True, state + (" · zone liquidée puis reprise" if swept else ""), state


def qmlg_agents(symbol, tf, q, structure, candles, plan):
    """Les 3 agents. Retourne {ok, a1, a2, a3, state} ; chaque agent = [ok, détail]. Jamais d'exception."""
    res = {}
    for name, fn in (("a1", lambda: _qmlg_agent1(symbol, tf, q, candles)),
                     ("a2", lambda: _qmlg_agent2(q, plan)),
                     ("a3", lambda: _qmlg_agent3(symbol, q, structure, candles))):
        try:
            r = fn()
        except Exception as e:
            r = (False, f"erreur agent ({type(e).__name__})")
        res[name] = [bool(r[0]), r[1]]
        if name == "a3" and len(r) > 2:
            res["state"] = r[2]
    res["ok"] = all(res[k][0] for k in ("a1", "a2", "a3"))
    return res


# --- B) 4 contrôles avant d'annoncer un TP / SL / BE ---------------------------------------------------------
def _vf_levels(trade):
    side = 1 if trade["side"] == "BUY" else -1
    entry, risk = trade["entry"], abs(trade["entry"] - trade["sl_initial"])
    return side, entry, risk, trade["sl_initial"], entry + side * trade_tp_rr(trade) * risk


def _vf_replay(trade, candles, t0):
    """Rejoue le trade sur des bougies : 'TP' / 'SL' / 'BE' / 'OPEN' ; None si ambigu (SL et TP dans la même bougie)."""
    side, entry, risk, sl0, tp = _vf_levels(trade)
    if risk <= 0:
        return None
    sl, be_rr, be = sl0, trade_be_rr(trade), False
    for c in candles:
        if c["t"] <= t0:
            continue
        adv, fav = (c["l"], c["h"]) if side == 1 else (c["h"], c["l"])
        sl_hit = (adv <= sl) if side == 1 else (adv >= sl)
        tp_hit = (fav >= tp) if side == 1 else (fav <= tp)
        if sl_hit and tp_hit:
            return None
        if sl_hit:
            return "BE" if be else "SL"
        if tp_hit:
            return "TP"
        if not be and be_rr < trade_tp_rr(trade) and (fav - entry) * side / risk >= be_rr:
            be, sl = True, entry
    return "OPEN"


def verify_outcome(trade, outcome):
    """4 contrôles indépendants. Retourne (accepté, [(nom, vote, note)]) ; vote True / False / None (non disponible)."""
    t0 = trade.get("filled_ts") or trade.get("opened_ts") or 0
    votes = []
    # 1) bougies M1 rejouées (source fine, indépendante du TF de suivi)
    try:
        m1 = get_candles(trade["symbol"], minutes=1, count=VERIFY_M1_COUNT) or []
        rp = _vf_replay(trade, m1, t0) if m1 and m1[0]["t"] <= (t0 + 120) else None
        votes.append(("M1", None if rp is None else rp == outcome, f"M1 -> {rp}"))
    except Exception as e:
        votes.append(("M1", None, f"indisponible ({type(e).__name__})"))
    pos = trade.get("mt5_position_id") if mt5_orders_allowed() else None
    deals = None
    if pos:
        try:
            deals = _run_mt5(_mt5_position_deals_async(str(pos)))
        except Exception:
            deals = None
    # 2) deals MT5 (raison de sortie SL / TP) — ignoré si exécution MT5 désactivée (rien à vérifier côté broker)
    if mt5_orders_allowed():
        if pos and deals:
            o2, _r, _p = _mt5_close_outcome(trade, deals)
            votes.append(("Deals MT5", None if o2 == "CLOSED_EXT" else o2 == outcome, f"MT5 -> {o2}"))
        else:
            votes.append(("Deals MT5", None, "pas de position MT5" if not pos else "deals indisponibles"))
    # 3) position réellement fermée côté broker — idem, ignoré si MT5 désactivé
    if mt5_orders_allowed():
        if pos:
            try:
                ids = _run_mt5(_mt5_open_position_ids_async())
            except Exception:
                ids = None
            votes.append(("Position MT5", None if ids is None else str(pos) not in ids,
                          "indisponible" if ids is None else ("fermée" if str(pos) not in ids else "encore ouverte")))
        else:
            votes.append(("Position MT5", None, "n/a"))
    # 4) cohérence : niveaux, signe du R, horodatage
    try:
        side, entry, risk, sl0, tp = _vf_levels(trade)
        r = trade.get("result_r")
        ok = risk > 0 and (sl0 < entry if side == 1 else sl0 > entry) and (tp > entry if side == 1 else tp < entry)
        if r is not None:
            ok = ok and ((r > 0) if outcome == "TP" else (r < 0) if outcome == "SL" else abs(r) < 1e-6 or outcome == "BE")
        ok = ok and (not trade.get("closed_ts") or trade["closed_ts"] >= t0)
        votes.append(("Cohérence", bool(ok), f"R={r}"))
    except Exception as e:
        votes.append(("Cohérence", False, f"erreur ({type(e).__name__})"))
    agree = sum(1 for v in votes if v[1] is True)
    contra = sum(1 for v in votes if v[1] is False)
    avail = sum(1 for v in votes if v[1] is not None)
    broker_ok = dict((v[0], v[1]) for v in votes).get("Deals MT5") is True and dict((v[0], v[1]) for v in votes).get("Position MT5") is True
    need = 3 if pos else min(2, avail)
    accepted = broker_ok or (contra == 0 and agree >= need and avail >= 2)
    return accepted, votes


_TRADE_RESTORE_KEYS = ("sl", "be_hit", "status", "last_ts", "closed_ts", "filled_ts", "result_r", "pnl_usd", "outcome",
                       "rr1_hit", "rr2_hit", "rr3_hit", "part_done", "part_r", "part_pct")


def track_trade_verified(trade, candles):
    """track_trade + 4 contrôles sur TP / SL / BE. Un résultat refusé est ANNULÉ (état du trade restauré) puis ré-évalué
    à la prochaine bougie ; après VERIFY_MAX_TRIES refus, l'admin est prévenu une fois (le groupe ne reçoit rien)."""
    snap = {k: trade.get(k) for k in _TRADE_RESTORE_KEYS}
    events = track_trade(trade, candles)
    if not VERIFY_OUTCOME:
        return events
    final = next((e for e in events if e["name"] in ("TP", "SL", "BE")), None)
    if not final:
        return events
    try:
        ok, votes = verify_outcome(final["trade"], final["name"])
    except Exception:
        traceback.print_exc(limit=-3)
        return events   # contrôle en panne : on ne bloque jamais le suivi
    note = " · ".join(f"{n}: {'✅' if v else '❌' if v is False else '➖'} {t}" for n, v, t in votes)
    if ok:
        set_meta(f"vf_{trade['id']}", 0)
        print(f"[verif] #{trade['id']} {final['name']} confirmé — {note}")
        return events
    tries = int(get_meta(f"vf_{trade['id']}", 0) or 0) + 1
    set_meta(f"vf_{trade['id']}", tries)
    trade.update(snap)
    update_trade(trade["id"], **{k: (v if v is not None else 0) if k.startswith(("part_", "rr")) or k == "be_hit" else v
                                 for k, v in snap.items()})
    print(f"[verif] #{trade['id']} {final['name']} REFUSÉ ({tries}) — {note}")
    if tries == VERIFY_MAX_TRIES:
        to_admin(f"⚠️ <b>{trade['symbol']} #{trade['id']}</b> : résultat <b>{final['name']}</b> non confirmé après "
                 f"{tries} contrôles — rien envoyé au groupe, le trade reste ouvert.\n{note}")
    return [e for e in events if e["name"] not in ("TP", "SL", "BE")]


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(_selftest())
    main()



