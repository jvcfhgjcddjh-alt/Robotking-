"""
ALPHABOT SMC PRO — FUSION (fichier unique, prêt à déployer sur Render)
========================================================================
Fusion des deux cahiers des charges (PDF "SMC PRO" + DOCX "Liquidity Sweep +
BOS Corps de Bougie"), confirmée avec Pie :

  - Actifs : Gold (XAUUSD), BTC/USD, Deriv Volatility 75, Deriv Volatility 25
  - Sessions : Gold + BTC -> session US/NY, 13h-22h UTC. Deriv (V75/V25) -> H24.
  - Timeframe : M5 uniquement, sur tous les actifs.
  - Signal : sweep de liquidité (mèche dépasse un niveau) PUIS cassure de
    structure validée UNIQUEMENT par la clôture du CORPS de la bougie au-delà
    du niveau de structure interne opposé (pas le niveau balayé lui-même).
    Une mèche seule ne valide jamais -> signal ignoré.
  - Entrée : directe (★★) si pas de FVG laissé par la bougie de cassure,
    retour dans l'imbalance (★★★) si un FVG est détecté (plus haute confiance).
  - SL : ATR(14) x 0,6 au-delà de l'extrémité du sweep, marge spread/commission incluse.
  - TP1 : RR3, avec BE proposé à RR1 et sécurisation partielle proposée à RR2.
  - TP2 : cible stratégique = prochain pool de liquidité non mitigé dans le sens
    du trade (peut donner RR > 10, pas de plafond — juste un badge d'alerte).
  - Score global 0-100 (étoiles + qualité du RR de TP2) ; seuil de publication
    configurable (MIN_SCORE_TO_PUBLISH).
  - Diffusion Telegram sur 2 groupes : "BTC + Gold" et "Deriv" (V75/V25).
  - Dashboard Flask : capital/levier/risque/lot, stats, profils de risque,
    statut des trades (pris/ignoré/clôturé), endpoint /health pour Render.
  - Persistance SQLite : historique complet, anti-doublon, cap quotidien de signaux.
  - Robustesse : chaque itération de scan est protégée par try/except, la
    boucle ne s'arrête jamais ; à faire tourner avec redémarrage automatique
    côté Render (Background Worker ou Web Service, cf. section DÉPLOIEMENT
    en bas de fichier).

  - Flux Deriv (V75/V25) : branché en direct sur l'API publique Deriv (WebSocket
    ticks_history, granularity M5). App ID configurable via DERIV_APP_ID
    (App ID public de démo "1089" utilisé par défaut).
  - Rapports automatiques : journalier (21h UTC), hebdomadaire (dimanche
    21h30 UTC), mensuel (dernier jour du mois, 22h UTC) — envoyés sur
    Telegram (TG_CHAT_REPORTS si défini, sinon sur les 2 groupes de signaux)
    et consultables via /api/reports/daily, /weekly, /monthly.

⚠️ POINTS OUVERTS avant la prod réelle (voir aussi les commentaires inline) :
  1. Flux Gold via yfinance (GC=F) est un flux "best effort" — remplacer par
     un flux broker/MT5 si tu veux un prix plus fidèle à ton exécution réelle.
  2. Génération d'image annotée (chart-img.com) non branchée ici — la fonction
     send_telegram_signal() accepte déjà un `image_path` optionnel, prêt à
     recevoir un screenshot si tu veux l'ajouter.
  3. Les rapports raisonnent en multiples de R (pas de $ réel), car aucun
     capital/lot n'est stocké par trade dans la base — cohérent avec le RR
     déjà affiché dans chaque signal.
"""

import os
import time
import json
import sqlite3
import logging
import threading
import traceback
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

import requests
from flask import Flask, request, jsonify, render_template_string

import matplotlib
matplotlib.use("Agg")  # pas d'affichage graphique — génération d'images en fichier uniquement
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle


# ============================================================================
# 1. CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class AssetConfig:
    symbol: str
    display_name: str
    data_source: str            # "yfinance" | "binance" | "deriv_ws"
    telegram_group: str         # "btc_gold" | "deriv"
    session_continuous: bool
    session_start_utc: Optional[int] = None
    session_end_utc: Optional[int] = None
    pip_size: float = 0.01
    contract_type: str = "classic"


ASSETS = {
    "XAUUSD": AssetConfig(
        symbol="XAUUSD", display_name="Gold (XAUUSD)", data_source="yfinance",
        telegram_group="btc_gold", session_continuous=False,
        session_start_utc=13, session_end_utc=22, contract_type="classic",
    ),
    "BTCUSD": AssetConfig(
        symbol="BTCUSD", display_name="BTC/USD", data_source="binance",
        telegram_group="btc_gold", session_continuous=False,
        session_start_utc=13, session_end_utc=22, contract_type="crypto",
    ),
    "V75": AssetConfig(
        symbol="V75", display_name="Volatility 75 Index", data_source="deriv_ws",
        telegram_group="deriv", session_continuous=True, contract_type="synthetic",
    ),
    "V25": AssetConfig(
        symbol="V25", display_name="Volatility 25 Index", data_source="deriv_ws",
        telegram_group="deriv", session_continuous=True, contract_type="synthetic",
    ),
}

TIMEFRAME = "M5"
SCAN_INTERVAL_SECONDS = 30

# Anti-doublon "temps" : délai minimum entre deux signaux sur le MÊME actif,
# quel que soit leur statut (même après clôture). Vient en plus de la
# protection structurelle par setup_key (symbol:direction:niveau balayé), qui
# ne couvre que les setups encore actifs. Ce cooldown couvre le cas où le
# même niveau est re-balayé peu après la clôture d'un premier signal.
SIGNAL_COOLDOWN_MINUTES = float(os.environ.get("SIGNAL_COOLDOWN_MINUTES", "15"))
SIGNAL_COOLDOWN_SECONDS = SIGNAL_COOLDOWN_MINUTES * 60

ATR_PERIOD = 14
ATR_SL_MULTIPLIER = 0.6
INCLUDE_SPREAD_COMMISSION_BUFFER = True

TP1_RR = 3.0
BE_TRIGGER_RR = 1.0
SECURE_TRIGGER_RR = 2.0
TP2_MIN_RR_WARNING = 10.0

ENTRY_TYPES = {
    "direct": {"stars": "★★", "label": "Entrée directe", "score_weight": 60},
    "fvg_return": {"stars": "★★★", "label": "Retour Imbalance (FVG)", "score_weight": 85},
}
MIN_SCORE_TO_PUBLISH = 70

MAX_SIGNALS_PER_DAY_GLOBAL = 3
MAX_SIGNALS_PER_DAY_PER_ASSET = 2

# ⚠️ SÉCURITÉ — valeurs par défaut codées en dur à la demande explicite du
# porteur du projet (tokens/chat IDs communiqués en clair dans la conversation
# de configuration). Ces valeurs sont utilisées UNIQUEMENT si la variable
# d'environnement correspondante n'est pas définie sur Render — donc si tu
# régénères un token plus tard, définis simplement la variable d'env
# correspondante et elle prendra le dessus automatiquement, sans toucher au
# code. Recommandé : régénère ces tokens via @BotFather dès que possible
# puisqu'ils ont transité en clair dans un chat, puis passe par les
# variables d'environnement Render au lieu de ce fallback.
_DEFAULT_TG_CHAT_BTC_GOLD = "-5281258868"
_DEFAULT_TG_TOKEN_BTC_GOLD = "6950706659:AAEeRW9Ld1zNz0xyDLwlKuIQxZst4S7RU0Q"
_DEFAULT_TG_CHAT_DERIV = "-1002335466840"
_DEFAULT_TG_TOKEN_DERIV = "8665812395:AAGdB95bYjQOcGrk5r84V7bosdGboRQJcUQ"

TELEGRAM_GROUPS = {
    # Groupe 1 : BTC/USD + XAU/USD (Gold) — bot Telegram dédié, indépendant du groupe Deriv.
    "btc_gold": {
        "token_env": "TELEGRAM_BOT_TOKEN_BTC_GOLD",
        "chat_id_env": "TG_CHAT_BTC_GOLD",
        "assets": ["XAUUSD", "BTCUSD"],
        "token_default": _DEFAULT_TG_TOKEN_BTC_GOLD,
        "chat_id_default": _DEFAULT_TG_CHAT_BTC_GOLD,
    },
    # Groupe 2 : Volatility 25 + Volatility 75 (Deriv) — bot Telegram dédié.
    "deriv": {
        "token_env": "TELEGRAM_BOT_TOKEN_DERIV",
        "chat_id_env": "TG_CHAT_DERIV",
        "assets": ["V25", "V75"],
        "token_default": _DEFAULT_TG_TOKEN_DERIV,
        "chat_id_default": _DEFAULT_TG_CHAT_DERIV,
    },
    # Optionnel : si TG_CHAT_REPORTS n'est pas défini, les rapports sont
    # envoyés sur les deux groupes de signaux ci-dessus à la place (chacun
    # avec son propre bot token). TELEGRAM_BOT_TOKEN_REPORTS est optionnel ;
    # à défaut, le token du groupe "btc_gold" est réutilisé pour ce canal.
    "reports": {"token_env": "TELEGRAM_BOT_TOKEN_REPORTS", "chat_id_env": "TG_CHAT_REPORTS", "assets": []},
}


def enforce_group_asset_whitelist():
    """Vérifie au démarrage que chaque actif n'est routé QUE vers le groupe
    Telegram autorisé pour lui (BTC/XAU -> btc_gold, V25/V75 -> deriv).
    Lève une erreur explicite si la config a été modifiée de façon incohérente."""
    for symbol, asset in ASSETS.items():
        allowed = TELEGRAM_GROUPS.get(asset.telegram_group, {}).get("assets", [])
        if allowed and symbol not in allowed:
            raise RuntimeError(
                f"Incohérence de configuration : l'actif '{symbol}' est routé vers le "
                f"groupe Telegram '{asset.telegram_group}', qui n'autorise que {allowed}."
            )

REPORT_DAILY_HOUR_UTC = 21
REPORT_WEEKLY_HOUR_UTC = 21
REPORT_WEEKLY_MINUTE_UTC = 30   # décalé de la journalière pour ne pas se chevaucher
REPORT_MONTHLY_HOUR_UTC = 22

DB_PATH = os.environ.get("DB_PATH", "alphabot_smc_fusion.db")
TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"

CHARTS_DIR = os.environ.get("CHARTS_DIR", "charts")
CHARTS_KEEP_LAST = 300  # nettoyage automatique — ne garde que les N dernières images

# --- Telegram : boutons interactifs -------------------------------------
# Secret optionnel utilisé pour vérifier l'origine des appels webhook Telegram
# (envoyé par Telegram dans le header X-Telegram-Bot-Api-Secret-Token si tu le
# configures lors de l'appel à setWebhook, cf. section DÉPLOIEMENT en bas).
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")

# --- Deriv WebSocket : flux temps réel -----------------------------------
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")
DERIV_RECONNECT_MIN_SECONDS = 2
DERIV_RECONNECT_MAX_SECONDS = 60
DERIV_WS_TIMEOUT_SECONDS = 15          # timeout socket (connexion + recv)
DERIV_CACHE_STALE_SECONDS = 900        # au-delà, le cache est jugé périmé (fallback one-shot)
DERIV_PING_INTERVAL_SECONDS = 20       # ping applicatif pour détecter les connexions mortes

# --- Watchdog VPS / Render ------------------------------------------------
WATCHDOG_CHECK_INTERVAL_SECONDS = 15
WATCHDOG_MAX_SILENCE_SECONDS = 180     # si aucun scan depuis ce délai -> considéré comme bloqué
WATCHDOG_RESTART_COOLDOWN_SECONDS = 30 # anti rage-restart

# --- TP2 intelligent SMC/ICT --------------------------------------------
# Poids de priorité utilisés uniquement comme repère en cas d'égalité de
# distance entre deux cibles candidates : un Order Block non mitigé est une
# empreinte institutionnelle plus fiable qu'un simple FVG, lui-même plus
# fiable qu'un pool de liquidité brut (cible "ultime" mais moins précise).
TP2_TARGET_PRIORITY = {"order_block": 3, "fvg": 2, "liquidity": 1}
TP2_TARGET_LABELS = {
    "order_block": "Order Block",
    "fvg": "Fair Value Gap",
    "liquidity_buy": "Liquidité (BSL)",
    "liquidity_sell": "Liquidité (SSL)",
}
OB_IMPULSE_ATR_MULTIPLIER = 1.3  # une bougie est jugée "impulsive" si son corps > ATR x ce facteur

os.makedirs("logs", exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/alphabot_smc.log")],
)
log = logging.getLogger("alphabot_smc")


# ============================================================================
# 2. PERSISTANCE SQLITE
# ============================================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    setup_key TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry_type TEXT NOT NULL,
    stars TEXT NOT NULL,
    score INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    stop_loss REAL NOT NULL,
    tp1 REAL NOT NULL,
    tp2 REAL,
    rr_tp1 REAL NOT NULL,
    rr_tp2 REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    telegram_group TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    closed_at REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_active_setup
    ON signals(setup_key)
    WHERE status NOT IN ('closed', 'invalidated', 'tp2_hit');

CREATE TABLE IF NOT EXISTS daily_counters (
    day TEXT NOT NULL,
    symbol TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, symbol)
);

CREATE TABLE IF NOT EXISTS report_log (
    report_type TEXT NOT NULL,
    period_key TEXT NOT NULL,
    sent_at REAL NOT NULL,
    PRIMARY KEY (report_type, period_key)
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    capital REAL NOT NULL,
    leverage REAL NOT NULL,
    risk_percent REAL NOT NULL,
    max_open_positions INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

-- Journal de toutes les actions déclenchées par les boutons Telegram
-- interactifs (✅ pris / ❌ ignoré / 🟡 BE / 🔒 sécurisé / 🔴 clôturé).
CREATE TABLE IF NOT EXISTS trade_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    previous_status TEXT,
    new_status TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'telegram',
    actor TEXT,
    created_at REAL NOT NULL,
    FOREIGN KEY (signal_id) REFERENCES signals(id)
);

CREATE INDEX IF NOT EXISTS idx_trade_actions_signal ON trade_actions(signal_id);

-- Table à ligne unique (id=1) utilisée par le watchdog pour suivre l'état de
-- vie de la boucle de scan (heartbeat) et le nombre de redémarrages forcés.
CREATE TABLE IF NOT EXISTS watchdog_heartbeat (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_scan_at REAL,
    last_heartbeat_at REAL,
    restart_count INTEGER NOT NULL DEFAULT 0,
    last_restart_at REAL,
    last_restart_reason TEXT
);
"""


def _migrate_schema():
    """Ajoute les colonnes introduites après la première mise en prod, sans
    jamais toucher aux données existantes (ALTER TABLE best-effort)."""
    with get_conn() as conn:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(signals)")}
        if "tp2_source" not in cols:
            conn.execute("ALTER TABLE signals ADD COLUMN tp2_source TEXT")

# ----------------------------------------------------------------------
# Paramètres modifiables depuis le dashboard (sans toucher au code).
# Stockés en SQLite (table app_settings, une ligne JSON), avec ces valeurs
# par défaut au premier démarrage.
# ----------------------------------------------------------------------
DEFAULT_SETTINGS = {
    "capital": 1000.0,                      # capital du compte ($)
    "leverage": 100,                        # levier (x)
    "risk_percent": 1.0,                    # risque par trade (%)
    "risk_mode": "standard",                # conservé pour compatibilité (voir table `profiles`)
    "active_profile_id": None,              # id du profil actuellement sélectionné (table `profiles`)
    "martingale_enabled": False,             # martingale ON/OFF
    "martingale_multiplier": 2.0,            # multiplicateur de risque après une perte
    "recovery_enabled": False,               # mode recovery ON/OFF
    "recovery_max_multiplier": 1.5,          # plafond du multiplicateur de risque en recovery
    "max_open_positions": 3,                 # nombre maximum de positions ouvertes simultanées
    "min_score_to_publish": MIN_SCORE_TO_PUBLISH,  # score minimum pour publier un signal
}

_SETTINGS_KEY = "config"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
    _migrate_schema()
    _seed_default_settings()
    _seed_default_profiles()
    _seed_watchdog_heartbeat()


def _seed_watchdog_heartbeat():
    now = time.time()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO watchdog_heartbeat (id, last_scan_at, last_heartbeat_at, restart_count)
               VALUES (1, ?, ?, 0)
               ON CONFLICT(id) DO NOTHING""",
            (now, now),
        )


def _seed_default_settings():
    """Insère les réglages par défaut au tout premier démarrage uniquement
    (n'écrase jamais des réglages déjà personnalisés depuis le dashboard)."""
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (_SETTINGS_KEY,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
                (_SETTINGS_KEY, json.dumps(DEFAULT_SETTINGS), time.time()),
            )


def get_settings() -> Dict:
    """Retourne les réglages actuels (fusionnés avec les défauts, pour rester
    compatible si de nouvelles clés sont ajoutées après une mise à jour)."""
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (_SETTINGS_KEY,)).fetchone()
    settings = dict(DEFAULT_SETTINGS)
    if row:
        try:
            settings.update(json.loads(row["value"]))
        except (json.JSONDecodeError, TypeError):
            log.error("app_settings corrompu, retour aux valeurs par défaut.")
    return settings


def update_settings(patch: Dict) -> Dict:
    """Met à jour uniquement les clés fournies (validation légère des types),
    persiste, et retourne les réglages complets à jour."""
    current = get_settings()

    validators = {
        "capital": (float, lambda v: v > 0),
        "leverage": (float, lambda v: v > 0),
        "risk_percent": (float, lambda v: 0 < v <= 100),
        "risk_mode": (str, lambda v: True),  # champ hérité, remplacé par le système de profils (table `profiles`)
        "martingale_enabled": (bool, lambda v: True),
        "martingale_multiplier": (float, lambda v: v >= 1),
        "recovery_enabled": (bool, lambda v: True),
        "recovery_max_multiplier": (float, lambda v: v >= 1),
        "max_open_positions": (int, lambda v: v >= 1),
        "min_score_to_publish": (int, lambda v: 0 <= v <= 100),
    }

    for key, raw_value in patch.items():
        if key not in validators:
            continue  # clé inconnue -> ignorée silencieusement (pas d'injection de champs arbitraires)
        cast, check = validators[key]
        try:
            value = cast(raw_value)
        except (TypeError, ValueError):
            raise ValueError(f"Valeur invalide pour '{key}': {raw_value!r}")
        if not check(value):
            raise ValueError(f"Valeur hors limites pour '{key}': {raw_value!r}")
        current[key] = value

    with get_conn() as conn:
        conn.execute(
            "UPDATE app_settings SET value=?, updated_at=? WHERE key=?",
            (json.dumps(current), time.time(), _SETTINGS_KEY),
        )
    return current


def _set_active_profile_id(profile_id: Optional[int]):
    current = get_settings()
    current["active_profile_id"] = profile_id
    with get_conn() as conn:
        conn.execute(
            "UPDATE app_settings SET value=?, updated_at=? WHERE key=?",
            (json.dumps(current), time.time(), _SETTINGS_KEY),
        )


# ----------------------------------------------------------------------
# 2bis. SYSTÈME DE PROFILS (sauvegardés en SQLite, gérables sans toucher au code)
# ----------------------------------------------------------------------
DEFAULT_PROFILES = [
    # (nom, capital, levier, risque %, positions max)
    ("Scalping",     1000.0, 200, 2.0, 5),
    ("Standard",     1000.0, 100, 1.0, 3),
    ("Conservative", 1000.0,  50, 0.5, 2),
]


def _seed_default_profiles():
    """Crée les 3 profils de départ (Scalping / Standard / Conservative)
    uniquement s'il n'existe encore aucun profil, et active 'Standard'."""
    with get_conn() as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM profiles").fetchone()["n"]
        if count > 0:
            return
        now = time.time()
        standard_id = None
        for name, capital, leverage, risk_percent, max_pos in DEFAULT_PROFILES:
            cur = conn.execute(
                "INSERT INTO profiles (name, capital, leverage, risk_percent, "
                "max_open_positions, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, capital, leverage, risk_percent, max_pos, now, now),
            )
            if name == "Standard":
                standard_id = cur.lastrowid
    if standard_id:
        _set_active_profile_id(standard_id)


def list_profiles() -> List[Dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM profiles ORDER BY id ASC").fetchall()
    active_id = get_settings().get("active_profile_id")
    return [dict(r, active=(r["id"] == active_id)) for r in rows]


def get_profile(profile_id: int) -> Optional[Dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM profiles WHERE id=?", (profile_id,)).fetchone()
    return dict(row) if row else None


def create_profile(name: str, capital: float, leverage: float,
                    risk_percent: float, max_open_positions: int) -> Dict:
    if not name or not name.strip():
        raise ValueError("Le nom du profil est obligatoire.")
    if capital <= 0 or leverage <= 0 or not (0 < risk_percent <= 100) or max_open_positions < 1:
        raise ValueError("Paramètres de profil invalides.")
    now = time.time()
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO profiles (name, capital, leverage, risk_percent, "
                "max_open_positions, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name.strip(), capital, leverage, risk_percent, max_open_positions, now, now),
            )
        except sqlite3.IntegrityError:
            raise ValueError(f"Un profil nommé '{name}' existe déjà.")
    return get_profile(cur.lastrowid)


def update_profile(profile_id: int, patch: Dict) -> Dict:
    existing = get_profile(profile_id)
    if not existing:
        raise ValueError("Profil introuvable.")
    fields = {
        "name": str, "capital": float, "leverage": float,
        "risk_percent": float, "max_open_positions": int,
    }
    updates = dict(existing)
    for key, cast in fields.items():
        if key in patch:
            updates[key] = cast(patch[key])
    if updates["capital"] <= 0 or updates["leverage"] <= 0 \
            or not (0 < updates["risk_percent"] <= 100) or updates["max_open_positions"] < 1:
        raise ValueError("Paramètres de profil invalides.")
    with get_conn() as conn:
        try:
            conn.execute(
                "UPDATE profiles SET name=?, capital=?, leverage=?, risk_percent=?, "
                "max_open_positions=?, updated_at=? WHERE id=?",
                (updates["name"], updates["capital"], updates["leverage"], updates["risk_percent"],
                 updates["max_open_positions"], time.time(), profile_id),
            )
        except sqlite3.IntegrityError:
            raise ValueError(f"Un profil nommé '{updates['name']}' existe déjà.")
    # Si le profil modifié est actif, on répercute immédiatement ses valeurs
    # sur les réglages en cours (capital/levier/risque/positions max).
    if get_settings().get("active_profile_id") == profile_id:
        activate_profile(profile_id)
    return get_profile(profile_id)


def delete_profile(profile_id: int):
    settings = get_settings()
    if settings.get("active_profile_id") == profile_id:
        raise ValueError("Impossible de supprimer le profil actif — active un autre profil d'abord.")
    with get_conn() as conn:
        conn.execute("DELETE FROM profiles WHERE id=?", (profile_id,))


def activate_profile(profile_id: int) -> Dict:
    """Sélectionne un profil comme actif ET applique immédiatement ses valeurs
    (capital, levier, risque %, positions max) aux réglages utilisés par le
    bot — aucune modification de code nécessaire."""
    p = get_profile(profile_id)
    if not p:
        raise ValueError("Profil introuvable.")
    update_settings({
        "capital": p["capital"],
        "leverage": p["leverage"],
        "risk_percent": p["risk_percent"],
        "max_open_positions": p["max_open_positions"],
    })
    _set_active_profile_id(profile_id)
    return get_settings()


def has_active_setup(setup_key: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM signals WHERE setup_key = ? "
            "AND status NOT IN ('closed', 'invalidated', 'tp2_hit') LIMIT 1",
            (setup_key,),
        ).fetchone()
        return row is not None


def seconds_since_last_signal(symbol: str) -> Optional[float]:
    """Ancienneté (en secondes) du dernier signal publié pour cet actif, quel
    que soit son statut (y compris déjà clôturé). Retourne None si aucun
    signal n'a jamais été publié pour cet actif."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT created_at FROM signals WHERE symbol = ? ORDER BY created_at DESC LIMIT 1",
            (symbol,),
        ).fetchone()
    if not row:
        return None
    return time.time() - row["created_at"]


def insert_signal(symbol, setup_key, direction, entry_type, stars, score,
                   entry_price, stop_loss, tp1, tp2, rr_tp1, rr_tp2, telegram_group,
                   tp2_source=None) -> int:
    now = time.time()
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO signals
               (symbol, setup_key, direction, entry_type, stars, score,
                entry_price, stop_loss, tp1, tp2, rr_tp1, rr_tp2, status,
                telegram_group, created_at, updated_at, tp2_source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'pending', ?, ?, ?, ?)""",
            (symbol, setup_key, direction, entry_type, stars, score,
             entry_price, stop_loss, tp1, tp2, rr_tp1, rr_tp2, telegram_group, now, now, tp2_source),
        )
        return cur.lastrowid


def update_status(signal_id: int, status: str):
    now = time.time()
    closed = now if status in ("closed", "invalidated", "tp2_hit") else None
    with get_conn() as conn:
        conn.execute(
            "UPDATE signals SET status=?, updated_at=?, closed_at=COALESCE(?, closed_at) WHERE id=?",
            (status, now, closed, signal_id),
        )


def get_signal(signal_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM signals WHERE id=?", (signal_id,)).fetchone()


def record_trade_action(signal_id: int, action: str, new_status: str,
                         source: str = "telegram", actor: Optional[str] = None) -> int:
    """Enregistre une action déclenchée par un bouton Telegram (ou l'API) dans
    trade_actions, met à jour le statut du signal, et retourne l'id de l'action.
    Les statistiques (get_stats / get_period_stats / ...) sont calculées à la
    volée depuis `signals`, donc elles reflètent immédiatement ce changement."""
    row = get_signal(signal_id)
    previous_status = row["status"] if row else None
    now = time.time()
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO trade_actions
               (signal_id, action, previous_status, new_status, source, actor, created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (signal_id, action, previous_status, new_status, source, actor, now),
        )
        action_id = cur.lastrowid
    update_status(signal_id, new_status)
    return action_id


def get_trade_actions(signal_id: int) -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM trade_actions WHERE signal_id=? ORDER BY created_at ASC",
            (signal_id,),
        ).fetchall()


def get_stats():
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) c FROM signals").fetchone()["c"]
        by_status = conn.execute("SELECT status, COUNT(*) c FROM signals GROUP BY status").fetchall()
        by_symbol = conn.execute("SELECT symbol, COUNT(*) c FROM signals GROUP BY symbol").fetchall()
        return {
            "total": total,
            "by_status": {r["status"]: r["c"] for r in by_status},
            "by_symbol": {r["symbol"]: r["c"] for r in by_symbol},
        }


# ----------------------------------------------------------------------
# Statistiques par période (pour les rapports auto)
# ----------------------------------------------------------------------

# Statuts considérés comme gain / perte / neutre pour le calcul du résultat
# en multiples de R (le SQLite ne stocke pas de solde/lot par trade, donc on
# raisonne en R — cohérent avec le RR affiché dans chaque signal).
WIN_STATUSES = {"tp1_hit", "tp2_hit", "secured"}
LOSS_STATUSES = {"invalidated"}
BE_STATUSES = {"be"}


def _r_result(row: sqlite3.Row) -> Optional[float]:
    status = row["status"]
    if status == "tp2_hit" and row["rr_tp2"]:
        return float(row["rr_tp2"])
    if status in ("tp1_hit", "secured"):
        return float(row["rr_tp1"])
    if status in BE_STATUSES:
        return 0.0
    if status in LOSS_STATUSES:
        return -1.0
    return None  # pending / taken / ignored / closed -> exclu du calcul R


def get_period_stats(start_ts: float, end_ts: float) -> Dict:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM signals WHERE created_at >= ? AND created_at < ? "
            "ORDER BY created_at ASC",
            (start_ts, end_ts),
        ).fetchall()

    total = len(rows)
    wins = losses = be = 0
    best = worst = None
    by_symbol: Dict[str, Dict] = {}
    cum, peak, max_dd = 0.0, 0.0, 0.0
    total_r = 0.0

    for row in rows:
        sym_stats = by_symbol.setdefault(row["symbol"], {"total": 0, "wins": 0, "losses": 0})
        sym_stats["total"] += 1

        r = _r_result(row)
        if r is None:
            continue

        total_r += r
        cum += r
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

        if r > 0:
            wins += 1
            sym_stats["wins"] += 1
        elif r < 0:
            losses += 1
            sym_stats["losses"] += 1
        else:
            be += 1

        entry = {"r": r, "symbol": row["symbol"], "direction": row["direction"]}
        if best is None or r > best["r"]:
            best = entry
        if worst is None or r < worst["r"]:
            worst = entry

    decided = wins + losses
    win_rate = (wins / decided * 100) if decided else 0.0

    return {
        "total_signals": total, "wins": wins, "losses": losses, "be": be,
        "win_rate": win_rate, "total_r": total_r, "max_drawdown_r": max_dd,
        "best": best, "worst": worst, "by_symbol": by_symbol,
    }


def get_all_time_stats() -> Dict:
    """Stats sur toute l'historique (pas de bornes de date)."""
    return get_period_stats(0, time.time() + 1)


def get_dashboard_overview() -> Dict:
    """Agrège réglages + stats globales + équivalent $ pour les cartes du
    dashboard (capital actuel, profits, pertes, drawdown, winrate)."""
    settings = get_settings()
    overall = get_all_time_stats()
    capital = settings["capital"]
    risk_percent = settings["risk_percent"]

    # NB : le $ par trade est estimé avec le risque par trade ACTUEL des
    # réglages (le risque effectif historique par trade n'est pas persisté
    # en base) — cohérent avec l'approche "raisonnement en R" déjà en place.
    dollar_per_r = capital * (risk_percent / 100.0)
    total_pnl = overall["total_r"] * dollar_per_r
    max_drawdown_dollar = overall["max_drawdown_r"] * dollar_per_r
    current_capital = capital + total_pnl

    return {
        "settings": settings,
        "capital_initial": capital,
        "capital_actuel": round(current_capital, 2),
        "profit_net_total": round(total_pnl, 2),
        "drawdown_max": round(max_drawdown_dollar, 2),
        "winrate": round(overall["win_rate"], 1),
        "total_signals": overall["total_signals"],
        "wins": overall["wins"],
        "losses": overall["losses"],
        "be": overall["be"],
        "total_r": round(overall["total_r"], 2),
        "open_positions": count_open_positions(),
    }


def get_stats_by_asset() -> Dict[str, Dict]:
    """Statistiques détaillées par actif : total, winrate, R cumulé, moyenne R."""
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM signals").fetchall()
    by_symbol: Dict[str, Dict] = {sym: {"total": 0, "wins": 0, "losses": 0, "be": 0, "total_r": 0.0}
                                   for sym in ASSETS}
    for row in rows:
        sym = row["symbol"]
        if sym not in by_symbol:
            by_symbol[sym] = {"total": 0, "wins": 0, "losses": 0, "be": 0, "total_r": 0.0}
        by_symbol[sym]["total"] += 1
        r = _r_result(row)
        if r is None:
            continue
        by_symbol[sym]["total_r"] += r
        if r > 0:
            by_symbol[sym]["wins"] += 1
        elif r < 0:
            by_symbol[sym]["losses"] += 1
        else:
            by_symbol[sym]["be"] += 1

    for sym, s in by_symbol.items():
        decided = s["wins"] + s["losses"]
        s["win_rate"] = round((s["wins"] / decided * 100) if decided else 0.0, 1)
        s["total_r"] = round(s["total_r"], 2)
        s["display_name"] = ASSETS[sym].display_name if sym in ASSETS else sym
    return by_symbol


def get_monthly_performance(n_months: int = 6) -> List[Dict]:
    """Performance des `n_months` derniers mois (le mois courant inclus, en
    dernier dans la liste)."""
    now = datetime.now(timezone.utc)
    months = []
    for i in range(n_months - 1, -1, -1):
        year = now.year
        month = now.month - i
        while month <= 0:
            month += 12
            year -= 1
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
        stats = get_period_stats(start.timestamp(), end.timestamp())
        months.append({
            "period_key": f"{year}-{month:02d}",
            "total_signals": stats["total_signals"],
            "wins": stats["wins"], "losses": stats["losses"], "be": stats["be"],
            "win_rate": round(stats["win_rate"], 1),
            "total_r": round(stats["total_r"], 2),
        })
    return months


def get_trade_history(limit: int = 100, symbol: Optional[str] = None,
                       status: Optional[str] = None) -> List[Dict]:
    query = "SELECT * FROM signals WHERE 1=1"
    params: List = []
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def _day_bounds(now: datetime):
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.timestamp(), now.timestamp()


def _week_bounds(now: datetime):
    start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    iso_year, iso_week, _ = now.isocalendar()
    return start.timestamp(), now.timestamp(), f"{iso_year}-W{iso_week:02d}"


def _month_bounds(now: datetime):
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start.timestamp(), now.timestamp()


def _is_last_day_of_month(now: datetime) -> bool:
    return (now + timedelta(days=1)).month != now.month


def format_daily_report(stats: Dict, day_key: str) -> str:
    lines = [
        "📅 *RAPPORT JOURNALIER*", f"🗓 {day_key}", "",
        f"📊 Signaux publiés : {stats['total_signals']}",
        f"✅ Gagnants : {stats['wins']} · ❌ Perdants : {stats['losses']} · ⚪ BE : {stats['be']}",
        f"🎯 Taux de réussite (winrate) : {stats['win_rate']:.1f}%",
        f"💰 Profit net : {stats['total_r']:+.2f} R",
        f"📉 Drawdown : {stats['max_drawdown_r']:.2f} R",
    ]
    if stats["best"]:
        b = stats["best"]
        lines.append(f"🏆 Meilleur trade : {b['symbol']} {b['direction']} ({b['r']:+.2f} R)")
    if stats["worst"]:
        w = stats["worst"]
        lines.append(f"💀 Pire trade : {w['symbol']} {w['direction']} ({w['r']:+.2f} R)")
    return "\n".join(lines)


def format_weekly_report(stats: Dict, period_key: str) -> str:
    lines = [
        "🗓 *RAPPORT HEBDOMADAIRE*", f"Semaine {period_key}", "",
        f"📊 Signaux publiés : {stats['total_signals']}",
        f"✅ Gagnants : {stats['wins']} · ❌ Perdants : {stats['losses']} · ⚪ BE : {stats['be']}",
        f"🎯 Taux de réussite : {stats['win_rate']:.1f}%",
        f"💰 Résultat net : {stats['total_r']:+.2f} R",
        f"📉 Drawdown max : {stats['max_drawdown_r']:.2f} R", "",
        "*Par actif :*",
    ]
    for symbol, s in stats["by_symbol"].items():
        lines.append(f"  • {symbol} : {s['total']} signaux ({s['wins']}✅ / {s['losses']}❌)")
    if stats["best"]:
        b = stats["best"]
        lines.append(f"🏆 Meilleur trade : {b['symbol']} {b['direction']} ({b['r']:+.2f} R)")
    if stats["worst"]:
        w = stats["worst"]
        lines.append(f"💀 Pire trade : {w['symbol']} {w['direction']} ({w['r']:+.2f} R)")
    return "\n".join(lines)


def format_monthly_report(stats: Dict, period_key: str) -> str:
    lines = [
        "📆 *BILAN MENSUEL*", f"{period_key}", "",
        f"📊 Signaux publiés : {stats['total_signals']}",
        f"✅ Gagnants : {stats['wins']} · ❌ Perdants : {stats['losses']} · ⚪ BE : {stats['be']}",
        f"🎯 Taux de réussite : {stats['win_rate']:.1f}%",
        f"💰 Profit net cumulé : {stats['total_r']:+.2f} R",
        f"📉 Drawdown maximal : {stats['max_drawdown_r']:.2f} R", "",
        "*Performance par actif :*",
    ]
    for symbol, s in stats["by_symbol"].items():
        lines.append(f"  • {symbol} : {s['total']} signaux ({s['wins']}✅ / {s['losses']}❌)")
    if stats["best"]:
        b = stats["best"]
        lines.append(f"🏆 Meilleur trade du mois : {b['symbol']} {b['direction']} ({b['r']:+.2f} R)")
    if stats["worst"]:
        w = stats["worst"]
        lines.append(f"💀 Pire trade du mois : {w['symbol']} {w['direction']} ({w['r']:+.2f} R)")
    return "\n".join(lines)


def _try_claim_report(report_type: str, period_key: str) -> bool:
    """Retourne True si ce rapport n'a pas encore été envoyé pour cette période
    (et l'enregistre comme envoyé), False s'il l'a déjà été -> évite les doublons."""
    with get_conn() as conn:
        try:
            conn.execute(
                "INSERT INTO report_log (report_type, period_key, sent_at) VALUES (?, ?, ?)",
                (report_type, period_key, time.time()),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def _dispatch_report(message: str):
    """Envoie sur TG_CHAT_REPORTS si configuré, sinon sur les deux groupes de signaux."""
    reports_env = TELEGRAM_GROUPS["reports"]["chat_id_env"]
    reports_chat_id = os.environ.get(reports_env)
    if reports_chat_id:
        try:
            token = _bot_token("reports")
            url = TELEGRAM_API.format(token=token, method="sendMessage")
            resp = requests.post(
                url, data={"chat_id": reports_chat_id, "text": message, "parse_mode": "Markdown"},
                timeout=15,
            )
            resp.raise_for_status()
            return
        except Exception:
            log.error(f"Échec d'envoi du rapport sur {reports_env}:\n{traceback.format_exc()}")
    for group in ("btc_gold", "deriv"):
        try:
            send_telegram_signal(group, message)
        except Exception:
            log.error(f"Échec d'envoi du rapport sur le groupe '{group}':\n{traceback.format_exc()}")


def _maybe_send_daily_report(now: datetime):
    day_key = now.strftime("%Y-%m-%d")
    if not _try_claim_report("daily", day_key):
        return
    start_ts, end_ts = _day_bounds(now)
    stats = get_period_stats(start_ts, end_ts)
    _dispatch_report(format_daily_report(stats, day_key))
    log.info(f"Rapport journalier envoyé ({day_key}).")


def _maybe_send_weekly_report(now: datetime):
    start_ts, end_ts, period_key = _week_bounds(now)
    if not _try_claim_report("weekly", period_key):
        return
    stats = get_period_stats(start_ts, end_ts)
    _dispatch_report(format_weekly_report(stats, period_key))
    log.info(f"Rapport hebdomadaire envoyé ({period_key}).")


def _maybe_send_monthly_report(now: datetime):
    period_key = now.strftime("%Y-%m")
    if not _try_claim_report("monthly", period_key):
        return
    start_ts, end_ts = _month_bounds(now)
    stats = get_period_stats(start_ts, end_ts)
    _dispatch_report(format_monthly_report(stats, period_key))
    log.info(f"Rapport mensuel envoyé ({period_key}).")


def report_scheduler_loop():
    """Vérifie chaque minute si un rapport doit être déclenché. L'idempotence
    est garantie par report_log (via _try_claim_report), donc une vérification
    sur toute la minute (pas seulement minute==0 pile) reste sûre."""
    log.info("Planificateur de rapports démarré.")
    while True:
        try:
            now = datetime.now(timezone.utc)
            if now.hour == REPORT_DAILY_HOUR_UTC:
                _maybe_send_daily_report(now)
            if now.weekday() == 6 and now.hour == REPORT_WEEKLY_HOUR_UTC and now.minute >= REPORT_WEEKLY_MINUTE_UTC:
                _maybe_send_weekly_report(now)
            if _is_last_day_of_month(now) and now.hour == REPORT_MONTHLY_HOUR_UTC:
                _maybe_send_monthly_report(now)
        except Exception:
            log.error(f"Erreur planificateur de rapports:\n{traceback.format_exc()}")
        time.sleep(60)


def _today_key() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def can_publish_today(symbol: str) -> bool:
    day = _today_key()
    with get_conn() as conn:
        global_count = conn.execute(
            "SELECT COALESCE(SUM(count),0) c FROM daily_counters WHERE day=?", (day,)
        ).fetchone()["c"]
        if global_count >= MAX_SIGNALS_PER_DAY_GLOBAL:
            return False
        row = conn.execute(
            "SELECT count FROM daily_counters WHERE day=? AND symbol=?", (day, symbol)
        ).fetchone()
        symbol_count = row["count"] if row else 0
        return symbol_count < MAX_SIGNALS_PER_DAY_PER_ASSET


OPEN_STATUSES = ("pending", "taken", "be", "secured", "tp1_hit")


def count_open_positions() -> int:
    """Nombre de positions actuellement considérées comme 'ouvertes'
    (pas encore fermées/invalidées), utilisé pour plafonner via
    settings['max_open_positions']."""
    with get_conn() as conn:
        row = conn.execute(
            f"SELECT COUNT(*) c FROM signals WHERE status IN "
            f"({','.join('?' for _ in OPEN_STATUSES)})",
            OPEN_STATUSES,
        ).fetchone()
        return row["c"]


def last_closed_result(symbol: Optional[str] = None) -> Optional[float]:
    """Résultat en R du dernier trade clôturé (tous actifs, ou un actif
    précis si `symbol` est fourni). Utilisé pour le calcul martingale/recovery."""
    query = "SELECT * FROM signals WHERE status IN ('tp1_hit','tp2_hit','secured','invalidated','be')"
    params: List = []
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    query += " ORDER BY COALESCE(closed_at, updated_at) DESC LIMIT 1"
    with get_conn() as conn:
        row = conn.execute(query, params).fetchone()
    return _r_result(row) if row else None


def get_effective_risk_percent(symbol: Optional[str] = None, settings: Optional[Dict] = None) -> float:
    """Calcule le risque par trade (%) réellement appliqué, en tenant compte
    du mode risque de base, de la martingale (augmente après une perte) et
    du recovery (augmente progressivement tant que le compte est en drawdown).
    Les deux logiques ne se cumulent pas au-delà du plafond recovery_max_multiplier
    / martingale_multiplier, pour ne jamais s'emballer."""
    s = settings or get_settings()
    base_risk = s["risk_percent"]

    if s.get("martingale_enabled"):
        r = last_closed_result(symbol)
        if r is not None and r < 0:
            return round(base_risk * s.get("martingale_multiplier", 2.0), 4)

    if s.get("recovery_enabled"):
        now = datetime.now(timezone.utc)
        start_ts, end_ts = _month_bounds(now)
        month_stats = get_period_stats(start_ts, end_ts)
        if month_stats["total_r"] < 0:
            # Plus le drawdown du mois est marqué, plus on augmente le risque,
            # borné par recovery_max_multiplier.
            factor = min(1 + abs(month_stats["total_r"]) / 10.0, s.get("recovery_max_multiplier", 1.5))
            return round(base_risk * factor, 4)

    return base_risk


def increment_daily_counter(symbol: str):
    day = _today_key()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO daily_counters (day, symbol, count) VALUES (?, ?, 1)
               ON CONFLICT(day, symbol) DO UPDATE SET count = count + 1""",
            (day, symbol),
        )


def record_heartbeat(mark_scan: bool = True):
    """Appelé à chaque itération de la boucle de scan (et périodiquement par
    le watchdog) pour prouver que le processus est vivant."""
    now = time.time()
    with get_conn() as conn:
        if mark_scan:
            conn.execute(
                "UPDATE watchdog_heartbeat SET last_scan_at=?, last_heartbeat_at=? WHERE id=1",
                (now, now),
            )
        else:
            conn.execute(
                "UPDATE watchdog_heartbeat SET last_heartbeat_at=? WHERE id=1",
                (now,),
            )


def get_heartbeat_status() -> Dict:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM watchdog_heartbeat WHERE id=1").fetchone()
    now = time.time()
    if not row:
        return {"last_scan_at": None, "seconds_since_scan": None, "healthy": False,
                "restart_count": 0}
    last_scan = row["last_scan_at"]
    seconds_since = (now - last_scan) if last_scan else None
    return {
        "last_scan_at": last_scan,
        "last_heartbeat_at": row["last_heartbeat_at"],
        "seconds_since_scan": seconds_since,
        "healthy": seconds_since is not None and seconds_since < WATCHDOG_MAX_SILENCE_SECONDS,
        "restart_count": row["restart_count"],
        "last_restart_at": row["last_restart_at"],
        "last_restart_reason": row["last_restart_reason"],
    }


def record_watchdog_restart(reason: str):
    now = time.time()
    with get_conn() as conn:
        conn.execute(
            """UPDATE watchdog_heartbeat
               SET restart_count = restart_count + 1, last_restart_at=?, last_restart_reason=?
               WHERE id=1""",
            (now, reason),
        )


# ============================================================================
# 3. SOURCES DE DONNÉES
# ============================================================================

def fetch_candles(symbol: str, limit: int = 200) -> List[Dict]:
    asset = ASSETS[symbol]
    if asset.data_source == "yfinance":
        return _fetch_yfinance(symbol, limit)
    elif asset.data_source == "binance":
        return _fetch_binance(symbol, limit)
    elif asset.data_source == "deriv_ws":
        return _fetch_deriv(symbol, limit)
    raise ValueError(f"Source de données inconnue pour {symbol}")


def _fetch_yfinance(symbol: str, limit: int) -> List[Dict]:
    import yfinance as yf
    ticker_map = {"XAUUSD": "GC=F"}
    ticker = ticker_map.get(symbol, symbol)
    data = yf.download(ticker, period="2d", interval="5m", progress=False)
    candles = []
    for idx, row in data.tail(limit).iterrows():
        candles.append({
            "time": idx.timestamp(),
            "open": float(row["Open"]), "high": float(row["High"]),
            "low": float(row["Low"]), "close": float(row["Close"]),
        })
    return candles


def _fetch_binance(symbol: str, limit: int) -> List[Dict]:
    pair = "BTCUSDT"
    url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval=5m&limit={limit}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        raw = json.loads(resp.read().decode())
    return [{
        "time": k[0] / 1000, "open": float(k[1]), "high": float(k[2]),
        "low": float(k[3]), "close": float(k[4]),
    } for k in raw]


DERIV_SYMBOL_MAP = {"V75": "R_75", "V25": "R_25"}
DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id={app_id}"
DERIV_GRANULARITY_SECONDS = 300  # M5


def _deriv_app_id() -> str:
    # 1089 = App ID public de démo Deriv, utilisable tel quel pour des requêtes
    # en lecture seule (ticks_history). Remplace par ton propre App ID gratuit
    # (https://api.deriv.com -> "Register Application") pour la prod, via la
    # variable d'environnement DERIV_APP_ID.
    return DERIV_APP_ID


def _fetch_deriv_snapshot(symbol: str, limit: int = 200) -> List[Dict]:
    """Requête ponctuelle (one-shot) à l'API publique Deriv, ticks_history en
    style 'candles'. Utilisée comme filet de sécurité si le flux WebSocket
    persistant (DerivFeed) n'a pas encore de données fraîches en cache."""
    import websocket  # pip install websocket-client

    deriv_symbol = DERIV_SYMBOL_MAP.get(symbol)
    if not deriv_symbol:
        raise ValueError(f"Pas de mapping Deriv pour le symbole '{symbol}'.")

    url = DERIV_WS_URL.format(app_id=_deriv_app_id())
    payload = {
        "ticks_history": deriv_symbol,
        "adjust_start_time": 1,
        "count": limit,
        "end": "latest",
        "start": 1,
        "style": "candles",
        "granularity": DERIV_GRANULARITY_SECONDS,
    }

    ws = None
    try:
        ws = websocket.create_connection(url, timeout=DERIV_WS_TIMEOUT_SECONDS)
        ws.send(json.dumps(payload))
        raw = ws.recv()
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    data = json.loads(raw)
    if "error" in data:
        raise RuntimeError(
            f"Erreur API Deriv pour {symbol}: {data['error'].get('message', data['error'])}"
        )

    candles_raw = data.get("candles") or []
    return [{
        "time": float(c["epoch"]),
        "open": float(c["open"]),
        "high": float(c["high"]),
        "low": float(c["low"]),
        "close": float(c["close"]),
    } for c in candles_raw]


class DerivFeed:
    """Client WebSocket persistant pour l'API publique Deriv.

    Maintient une connexion permanente (avec reconnexion automatique, gestion
    des timeouts et des erreurs réseau) et s'abonne en continu (`subscribe: 1`)
    aux bougies M5 de Volatility 75 (R_75) et Volatility 25 (R_25). Les
    dernières bougies reçues sont mises en cache en mémoire (thread-safe) pour
    que `fetch_candles()` n'ait jamais besoin d'attendre un aller-retour
    réseau pendant un scan.

    Robustesse :
      - reconnexion automatique avec backoff exponentiel borné
        (DERIV_RECONNECT_MIN/MAX_SECONDS) ;
      - timeout de connexion/réception (DERIV_WS_TIMEOUT_SECONDS) ;
      - ping applicatif régulier pour détecter une socket morte ;
      - toute exception réseau est capturée -> log + reconnexion, jamais de
        crash du thread ;
      - expose un heartbeat (`last_message_at`) consommé par le watchdog.
    """

    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self._lock = threading.Lock()
        self._cache: Dict[str, Dict] = {
            sym: {"candles": [], "updated_at": 0.0} for sym in symbols
        }
        self._ws = None
        self._connected = False
        self._last_message_at = 0.0
        self._stop = False
        self._thread: Optional[threading.Thread] = None

    # -- API publique --------------------------------------------------
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._run_forever, daemon=True,
                                         name="deriv-ws-feed")
        self._thread.start()
        log.info("DerivFeed : thread WebSocket démarré pour %s.", self.symbols)

    def stop(self):
        self._stop = True
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass

    def is_connected(self) -> bool:
        return self._connected

    def seconds_since_last_message(self) -> Optional[float]:
        if not self._last_message_at:
            return None
        return time.time() - self._last_message_at

    def get_candles(self, symbol: str, limit: int = 200) -> Optional[List[Dict]]:
        """Retourne les bougies en cache si elles sont assez fraîches, sinon None
        (l'appelant doit alors utiliser le fallback one-shot)."""
        with self._lock:
            entry = self._cache.get(symbol)
        if not entry or not entry["candles"]:
            return None
        if time.time() - entry["updated_at"] > DERIV_CACHE_STALE_SECONDS:
            return None
        return entry["candles"][-limit:]

    # -- Boucle interne --------------------------------------------------
    def _run_forever(self):
        import websocket  # pip install websocket-client
        backoff = DERIV_RECONNECT_MIN_SECONDS
        while not self._stop:
            try:
                url = DERIV_WS_URL.format(app_id=_deriv_app_id())
                ws = websocket.create_connection(url, timeout=DERIV_WS_TIMEOUT_SECONDS)
                self._ws = ws
                self._connected = True
                self._last_message_at = time.time()
                backoff = DERIV_RECONNECT_MIN_SECONDS  # connexion OK -> reset du backoff
                log.info("DerivFeed : connecté à %s.", url)

                for sym in self.symbols:
                    deriv_symbol = DERIV_SYMBOL_MAP.get(sym)
                    if not deriv_symbol:
                        continue
                    ws.send(json.dumps({
                        "ticks_history": deriv_symbol,
                        "adjust_start_time": 1,
                        "count": 200,
                        "end": "latest",
                        "start": 1,
                        "style": "candles",
                        "granularity": DERIV_GRANULARITY_SECONDS,
                        "subscribe": 1,
                        "req_id": self.symbols.index(sym) + 1,
                    }))

                last_ping = time.time()
                while not self._stop:
                    if time.time() - last_ping > DERIV_PING_INTERVAL_SECONDS:
                        try:
                            ws.ping()
                        except Exception:
                            raise ConnectionError("échec du ping Deriv, reconnexion nécessaire.")
                        last_ping = time.time()

                    try:
                        raw = ws.recv()
                    except Exception as exc:
                        # Timeout, coupure réseau, socket fermée... : on
                        # considère la socket potentiellement morte -> on la
                        # recycle proprement pour forcer une reconnexion.
                        raise ConnectionError(f"timeout/erreur de réception Deriv : {exc}")

                    if not raw:
                        continue
                    self._last_message_at = time.time()
                    self._handle_message(raw)

            except Exception as exc:
                self._connected = False
                log.warning(f"DerivFeed : connexion perdue/erreur ({exc}). "
                            f"Reconnexion dans {backoff:.0f}s.")
            finally:
                try:
                    if self._ws is not None:
                        self._ws.close()
                except Exception:
                    pass
                self._ws = None
                self._connected = False

            if self._stop:
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, DERIV_RECONNECT_MAX_SECONDS)

        log.info("DerivFeed : thread arrêté proprement.")

    def _handle_message(self, raw: str):
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return

        if data.get("msg_type") == "error" or "error" in data:
            err = data.get("error", {})
            log.warning(f"DerivFeed : message d'erreur reçu ({err.get('message', err)}).")
            return

        req_id = data.get("echo_req", {}).get("req_id")
        deriv_symbol = data.get("echo_req", {}).get("ticks_history")
        symbol = next((s for s, d in DERIV_SYMBOL_MAP.items() if d == deriv_symbol), None)
        if symbol is None and req_id:
            idx = req_id - 1
            if 0 <= idx < len(self.symbols):
                symbol = self.symbols[idx]
        if symbol is None:
            return

        # Snapshot initial (history) : liste de bougies.
        candles_raw = data.get("candles")
        if candles_raw:
            candles = [{
                "time": float(c["epoch"]), "open": float(c["open"]),
                "high": float(c["high"]), "low": float(c["low"]), "close": float(c["close"]),
            } for c in candles_raw]
            with self._lock:
                self._cache[symbol] = {"candles": candles, "updated_at": time.time()}
            return

        # Mise à jour incrémentale (ohlc) envoyée en continu grâce à subscribe:1.
        ohlc = data.get("ohlc")
        if ohlc:
            try:
                new_candle = {
                    "time": float(ohlc["open_time"]),
                    "open": float(ohlc["open"]), "high": float(ohlc["high"]),
                    "low": float(ohlc["low"]), "close": float(ohlc["close"]),
                }
            except (KeyError, TypeError, ValueError):
                return
            with self._lock:
                entry = self._cache.setdefault(symbol, {"candles": [], "updated_at": 0.0})
                candles = entry["candles"]
                if candles and candles[-1]["time"] == new_candle["time"]:
                    candles[-1] = new_candle
                else:
                    candles.append(new_candle)
                    if len(candles) > 500:
                        del candles[: len(candles) - 500]
                entry["updated_at"] = time.time()


# Instance unique partagée par tout le process (démarrée dans le bloc __main__).
deriv_feed = DerivFeed(list(DERIV_SYMBOL_MAP.keys()))


def _fetch_deriv(symbol: str, limit: int = 200) -> List[Dict]:
    """Point d'entrée utilisé par fetch_candles(). Sert en priorité les
    bougies en cache du flux WebSocket persistant (rapide, pas d'aller-retour
    réseau bloquant) ; si le cache est absent/périmé (flux pas encore connecté,
    coupure réseau prolongée...), retombe sur une requête one-shot classique,
    elle-même protégée par un try/except au niveau de process_asset()."""
    cached = deriv_feed.get_candles(symbol, limit)
    if cached:
        return cached
    log.info(f"[{symbol}] cache Deriv WS indisponible/périmé -> requête one-shot de secours.")
    return _fetch_deriv_snapshot(symbol, limit)


# ============================================================================
# 4. STRUCTURE : SWING POINTS, LIQUIDITÉ, SWEEP
# ============================================================================

@dataclass
class SwingPoint:
    index: int
    price: float
    kind: str          # "high" | "low"
    mitigated: bool = False


@dataclass
class LiquiditySweep:
    swept_point: SwingPoint
    sweep_index: int
    sweep_wick_price: float
    direction: str      # "bullish" | "bearish"


def find_swing_points(candles: List[Dict], lookback: int = 3) -> List[SwingPoint]:
    points = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback:i + lookback + 1]
        high_i, low_i = candles[i]["high"], candles[i]["low"]
        if high_i == max(c["high"] for c in window):
            points.append(SwingPoint(index=i, price=high_i, kind="high"))
        if low_i == min(c["low"] for c in window):
            points.append(SwingPoint(index=i, price=low_i, kind="low"))
    return points


def detect_liquidity_sweep(
    candles: List[Dict], swing_points: List[SwingPoint], search_window: int = 8
) -> Optional[LiquiditySweep]:
    """Cherche un sweep sur les `search_window` dernières bougies (pas seulement
    la toute dernière — nécessaire en scan continu pour ne pas perdre le setup
    le temps que la BOS se confirme sur les bougies suivantes)."""
    if not candles:
        return None
    n = len(candles)
    start_scan = max(0, n - search_window)

    for idx in range(n - 1, start_scan - 1, -1):
        candle = candles[idx]
        body_high = max(candle["open"], candle["close"])
        body_low = min(candle["open"], candle["close"])
        candidates = sorted(
            [p for p in swing_points if not p.mitigated and p.index < idx],
            key=lambda p: -p.index,
        )
        for point in candidates:
            if point.kind == "high" and candle["high"] > point.price and body_high <= point.price:
                return LiquiditySweep(point, idx, candle["high"], "bearish")
            if point.kind == "low" and candle["low"] < point.price and body_low >= point.price:
                return LiquiditySweep(point, idx, candle["low"], "bullish")
    return None


# ============================================================================
# 5. BOS / CHoCH + FVG
# ============================================================================

@dataclass
class BOSConfirmation:
    confirmed: bool
    break_index: int
    break_level: float
    direction: str


@dataclass
class FVGZone:
    top: float
    bottom: float
    index: int
    direction: str
    mitigated: bool = False


def _find_structure_level(swing_points: List[SwingPoint], sweep: LiquiditySweep) -> Optional[float]:
    """Le niveau à casser pour valider le BOS = dernier swing OPPOSÉ avant le sweep
    (pas le niveau balayé lui-même, qui serait trivialement toujours franchi)."""
    opposite_kind = "high" if sweep.direction == "bullish" else "low"
    prior_points = [p for p in swing_points if p.kind == opposite_kind and p.index < sweep.sweep_index]
    if not prior_points:
        return None
    return max(prior_points, key=lambda p: p.index).price


def confirm_bos(
    candles: List[Dict], sweep: LiquiditySweep, swing_points: List[SwingPoint], max_lookahead: int = 6
) -> Optional[BOSConfirmation]:
    """RÈGLE STRICTE : validée uniquement par clôture du CORPS au-delà du niveau.
    Une mèche seule ne valide jamais -> signal ignoré."""
    level = _find_structure_level(swing_points, sweep)
    if level is None:
        return None
    start = sweep.sweep_index + 1
    end = min(len(candles), start + max_lookahead)
    for i in range(start, end):
        c = candles[i]
        if sweep.direction == "bullish" and c["close"] > level and c["close"] > c["open"]:
            return BOSConfirmation(True, i, level, "bullish")
        if sweep.direction == "bearish" and c["close"] < level and c["close"] < c["open"]:
            return BOSConfirmation(True, i, level, "bearish")
    return None


def detect_fvg(candles: List[Dict], around_index: int, direction: str) -> Optional[FVGZone]:
    """FVG sur la séquence à 3 bougies de la cassure (méthode ICT)."""
    i = around_index
    if i < 2 or i >= len(candles):
        return None
    c0, c2 = candles[i - 2], candles[i]
    if direction == "bullish" and c2["low"] > c0["high"]:
        return FVGZone(top=c2["low"], bottom=c0["high"], index=i, direction="bullish")
    if direction == "bearish" and c2["high"] < c0["low"]:
        return FVGZone(top=c0["low"], bottom=c2["high"], index=i, direction="bearish")
    return None


def scan_all_fvgs(candles: List[Dict]) -> List[FVGZone]:
    """Scanne TOUTES les bougies (pas seulement autour de la cassure) pour
    lister les FVG (Fair Value Gap / imbalance) présentes sur la fenêtre, avec
    leur statut de mitigation (comblée ou non par le prix depuis sa formation)."""
    zones: List[FVGZone] = []
    n = len(candles)
    for i in range(2, n):
        c0, c2 = candles[i - 2], candles[i]
        if c2["low"] > c0["high"]:
            zones.append(FVGZone(top=c2["low"], bottom=c0["high"], index=i, direction="bullish"))
        elif c2["high"] < c0["low"]:
            zones.append(FVGZone(top=c0["low"], bottom=c2["high"], index=i, direction="bearish"))
    for z in zones:
        for c in candles[z.index + 1:]:
            if c["low"] <= z.top and c["high"] >= z.bottom:
                z.mitigated = True
                break
    return zones


# ============================================================================
# 5bis. ORDER BLOCKS (pour la sélection intelligente de TP2)
# ============================================================================

@dataclass
class OBZone:
    top: float
    bottom: float
    index: int
    direction: str      # "bullish" (support) | "bearish" (résistance)
    mitigated: bool = False


def find_order_blocks(candles: List[Dict]) -> List[OBZone]:
    """Order Block ICT simplifié : dernière bougie de couleur opposée juste
    avant un déplacement impulsif (corps > ATR x facteur). Bullish OB = zone
    de support (dernière bougie baissière avant une impulsion haussière) ;
    Bearish OB = zone de résistance (dernière bougie haussière avant une
    impulsion baissière)."""
    n = len(candles)
    if n < ATR_PERIOD + 3:
        return []
    atr = compute_atr(candles)
    if atr <= 0:
        return []

    zones: List[OBZone] = []
    for i in range(1, n):
        c = candles[i]
        body = abs(c["close"] - c["open"])
        if body < atr * OB_IMPULSE_ATR_MULTIPLIER:
            continue
        prev = candles[i - 1]
        is_impulse_bullish = c["close"] > c["open"]
        is_impulse_bearish = c["close"] < c["open"]
        prev_bearish = prev["close"] < prev["open"]
        prev_bullish = prev["close"] > prev["open"]

        if is_impulse_bullish and prev_bearish:
            zones.append(OBZone(top=prev["high"], bottom=prev["low"], index=i - 1, direction="bullish"))
        elif is_impulse_bearish and prev_bullish:
            zones.append(OBZone(top=prev["high"], bottom=prev["low"], index=i - 1, direction="bearish"))

    for z in zones:
        for c in candles[z.index + 1:]:
            if c["low"] <= z.top and c["high"] >= z.bottom:
                z.mitigated = True
                break
    return zones


# ============================================================================
# 5ter. TP2 INTELLIGENT SMC/ICT — sélection dynamique de la cible
# ============================================================================

@dataclass
class TP2Target:
    price: Optional[float]
    label: Optional[str]


def select_smart_tp2(candles: List[Dict], swing_points: List["SwingPoint"],
                      entry_price: float, direction: str,
                      tp1_price: float) -> "TP2Target":
    """Choisit le TP2 parmi les cibles SMC/ICT réellement disponibles sur le
    graphique au lieu d'un simple multiple de RR fixe :
      - Order Block non mitigé
      - Fair Value Gap non mitigée
      - Liquidité (Buy Side / Sell Side) non balayée
    Règle de sélection : la cible non mitigée la plus proche AU-DELÀ de TP1
    dans le sens du trade l'emporte ; en cas d'égalité de distance (< 0.05%
    d'écart), l'empreinte la plus fiable gagne (Order Block > FVG > liquidité
    pool), conformément à la hiérarchie SMC/ICT usuelle. Si rien n'existe
    au-delà de TP1, on retombe sur la cible non mitigée la plus proche
    au-delà de l'entrée."""
    candidates = []  # (price, kind, priority)

    for ob in find_order_blocks(candles):
        if ob.mitigated:
            continue
        if direction == "BUY" and ob.direction == "bearish" and ob.bottom > entry_price:
            candidates.append((ob.bottom, "order_block", TP2_TARGET_PRIORITY["order_block"]))
        elif direction == "SELL" and ob.direction == "bullish" and ob.top < entry_price:
            candidates.append((ob.top, "order_block", TP2_TARGET_PRIORITY["order_block"]))

    for fvg in scan_all_fvgs(candles):
        if fvg.mitigated:
            continue
        if direction == "BUY" and fvg.bottom > entry_price:
            candidates.append((fvg.bottom, "fvg", TP2_TARGET_PRIORITY["fvg"]))
        elif direction == "SELL" and fvg.top < entry_price:
            candidates.append((fvg.top, "fvg", TP2_TARGET_PRIORITY["fvg"]))

    unswept_highs = [p.price for p in swing_points if p.kind == "high"
                      and p.price > entry_price
                      and not any(c["high"] > p.price for c in candles[p.index + 1:])]
    unswept_lows = [p.price for p in swing_points if p.kind == "low"
                     and p.price < entry_price
                     and not any(c["low"] < p.price for c in candles[p.index + 1:])]
    if direction == "BUY":
        for price in unswept_highs:
            candidates.append((price, "liquidity_buy", TP2_TARGET_PRIORITY["liquidity"]))
    else:
        for price in unswept_lows:
            candidates.append((price, "liquidity_sell", TP2_TARGET_PRIORITY["liquidity"]))

    if not candidates:
        return TP2Target(None, None)

    def _beyond(price):
        return price > tp1_price if direction == "BUY" else price < tp1_price

    beyond_tp1 = [c for c in candidates if _beyond(c[0])]
    pool = beyond_tp1 if beyond_tp1 else candidates

    def _distance(price):
        return abs(price - entry_price)

    min_dist = min(_distance(c[0]) for c in pool)
    tolerance = min_dist * 0.0005 if min_dist else 0.0
    near_best = [c for c in pool if _distance(c[0]) - min_dist <= tolerance]
    best = max(near_best, key=lambda c: c[2])  # priorité SMC/ICT en cas d'égalité

    label = TP2_TARGET_LABELS[best[1]]
    return TP2Target(best[0], label)


# ============================================================================
# 6. RISQUE : ATR, SL, TP1/TP2, LOT
# ============================================================================

@dataclass
class TradeLevels:
    entry: float
    stop_loss: float
    tp1: float
    tp2: Optional[float]
    rr_tp1: float
    rr_tp2: Optional[float]
    high_rr_warning: bool
    tp2_source: Optional[str] = None


def compute_atr(candles: List[Dict], period: int = ATR_PERIOD) -> float:
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, prev_c = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return sum(trs[-period:]) / period


def compute_levels(entry, direction, invalidation_price, candles, swing_points=None,
                    spread_commission_buffer=0.0) -> TradeLevels:
    """TP2 n'est plus un simple multiple de RR fixe : il est choisi
    dynamiquement parmi les cibles SMC/ICT non mitigées disponibles
    (Order Block / FVG / Liquidité) via select_smart_tp2()."""
    atr = compute_atr(candles)
    sl_distance = max(atr * ATR_SL_MULTIPLIER, 1e-9)
    if INCLUDE_SPREAD_COMMISSION_BUFFER:
        sl_distance += spread_commission_buffer

    if direction == "BUY":
        stop_loss = min(invalidation_price, entry) - sl_distance
        risk = entry - stop_loss
        tp1 = entry + risk * TP1_RR
    else:
        stop_loss = max(invalidation_price, entry) + sl_distance
        risk = stop_loss - entry
        tp1 = entry - risk * TP1_RR

    tp2, tp2_source = None, None
    if swing_points is not None:
        target = select_smart_tp2(candles, swing_points, entry, direction, tp1)
        valid = target.price and (target.price > tp1 if direction == "BUY" else target.price < tp1)
        if valid:
            tp2, tp2_source = target.price, target.label

    rr_tp2, high_rr_warning = None, False
    if tp2 is not None and risk > 0:
        rr_tp2 = abs(tp2 - entry) / risk
        high_rr_warning = rr_tp2 >= TP2_MIN_RR_WARNING

    return TradeLevels(entry, stop_loss, tp1, tp2, TP1_RR, rr_tp2, high_rr_warning, tp2_source)


def compute_lot_size(capital, risk_percent, entry, stop_loss,
                      pip_value_per_lot=10.0, pip_size=0.01) -> float:
    risk_amount = capital * (risk_percent / 100)
    sl_pips = abs(entry - stop_loss) / pip_size
    if sl_pips <= 0:
        return 0.0
    return round(risk_amount / (sl_pips * pip_value_per_lot), 2)


# ============================================================================
# 7. SCORING
# ============================================================================

def compute_score(entry_type: str, rr_tp2: Optional[float], sweep_clean: bool = True) -> int:
    base = ENTRY_TYPES[entry_type]["score_weight"]
    bonus = 0
    if rr_tp2 and rr_tp2 >= 5:
        bonus += 15
    elif rr_tp2 and rr_tp2 >= 3:
        bonus += 8
    if sweep_clean:
        bonus += 5
    return min(100, base + bonus)


def get_stars(entry_type: str) -> str:
    return ENTRY_TYPES[entry_type]["stars"]


def passes_threshold(score: int, settings: Optional[Dict] = None) -> bool:
    min_score = (settings or get_settings())["min_score_to_publish"]
    return score >= min_score


# ============================================================================
# 8. TELEGRAM
# ============================================================================

def _bot_token(group: str = "btc_gold") -> str:
    """Chaque groupe Telegram a son propre bot (donc son propre token).
    'reports' retombe sur le token du groupe 'btc_gold' si
    TELEGRAM_BOT_TOKEN_REPORTS n'est pas défini. Si la variable d'env n'est
    pas définie du tout, on retombe sur token_default (cf. avertissement de
    sécurité au niveau de TELEGRAM_GROUPS)."""
    group_cfg = TELEGRAM_GROUPS[group]
    env_key = group_cfg["token_env"]
    token = os.environ.get(env_key) or group_cfg.get("token_default")
    if not token and group == "reports":
        bg = TELEGRAM_GROUPS["btc_gold"]
        token = os.environ.get(bg["token_env"]) or bg.get("token_default")
    if not token:
        raise RuntimeError(f"Variable d'environnement {env_key} manquante pour le groupe '{group}'.")
    return token


def _chat_id_for_group(group: str) -> str:
    group_cfg = TELEGRAM_GROUPS[group]
    env_key = group_cfg["chat_id_env"]
    chat_id = os.environ.get(env_key) or group_cfg.get("chat_id_default")
    if not chat_id:
        raise RuntimeError(f"Variable d'environnement {env_key} manquante pour le groupe '{group}'.")
    return chat_id


def format_signal_message(symbol, display_name, direction, entry_type_label, stars, score,
                           entry, sl, tp1, tp2, rr_tp1, rr_tp2, high_rr_warning,
                           tp2_source=None) -> str:
    direction_emoji = "🟢 ACHAT" if direction == "BUY" else "🔴 VENTE"
    ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    lines = [
        "⚡ *ALPHABOT SMC PRO* ⚡", "",
        f"📊 *Actif* : {display_name} ({TIMEFRAME})",
        f"🎯 *Setup* : {entry_type_label} {stars}",
        f"{direction_emoji}", "",
        f"🔹 *Entrée* : `{entry:.5f}`",
        f"🛑 *Stop Loss* : `{sl:.5f}`",
        f"✅ *TP1 (RR{rr_tp1:g})* : `{tp1:.5f}`",
    ]
    if tp2:
        rr2_txt = f"RR{rr_tp2:.1f}" + (" 🔥" if high_rr_warning else "")
        source_txt = f" — {tp2_source}" if tp2_source else ""
        lines.append(f"🚀 *TP2 ({rr2_txt}){source_txt}* : `{tp2:.5f}`")
    lines += ["", f"⭐ *Score* : {score}/100",
              "🔁 BE proposé à RR1 · Sécurisation proposée à RR2", "", f"🕒 {ts}"]
    return "\n".join(lines)


def _cleanup_old_charts():
    """Garde uniquement les CHARTS_KEEP_LAST images les plus récentes pour ne
    pas saturer le disque sur un déploiement de longue durée."""
    try:
        files = [os.path.join(CHARTS_DIR, f) for f in os.listdir(CHARTS_DIR) if f.endswith(".png")]
        files.sort(key=os.path.getmtime, reverse=True)
        for f in files[CHARTS_KEEP_LAST:]:
            os.remove(f)
    except Exception:
        pass


def generate_signal_chart(symbol: str, display_name: str, direction: str,
                           candles: List[Dict], sweep: "LiquiditySweep", bos: "BOSConfirmation",
                           entry: float, sl: float, tp1: float, tp2: Optional[float],
                           tp2_source: Optional[str] = None, lookback: int = 60,
                           signal_id: Optional[int] = None) -> Optional[str]:
    """Génère une image professionnelle du setup AVANT l'envoi Telegram :
    chandeliers, sweep de liquidité, BOS, zone d'entrée, SL, TP1, TP2 —
    avec annotations couleurs + labels + emojis. Retourne le chemin du
    fichier PNG, ou None en cas d'échec (le signal reste envoyé en texte)."""
    try:
        view = candles[-lookback:] if len(candles) > lookback else candles
        offset = len(candles) - len(view)
        x = list(range(len(view)))

        fig, ax = plt.subplots(figsize=(11, 6.5), dpi=150)
        fig.patch.set_facecolor("#0f1420")
        ax.set_facecolor("#0f1420")

        up_color, down_color = "#22c55e", "#ef4444"
        for i, c in enumerate(view):
            color = up_color if c["close"] >= c["open"] else down_color
            ax.plot([i, i], [c["low"], c["high"]], color=color, linewidth=1, zorder=2)
            body_bottom = min(c["open"], c["close"])
            body_height = max(abs(c["close"] - c["open"]), 1e-9)
            ax.add_patch(Rectangle((i - 0.3, body_bottom), 0.6, body_height,
                                    facecolor=color, edgecolor=color, zorder=3))

        # --- Sweep de liquidité 💧 ---
        sweep_i = sweep.sweep_index - offset
        if 0 <= sweep_i < len(view):
            ax.scatter([sweep_i], [sweep.sweep_wick_price], color="#38bdf8", s=90,
                       marker="D", zorder=5, edgecolors="white", linewidths=0.8)
            ax.annotate("💧 Liquidity Sweep", xy=(sweep_i, sweep.sweep_wick_price),
                        xytext=(sweep_i, sweep.sweep_wick_price), textcoords="data",
                        color="#38bdf8", fontsize=10, fontweight="bold",
                        va="bottom" if direction == "SELL" else "top", ha="left")

        # --- BOS confirmé 📈 ---
        ax.axhline(bos.break_level, color="#f59e0b", linestyle="--", linewidth=1.2, zorder=1)
        ax.text(len(view) - 1, bos.break_level, "  📈 BOS confirmé", color="#f59e0b",
                fontsize=10, fontweight="bold", va="bottom", ha="left")

        # --- Zone d'entrée / SL / TP1 / TP2 ---
        entry_emoji = "🟢 BUY" if direction == "BUY" else "🔴 SELL"
        ax.axhline(entry, color="#3b82f6", linewidth=1.4, zorder=1)
        ax.text(len(view) - 1, entry, f"  {entry_emoji}  Entrée {entry:.5f}", color="#3b82f6",
                fontsize=10, fontweight="bold", va="center", ha="left")

        ax.axhline(sl, color="#ef4444", linewidth=1.4, zorder=1)
        ax.text(len(view) - 1, sl, f"  🛑 SL {sl:.5f}", color="#ef4444",
                fontsize=10, fontweight="bold", va="center", ha="left")

        ax.axhline(tp1, color="#22c55e", linewidth=1.4, zorder=1)
        ax.text(len(view) - 1, tp1, f"  🎯 TP1 {tp1:.5f}", color="#22c55e",
                fontsize=10, fontweight="bold", va="center", ha="left")

        if tp2:
            ax.axhline(tp2, color="#a855f7", linewidth=1.4, linestyle=":", zorder=1)
            label = f"  🚀 TP2 {tp2:.5f}" + (f" ({tp2_source})" if tp2_source else "")
            ax.text(len(view) - 1, tp2, label, color="#a855f7",
                    fontsize=10, fontweight="bold", va="center", ha="left")

        ax.set_xlim(-1, len(view) + 22)
        ax.tick_params(colors="#8b93a7")
        for spine in ax.spines.values():
            spine.set_color("#262e42")
        ax.set_xticks([])
        ax.grid(axis="y", color="#262e42", linewidth=0.5, alpha=0.5)

        title_emoji = "🟢 BUY" if direction == "BUY" else "🔴 SELL"
        ax.set_title(f"⚡ ALPHABOT SMC PRO — {display_name} ({TIMEFRAME})  ·  {title_emoji}",
                     color="#e8ecf4", fontsize=13, fontweight="bold", loc="left", pad=14)

        fname = f"{symbol}_{signal_id or int(time.time())}_{int(time.time())}.png"
        path = os.path.join(CHARTS_DIR, fname)
        fig.tight_layout()
        fig.savefig(path, facecolor=fig.get_facecolor())
        plt.close(fig)
        _cleanup_old_charts()
        return path
    except Exception:
        log.error(f"[{symbol}] échec de génération de l'image du signal:\n{traceback.format_exc()}")
        try:
            plt.close("all")
        except Exception:
            pass
        return None


# --- Boutons interactifs -------------------------------------------------
# Actions disponibles sous chaque signal Telegram. callback_data suit le
# format compact "act:<signal_id>:<action>" (largement sous la limite de 64
# octets imposée par Telegram pour callback_data).
TRADE_ACTIONS = {
    "taken":   {"emoji": "✅", "label": "Trade pris",   "status": "taken"},
    "ignored": {"emoji": "❌", "label": "Trade ignoré",  "status": "ignored"},
    "be":      {"emoji": "🟡", "label": "Break Even",   "status": "be"},
    "secured": {"emoji": "🔒", "label": "Sécuriser",    "status": "secured"},
    "closed":  {"emoji": "🔴", "label": "Clôturer",     "status": "closed"},
}


def _signal_inline_keyboard(signal_id: int) -> Dict:
    """Construit le clavier inline Telegram (2 boutons par ligne) affiché
    sous chaque signal, pour permettre au trader de reporter en un tap ce
    qu'il a réellement fait du trade."""
    buttons = [
        {"text": f"{a['emoji']} {a['label']}", "callback_data": f"act:{signal_id}:{key}"}
        for key, a in TRADE_ACTIONS.items()
    ]
    rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    return {"inline_keyboard": rows}


def send_telegram_signal(group: str, text: str, image_path: str = None, signal_id: int = None):
    chat_id = _chat_id_for_group(group)
    token = _bot_token(group)
    reply_markup = _signal_inline_keyboard(signal_id) if signal_id is not None else None
    if image_path:
        url = TELEGRAM_API.format(token=token, method="sendPhoto")
        data = {"chat_id": chat_id, "caption": text, "parse_mode": "Markdown"}
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        with open(image_path, "rb") as img:
            resp = requests.post(url, data=data, files={"photo": img}, timeout=15)
    else:
        url = TELEGRAM_API.format(token=token, method="sendMessage")
        data = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        resp = requests.post(url, data=data, timeout=15)
    resp.raise_for_status()
    return resp.json()


# --- Message de démarrage --------------------------------------------------
# Envoyé sur les 2 groupes de signaux (btc_gold + deriv) à chaque lancement OU
# redémarrage du process, pour que tu saches immédiatement sur Telegram que le
# bot est bien en ligne (et pas seulement silencieusement en train de tourner
# côté serveur). Si le redémarrage a été forcé par le watchdog, le message le
# précise avec la raison, pour distinguer un déploiement normal d'un incident.
STARTUP_NOTIFY_GROUPS = ("btc_gold", "deriv")


def format_startup_message() -> str:
    hb = get_heartbeat_status()
    now = time.time()
    forced_restart = bool(hb.get("last_restart_at")) and (now - hb["last_restart_at"]) < 300
    ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())

    lines = ["🟢 *ALPHABOT SMC PRO* — bot en ligne", ""]
    if forced_restart:
        lines += [
            "⚠️ *Redémarrage automatique* déclenché par le watchdog.",
            f"Raison : {hb.get('last_restart_reason') or 'inconnue'}",
            f"Nombre total de redémarrages forcés : {hb.get('restart_count', 0)}",
            "",
        ]
    lines += [
        f"📊 *Actifs surveillés* : {', '.join(a.display_name for a in ASSETS.values())} ({TIMEFRAME})",
        f"🛰️ *Flux Deriv WS* : connexion en cours…",
        f"🕒 {ts}",
    ]
    return "\n".join(lines)


def send_startup_notification():
    """Diffuse le message de démarrage sur les groupes Telegram configurés.
    Best-effort : une erreur (token manquant, réseau...) est loggée mais ne
    doit jamais empêcher le bot de démarrer/scanner."""
    message = format_startup_message()
    for group in STARTUP_NOTIFY_GROUPS:
        try:
            send_telegram_signal(group, message)
            log.info(f"Message de démarrage envoyé sur le groupe Telegram '{group}'.")
        except Exception:
            log.warning(f"Échec d'envoi du message de démarrage sur '{group}':\n{traceback.format_exc()}")


def _telegram_answer_callback(group: str, callback_query_id: str, text: str = "", show_alert: bool = False):
    token = _bot_token(group)
    url = TELEGRAM_API.format(token=token, method="answerCallbackQuery")
    resp = requests.post(url, data={
        "callback_query_id": callback_query_id, "text": text[:200], "show_alert": show_alert,
    }, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _telegram_edit_reply_markup(group: str, chat_id, message_id: int, reply_markup: Optional[Dict]):
    token = _bot_token(group)
    url = TELEGRAM_API.format(token=token, method="editMessageReplyMarkup")
    payload = {"chat_id": chat_id, "message_id": message_id}
    payload["reply_markup"] = json.dumps(reply_markup or {"inline_keyboard": []})
    resp = requests.post(url, data=payload, timeout=10)
    # best-effort : un message déjà édité/supprimé ne doit jamais faire planter le webhook.
    if not resp.ok:
        log.warning(f"Telegram editMessageReplyMarkup a échoué ({resp.status_code}): {resp.text[:200]}")
    return resp


# ============================================================================
# 9. SIGNAL PIPELINE
# ============================================================================

def is_session_open(asset_symbol: str) -> bool:
    asset = ASSETS[asset_symbol]
    if asset.session_continuous:
        return True
    hour_utc = datetime.now(timezone.utc).hour
    if asset.session_start_utc <= asset.session_end_utc:
        return asset.session_start_utc <= hour_utc < asset.session_end_utc
    return hour_utc >= asset.session_start_utc or hour_utc < asset.session_end_utc


def process_asset(symbol: str):
    asset = ASSETS[symbol]
    if not is_session_open(symbol):
        return
    if not can_publish_today(symbol):
        return

    settings = get_settings()
    if count_open_positions() >= settings["max_open_positions"]:
        return

    candles = fetch_candles(symbol, limit=200)
    if len(candles) < 30:
        log.warning(f"[{symbol}] pas assez de données ({len(candles)} bougies)")
        return

    swing_points = find_swing_points(candles)
    sweep = detect_liquidity_sweep(candles, swing_points)
    if not sweep:
        return

    bos = confirm_bos(candles, sweep, swing_points)
    if not bos or not bos.confirmed:
        return

    direction = "BUY" if bos.direction == "bullish" else "SELL"
    entry_price = candles[bos.break_index]["close"]

    fvg = detect_fvg(candles, bos.break_index, bos.direction)
    entry_type = "fvg_return" if fvg else "direct"

    levels = compute_levels(entry_price, direction, sweep.sweep_wick_price, candles, swing_points)

    score = compute_score(entry_type, levels.rr_tp2)
    if not passes_threshold(score, settings):
        return

    setup_key = f"{symbol}:{direction}:{round(sweep.swept_point.price, 5)}"
    if has_active_setup(setup_key):
        return

    # Anti-doublon temporel : même si le setup précédent est déjà clôturé,
    # on n'autorise pas un nouveau signal sur le même actif avant
    # SIGNAL_COOLDOWN_SECONDS (défaut 15 min) — évite qu'un même niveau
    # re-balayé à quelques minutes d'intervalle ne spamme le groupe Telegram.
    age = seconds_since_last_signal(symbol)
    if age is not None and age < SIGNAL_COOLDOWN_SECONDS:
        log.info(f"[{symbol}] signal ignoré : dernier signal il y a {age:.0f}s "
                 f"(cooldown = {SIGNAL_COOLDOWN_SECONDS:.0f}s).")
        return

    signal_id = insert_signal(
        symbol, setup_key, direction, entry_type, get_stars(entry_type), score,
        entry_price, levels.stop_loss, levels.tp1, levels.tp2, levels.rr_tp1, levels.rr_tp2,
        asset.telegram_group, levels.tp2_source,
    )
    increment_daily_counter(symbol)

    message = format_signal_message(
        symbol, asset.display_name, direction, ENTRY_TYPES[entry_type]["label"],
        get_stars(entry_type), score, entry_price, levels.stop_loss, levels.tp1,
        levels.tp2, levels.rr_tp1, levels.rr_tp2 or 0, levels.high_rr_warning,
        levels.tp2_source,
    )
    image_path = generate_signal_chart(
        symbol, asset.display_name, direction, candles, sweep, bos,
        entry_price, levels.stop_loss, levels.tp1, levels.tp2, levels.tp2_source,
        signal_id=signal_id,
    )
    try:
        send_telegram_signal(asset.telegram_group, message, image_path=image_path, signal_id=signal_id)
        log.info(f"[{symbol}] signal #{signal_id} publié ({direction}, {entry_type}, score={score})")
    except Exception:
        log.error(f"[{symbol}] échec d'envoi Telegram pour le signal #{signal_id}:\n{traceback.format_exc()}")


def scan_loop():
    log.info("AlphaBot SMC PRO (fusion) — démarrage de la boucle de scan.")
    while True:
        for symbol in ASSETS:
            try:
                process_asset(symbol)
            except Exception:
                log.error(f"[{symbol}] erreur inattendue:\n{traceback.format_exc()}")
        # Heartbeat watchdog : preuve que la boucle de scan tourne toujours.
        # Écrit en DB (visible via /health et /api/watchdog, survit à un
        # redémarrage) ET en mémoire (lu par watchdog_loop sans I/O disque).
        global _LAST_SCAN_MONOTONIC
        _LAST_SCAN_MONOTONIC = time.monotonic()
        try:
            record_heartbeat(mark_scan=True)
        except Exception:
            log.error("watchdog: échec d'écriture du heartbeat en base:\n" + traceback.format_exc())
        time.sleep(SCAN_INTERVAL_SECONDS)


# ============================================================================
# 9bis. WATCHDOG — surveillance du thread principal + auto-redémarrage
# ============================================================================
# Le heartbeat en mémoire (horloge monotone, insensible aux changements
# d'heure système) est mis à jour par scan_loop() à chaque itération.
_LAST_SCAN_MONOTONIC = time.monotonic()
_WATCHDOG_LAST_RESTART_MONOTONIC = 0.0


def watchdog_loop():
    """Tourne dans un thread daemon dédié. Vérifie périodiquement que la
    boucle de scan (thread principal) donne toujours signe de vie. Si elle
    reste silencieuse plus de WATCHDOG_MAX_SILENCE_SECONDS (bloquée, deadlock,
    exception non catchée qui aurait échappé à scan_loop, etc.), le watchdog :
      1. logge l'incident en CRITICAL,
      2. enregistre l'événement en SQLite (table watchdog_heartbeat),
      3. force l'arrêt du process via os._exit(1).

    Sur Render (Web/Background service) comme sur un VPS avec un superviseur
    de process (systemd, supervisord, pm2, ou un simple `while true; do python
    main.py; done`), un process qui se termine est automatiquement relancé —
    c'est le mécanisme de redémarrage utilisé ici, volontairement simple et
    donc fiable (pas de gestion de threads zombies à l'intérieur du même
    process, qui serait beaucoup plus fragile)."""
    log.info("Watchdog : thread de surveillance démarré "
              f"(silence max toléré = {WATCHDOG_MAX_SILENCE_SECONDS}s).")
    global _WATCHDOG_LAST_RESTART_MONOTONIC
    while True:
        try:
            time.sleep(WATCHDOG_CHECK_INTERVAL_SECONDS)
            silence = time.monotonic() - _LAST_SCAN_MONOTONIC

            # Heartbeat "je suis vivant" pour le watchdog lui-même (utile si on
            # veut un jour distinguer "scan bloqué" de "process entier mort").
            try:
                record_heartbeat(mark_scan=False)
            except Exception:
                pass

            if silence <= WATCHDOG_MAX_SILENCE_SECONDS:
                continue

            since_last_restart = time.monotonic() - _WATCHDOG_LAST_RESTART_MONOTONIC
            if since_last_restart < WATCHDOG_RESTART_COOLDOWN_SECONDS:
                # Anti rage-restart : on vient déjà de forcer un redémarrage,
                # on laisse le temps au nouveau process de démarrer proprement.
                continue

            reason = f"scan silencieux depuis {silence:.0f}s (seuil={WATCHDOG_MAX_SILENCE_SECONDS}s)"
            log.critical(f"WATCHDOG : boucle de scan considérée bloquée ({reason}). "
                         f"Redémarrage forcé du process.")
            try:
                record_watchdog_restart(reason)
            except Exception:
                log.error("watchdog: échec d'écriture du redémarrage en base:\n" + traceback.format_exc())

            _WATCHDOG_LAST_RESTART_MONOTONIC = time.monotonic()
            # os._exit() coupe le process immédiatement (pas de cleanup Python
            # ni d'exceptions relancées) : c'est volontaire, on ne peut pas se
            # fier à un état potentiellement bloqué/incohérent pour faire un
            # arrêt "propre". Render / systemd / le superviseur du VPS relance
            # alors un process neuf.
            os._exit(1)
        except Exception:
            # Le watchdog lui-même ne doit JAMAIS s'arrêter silencieusement.
            log.error("Erreur inattendue dans watchdog_loop (le watchdog continue):\n"
                       + traceback.format_exc())


# ============================================================================
# 10. DASHBOARD FLASK
# ============================================================================

app = Flask(__name__)


@app.route("/health")
def health():
    hb = get_heartbeat_status()
    status = "ok" if hb["healthy"] else "degraded"
    return jsonify({
        "status": status,
        "scan_healthy": hb["healthy"],
        "seconds_since_last_scan": hb["seconds_since_scan"],
        "restart_count": hb["restart_count"],
        "deriv_ws_connected": deriv_feed.is_connected(),
    }), (200 if hb["healthy"] else 503)


@app.route("/api/watchdog")
def watchdog_status():
    hb = get_heartbeat_status()
    return jsonify({
        "heartbeat": hb,
        "deriv_feed": {
            "connected": deriv_feed.is_connected(),
            "seconds_since_last_message": deriv_feed.seconds_since_last_message(),
            "symbols": deriv_feed.symbols,
        },
    })


@app.route("/api/lot-size", methods=["POST"])
def lot_size():
    data = request.get_json(force=True)
    settings = get_settings()
    symbol = data.get("symbol")
    risk_percent = float(data.get("risk_percent") or get_effective_risk_percent(symbol, settings))
    lot = compute_lot_size(
        capital=float(data.get("capital", settings["capital"])),
        risk_percent=risk_percent,
        entry=float(data["entry"]), stop_loss=float(data["stop_loss"]),
        pip_value_per_lot=float(data.get("pip_value_per_lot", 10.0)),
        pip_size=float(data.get("pip_size", 0.01)),
    )
    return jsonify({"lot_size": lot, "risk_percent_used": risk_percent})


@app.route("/api/profile", methods=["GET", "POST"])
def profile():
    """Conservé pour compatibilité ascendante — délègue maintenant au
    profil actif du nouveau système (table `profiles`, voir /api/profiles)."""
    if request.method == "POST":
        data = request.get_json(force=True)
        name = data.get("name")
        match = next((p for p in list_profiles() if p["name"] == name), None)
        if not match:
            return jsonify({"error": "profil inconnu"}), 400
        activate_profile(match["id"])
    settings = get_settings()
    active = next((p for p in list_profiles() if p["id"] == settings.get("active_profile_id")), None)
    return jsonify({"active_profile": active["name"] if active else None, "settings": active})


@app.route("/api/profiles", methods=["GET", "POST"])
def profiles_endpoint():
    """Liste tous les profils sauvegardés, ou en crée un nouveau.
    Body POST attendu : {name, capital, leverage, risk_percent, max_open_positions}."""
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        try:
            p = create_profile(
                name=data.get("name"),
                capital=float(data.get("capital", 0)),
                leverage=float(data.get("leverage", 0)),
                risk_percent=float(data.get("risk_percent", 0)),
                max_open_positions=int(data.get("max_open_positions", 0)),
            )
        except (ValueError, TypeError) as e:
            return jsonify({"error": str(e)}), 400
        return jsonify(p), 201
    return jsonify(list_profiles())


@app.route("/api/profiles/<int:profile_id>", methods=["GET", "PUT", "DELETE"])
def profile_detail(profile_id):
    if request.method == "GET":
        p = get_profile(profile_id)
        if not p:
            return jsonify({"error": "profil introuvable"}), 404
        return jsonify(p)
    if request.method == "PUT":
        data = request.get_json(force=True) or {}
        try:
            p = update_profile(profile_id, data)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify(p)
    try:
        delete_profile(profile_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True})


@app.route("/api/profiles/<int:profile_id>/activate", methods=["POST"])
def profile_activate(profile_id):
    """Sélectionne ce profil comme actif ET applique immédiatement ses
    valeurs (capital, levier, risque %, positions max) — sans redémarrage."""
    try:
        settings = activate_profile(profile_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True, "active_profile_id": profile_id, "settings": settings})


@app.route("/api/settings", methods=["GET", "POST"])
def settings_endpoint():
    """Point central de configuration du dashboard : capital, levier, risque
    par trade, mode risque, martingale, recovery, positions max, score min."""
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        try:
            updated = update_settings(data)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify(updated)
    return jsonify(get_settings())


@app.route("/api/stats")
def stats():
    return jsonify(get_stats())


@app.route("/api/overview")
def overview():
    return jsonify(get_dashboard_overview())


@app.route("/api/stats/by-asset")
def stats_by_asset():
    return jsonify(get_stats_by_asset())


@app.route("/api/stats/monthly")
def stats_monthly():
    n = request.args.get("months", default=6, type=int)
    return jsonify(get_monthly_performance(n))


@app.route("/api/history")
def history():
    limit = request.args.get("limit", default=100, type=int)
    symbol = request.args.get("symbol")
    status = request.args.get("status")
    return jsonify(get_trade_history(limit=limit, symbol=symbol, status=status))


@app.route("/api/reports/daily")
def report_daily():
    now = datetime.now(timezone.utc)
    start_ts, end_ts = _day_bounds(now)
    return jsonify(get_period_stats(start_ts, end_ts))


@app.route("/api/reports/weekly")
def report_weekly():
    now = datetime.now(timezone.utc)
    start_ts, end_ts, period_key = _week_bounds(now)
    stats_data = get_period_stats(start_ts, end_ts)
    stats_data["period_key"] = period_key
    return jsonify(stats_data)


@app.route("/api/reports/monthly")
def report_monthly():
    now = datetime.now(timezone.utc)
    start_ts, end_ts = _month_bounds(now)
    stats_data = get_period_stats(start_ts, end_ts)
    stats_data["period_key"] = now.strftime("%Y-%m")
    return jsonify(stats_data)


@app.route("/api/trade/<int:signal_id>/status", methods=["POST"])
def set_trade_status(signal_id):
    data = request.get_json(force=True)
    status = data.get("status")
    valid = {"taken", "ignored", "closed", "be", "secured", "tp1_hit", "tp2_hit", "invalidated"}
    if status not in valid:
        return jsonify({"error": f"status invalide, attendu un de {valid}"}), 400
    record_trade_action(signal_id, action=status, new_status=status, source="dashboard")
    return jsonify({"ok": True, "signal_id": signal_id, "status": status})


@app.route("/api/trade/<int:signal_id>")
def get_trade(signal_id):
    row = get_signal(signal_id)
    if not row:
        return jsonify({"error": "introuvable"}), 404
    return jsonify(dict(row))


@app.route("/api/trade/<int:signal_id>/actions")
def get_trade_actions_endpoint(signal_id):
    rows = get_trade_actions(signal_id)
    return jsonify([dict(r) for r in rows])


@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    """Reçoit les updates Telegram (callback_query des boutons interactifs
    ✅❌🟡🔒🔴 sous chaque signal). Voir section DÉPLOIEMENT en bas de fichier
    pour la commande setWebhook à exécuter une fois par bot."""
    if TELEGRAM_WEBHOOK_SECRET:
        header = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if header != TELEGRAM_WEBHOOK_SECRET:
            return jsonify({"error": "secret invalide"}), 403

    update = request.get_json(force=True, silent=True) or {}
    callback = update.get("callback_query")
    if not callback:
        return jsonify({"ok": True})  # autre type d'update (message, etc.) -> ignoré

    try:
        data = callback.get("data", "")
        parts = data.split(":")
        if len(parts) != 3 or parts[0] != "act":
            return jsonify({"ok": True})

        _, signal_id_str, action_key = parts
        signal_id = int(signal_id_str)
        action_def = TRADE_ACTIONS.get(action_key)
        row = get_signal(signal_id)

        if not action_def or not row:
            return jsonify({"ok": True})

        actor_info = callback.get("from") or {}
        actor = actor_info.get("username") or str(actor_info.get("id", "")) or None
        record_trade_action(signal_id, action=action_key, new_status=action_def["status"],
                             source="telegram", actor=actor)

        group = row["telegram_group"]
        confirm_text = f"{action_def['emoji']} {action_def['label']} enregistré pour le signal #{signal_id}."
        try:
            _telegram_answer_callback(group, callback["id"], text=confirm_text)
        except Exception:
            log.warning("Échec answerCallbackQuery Telegram:\n" + traceback.format_exc())

        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        if chat.get("id") and message.get("message_id"):
            try:
                _telegram_edit_reply_markup(group, chat["id"], message["message_id"], None)
            except Exception:
                log.warning("Échec editMessageReplyMarkup Telegram:\n" + traceback.format_exc())

        return jsonify({"ok": True, "signal_id": signal_id, "action": action_key,
                         "status": action_def["status"]})
    except Exception:
        log.error("Erreur traitement webhook Telegram:\n" + traceback.format_exc())
        # 200 volontaire : éviter que Telegram ne retente indéfiniment un update cassé.
        return jsonify({"ok": False}), 200


DASHBOARD_HTML = """
<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AlphaBot SMC PRO — Dashboard</title>
<style>
  :root { --bg:#0f1420; --card:#171d2b; --border:#262e42; --text:#e8ecf4; --muted:#8b93a7;
          --green:#22c55e; --red:#ef4444; --blue:#3b82f6; --amber:#f59e0b; }
  * { box-sizing: border-box; }
  body { margin:0; font-family: -apple-system, Segoe UI, Roboto, sans-serif; background:var(--bg); color:var(--text); }
  header { padding:20px 28px; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; align-items:center; }
  header h1 { font-size:18px; margin:0; }
  header span { color:var(--muted); font-size:13px; }
  main { max-width:1180px; margin:0 auto; padding:24px 20px 60px; }
  .cards { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px,1fr)); gap:14px; margin-bottom:26px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:16px; }
  .card .label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
  .card .value { font-size:22px; font-weight:700; margin-top:6px; }
  .pos { color:var(--green); } .neg { color:var(--red); }
  section { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:20px; margin-bottom:20px; }
  section h2 { margin:0 0 16px; font-size:15px; }
  .grid-form { display:grid; grid-template-columns: repeat(auto-fit, minmax(200px,1fr)); gap:14px; }
  label { display:block; font-size:12px; color:var(--muted); margin-bottom:5px; }
  input[type=number], select { width:100%; background:#0f1420; border:1px solid var(--border); color:var(--text);
        padding:9px 10px; border-radius:8px; font-size:14px; }
  .toggle-row { display:flex; align-items:center; justify-content:space-between; background:#0f1420;
        border:1px solid var(--border); border-radius:8px; padding:10px 12px; }
  .switch { position:relative; width:42px; height:24px; }
  .switch input { opacity:0; width:0; height:0; }
  .slider { position:absolute; cursor:pointer; inset:0; background:#333c50; border-radius:24px; transition:.2s; }
  .slider:before { content:""; position:absolute; height:18px; width:18px; left:3px; bottom:3px; background:white; border-radius:50%; transition:.2s; }
  input:checked + .slider { background:var(--blue); }
  input:checked + .slider:before { transform: translateX(18px); }
  button { background:var(--blue); color:white; border:none; padding:10px 18px; border-radius:8px; font-size:14px;
        cursor:pointer; font-weight:600; }
  button:hover { opacity:.9; }
  #saveMsg { margin-left:12px; font-size:13px; color:var(--green); }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:9px 10px; border-bottom:1px solid var(--border); }
  th { color:var(--muted); font-weight:600; font-size:12px; text-transform:uppercase; }
  .badge { padding:2px 8px; border-radius:20px; font-size:11px; font-weight:600; }
  .b-buy { background:rgba(34,197,94,.15); color:var(--green); }
  .b-sell { background:rgba(239,68,68,.15); color:var(--red); }
  .b-status { background:rgba(59,130,246,.15); color:var(--blue); }
  .filters { display:flex; gap:10px; margin-bottom:14px; flex-wrap:wrap; }
  .filters select { width:auto; min-width:140px; }
  .tabs { display:flex; gap:8px; margin-bottom:16px; }
  .tab-btn { background:#0f1420; border:1px solid var(--border); color:var(--muted); padding:8px 14px;
        border-radius:8px; cursor:pointer; font-size:13px; }
  .tab-btn.active { background:var(--blue); color:white; border-color:var(--blue); }
</style>
</head>
<body>
<header>
  <h1>⚡ AlphaBot SMC PRO — Dashboard</h1>
  <span id="lastUpdate">chargement…</span>
</header>
<main>
  <div class="cards" id="overviewCards"></div>

  <section>
    <h2>👤 Profils (Scalping, Conservative, personnalisés…)</h2>
    <table id="profilesTable">
      <thead><tr><th>Nom</th><th>Capital</th><th>Levier</th><th>Risque %</th><th>Pos. max</th><th>Statut</th><th></th></tr></thead>
      <tbody></tbody>
    </table>
    <div class="grid-form" style="margin-top:16px;">
      <div><label>Nom du profil</label><input type="text" id="p_name" placeholder="ex: Mon setup New York"></div>
      <div><label>Capital ($)</label><input type="number" step="0.01" id="p_capital" value="1000"></div>
      <div><label>Levier (x)</label><input type="number" step="1" id="p_leverage" value="100"></div>
      <div><label>Risque par trade (%)</label><input type="number" step="0.1" id="p_risk_percent" value="1"></div>
      <div><label>Positions ouvertes max</label><input type="number" step="1" id="p_max_open_positions" value="3"></div>
    </div>
    <div style="margin-top:14px;">
      <button onclick="createOrUpdateProfile()">💾 Enregistrer le profil</button>
      <button style="background:#333c50; margin-left:8px;" onclick="resetProfileForm()">Annuler l'édition</button>
      <span id="profileMsg"></span>
    </div>
  </section>

  <section>
    <h2>Réglages (appliqués immédiatement, aucune modification du code nécessaire)</h2>
    <div class="grid-form">
      <div><label>Capital du compte ($)</label><input type="number" step="0.01" id="s_capital"></div>
      <div><label>Levier (x)</label><input type="number" step="1" id="s_leverage"></div>
      <div><label>Risque par trade (%)</label><input type="number" step="0.1" id="s_risk_percent"></div>
      <div><label>Positions ouvertes max</label><input type="number" step="1" id="s_max_open_positions"></div>
      <div><label>Score minimum pour publier</label><input type="number" step="1" min="0" max="100" id="s_min_score_to_publish"></div>
      <div><label>Multiplicateur martingale</label><input type="number" step="0.1" id="s_martingale_multiplier"></div>
      <div><label>Multiplicateur max recovery</label><input type="number" step="0.1" id="s_recovery_max_multiplier"></div>
    </div>
    <div class="grid-form" style="margin-top:14px;">
      <div class="toggle-row"><span>Martingale</span>
        <label class="switch"><input type="checkbox" id="s_martingale_enabled"><span class="slider"></span></label>
      </div>
      <div class="toggle-row"><span>Recovery</span>
        <label class="switch"><input type="checkbox" id="s_recovery_enabled"><span class="slider"></span></label>
      </div>
    </div>
    <div style="margin-top:16px;">
      <button onclick="saveSettings()">💾 Enregistrer les réglages</button>
      <span id="saveMsg"></span>
    </div>
  </section>

  <section>
    <h2>Performance par actif</h2>
    <table id="byAssetTable">
      <thead><tr><th>Actif</th><th>Total</th><th>Gagnants</th><th>Perdants</th><th>Winrate</th><th>R cumulé</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section>
    <h2>Performances mensuelles (6 derniers mois)</h2>
    <table id="monthlyTable">
      <thead><tr><th>Mois</th><th>Signaux</th><th>Gagnants</th><th>Perdants</th><th>Winrate</th><th>R</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section>
    <h2>Historique des trades</h2>
    <div class="filters">
      <select id="f_symbol"><option value="">Tous les actifs</option></select>
      <select id="f_status">
        <option value="">Tous les statuts</option>
        <option value="pending">pending</option><option value="taken">taken</option>
        <option value="be">be</option><option value="secured">secured</option>
        <option value="tp1_hit">tp1_hit</option><option value="tp2_hit">tp2_hit</option>
        <option value="invalidated">invalidated</option><option value="closed">closed</option>
        <option value="ignored">ignored</option>
      </select>
      <button onclick="loadHistory()">Filtrer</button>
    </div>
    <table id="historyTable">
      <thead><tr><th>Date</th><th>Actif</th><th>Sens</th><th>Type</th><th>Score</th><th>Entrée</th><th>SL</th><th>TP1</th><th>Statut</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>
</main>

<script>
const ASSETS = {{ assets_json | safe }};

let editingProfileId = null;

async function loadProfiles() {
  const r = await fetch('/api/profiles'); const profiles = await r.json();
  const tbody = document.querySelector('#profilesTable tbody');
  tbody.innerHTML = profiles.map(p => `
    <tr>
      <td>${p.name}</td><td>$${p.capital}</td><td>${p.leverage}x</td>
      <td>${p.risk_percent}%</td><td>${p.max_open_positions}</td>
      <td>${p.active ? '<span class="badge b-buy">✓ actif</span>' : ''}</td>
      <td>
        ${p.active ? '' : `<button style="padding:5px 10px;font-size:12px;" onclick="activateProfile(${p.id})">Activer</button>`}
        <button style="padding:5px 10px;font-size:12px;background:#333c50;margin-left:4px;" onclick="editProfile(${p.id}, '${p.name.replace(/'/g,"\\'")}', ${p.capital}, ${p.leverage}, ${p.risk_percent}, ${p.max_open_positions})">✎</button>
        ${p.active ? '' : `<button style="padding:5px 10px;font-size:12px;background:var(--red);margin-left:4px;" onclick="deleteProfile(${p.id})">🗑</button>`}
      </td>
    </tr>`).join('');
}

async function activateProfile(id) {
  const r = await fetch(`/api/profiles/${id}/activate`, {method:'POST'});
  if (r.ok) { loadProfiles(); loadOverview(); }
}

function editProfile(id, name, capital, leverage, risk_percent, max_open_positions) {
  editingProfileId = id;
  document.getElementById('p_name').value = name;
  document.getElementById('p_capital').value = capital;
  document.getElementById('p_leverage').value = leverage;
  document.getElementById('p_risk_percent').value = risk_percent;
  document.getElementById('p_max_open_positions').value = max_open_positions;
}

function resetProfileForm() {
  editingProfileId = null;
  document.getElementById('p_name').value = '';
  document.getElementById('p_capital').value = 1000;
  document.getElementById('p_leverage').value = 100;
  document.getElementById('p_risk_percent').value = 1;
  document.getElementById('p_max_open_positions').value = 3;
}

async function createOrUpdateProfile() {
  const payload = {
    name: document.getElementById('p_name').value,
    capital: parseFloat(document.getElementById('p_capital').value),
    leverage: parseFloat(document.getElementById('p_leverage').value),
    risk_percent: parseFloat(document.getElementById('p_risk_percent').value),
    max_open_positions: parseInt(document.getElementById('p_max_open_positions').value),
  };
  const url = editingProfileId ? `/api/profiles/${editingProfileId}` : '/api/profiles';
  const method = editingProfileId ? 'PUT' : 'POST';
  const r = await fetch(url, {method, headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  const msg = document.getElementById('profileMsg');
  if (r.ok) {
    msg.style.color = 'var(--green)'; msg.textContent = '✓ Profil enregistré';
    resetProfileForm(); loadProfiles(); loadOverview();
  } else {
    const e = await r.json(); msg.style.color = 'var(--red)'; msg.textContent = '✗ ' + e.error;
  }
  setTimeout(() => msg.textContent = '', 3000);
}

async function deleteProfile(id) {
  if (!confirm('Supprimer ce profil ?')) return;
  const r = await fetch(`/api/profiles/${id}`, {method:'DELETE'});
  if (r.ok) loadProfiles();
  else { const e = await r.json(); alert(e.error); }
}

async function loadOverview() {
  const r = await fetch('/api/overview'); const d = await r.json();
  const pnlClass = d.profit_net_total >= 0 ? 'pos' : 'neg';
  document.getElementById('overviewCards').innerHTML = `
    <div class="card"><div class="label">Capital actuel</div><div class="value">$${d.capital_actuel}</div></div>
    <div class="card"><div class="label">Profit / Perte net</div><div class="value ${pnlClass}">$${d.profit_net_total}</div></div>
    <div class="card"><div class="label">Drawdown max</div><div class="value neg">$${d.drawdown_max}</div></div>
    <div class="card"><div class="label">Winrate</div><div class="value">${d.winrate}%</div></div>
    <div class="card"><div class="label">Signaux totaux</div><div class="value">${d.total_signals}</div></div>
    <div class="card"><div class="label">Positions ouvertes</div><div class="value">${d.open_positions} / ${d.settings.max_open_positions}</div></div>
  `;
  const s = d.settings;
  document.getElementById('s_capital').value = s.capital;
  document.getElementById('s_leverage').value = s.leverage;
  document.getElementById('s_risk_percent').value = s.risk_percent;
  document.getElementById('s_max_open_positions').value = s.max_open_positions;
  document.getElementById('s_min_score_to_publish').value = s.min_score_to_publish;
  document.getElementById('s_martingale_multiplier').value = s.martingale_multiplier;
  document.getElementById('s_recovery_max_multiplier').value = s.recovery_max_multiplier;
  document.getElementById('s_martingale_enabled').checked = s.martingale_enabled;
  document.getElementById('s_recovery_enabled').checked = s.recovery_enabled;
  document.getElementById('lastUpdate').textContent = 'mis à jour ' + new Date().toLocaleTimeString();
}

async function saveSettings() {
  const payload = {
    capital: parseFloat(document.getElementById('s_capital').value),
    leverage: parseFloat(document.getElementById('s_leverage').value),
    risk_percent: parseFloat(document.getElementById('s_risk_percent').value),
    max_open_positions: parseInt(document.getElementById('s_max_open_positions').value),
    min_score_to_publish: parseInt(document.getElementById('s_min_score_to_publish').value),
    martingale_multiplier: parseFloat(document.getElementById('s_martingale_multiplier').value),
    recovery_max_multiplier: parseFloat(document.getElementById('s_recovery_max_multiplier').value),
    martingale_enabled: document.getElementById('s_martingale_enabled').checked,
    recovery_enabled: document.getElementById('s_recovery_enabled').checked,
  };
  const r = await fetch('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  const msg = document.getElementById('saveMsg');
  if (r.ok) { msg.style.color = 'var(--green)'; msg.textContent = '✓ Enregistré'; loadOverview(); }
  else { const e = await r.json(); msg.style.color = 'var(--red)'; msg.textContent = '✗ ' + e.error; }
  setTimeout(() => msg.textContent = '', 3000);
}

async function loadByAsset() {
  const r = await fetch('/api/stats/by-asset'); const d = await r.json();
  const tbody = document.querySelector('#byAssetTable tbody');
  tbody.innerHTML = Object.entries(d).map(([sym, s]) => `
    <tr><td>${s.display_name || sym}</td><td>${s.total}</td><td>${s.wins}</td><td>${s.losses}</td>
    <td>${s.win_rate}%</td><td class="${s.total_r>=0?'pos':'neg'}">${s.total_r}</td></tr>`).join('');
}

async function loadMonthly() {
  const r = await fetch('/api/stats/monthly?months=6'); const d = await r.json();
  const tbody = document.querySelector('#monthlyTable tbody');
  tbody.innerHTML = d.map(m => `
    <tr><td>${m.period_key}</td><td>${m.total_signals}</td><td>${m.wins}</td><td>${m.losses}</td>
    <td>${m.win_rate}%</td><td class="${m.total_r>=0?'pos':'neg'}">${m.total_r}</td></tr>`).join('');
}

async function loadHistory() {
  const symbol = document.getElementById('f_symbol').value;
  const status = document.getElementById('f_status').value;
  const params = new URLSearchParams({limit: 150});
  if (symbol) params.set('symbol', symbol);
  if (status) params.set('status', status);
  const r = await fetch('/api/history?' + params.toString()); const d = await r.json();
  const tbody = document.querySelector('#historyTable tbody');
  tbody.innerHTML = d.map(t => {
    const date = new Date(t.created_at * 1000).toLocaleString();
    const dirBadge = t.direction === 'BUY' ? '<span class="badge b-buy">ACHAT</span>' : '<span class="badge b-sell">VENTE</span>';
    return `<tr><td>${date}</td><td>${t.symbol}</td><td>${dirBadge}</td><td>${t.entry_type}</td>
      <td>${t.score}</td><td>${t.entry_price.toFixed(5)}</td><td>${t.stop_loss.toFixed(5)}</td>
      <td>${t.tp1.toFixed(5)}</td><td><span class="badge b-status">${t.status}</span></td></tr>`;
  }).join('');
}

function initFilters() {
  const sel = document.getElementById('f_symbol');
  ASSETS.forEach(a => { const o = document.createElement('option'); o.value = a.symbol; o.textContent = a.display_name; sel.appendChild(o); });
}

initFilters();
loadOverview();
loadProfiles();
loadByAsset();
loadMonthly();
loadHistory();
setInterval(loadOverview, 30000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    assets_json = json.dumps([
        {"symbol": sym, "display_name": a.display_name} for sym, a in ASSETS.items()
    ])
    return render_template_string(DASHBOARD_HTML, assets_json=assets_json)


# ============================================================================
# 11. POINT D'ENTRÉE — lance le dashboard Flask dans un thread + la boucle de
#     scan dans le thread principal, pour ne déployer qu'UN seul process.
# ============================================================================

if __name__ == "__main__":
    enforce_group_asset_whitelist()
    init_db()

    port = int(os.environ.get("PORT", 5000))
    flask_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, use_reloader=False),
        daemon=True,
    )
    flask_thread.start()
    log.info(f"Dashboard Flask démarré sur le port {port} (thread daemon).")

    report_thread = threading.Thread(target=report_scheduler_loop, daemon=True)
    report_thread.start()
    log.info("Thread du planificateur de rapports démarré.")

    deriv_feed.start()  # connexion WebSocket Deriv persistante (V75 + V25)

    watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
    watchdog_thread.start()
    log.info("Thread watchdog démarré.")

    # Message "bot en ligne" sur Telegram — en thread séparé (non bloquant),
    # pour ne jamais retarder le démarrage de la boucle de scan si Telegram
    # est lent/indisponible au moment du déploiement.
    startup_notify_thread = threading.Thread(target=send_startup_notification, daemon=True)
    startup_notify_thread.start()

    scan_loop()  # boucle infinie dans le thread principal


# ============================================================================
# DÉPLOIEMENT SUR RENDER
# ============================================================================
# 1. Type de service : "Web Service" (pas "Background Worker") — car ce fichier
#    expose aussi le dashboard Flask sur $PORT, ce que Render attend d'un Web
#    Service pour le healthcheck HTTP. Render redémarre automatiquement tout
#    process qui se termine (crash, os._exit du watchdog, etc.) : c'est ce
#    mécanisme qui est utilisé par le watchdog intégré (section 9bis).
# 2. Start command : python main.py
# 3. Variables d'environnement à définir dans Render (Settings > Environment) :
#      Groupe BTC + Gold : TELEGRAM_BOT_TOKEN_BTC_GOLD, TG_CHAT_BTC_GOLD
#      Groupe Deriv (V25/V75) : TELEGRAM_BOT_TOKEN_DERIV, TG_CHAT_DERIV
#      DERIV_APP_ID (optionnel, sinon "1089" par défaut — App ID public de démo)
#      TG_CHAT_REPORTS (optionnel — sinon les rapports partent sur les 2 groupes ci-dessus)
#      TELEGRAM_BOT_TOKEN_REPORTS (optionnel — sinon le bot "btc_gold" est réutilisé)
#      TELEGRAM_WEBHOOK_SECRET (optionnel mais recommandé — sécurise /telegram/webhook)
#      SIGNAL_COOLDOWN_MINUTES (optionnel, défaut 15 — délai mini entre 2 signaux sur le même actif)
#      DB_PATH (optionnel — chemin du fichier SQLite, ex. un disque persistant Render)
# 4. Healthcheck path côté Render : /health
#      -> renvoie 200 si le scan a tourné il y a moins de WATCHDOG_MAX_SILENCE_SECONDS,
#         503 sinon (Render peut alors, en plus du watchdog interne, redémarrer le service).
# 5. Dépendances supplémentaires à ajouter dans requirements.txt : websocket-client
# 6. Boutons Telegram interactifs — à faire UNE FOIS par bot (btc_gold ET deriv),
#    après le premier déploiement, pour que Telegram pousse les clics de bouton
#    vers /telegram/webhook au lieu qu'on ait à faire du polling :
#      curl -X POST "https://api.telegram.org/bot<TOKEN>/setWebhook" \
#           -d "url=https://<ton-service>.onrender.com/telegram/webhook" \
#           -d "secret_token=<TELEGRAM_WEBHOOK_SECRET>"
#    (répéter avec le token de chaque bot : TELEGRAM_BOT_TOKEN_BTC_GOLD et
#    TELEGRAM_BOT_TOKEN_DERIV — un webhook par bot).

