"""
main.py
=======
AlphaBot — MOTEUR CRT V1 (signaux + paper trading, AUCUN ordre réel)

Moteur simple, déterministe et testable. Pas d'ATR, pas de score, pas de
BOS/OB. Deux modes cohabitent selon la config (config.json / strategy) :

  - V1 timeframe unique (recommandé, crt_tf == entry_tf, ex. "M5" partout) :
        CRT (sweep + clôture dans le range, bougie M5 clôturée)
        -> ENTRÉE DIRECTE immédiate, au prix de clôture de la bougie de sweep
        -> SL au sweep -> TP = RR x risque -> suivi RR1/RR2/RR3 (paper)
    Aucune attente de nouvelle bougie ni de retest FVG.

  - Mode legacy CRT (HTF) -> FVG (LTF) -> retest -> entrée, ou repli "entrée
    directe" après N bougies sans FVG (crt_tf != entry_tf, ex. M30 -> M5) :
    conservé pour compatibilité, activé par fvg_enabled=true.

Le scan (bougies M5 clôturées) et le suivi des positions ouvertes (ticks)
tournent sur les mêmes flux asynchrones non bloquants : un marché en erreur
ou une position ouverte ne bloquent jamais le scan des autres marchés.

Marchés : V75, V25, GOLD, BTC (jamais Boom / Crash, jamais les "(1s)").
Données : API Deriv (ticks -> bougies). Publique par défaut ; authentifiée par PAT/OTP si
DERIV_PAT + DERIV_PAT_APP_ID + DERIV_ACCOUNT_ID sont définis dans .env.

Utilisation :
    pip install -r requirements.txt
    python3 main.py                   # lance le moteur
    python3 main.py --stats           # statistiques par marché (trades.jsonl)
    python3 main.py --test            # tests intégrés de la logique (sans réseau)
    python3 main.py --init-config     # écrit config.json (modèle intégré) s'il n'existe pas

Réglages : config.json (section "strategy"), voir StrategyConfig ci-dessous.
Modèle de config : EXAMPLE_CONFIG_JSON (fichier unique : moteur + tests + modèle).

Telegram (texte + image PNG par signal) : mettre "telegram": {"enabled": true}
dans config.json. Token / chat / admin : valeurs par défaut intégrées
(DEFAULT_TELEGRAM_*, section 1), remplacées par TELEGRAM_BOT_TOKEN /
TELEGRAM_CHAT_ID / TELEGRAM_ADMIN_ID dans .env quand ils sont définis. Sans
telegram.enabled, ou sans le paquet 'requests', les signaux restent écrits
dans data/signals.log comme en V1.

Images de signal : nécessite matplotlib (voir requirements.txt). Désactivable
via "chart_enabled": false dans config.json.

Rapport journalier : envoyé chaque jour à l'heure définie par config.json
("report": {"hour": 21, "minute": 0}), via le même notifier (Telegram ou fichier).

Exness (exécution d'ordres réels) : NON inclus. Ce moteur reste 100% paper
trading — aucun ordre n'est jamais envoyé à un broker.
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import itertools
import json
import logging
import os
import re
import sys
import tempfile
import time as _time
import unittest
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from unittest import mock

try:  # la logique et les tests fonctionnent sans le réseau
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore[assignment]

    class ConnectionClosed(Exception):  # type: ignore[no-redef]
        pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # pragma: no cover
    pass

try:  # requis uniquement pour l'envoi Telegram
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore[assignment]

try:  # requis uniquement pour la génération d'images de signal
    import matplotlib
    matplotlib.use("Agg")  # pas d'affichage : on écrit directement des PNG
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
except ImportError:  # pragma: no cover
    plt = None  # type: ignore[assignment]


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.getenv("CONFIG_PATH", BASE_DIR / "config.json"))

# Valeurs Telegram par défaut : AUCUN secret en dur ici. Définis
# TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID / TELEGRAM_ADMIN_ID dans un fichier
# .env local (jamais commité). Sans ces variables, telegram.enabled=true
# retombe proprement sur data/signals.log (voir build_notifier).
# ⚠️ Un token était codé en dur ici auparavant : considère-le compromis et
# régénère-le via @BotFather avant toute réutilisation.
DEFAULT_TELEGRAM_BOT_TOKEN = ""
DEFAULT_TELEGRAM_CHAT_ID = ""
DEFAULT_TELEGRAM_ADMIN_ID = ""

# Granularités Deriv valides (secondes) pour ticks_history / style=candles
TF_SECONDS: dict[str, int] = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800, "H1": 3600}

CRT_TIMEFRAMES = ("M5", "M15", "M30", "H1")
ENTRY_TIMEFRAMES = ("M1", "M5")

# Marchés décrits par des ALIAS (jamais un code symbole supposé) :
# discover_symbols() fait le matching réel via active_symbols.
# "exclude" écarte les variantes (ex. "Volatility 75 (1s) Index").
ALL_MARKETS: list[dict[str, Any]] = [
    {"key": "V75", "label": "Volatility 75 Index",
     "aliases": ["volatility 75 index", "volatility 75", "vol 75"],
     "exclude": ["(1s)", "1hz"]},
    {"key": "V25", "label": "Volatility 25 Index",
     "aliases": ["volatility 25 index", "volatility 25", "vol 25"],
     "exclude": ["(1s)", "1hz"]},
    {"key": "GOLD", "label": "GOLD / XAUUSD",
     "aliases": ["gold/usd", "gold", "xau/usd", "xauusd", "xau"]},
    {"key": "BTC", "label": "BTC / BTCUSD",
     "aliases": ["btc/usd", "bitcoin", "btcusd", "btc"]},
]
ALL_MARKET_KEYS = [m["key"] for m in ALL_MARKETS]


@dataclass
class StrategyConfig:
    """Réglages de la stratégie (mutable : un menu Telegram pourra les changer)."""

    crt_tf: str = "M30"              # M15 | M30 | H1
    entry_tf: str = "M5"             # M5 (M1 possible)
    rr: float = 3.0                  # RR cible : TP = entrée ± risque x rr

    fvg_enabled: bool = True         # entrée sur retest d'une FVG
    direct_entry: bool = False       # entrée directe sur confirmation M5 (mode secours, OFF par défaut)
    direct_fallback_bars: int = 12   # FVG ON + DIRECT ON : bougies d'entrée à attendre sans FVG avant l'entrée directe

    trend_filter: str = "OFF"        # OFF | EMA | STRUCTURE
    trend_tf: str = ""               # vide = même timeframe que le CRT
    ema_fast: int = 50
    ema_slow: int = 200
    structure_swing_length: int = 3

    break_even: str = "OFF"          # OFF | 1 | 1.5 | 2 (RR auquel le SL passe à l'entrée)
    sl_buffer: float = 0.0           # marge ajoutée au SL (en prix), 0 par défaut
    max_positions: int = 1           # trades paper ouverts simultanément, PAR MARCHÉ

    setup_expiry_crt_candles: int = 2  # un setup expire après N bougies CRT sans entrée

    # --- utilitaires ---------------------------------------------------------

    @property
    def be_rr(self) -> float:
        value = str(self.break_even).strip().upper().replace("RR", "")
        return 0.0 if value in ("", "OFF", "0") else float(value)

    @property
    def effective_trend_tf(self) -> str:
        return self.trend_tf or self.crt_tf

    def validate(self) -> None:
        if self.crt_tf not in CRT_TIMEFRAMES:
            raise ValueError(f"crt_tf doit être dans {CRT_TIMEFRAMES}, reçu {self.crt_tf!r}")
        if self.entry_tf not in ENTRY_TIMEFRAMES:
            raise ValueError(f"entry_tf doit être dans {ENTRY_TIMEFRAMES}, reçu {self.entry_tf!r}")
        if self.rr <= 0:
            raise ValueError("rr doit être > 0")
        if str(self.trend_filter).upper() not in ("OFF", "EMA", "STRUCTURE"):
            raise ValueError("trend_filter doit être OFF, EMA ou STRUCTURE")
        if self.effective_trend_tf not in TF_SECONDS:
            raise ValueError(f"trend_tf inconnu : {self.effective_trend_tf!r}")
        if self.ema_fast >= self.ema_slow:
            raise ValueError("ema_fast doit être < ema_slow")
        if self.be_rr < 0 or (self.be_rr and self.be_rr >= self.rr):
            raise ValueError("break_even doit être OFF ou un RR strictement inférieur à rr")
        if self.sl_buffer < 0:
            raise ValueError("sl_buffer doit être >= 0")
        if self.max_positions < 1:
            raise ValueError("max_positions doit être >= 1")
        if not self.fvg_enabled and not self.direct_entry:
            raise ValueError("fvg_enabled et direct_entry sont tous deux OFF : aucune entrée possible")

    @classmethod
    def from_dict(cls, data: dict) -> "StrategyConfig":
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"Paramètres de stratégie inconnus : {sorted(unknown)}")
        cfg = cls(**data)
        cfg.validate()
        return cfg


@dataclass(frozen=True)
class ConnectionSettings:
    deriv_app_id: str = field(default_factory=lambda: os.getenv("DERIV_APP_ID", "1089"))
    deriv_ws_base: str = "wss://ws.derivws.com/websockets/v3"
    history_candle_count: int = 500

    # Nouvelle API Deriv (authentifiée par PAT). AUCUN secret en dur : tout vient de .env.
    #   DERIV_PAT         : Personal Access Token (pat_...)
    #   DERIV_PAT_APP_ID  : App ID de l'application PAT enregistrée sur developers.deriv.com
    #                       (les anciens App IDs, dont 1089, ne fonctionnent PAS avec la nouvelle API)
    #   DERIV_ACCOUNT_ID  : ID du compte (ex. DOT90004580) pour lequel demander l'OTP
    # Si l'un des trois manque -> repli automatique sur l'API publique historique.
    deriv_pat: str = field(default_factory=lambda: os.getenv("DERIV_PAT", "").strip())
    deriv_pat_app_id: str = field(default_factory=lambda: os.getenv("DERIV_PAT_APP_ID", "").strip())
    deriv_account_id: str = field(default_factory=lambda: os.getenv("DERIV_ACCOUNT_ID", "").strip())
    deriv_rest_base: str = "https://api.derivws.com"

    # Reconnexion : délai progressif 5s -> 10s -> 20s -> 30s -> 60s, puis 60s
    # en boucle tant que la connexion n'est pas rétablie.
    reconnect_delays: tuple[float, ...] = (5.0, 10.0, 20.0, 30.0, 60.0)
    heartbeat_interval_sec: float = 20.0
    heartbeat_timeout_sec: float = 45.0

    @property
    def deriv_ws_url(self) -> str:
        return f"{self.deriv_ws_base}?app_id={self.deriv_app_id}"

    @property
    def pat_enabled(self) -> bool:
        return bool(self.deriv_pat and self.deriv_pat_app_id and self.deriv_account_id)

    @property
    def otp_url(self) -> str:
        return f"{self.deriv_rest_base}/trading/v1/options/accounts/{self.deriv_account_id}/otp"


@dataclass
class AppConfig:
    strategy: StrategyConfig
    markets: list[str]
    data_dir: Path
    history_candle_count: int = 500
    telegram_enabled: bool = False       # jetons/chat_id lus depuis .env, jamais depuis config.json
    chart_enabled: bool = True           # image PNG annotée à chaque entrée (nécessite matplotlib)
    report_hour: int = 21                # heure locale du serveur pour le rapport journalier
    report_minute: int = 0
    # Override de strategy.max_positions PAR MARCHÉ, ex. {"V75": 2, "GOLD": 1}.
    # Un marché absent de ce mapping utilise strategy.max_positions (comportement inchangé).
    max_positions_by_market: dict[str, int] = field(default_factory=dict)


def load_app_config(path: Path = CONFIG_PATH) -> AppConfig:
    raw: dict = {}
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

    strategy = StrategyConfig.from_dict(raw.get("strategy", {}))

    markets = raw.get("markets", ALL_MARKET_KEYS)
    bad = [m for m in markets if m not in ALL_MARKET_KEYS]
    if bad:
        raise ValueError(f"Marchés non supportés : {bad}. Autorisés : {ALL_MARKET_KEYS}")

    data_dir = Path(raw.get("data_dir", "data"))
    if not data_dir.is_absolute():
        data_dir = BASE_DIR / data_dir

    telegram = raw.get("telegram", {})
    report = raw.get("report", {})
    report_hour = int(report.get("hour", 21))
    report_minute = int(report.get("minute", 0))
    if not (0 <= report_hour <= 23 and 0 <= report_minute <= 59):
        raise ValueError("report.hour doit être 0-23 et report.minute 0-59")

    max_positions_by_market = raw.get("max_positions_by_market", {})
    bad_mp_keys = [k for k in max_positions_by_market if k not in ALL_MARKET_KEYS]
    if bad_mp_keys:
        raise ValueError(f"max_positions_by_market : marchés inconnus {bad_mp_keys}")
    max_positions_by_market = {k: int(v) for k, v in max_positions_by_market.items()}
    if any(v < 1 for v in max_positions_by_market.values()):
        raise ValueError("max_positions_by_market : toutes les valeurs doivent être >= 1")

    return AppConfig(strategy=strategy, markets=list(markets), data_dir=data_dir,
                     history_candle_count=int(raw.get("history_candle_count", 500)),
                     telegram_enabled=bool(telegram.get("enabled", False)),
                     chart_enabled=bool(raw.get("chart_enabled", True)),
                     report_hour=report_hour, report_minute=report_minute,
                     max_positions_by_market=max_positions_by_market)


# Modèle de config.json (ancien config_example.json), intégré pour le fichier unique.
# Écrit tel quel par `python3 main.py --init-config`.
EXAMPLE_CONFIG_JSON = '''{
  "markets": ["V75", "V25", "GOLD", "BTC"],
  "history_candle_count": 500,
  "data_dir": "data",
  "chart_enabled": true,
  "telegram": {
    "enabled": false
  },
  "report": {
    "hour": 21,
    "minute": 0
  },
  "max_positions_by_market": {},
  "strategy": {
    "crt_tf": "M30",
    "entry_tf": "M5",
    "rr": 3,
    "fvg_enabled": true,
    "direct_entry": false,
    "direct_fallback_bars": 12,
    "trend_filter": "OFF",
    "trend_tf": "",
    "ema_fast": 50,
    "ema_slow": 200,
    "structure_swing_length": 3,
    "break_even": "OFF",
    "sl_buffer": 0,
    "max_positions": 1,
    "setup_expiry_crt_candles": 2
  }
}
'''


def write_example_config(path: Path) -> bool:
    """Écrit le modèle de config s'il n'existe pas (jamais d'écrasement). True si écrit."""
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(EXAMPLE_CONFIG_JSON, encoding="utf-8")
    return True


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s UTC | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.Formatter.converter = _time.gmtime


# =============================================================================
# 2. CLIENT WEBSOCKET DERIV
# =============================================================================

log_client = logging.getLogger("deriv.client")

MessageHandler = Callable[[dict], Awaitable[None]]


class DerivClient:
    def __init__(self, settings: ConnectionSettings):
        self.settings = settings
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._req_id_counter = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._msg_type_handlers: dict[str, list[MessageHandler]] = {}
        self._subscribed_requests: list[dict] = []
        self._connected_event = asyncio.Event()
        self._stop = False
        self._last_message_at = 0.0

    def on(self, msg_type: str, handler: MessageHandler) -> None:
        self._msg_type_handlers.setdefault(msg_type, []).append(handler)

    async def run_forever(self) -> None:
        """Boucle de connexion permanente : ne se termine jamais d'elle-même
        (seul self.stop() l'arrête). Toute erreur — réseau, protocole, ou
        inattendue — déclenche une reconnexion avec palier progressif, sans
        jamais laisser une exception remonter et tuer le programme."""
        attempt = 0
        while not self._stop:
            try:
                await self._run_once()
                attempt = 0   # connexion réussie : on repart du 1er palier au prochain incident
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                log_client.warning("Connexion Deriv perdue (%s).", exc)
            except Exception:
                log_client.exception("Erreur inattendue dans la boucle WebSocket (le bot continue).")
            finally:
                self._connected_event.clear()
                self._ws = None
                self._fail_all_pending("connexion fermée")

            if self._stop:
                break
            delays = self.settings.reconnect_delays
            delay = delays[min(attempt, len(delays) - 1)]
            log_client.info("Reconnexion dans %.0fs (tentative %d).", delay, attempt + 1)
            await asyncio.sleep(delay)
            attempt += 1

    async def stop(self) -> None:
        self._stop = True
        if self._ws is not None:
            await self._ws.close()

    async def request(self, payload: dict, timeout: float = 15.0) -> dict:
        await self._connected_event.wait()
        req_id = next(self._req_id_counter)
        payload = dict(payload, req_id=req_id)
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[req_id] = fut
        await self._send(payload)
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(req_id, None)

    async def subscribe(self, payload: dict) -> None:
        self._subscribed_requests.append(payload)
        await self._connected_event.wait()
        await self._send(dict(payload, req_id=next(self._req_id_counter)))

    async def _send(self, payload: dict) -> None:
        if self._ws is None:
            raise ConnectionClosed(None, None)
        await self._ws.send(json.dumps(payload))

    def _fetch_otp_url(self) -> str:
        """POST /otp avec le PAT -> URL WebSocket authentifiée (OTP à usage unique,
        valable 120 s : on en redemande un à chaque (re)connexion). Bloquant : appelé via to_thread."""
        st = self.settings
        if requests is None:
            raise RuntimeError("le paquet 'requests' est requis pour l'authentification PAT")
        resp = requests.post(
            st.otp_url,
            headers={"Authorization": f"Bearer {st.deriv_pat}", "Deriv-App-ID": st.deriv_pat_app_id},
            timeout=10,
        )
        if resp.status_code != 200:
            # jamais de token dans les logs : on n'affiche que le code HTTP et le message serveur
            raise ConnectionError(f"OTP Deriv refusé (HTTP {resp.status_code}) : {resp.text[:200]}")
        url = (resp.json().get("data") or {}).get("url")
        if not url:
            raise ConnectionError("réponse OTP Deriv sans champ data.url")
        return url

    async def _resolve_ws_url(self) -> str:
        if self.settings.pat_enabled:
            return await asyncio.to_thread(self._fetch_otp_url)
        return self.settings.deriv_ws_url

    async def _run_once(self) -> None:
        url = await self._resolve_ws_url()
        log_client.info("Connexion à Deriv (%s) : %s",
                        "PAT/OTP" if self.settings.pat_enabled else "API publique",
                        url.split("?")[0])
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            self._ws = ws
            self._connected_event.set()
            self._last_message_at = asyncio.get_event_loop().time()
            log_client.info("Connecté à Deriv.")

            await self._resubscribe_all()

            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            try:
                async for raw in ws:
                    self._last_message_at = asyncio.get_event_loop().time()
                    await self._handle_raw_message(raw)
            finally:
                heartbeat_task.cancel()

    async def _resubscribe_all(self) -> None:
        for payload in self._subscribed_requests:
            try:
                await self._send(dict(payload, req_id=next(self._req_id_counter)))
            except Exception:
                log_client.exception("Échec de réabonnement pour %s", payload)

    async def _heartbeat_loop(self) -> None:
        interval = self.settings.heartbeat_interval_sec
        timeout = self.settings.heartbeat_timeout_sec
        try:
            while True:
                await asyncio.sleep(interval)
                if self._ws is None:
                    return
                idle = asyncio.get_event_loop().time() - self._last_message_at
                if idle > timeout:
                    log_client.warning("Heartbeat : aucune donnée reçue depuis %.0fs, connexion morte.", idle)
                    await self._ws.close()
                    return
                try:
                    await self._send({"ping": 1, "req_id": next(self._req_id_counter)})
                except Exception:
                    return
        except asyncio.CancelledError:
            return

    async def _handle_raw_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log_client.error("Message non-JSON reçu, ignoré.")
            return

        if msg.get("error"):
            log_client.error("Erreur API Deriv : %s", msg["error"])

        req_id = msg.get("req_id")
        fut = self._pending.get(req_id) if req_id is not None else None
        if fut is not None and not fut.done():
            fut.set_result(msg)

        msg_type = msg.get("msg_type")
        for handler in self._msg_type_handlers.get(msg_type, []):
            try:
                await handler(msg)
            except Exception:
                log_client.exception("Erreur dans un handler pour msg_type=%s", msg_type)

    def _fail_all_pending(self, reason: str) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError(reason))
        self._pending.clear()


# =============================================================================
# 3. DÉCOUVERTE DES SYMBOLES (active_symbols)
# =============================================================================

log_symbols = logging.getLogger("deriv.symbols")

_FUZZY_THRESHOLD = 0.55


@dataclass
class MarketSymbol:
    key: str
    label: str
    display_name: str
    symbol: str
    market: str
    available: bool


def _normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9 /]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _match_score(aliases: list[str], candidate_name: str) -> float:
    norm_candidate = _normalize(candidate_name)
    best = 0.0
    for alias in aliases:
        norm_alias = _normalize(alias)
        if norm_alias == norm_candidate:
            return 1.0
        if norm_alias in norm_candidate or norm_candidate in norm_alias:
            best = max(best, 0.9)
        ratio = difflib.SequenceMatcher(None, norm_alias, norm_candidate).ratio()
        best = max(best, ratio)
    return best


async def discover_symbols(client: DerivClient, market_definitions: list[dict]) -> dict[str, MarketSymbol]:
    """Interroge active_symbols et retourne {key: MarketSymbol} pour les
    marchés trouvés. Les marchés introuvables sont journalisés et omis :
    le bot continue avec les marchés disponibles."""
    response = await client.request({"active_symbols": "brief", "product_type": "basic"})

    if response.get("error"):
        log_symbols.error("active_symbols a échoué : %s", response["error"])
        return {}

    raw_symbols = response.get("active_symbols", [])
    log_symbols.info("active_symbols : %d marchés actifs reçus depuis Deriv.", len(raw_symbols))

    result: dict[str, MarketSymbol] = {}

    for market_def in market_definitions:
        key = market_def["key"]
        label = market_def["label"]
        aliases = market_def["aliases"]

        excludes = [t.lower() for t in market_def.get("exclude", [])]

        best_entry = None
        best_score = 0.0
        for entry in raw_symbols:
            haystack = f"{entry.get('display_name', '')} {entry.get('symbol', '')}".lower()
            if any(tok in haystack for tok in excludes):
                continue
            candidate_names = [entry.get("display_name", ""), entry.get("symbol", "")]
            score = max(_match_score(aliases, name) for name in candidate_names if name)
            if score > best_score:
                best_score = score
                best_entry = entry

        if best_entry is not None and best_score >= _FUZZY_THRESHOLD:
            market_symbol = MarketSymbol(
                key=key, label=label,
                display_name=best_entry.get("display_name", ""),
                symbol=best_entry.get("symbol", ""),
                market=best_entry.get("market", ""),
                available=bool(best_entry.get("exchange_is_open", 1))
                and not bool(best_entry.get("is_trading_suspended", 0)),
            )
            result[key] = market_symbol
            log_symbols.info("%s -> symbol réel : %s (display_name=%r, score=%.2f)",
                              label, market_symbol.symbol, market_symbol.display_name, best_score)
        else:
            log_symbols.warning("[MARCHÉ INDISPONIBLE] %s", label)

    return result


# =============================================================================
# 4. TICKS
# =============================================================================

log_ticks = logging.getLogger("deriv.ticks")

TickListener = Callable[[str, "Tick"], Awaitable[None]]


@dataclass(frozen=True)
class Tick:
    symbol: str
    quote: float
    epoch: int
    pip_size: Optional[float] = None


class TickCache:
    def __init__(self, client: DerivClient):
        self.client = client
        self._latest: dict[str, Tick] = {}
        self._listeners: list[TickListener] = []
        self.client.on("tick", self._on_tick_message)

    def add_listener(self, listener: TickListener) -> None:
        self._listeners.append(listener)

    async def subscribe(self, symbols: list[str]) -> None:
        for symbol in symbols:
            await self.client.subscribe({"ticks": symbol, "subscribe": 1})
            log_ticks.info("Abonné aux ticks de %s", symbol)

    def latest(self, symbol: str) -> Optional[Tick]:
        return self._latest.get(symbol)

    async def _on_tick_message(self, msg: dict) -> None:
        raw = msg.get("tick")
        if not raw:
            return
        tick = Tick(symbol=raw["symbol"], quote=float(raw["quote"]),
                    epoch=int(raw["epoch"]), pip_size=raw.get("pip_size"))
        self._latest[tick.symbol] = tick
        log_ticks.debug("%s\nLTP = %s", tick.symbol, tick.quote)

        for listener in self._listeners:
            await listener(tick.symbol, tick)


# =============================================================================
# 5. BOUGIES (construites depuis les ticks, jamais de bougie en cours utilisée)
# =============================================================================

log_candles = logging.getLogger("deriv.candles")

CloseListener = Callable[[str, str, "Candle"], Awaitable[None]]


@dataclass
class Candle:
    epoch: int
    open: float
    high: float
    low: float
    close: float

    def update(self, price: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price


class CandleStore:
    def __init__(self, client: DerivClient, tick_cache: TickCache, max_buffer: int = 1000):
        self.client = client
        self.tick_cache = tick_cache
        self.max_buffer = max_buffer

        self._closed: dict[str, dict[str, deque]] = defaultdict(dict)
        self._forming: dict[str, dict[str, Optional[Candle]]] = defaultdict(dict)
        self._close_listeners: list[CloseListener] = []
        self._timeframes_by_symbol: dict[str, dict[str, int]] = {}

        self.tick_cache.add_listener(self._on_tick)

    def add_close_listener(self, listener: CloseListener) -> None:
        self._close_listeners.append(listener)

    def get_closed_candles(self, symbol: str, timeframe: str, n: Optional[int] = None) -> list[Candle]:
        buf = self._closed.get(symbol, {}).get(timeframe, deque())
        data = list(buf)
        return data[-n:] if n else data

    def get_forming_candle(self, symbol: str, timeframe: str) -> Optional[Candle]:
        return self._forming.get(symbol, {}).get(timeframe)

    async def load_history(self, symbol: str, timeframes: dict[str, int], count: int) -> None:
        for tf_name, granularity in timeframes.items():
            response = await self.client.request({
                "ticks_history": symbol, "style": "candles", "granularity": granularity,
                "count": count, "end": "latest", "subscribe": 0,
            })

            if response.get("error"):
                log_candles.error("Historique indisponible pour %s/%s : %s",
                                   symbol, tf_name, response["error"])
                self._closed[symbol][tf_name] = deque(maxlen=self.max_buffer)
                continue

            raw_candles = response.get("candles", [])
            candles = [
                Candle(epoch=int(c["epoch"]), open=float(c["open"]),
                       high=float(c["high"]), low=float(c["low"]), close=float(c["close"]))
                for c in raw_candles
            ]
            # La dernière bougie renvoyée est la bougie encore ouverte côté serveur :
            # jamais gardée comme "clôturée". On la garde comme bougie "en cours" (avec
            # son OHLC serveur) pour que les ticks la complètent, au lieu de repartir
            # d'une bougie vide qui serait partielle à sa clôture.
            forming = candles.pop() if candles else None

            self._closed[symbol][tf_name] = deque(candles, maxlen=self.max_buffer)
            self._forming[symbol][tf_name] = forming
            log_candles.info("Historique chargé : %s/%s -> %d bougies clôturées.",
                              symbol, tf_name, len(candles))

    async def _on_tick(self, symbol: str, tick: Tick) -> None:
        timeframes = self._forming.get(symbol)
        if timeframes is None:
            return

        for tf_name, forming in list(timeframes.items()):
            granularity = self._granularity_for(symbol, tf_name)
            if granularity is None:
                continue
            bucket_epoch = tick.epoch - (tick.epoch % granularity)

            if forming is None:
                self._forming[symbol][tf_name] = Candle(
                    epoch=bucket_epoch, open=tick.quote, high=tick.quote,
                    low=tick.quote, close=tick.quote)
                continue

            if bucket_epoch < forming.epoch:
                log_candles.debug("Tick hors-ordre ignoré pour %s/%s", symbol, tf_name)
                continue

            if bucket_epoch == forming.epoch:
                forming.update(tick.quote)
                continue

            closed_candle = forming
            self._closed[symbol].setdefault(
                tf_name, deque(maxlen=self.max_buffer)).append(closed_candle)
            self._forming[symbol][tf_name] = Candle(
                epoch=bucket_epoch, open=tick.quote, high=tick.quote,
                low=tick.quote, close=tick.quote)

            for listener in self._close_listeners:
                await listener(symbol, tf_name, closed_candle)

    def register_timeframes(self, symbol: str, timeframes: dict[str, int]) -> None:
        self._timeframes_by_symbol[symbol] = timeframes
        forming = self._forming.setdefault(symbol, {})
        for tf in timeframes:
            forming.setdefault(tf, None)

    def _granularity_for(self, symbol: str, tf_name: str) -> Optional[int]:
        return self._timeframes_by_symbol.get(symbol, {}).get(tf_name)


# =============================================================================
# 6. LOGIQUE PURE (sans I/O, entièrement testable)
# =============================================================================

def iso(epoch: Optional[int]) -> Optional[str]:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return f"{x:.4f}".rstrip("0").rstrip(".")


def fmt_r(k: float) -> str:
    return f"{k:g}"


# ---- 6.1 CRT ----------------------------------------------------------------

def detect_crt(c1: Candle, c2: Candle) -> Optional[str]:
    """c1 = bougie 1 (range), c2 = bougie 2 (sweep) : les deux CLÔTURÉES.

    SELL : HIGH_2 > HIGH_1 puis CLOSE_2 < HIGH_1
    BUY  : LOW_2  < LOW_1  puis CLOSE_2 > LOW_1

    Si c2 balaie les DEUX côtés (outside bar) et clôture dans le range, le
    sens est ambigu : aucun signal (None)."""
    sell = c2.high > c1.high and c2.close < c1.high
    buy = c2.low < c1.low and c2.close > c1.low
    if sell and buy:
        return None
    if sell:
        return "sell"
    if buy:
        return "buy"
    return None


# ---- 6.2 FVG (imbalance) -----------------------------------------------------

def detect_fvg(c1: Candle, c2: Candle, c3: Candle, direction: str) -> Optional[tuple[float, float]]:
    """Retourne (bas, haut) de la FVG dans le sens du trade, sinon None.

    SELL : FVG baissière  -> HIGH_3 < LOW_1   ; zone [HIGH_3, LOW_1]
    BUY  : FVG haussière  -> LOW_3  > HIGH_1  ; zone [HIGH_1, LOW_3]"""
    if direction == "sell" and c3.high < c1.low:
        return (c3.high, c1.low)
    if direction == "buy" and c3.low > c1.high:
        return (c1.high, c3.low)
    return None


def m5_confirmation(candles: list[Candle], direction: str, lookback: int = 2) -> bool:
    """Mode d'entrée directe : la dernière bougie d'entrée clôturée va dans le
    sens du trade ET sort du micro-range des `lookback` bougies précédentes."""
    if len(candles) < lookback + 1:
        return False
    last = candles[-1]
    window = candles[-(lookback + 1):-1]
    if direction == "buy":
        return last.close > last.open and last.close > max(c.high for c in window)
    return last.close < last.open and last.close < min(c.low for c in window)


# ---- 6.3 Filtre de tendance (optionnel) --------------------------------------

def ema(values: list[float], period: int) -> Optional[float]:
    if period <= 0 or len(values) < period:
        return None
    k = 2.0 / (period + 1)
    value = sum(values[:period]) / period
    for v in values[period:]:
        value = v * k + value * (1 - k)
    return value


def trend_from_ema(candles: list[Candle], fast: int, slow: int) -> Optional[str]:
    closes = [c.close for c in candles]
    ef, es = ema(closes, fast), ema(closes, slow)
    if ef is None or es is None or ef == es:
        return None
    return "buy" if ef > es else "sell"


def detect_swings(candles: list[Candle], length: int) -> tuple[list[float], list[float]]:
    """Swings confirmés (fractales strictes sur `length` bougies de chaque côté)."""
    highs: list[float] = []
    lows: list[float] = []
    for i in range(length, len(candles) - length):
        neighbours = candles[i - length:i] + candles[i + 1:i + length + 1]
        if all(candles[i].high > n.high for n in neighbours):
            highs.append(candles[i].high)
        if all(candles[i].low < n.low for n in neighbours):
            lows.append(candles[i].low)
    return highs, lows


def trend_from_structure(candles: list[Candle], length: int) -> Optional[str]:
    highs, lows = detect_swings(candles, length)
    if len(highs) < 2 or len(lows) < 2:
        return None
    if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
        return "buy"
    if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
        return "sell"
    return None


def trend_allows(cfg: StrategyConfig, direction: str, candles: list[Candle]) -> tuple[bool, str]:
    """Le filtre ne fabrique JAMAIS un signal : il autorise ou bloque un signal CRT."""
    mode = str(cfg.trend_filter).upper()
    if mode == "OFF":
        return True, "OFF"
    if mode == "EMA":
        trend = trend_from_ema(candles, cfg.ema_fast, cfg.ema_slow)
    else:
        trend = trend_from_structure(candles, cfg.structure_swing_length)
    if trend is None:
        return False, f"{mode}: tendance indéterminée"
    return trend == direction, f"{mode}: {trend}"


# =============================================================================
# 7. TRADE PAPER + STATISTIQUES
# =============================================================================

@dataclass
class PaperTrade:
    id: str
    market_key: str
    label: str
    symbol: str
    direction: str                 # "buy" | "sell"
    crt_tf: str
    entry_tf: str
    mode: str                      # "FVG" | "DIRECT"
    trend_filter: str

    c1_epoch: int
    c1_high: float
    c1_low: float
    sweep_epoch: int
    sweep_high: float
    sweep_low: float
    fvg_low: Optional[float]
    fvg_high: Optional[float]

    entry_epoch: int
    entry: float
    sl: float
    tp: float
    risk: float
    rr_target: float
    be_rr: float = 0.0

    sl_current: float = 0.0
    be_active: bool = False
    rr_hits: dict = field(default_factory=dict)   # "1" -> epoch
    max_r: float = 0.0
    status: str = "OPEN"           # OPEN | WIN | LOSS | BE
    close_epoch: Optional[int] = None
    close_price: Optional[float] = None
    r_result: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.sl_current:
            self.sl_current = self.sl

    @property
    def sign(self) -> int:
        return 1 if self.direction == "buy" else -1

    @property
    def milestones(self) -> list[float]:
        """RR1, RR2, ... jusqu'à la cible (la cible = TP)."""
        marks = {float(k) for k in range(1, int(self.rr_target) + 1)}
        marks.add(float(self.rr_target))
        return sorted(marks)

    def r_at(self, price: float) -> float:
        return self.sign * (price - self.entry) / self.risk

    def on_price(self, price: float, epoch: int) -> list[tuple[str, Any]]:
        """Fait avancer le suivi virtuel. Retourne les événements produits :
        ("RR", k) | ("BE_ACTIVATED", rr) | ("CLOSE", "WIN"|"LOSS"|"BE")."""
        events: list[tuple[str, Any]] = []
        if self.status != "OPEN":
            return events

        r_now = self.r_at(price)
        self.max_r = max(self.max_r, r_now)

        # 1) stop (SL initial, ou entrée si break-even actif)
        if self.sign * (price - self.sl_current) <= 0:
            self.status = "BE" if self.be_active else "LOSS"
            self.r_result = 0.0 if self.be_active else -1.0
            self.close_epoch, self.close_price = epoch, self.sl_current
            events.append(("CLOSE", self.status))
            return events

        # 2) break-even
        if self.be_rr and not self.be_active and r_now >= self.be_rr:
            self.be_active = True
            self.sl_current = self.entry
            events.append(("BE_ACTIVATED", self.be_rr))

        # 3) jalons RR (plusieurs peuvent tomber sur le même tick si gap)
        for k in self.milestones:
            key = fmt_r(k)
            if key not in self.rr_hits and r_now >= k:
                self.rr_hits[key] = epoch
                events.append(("RR", k))

        # 4) TP = dernier jalon
        if r_now >= self.rr_target:
            self.status = "WIN"
            self.r_result = float(self.rr_target)
            self.close_epoch, self.close_price = epoch, self.tp
            events.append(("CLOSE", "WIN"))
        return events

    def to_record(self) -> dict:
        hits = self.rr_hits
        return {
            "id": self.id,
            "market": self.label,
            "market_key": self.market_key,
            "symbol": self.symbol,
            "direction": self.direction,
            "crt_timeframe": self.crt_tf,
            "entry_timeframe": self.entry_tf,
            "mode": self.mode,
            "trend_filter": self.trend_filter,
            "candle_1_time": iso(self.c1_epoch),
            "candle_1_high": self.c1_high,
            "candle_1_low": self.c1_low,
            "sweep_time": iso(self.sweep_epoch),
            "sweep_high": self.sweep_high,
            "sweep_low": self.sweep_low,
            "fvg_high": self.fvg_high,
            "fvg_low": self.fvg_low,
            "entry_time": iso(self.entry_epoch),
            "entry": self.entry,
            "sl": self.sl,
            "tp": self.tp,
            "risk": self.risk,
            "rr_target": self.rr_target,
            "rr1_time": iso(hits.get("1")),
            "rr2_time": iso(hits.get("2")),
            "rr3_time": iso(hits.get("3")),
            "max_rr": round(self.max_r, 2),
            "close_time": iso(self.close_epoch),
            "close_price": self.close_price,
            "result": self.status,
            "r_result": self.r_result,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PaperTrade":
        return cls(**data)


def compute_stats(records: list[dict]) -> dict[str, dict]:
    """Statistiques PAR MARCHÉ (+ ligne ALL) à partir des trades clôturés."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        buckets[rec["market"]].append(rec)
        buckets["ALL"].append(rec)

    out: dict[str, dict] = {}
    for name, recs in buckets.items():
        wins = sum(1 for r in recs if r["result"] == "WIN")
        losses = sum(1 for r in recs if r["result"] == "LOSS")
        be = sum(1 for r in recs if r["result"] == "BE")
        total_r = sum(float(r.get("r_result") or 0.0) for r in recs)
        n = len(recs)
        out[name] = {
            "trades": n, "wins": wins, "losses": losses, "be": be,
            "winrate": (100.0 * wins / n) if n else 0.0,
            "total_r": total_r,
            "avg_r": (total_r / n) if n else 0.0,
        }
    return out


def format_stats(stats: dict[str, dict]) -> str:
    if not stats:
        return "Aucun trade clôturé pour le moment."
    lines = [f"{'MARCHÉ':<24}{'TRADES':>7}{'WIN':>5}{'LOSS':>6}{'BE':>4}{'WR%':>7}{'TOTAL R':>9}{'AVG R':>8}"]
    for name in sorted(stats, key=lambda n: (n == "ALL", n)):
        s = stats[name]
        lines.append(f"{name:<24}{s['trades']:>7}{s['wins']:>5}{s['losses']:>6}{s['be']:>4}"
                     f"{s['winrate']:>7.1f}{s['total_r']:>9.2f}{s['avg_r']:>8.2f}")
    return "\n".join(lines)


def format_daily_report(date_label: str, market_keys: list[str],
                        signals_today: list[dict], closed_today: list[dict]) -> str:
    """Rapport façon Telegram (voir doc de specs) à partir de :
    - signals_today : lignes de signals.jsonl filtrées sur la date (TOUS les signaux, même encore ouverts)
    - closed_today  : lignes de trades.jsonl filtrées sur la date d'ENTRÉE (résultat connu uniquement
                      pour ceux déjà clôturés ; un trade encore ouvert compte dans "Total" mais pas
                      dans WIN/LOSS/winrate tant qu'il n'est pas clôturé)."""
    total = len(signals_today)
    buy = sum(1 for s in signals_today if s["direction"] == "BUY")
    sell = sum(1 for s in signals_today if s["direction"] == "SELL")

    wins = sum(1 for c in closed_today if c["result"] == "WIN")
    losses = sum(1 for c in closed_today if c["result"] == "LOSS")
    be = sum(1 for c in closed_today if c["result"] == "BE")
    decided = wins + losses          # le BE n'entre ni au numérateur ni au dénominateur du winrate
    winrate = (100.0 * wins / decided) if decided else 0.0
    avg_r = (sum(float(c.get("r_result") or 0.0) for c in closed_today) / len(closed_today)) if closed_today else 0.0

    rr1 = sum(1 for c in closed_today if c.get("rr1_time"))
    rr2 = sum(1 for c in closed_today if c.get("rr2_time"))
    rr3 = sum(1 for c in closed_today if c.get("rr3_time"))

    lines = [
        "📊 RAPPORT JOURNALIER", f"📅 {date_label}", "",
        "MARCHÉS", *market_keys, "",
        "━━━━━━━━━━━━━━", "",
        "SIGNALS", f"Total : {total}", "",
        f"BUY : {buy}", f"SELL : {sell}", "",
        f"WIN : {wins}", f"LOSS : {losses}" + (f" | BE : {be}" if be else ""), "",
        f"Winrate : {winrate:.1f}%", "",
        f"RR moyen : {avg_r:.1f}", "",
        f"RR1 atteint : {rr1}", f"RR2 atteint : {rr2}", f"RR3 atteint : {rr3}",
        "", "━━━━━━━━━━━━━━",
    ]
    for key in market_keys:
        m_signals = [s for s in signals_today if s["market_key"] == key]
        m_closed = [c for c in closed_today if c.get("market_key") == key]
        m_wins = sum(1 for c in m_closed if c["result"] == "WIN")
        m_losses = sum(1 for c in m_closed if c["result"] == "LOSS")
        lines += ["", key, f"Signaux : {len(m_signals)}", f"WIN : {m_wins}", f"LOSS : {m_losses}"]
    return "\n".join(lines)


def _next_report_time(now: datetime, hour: int, minute: int) -> datetime:
    """Prochaine occurrence de hour:minute strictement après `now` (heure locale, naïve)."""
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


# =============================================================================
# 8. NOTIFICATIONS + JOURNAL (fichiers)
# =============================================================================

log_sig = logging.getLogger("signals")


class Notifier:
    """Point d'accroche des messages. V1 : console + data/signals.log.
    `photo_path`, si fourni, est le chemin d'une image PNG à joindre (voir
    render_signal_chart) ; la classe de base se contente de le noter dans le
    journal local. Voir TelegramNotifier pour l'envoi réel sur Telegram."""

    def __init__(self, path: Optional[Path] = None):
        self.path = path
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, text: str, photo_path: Optional[Path] = None) -> None:
        log_sig.info("\n%s", text)
        if self.path is not None:
            suffix = f" [image: {photo_path}]" if photo_path else ""
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC]{suffix}\n{text}\n\n")


class TelegramNotifier(Notifier):
    """Envoie chaque message sur Telegram (texte seul, ou texte + photo).

    Nécessite TELEGRAM_BOT_TOKEN et TELEGRAM_CHAT_ID dans l'environnement
    (.env). Pour obtenir chat_id : parler au bot puis ouvrir
    https://api.telegram.org/bot<token>/getUpdates et lire "chat":{"id":...}.

    Un échec réseau est journalisé mais ne fait jamais planter le moteur —
    le paper trading continue même si Telegram est indisponible."""

    API_BASE = "https://api.telegram.org"
    MAX_TEXT = 4096
    MAX_CAPTION = 1024

    def __init__(self, bot_token: str, chat_id: str, path: Optional[Path] = None, timeout: float = 15.0):
        super().__init__(path)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout = timeout

    def send(self, text: str, photo_path: Optional[Path] = None) -> None:
        super().send(text, photo_path)   # trace locale conservée dans tous les cas
        if requests is None:
            log_sig.warning("Telegram demandé mais le paquet 'requests' est absent : pip install requests")
            return
        try:
            if photo_path is not None and Path(photo_path).exists():
                self._send_photo(text, Path(photo_path))
            else:
                self._send_message(text)
        except Exception as exc:  # pragma: no cover - dépend du réseau
            log_sig.warning("Envoi Telegram échoué : %s", exc)

    def _send_message(self, text: str) -> None:
        resp = requests.post(f"{self.API_BASE}/bot{self.bot_token}/sendMessage",
                              data={"chat_id": self.chat_id, "text": text[: self.MAX_TEXT]},
                              timeout=self.timeout)
        if not resp.ok:
            log_sig.warning("Telegram sendMessage %s : %s", resp.status_code, resp.text[:200])

    def _send_photo(self, caption: str, photo_path: Path) -> None:
        with open(photo_path, "rb") as f:
            resp = requests.post(f"{self.API_BASE}/bot{self.bot_token}/sendPhoto",
                                  data={"chat_id": self.chat_id, "caption": caption[: self.MAX_CAPTION]},
                                  files={"photo": f}, timeout=self.timeout)
        if not resp.ok:
            log_sig.warning("Telegram sendPhoto %s : %s", resp.status_code, resp.text[:200])


class Journal:
    """trades.jsonl : un trade clôturé par ligne (données pour backtest/stats).
    open_trades.json : trades paper encore ouverts (survivent à un redémarrage)."""

    def __init__(self, data_dir: Path):
        data_dir.mkdir(parents=True, exist_ok=True)
        self.trades_path = data_dir / "trades.jsonl"
        self.open_path = data_dir / "open_trades.json"
        self.signals_path = data_dir / "signals.jsonl"
        self._open: dict[str, list[dict]] = {}

    def append_signal(self, trade: PaperTrade) -> None:
        """Une ligne par ENTRÉE (contrairement à trades.jsonl qui n'a une ligne
        qu'à la CLÔTURE) : permet au rapport journalier de compter tous les
        signaux du jour, même ceux encore ouverts au moment du rapport."""
        rec = {
            "id": trade.id, "date": iso(trade.entry_epoch)[:10], "time": iso(trade.entry_epoch),
            "market": trade.label, "market_key": trade.market_key, "direction": trade.direction.upper(),
        }
        with open(self.signals_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def read_signals(self) -> list[dict]:
        if not self.signals_path.exists():
            return []
        out = []
        with open(self.signals_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out

    def append_closed(self, trade: PaperTrade) -> None:
        with open(self.trades_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(trade.to_record(), ensure_ascii=False) + "\n")

    def read_closed(self) -> list[dict]:
        if not self.trades_path.exists():
            return []
        out = []
        with open(self.trades_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out

    def save_open(self, market_key: str, trades: list[PaperTrade]) -> None:
        self._open[market_key] = [asdict(t) for t in trades]
        tmp = self.open_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._open, f, ensure_ascii=False)
        os.replace(tmp, self.open_path)

    def load_open(self) -> dict[str, list[dict]]:
        if not self.open_path.exists():
            return {}
        with open(self.open_path, "r", encoding="utf-8") as f:
            self._open = json.load(f)
        return {k: list(v) for k, v in self._open.items()}


# =============================================================================
# 8.5. IMAGE DE SIGNAL (matplotlib, optionnel)
# =============================================================================

log_chart = logging.getLogger("chart")


def _draw_candles(ax, candles: list[Candle]) -> None:
    """Bougies simples (mèche + corps) sur un axe indexé (pas un axe temporel)."""
    for i, c in enumerate(candles):
        up = c.close >= c.open
        color = "#2e7d32" if up else "#c62828"
        ax.vlines(i, c.low, c.high, color=color, linewidth=1)
        lo, hi = (c.open, c.close) if up else (c.close, c.open)
        height = max(hi - lo, (c.high - c.low) * 0.03 or 0.0001)
        ax.add_patch(Rectangle((i - 0.3, lo), 0.6, height, facecolor=color, edgecolor=color))
    if candles:
        step = max(1, len(candles) // 6)
        ticks = list(range(0, len(candles), step))
        ax.set_xticks(ticks)
        ax.set_xticklabels(
            [datetime.fromtimestamp(candles[i].epoch, tz=timezone.utc).strftime("%H:%M") for i in ticks],
            fontsize=7)
    ax.set_xlim(-1, max(len(candles), 1))
    ax.grid(axis="y", color="#eeeeee", linewidth=0.6)


def render_signal_chart(trade: "PaperTrade", crt_candles: list[Candle], entry_candles: list[Candle],
                        cfg: StrategyConfig, out_path: Path, crt_context: int = 10) -> Optional[Path]:
    """Génère le PNG annoté d'un signal : volet CRT_TF (range + sweep) en haut,
    volet ENTRY_TF (FVG + entrée + SL/TP/RR) en bas, plus la configuration du
    moment (point 9 de la spec). Best-effort : retourne None et journalise un
    avertissement en cas de souci (matplotlib absent, données incomplètes...),
    sans jamais interrompre le moteur."""
    if plt is None:
        log_chart.warning("matplotlib indisponible : image non générée pour %s.", trade.id)
        return None

    fig = None
    try:
        crt_view = [c for c in crt_candles if c.epoch <= trade.sweep_epoch][-crt_context:] or crt_candles[-crt_context:]
        window = [c for c in entry_candles if trade.sweep_epoch <= c.epoch <= trade.entry_epoch]
        tail = [c for c in entry_candles if c.epoch > trade.entry_epoch][:2]
        entry_view = (window + tail) or entry_candles[-10:]
        if not crt_view:
            crt_view = [Candle(trade.c1_epoch, trade.c1_high, trade.c1_high, trade.c1_low, trade.c1_low)]
        if not entry_view:
            entry_view = [Candle(trade.entry_epoch, trade.entry, trade.entry, trade.entry, trade.entry)]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 8), gridspec_kw={"height_ratios": [1, 1.4]})
        fig.suptitle(f"{trade.label} — {trade.direction.upper()} — CRT {trade.crt_tf} → {trade.entry_tf}",
                    fontsize=12, fontweight="bold")

        _draw_candles(ax1, crt_view)
        ax1.axhline(trade.c1_high, color="#607d8b", ls="--", lw=1)
        ax1.axhline(trade.c1_low, color="#607d8b", ls="--", lw=1)
        ax1.text(0, trade.c1_high, " HIGH CRT", fontsize=7, va="bottom", color="#607d8b")
        ax1.text(0, trade.c1_low, " LOW CRT", fontsize=7, va="top", color="#607d8b")
        ax1.set_title(f"CRT {trade.crt_tf} — range + sweep", fontsize=9, loc="left")

        _draw_candles(ax2, entry_view)
        if trade.fvg_low is not None and trade.fvg_high is not None:
            ax2.axhspan(trade.fvg_low, trade.fvg_high, color="#fbc02d", alpha=0.25)
            ax2.text(0, trade.fvg_high, " FVG", fontsize=7, va="bottom", color="#a17e00")
        entry_color = "#2e7d32" if trade.direction == "buy" else "#c62828"
        ax2.axhline(trade.entry, color=entry_color, lw=1.6)
        ax2.axhline(trade.sl, color="#c62828", ls="--", lw=1.2)
        ax2.axhline(trade.tp, color="#2e7d32", ls="--", lw=1.2)
        last_x = len(entry_view) - 1
        ax2.text(last_x, trade.entry, f" ENTRY {fmt_price(trade.entry)}", fontsize=7,
                 color=entry_color, ha="right", va="bottom")
        ax2.text(last_x, trade.sl, f" SL {fmt_price(trade.sl)}", fontsize=7, color="#c62828", ha="right", va="bottom")
        ax2.text(last_x, trade.tp, f" TP {fmt_price(trade.tp)}", fontsize=7, color="#2e7d32", ha="right", va="top")
        for k in trade.milestones:
            level = trade.entry + trade.sign * k * trade.risk
            ax2.axhline(level, color="#9e9e9e", ls=":", lw=0.8)
            ax2.text(0, level, f" RR{fmt_r(k)}", fontsize=6, color="#757575", va="bottom")
        ax2.set_title(f"{trade.entry_tf} — FVG / retest / entrée — RR cible 1:{fmt_r(trade.rr_target)}",
                     fontsize=9, loc="left")

        config_lines = [
            f"CRT: {cfg.crt_tf}", f"ENTRY: {cfg.entry_tf}",
            f"FVG: {'ON' if cfg.fvg_enabled else 'OFF'}",
            f"TREND: {str(cfg.trend_filter).upper()}",
            f"RR: {fmt_r(cfg.rr)}", f"BE: {cfg.break_even}",
        ]
        fig.text(0.995, 0.5, "\n".join(config_lines), fontsize=7, family="monospace",
                ha="right", va="center", bbox=dict(boxstyle="round", fc="#f5f5f5", ec="#bdbdbd"))

        fig.tight_layout(rect=(0, 0, 0.9, 0.96))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out_path, dpi=150)
        return out_path
    except Exception as exc:  # pragma: no cover - rendu best-effort, ne doit jamais interrompre le moteur
        log_chart.warning("Génération d'image échouée pour %s : %s", trade.id, exc)
        return None
    finally:
        if fig is not None:
            plt.close(fig)


# =============================================================================
# 9. MOTEUR PAR MARCHÉ
# =============================================================================

log_engine = logging.getLogger("engine")

CandleProvider = Callable[[str], list]   # timeframe -> bougies clôturées


@dataclass
class Setup:
    """Un CRT confirmé en attente d'entrée."""
    direction: str
    crt_tf: str
    c1_epoch: int
    c1_high: float
    c1_low: float
    sweep_epoch: int
    sweep_high: float
    sweep_low: float
    confirm_epoch: int            # instant de clôture de la bougie de sweep
    expires_epoch: int
    trend_state: str = "OFF"
    fvg_low: Optional[float] = None
    fvg_high: Optional[float] = None
    fvg_ready_epoch: int = 0      # instant de clôture de la 3e bougie de la FVG


class MarketEngine:
    """Machine à états d'UN marché, indépendante des autres :

        (rien) -> CRT confirmé -> [FVG détectée] -> retest -> trade paper -> RR1/RR2/RR3

    Entrées d'événements : on_candle_close(tf, candle) et on_price(price, epoch)."""

    def __init__(self, key: str, label: str, symbol: str, cfg: StrategyConfig,
                 get_candles: CandleProvider, notifier: Notifier, journal: Journal,
                 now_fn: Callable[[], float] = _time.time, chart_dir: Optional[Path] = None,
                 max_positions: Optional[int] = None):
        self.key, self.label, self.symbol = key, label, symbol
        self.cfg = cfg
        self.get_candles = get_candles
        self.notifier = notifier
        self.journal = journal
        self.now_fn = now_fn
        self.chart_dir = chart_dir   # None = pas d'image générée (défaut des tests)
        # max_positions : réglage global (cfg.max_positions) sauf override par marché
        # (voir AppConfig.max_positions_by_market / config.json).
        self.max_positions = cfg.max_positions if max_positions is None else max_positions

        self.setup: Optional[Setup] = None
        self.open_trades: list[PaperTrade] = []
        self._trade_seq = 0

    # ---- événements ---------------------------------------------------------

    def on_candle_close(self, tf: str, candle: Candle) -> None:
        if tf == self.cfg.crt_tf:
            self._on_crt_close(candle)
        if tf == self.cfg.entry_tf:
            self._on_entry_close(candle)

    def on_price(self, price: float, epoch: int) -> None:
        self._track_trades(price, epoch)

        s = self.setup
        if s is None or s.fvg_low is None or s.fvg_high is None:
            return
        if epoch < s.fvg_ready_epoch:
            return

        if epoch >= s.expires_epoch:
            log_engine.info("%s | setup %s expiré sans entrée.", self.label, s.direction.upper())
            self.setup = None
            return

        if self._sl_breached(s, price):
            log_engine.info("%s | setup %s annulé : le prix a atteint le niveau du sweep avant l'entrée.",
                            self.label, s.direction.upper())
            self.setup = None
            return

        touched = price >= s.fvg_low if s.direction == "sell" else price <= s.fvg_high
        if touched:
            self._open_trade(price, epoch, mode="FVG")

    # ---- CRT ----------------------------------------------------------------

    def _on_crt_close(self, candle: Candle) -> None:
        cfg = self.cfg
        gran = TF_SECONDS[cfg.crt_tf]
        candles = self.get_candles(cfg.crt_tf)
        if len(candles) < 2:
            return
        if candles[-1].epoch != candle.epoch:
            log_engine.warning("%s | CRT %s ignoré : historique incohérent.", self.label, cfg.crt_tf)
            return

        c1, c2 = candles[-2], candles[-1]
        if c2.epoch - c1.epoch != gran:
            log_engine.info("%s | CRT ignoré : bougies %s non consécutives.", self.label, cfg.crt_tf)
            return

        direction = detect_crt(c1, c2)
        if direction is None:
            log_engine.debug("%s | %s clôturée : pas de CRT.", self.label, cfg.crt_tf)
            return

        confirm_epoch = c2.epoch + gran
        expiry = cfg.setup_expiry_crt_candles * gran
        if self.now_fn() - confirm_epoch > expiry:
            log_engine.info("%s | CRT %s périmé (données anciennes), ignoré.", self.label, direction.upper())
            return

        trend_candles = self.get_candles(cfg.effective_trend_tf) if str(cfg.trend_filter).upper() != "OFF" else []
        allowed, trend_state = trend_allows(cfg, direction, trend_candles)
        if not allowed:
            log_engine.info("%s | CRT %s bloqué par le filtre de tendance (%s).",
                            self.label, direction.upper(), trend_state)
            return

        if self.setup is not None:
            log_engine.info("%s | ancien setup %s remplacé par le nouveau CRT.",
                            self.label, self.setup.direction.upper())

        self.setup = Setup(
            direction=direction, crt_tf=cfg.crt_tf,
            c1_epoch=c1.epoch, c1_high=c1.high, c1_low=c1.low,
            sweep_epoch=c2.epoch, sweep_high=c2.high, sweep_low=c2.low,
            confirm_epoch=confirm_epoch, expires_epoch=confirm_epoch + expiry,
            trend_state=trend_state,
        )
        log_engine.info("%s | CRT %s CONFIRMÉ sur %s (range %s–%s, sweep %s) | tendance: %s",
                        self.label, direction.upper(), cfg.crt_tf,
                        fmt_price(c1.low), fmt_price(c1.high),
                        fmt_price(c2.high if direction == "sell" else c2.low), trend_state)

        # ---- V1 : timeframe unique (crt_tf == entry_tf, ex. M5) ------------
        # CRT validé -> ENTRÉE DIRECTE immédiate, au prix disponible à l'instant
        # de la confirmation (clôture de la bougie de sweep). On n'attend ni
        # une nouvelle bougie, ni un retest FVG (FVG désactivée dans ce mode).
        if cfg.crt_tf == cfg.entry_tf and cfg.direct_entry and not cfg.fvg_enabled:
            self._open_trade(c2.close, confirm_epoch, mode="DIRECT")

    # ---- FVG / entrée directe -------------------------------------------------

    def _on_entry_close(self, candle: Candle) -> None:
        s = self.setup
        if s is None:
            return
        cfg = self.cfg
        gran = TF_SECONDS[cfg.entry_tf]

        if candle.epoch < s.confirm_epoch:
            return                      # bougie appartenant encore à la bougie de sweep
        if candle.epoch >= s.expires_epoch:
            log_engine.info("%s | setup %s expiré sans entrée.", self.label, s.direction.upper())
            self.setup = None
            return

        candles = self.get_candles(cfg.entry_tf)
        if not candles or candles[-1].epoch != candle.epoch:
            return

        # 1) FVG : les 3 bougies doivent s'ouvrir APRÈS la clôture de la bougie de sweep
        #    (une bougie M5 appartenant encore au sweep ne peut pas faire partie de la FVG)
        if cfg.fvg_enabled and s.fvg_low is None and len(candles) >= 3:
            c1, c2, c3 = candles[-3], candles[-2], candles[-1]
            consecutive = (c3.epoch - c1.epoch) == 2 * gran and c1.epoch >= s.confirm_epoch
            zone = detect_fvg(c1, c2, c3, s.direction) if consecutive else None
            if zone is not None:
                s.fvg_low, s.fvg_high = zone
                s.fvg_ready_epoch = candle.epoch + gran
                log_engine.info("%s | FVG %s détectée [%s – %s] : attente du retest.",
                                self.label, "baissière" if s.direction == "sell" else "haussière",
                                fmt_price(s.fvg_low), fmt_price(s.fvg_high))
                return

        # 2) Entrée directe (mode secours)
        if not cfg.direct_entry or s.fvg_low is not None:
            return
        bars_since = (candle.epoch - s.confirm_epoch) // gran + 1
        if cfg.fvg_enabled and bars_since < cfg.direct_fallback_bars:
            return
        if self._sl_breached(s, candle.close):
            log_engine.info("%s | setup %s annulé : le prix a atteint le niveau du sweep.",
                            self.label, s.direction.upper())
            self.setup = None
            return
        if m5_confirmation(candles, s.direction):
            self._open_trade(candle.close, candle.epoch + gran, mode="DIRECT")

    # ---- ouverture / suivi du trade -------------------------------------------

    @staticmethod
    def _sl_breached(s: Setup, price: float) -> bool:
        return price >= s.sweep_high if s.direction == "sell" else price <= s.sweep_low

    def _open_trade(self, price: float, epoch: int, mode: str) -> None:
        s = self.setup
        cfg = self.cfg
        if s is None:
            return
        self.setup = None                     # un setup = au plus un trade

        if len(self.open_trades) >= self.max_positions:
            log_engine.info("%s | entrée %s ignorée : max_positions (%d) atteint.",
                            self.label, s.direction.upper(), self.max_positions)
            return

        if s.direction == "sell":
            sl = s.sweep_high + cfg.sl_buffer
            risk = sl - price
            tp = price - risk * cfg.rr
        else:
            sl = s.sweep_low - cfg.sl_buffer
            risk = price - sl
            tp = price + risk * cfg.rr

        if risk <= 0:
            log_engine.warning("%s | entrée %s ignorée : risque <= 0 (prix %s, SL %s).",
                               self.label, s.direction.upper(), fmt_price(price), fmt_price(sl))
            return

        self._trade_seq += 1
        trade = PaperTrade(
            id=f"{self.key}-{epoch}-{self._trade_seq}",
            market_key=self.key, label=self.label, symbol=self.symbol,
            direction=s.direction, crt_tf=cfg.crt_tf, entry_tf=cfg.entry_tf,
            mode=mode, trend_filter=str(cfg.trend_filter).upper(),
            c1_epoch=s.c1_epoch, c1_high=s.c1_high, c1_low=s.c1_low,
            sweep_epoch=s.sweep_epoch, sweep_high=s.sweep_high, sweep_low=s.sweep_low,
            fvg_low=s.fvg_low, fvg_high=s.fvg_high,
            entry_epoch=epoch, entry=price, sl=sl, tp=tp, risk=risk,
            rr_target=cfg.rr, be_rr=cfg.be_rr,
        )
        self.open_trades.append(trade)
        self.journal.save_open(self.key, self.open_trades)
        self.journal.append_signal(trade)
        photo_path = self._render_chart(trade) if self.chart_dir is not None else None
        self.notifier.send(self._entry_message(trade), photo_path=photo_path)

    def _render_chart(self, trade: "PaperTrade") -> Optional[Path]:
        try:
            crt_candles = self.get_candles(trade.crt_tf)
            entry_candles = self.get_candles(trade.entry_tf)
            out_path = self.chart_dir / f"{trade.id}.png"
            return render_signal_chart(trade, crt_candles, entry_candles, self.cfg, out_path)
        except Exception as exc:  # ne doit jamais empêcher l'envoi du signal texte
            log_engine.warning("%s | génération d'image échouée pour %s : %s", self.label, trade.id, exc)
            return None

    def _track_trades(self, price: float, epoch: int) -> None:
        if not self.open_trades:
            return
        changed = False
        for trade in list(self.open_trades):
            for kind, value in trade.on_price(price, epoch):
                changed = True
                if kind == "RR":
                    if value != trade.rr_target:     # le dernier jalon est annoncé par CLOSE
                        self.notifier.send(self._header(trade) + f"\n\nRR {fmt_r(value)} atteint ✅")
                elif kind == "BE_ACTIVATED":
                    self.notifier.send(self._header(trade) + f"\n\nBreak-even activé (SL → entrée) à RR {fmt_r(value)}")
                elif kind == "CLOSE":
                    self.open_trades.remove(trade)
                    self.journal.append_closed(trade)
                    self.notifier.send(self._close_message(trade))
        if changed:
            self.journal.save_open(self.key, self.open_trades)

    # ---- restauration ----------------------------------------------------------

    def restore_trades(self, trades: list[PaperTrade]) -> None:
        self.open_trades = list(trades)
        if trades:
            log_engine.info("%s | %d trade(s) paper restauré(s) après redémarrage.", self.label, len(trades))

    # ---- messages ---------------------------------------------------------------

    @staticmethod
    def _icon(trade: PaperTrade) -> str:
        return "🟢" if trade.direction == "buy" else "🔴"

    def _header(self, t: PaperTrade) -> str:
        return (f"{self._icon(t)} CRT {t.direction.upper()}\n\n"
                f"📊 Market: {t.label}\n⏱️ TF: {t.entry_tf}")

    def _entry_message(self, t: PaperTrade) -> str:
        fvg = f"\n🟨 FVG: {fmt_price(t.fvg_low)} – {fmt_price(t.fvg_high)}" if t.fvg_low is not None else ""
        mode_line = "⚡ Entrée directe" if t.mode == "DIRECT" else "🔁 Entrée sur retest FVG"
        return (self._header(t) +
                f"\n\n🎯 Entry: {fmt_price(t.entry)}\n🛑 SL: {fmt_price(t.sl)}\n💰 TP: {fmt_price(t.tp)}"
                f"\n📐 RR: 1:{fmt_r(t.rr_target)}" + fvg +
                f"\n\n{mode_line}"
                f"\n(paper trading — aucun ordre réel)")

    def _close_message(self, t: PaperTrade) -> str:
        if t.status == "WIN":
            return (f"🎯 {t.label} {t.direction.upper()}\n\n"
                    f"TP RR{fmt_r(t.rr_target)} atteint ✅\nTrade WIN (+{fmt_r(t.rr_target)}R)")
        if t.status == "BE":
            return f"⚪ {t.label} {t.direction.upper()}\n\nRetour à l'entrée après break-even\nTrade BE (0R)"
        return (f"❌ {t.label} {t.direction.upper()}\n\nStop loss touché ({fmt_price(t.sl)})\n"
                f"Trade LOSS (-1R) | max atteint: {t.max_r:.2f}R")


# =============================================================================
# 10. ORCHESTRATION
# =============================================================================

log_main = logging.getLogger("main")


def needed_timeframes(cfg: StrategyConfig) -> dict[str, int]:
    tfs = {cfg.entry_tf, cfg.crt_tf}
    if str(cfg.trend_filter).upper() != "OFF":
        tfs.add(cfg.effective_trend_tf)
    return {tf: TF_SECONDS[tf] for tf in sorted(tfs, key=lambda t: TF_SECONDS[t])}


def wire_engines(cfg: StrategyConfig, markets: dict, candle_store: "CandleStore", tick_cache: "TickCache",
                 notifier: Notifier, journal: Journal,
                 now_fn: Callable[[], float] = _time.time,
                 chart_dir: Optional[Path] = None,
                 max_positions_by_market: Optional[dict[str, int]] = None) -> dict[str, MarketEngine]:
    """Crée un MarketEngine par marché, restaure les trades ouverts et branche
    les écouteurs (bougies clôturées + ticks). Retourne {symbole: moteur}.

    max_positions_by_market permet un max_positions différent par marché
    (clé = market.key, ex. "V75") ; un marché absent garde cfg.max_positions."""
    saved_open = journal.load_open()
    overrides = max_positions_by_market or {}
    engines: dict[str, MarketEngine] = {}

    for market in markets.values():
        engine = MarketEngine(
            key=market.key, label=market.label, symbol=market.symbol, cfg=cfg,
            get_candles=lambda tf, sym=market.symbol: candle_store.get_closed_candles(sym, tf),
            notifier=notifier, journal=journal, now_fn=now_fn, chart_dir=chart_dir,
            max_positions=overrides.get(market.key),
        )
        engine.restore_trades([PaperTrade.from_dict(d) for d in saved_open.get(market.key, [])])
        engines[market.symbol] = engine

    async def on_candle_closed(symbol: str, timeframe: str, candle: Candle) -> None:
        engine = engines.get(symbol)
        if engine is not None:
            engine.on_candle_close(timeframe, candle)

    async def on_tick(symbol: str, tick: Tick) -> None:
        engine = engines.get(symbol)
        if engine is not None:
            engine.on_price(tick.quote, tick.epoch)

    candle_store.add_close_listener(on_candle_closed)
    tick_cache.add_listener(on_tick)      # enregistré APRÈS CandleStore : bougie d'abord, prix ensuite
    return engines


def telegram_credentials() -> tuple[str, str, str]:
    """(token, chat_id, admin_id) : variables d'environnement (.env) si définies et
    non vides, sinon valeurs DEFAULT_TELEGRAM_* intégrées."""
    return (os.getenv("TELEGRAM_BOT_TOKEN") or DEFAULT_TELEGRAM_BOT_TOKEN,
            os.getenv("TELEGRAM_CHAT_ID") or DEFAULT_TELEGRAM_CHAT_ID,
            os.getenv("TELEGRAM_ADMIN_ID") or DEFAULT_TELEGRAM_ADMIN_ID)


def build_notifier(app: AppConfig) -> Notifier:
    """TelegramNotifier si telegram.enabled=true dans config.json (token et chat
    via telegram_credentials : .env sinon valeurs par défaut intégrées) ; sinon
    repli sur le Notifier fichier (data/signals.log), comme en V1."""
    log_path = app.data_dir / "signals.log"
    if not app.telegram_enabled:
        return Notifier(log_path)
    token, chat_id, _admin_id = telegram_credentials()
    if not token or not chat_id:
        log_main.warning("telegram.enabled=true mais token/chat_id Telegram vides : "
                         "repli sur data/signals.log.")
        return Notifier(log_path)
    if requests is None:
        log_main.warning("telegram.enabled=true mais le paquet 'requests' est absent "
                         "(pip install requests) : repli sur data/signals.log.")
        return Notifier(log_path)
    return TelegramNotifier(token, chat_id, log_path)


async def daily_report_task(app: AppConfig, journal: Journal, notifier: Notifier) -> None:
    """Boucle infinie : dort jusqu'à app.report_hour:report_minute (heure locale
    du serveur) puis envoie le rapport journalier, chaque jour."""
    while True:
        now = datetime.now()
        target = _next_report_time(now, app.report_hour, app.report_minute)
        await asyncio.sleep((target - now).total_seconds())

        cutoff_date = target.strftime("%Y-%m-%d")
        date_label = target.strftime("%d/%m/%Y")
        signals_today = [s for s in journal.read_signals() if s["date"] == cutoff_date]
        closed_today = [c for c in journal.read_closed() if (c.get("entry_time") or "")[:10] == cutoff_date]
        notifier.send(format_daily_report(date_label, app.markets, signals_today, closed_today))
        log_main.info("Rapport journalier envoyé (%s).", cutoff_date)


async def run(app: AppConfig) -> None:
    cfg = app.strategy
    conn = ConnectionSettings(history_candle_count=app.history_candle_count)
    journal = Journal(app.data_dir)
    notifier = build_notifier(app)

    chart_dir: Optional[Path] = app.data_dir / "charts" if app.chart_enabled else None
    if chart_dir is not None and plt is None:
        log_main.warning("chart_enabled=true mais matplotlib est absent (pip install matplotlib) : "
                         "images de signal désactivées.")
        chart_dir = None

    single_tf_direct = cfg.crt_tf == cfg.entry_tf and cfg.direct_entry and not cfg.fvg_enabled
    log_main.info("Démarrage moteur CRT V1 — signaux + paper trading, AUCUN ordre réel.")
    log_main.info("Mode=%s | CRT=%s | entrée=%s | RR=%g | FVG=%s | direct=%s | tendance=%s | BE=%s | "
                  "SL buffer=%g | max pos/marché=%d | override par marché=%s",
                  "TIMEFRAME UNIQUE (entrée immédiate)" if single_tf_direct else "CRT->FVG/retest",
                  cfg.crt_tf, cfg.entry_tf, cfg.rr, "ON" if cfg.fvg_enabled else "OFF",
                  "ON" if cfg.direct_entry else "OFF", str(cfg.trend_filter).upper(),
                  cfg.break_even, cfg.sl_buffer, cfg.max_positions,
                  app.max_positions_by_market or "aucun")
    log_main.info("Telegram=%s | images=%s | rapport quotidien=%02d:%02d",
                  "ON" if isinstance(notifier, TelegramNotifier) else "OFF",
                  "ON" if chart_dir else "OFF", app.report_hour, app.report_minute)

    client = DerivClient(conn)
    tick_cache = TickCache(client)
    candle_store = CandleStore(client, tick_cache)
    client_task = asyncio.create_task(client.run_forever())

    definitions = [m for m in ALL_MARKETS if m["key"] in app.markets]
    markets: dict[str, MarketSymbol] = {}
    discover_delays = (10.0, 20.0, 30.0, 60.0, 120.0)
    attempt = 0
    while not markets and not client._stop:
        attempt += 1
        markets = await discover_symbols(client, definitions)
        if not markets:
            delay = discover_delays[min(attempt - 1, len(discover_delays) - 1)]
            log_main.error(
                "Aucun marché résolu via active_symbols (tentative %d). Cause fréquente : "
                "le serveur se connecte depuis un pays où Deriv n'opère pas (ex. Etats-Unis, "
                "Canada, Israël, Hong Kong, Malaisie, Singapour, EAU, Belarus) -> liste renvoyée "
                "vide sans erreur explicite. Nouvelle tentative dans %.0fs.",
                attempt, delay,
            )
            await asyncio.sleep(delay)
    if not markets:
        client_task.cancel()
        return

    timeframes = needed_timeframes(cfg)
    for market in markets.values():
        candle_store.register_timeframes(market.symbol, timeframes)
        await candle_store.load_history(market.symbol, timeframes, conn.history_candle_count)

    wire_engines(cfg, markets, candle_store, tick_cache, notifier, journal, chart_dir=chart_dir,
                max_positions_by_market=app.max_positions_by_market)
    await tick_cache.subscribe([m.symbol for m in markets.values()])

    report_task = asyncio.create_task(daily_report_task(app, journal, notifier))

    log_main.info("Moteur actif sur %d marché(s) : %s",
                  len(markets), ", ".join(m.label for m in markets.values()))
    await asyncio.gather(client_task, report_task)


def print_stats(app: AppConfig) -> None:
    records = Journal(app.data_dir).read_closed()
    print(format_stats(compute_stats(records)))


def main() -> None:
    parser = argparse.ArgumentParser(description="AlphaBot — moteur CRT V1 (paper trading)")
    parser.add_argument("--stats", action="store_true", help="affiche les statistiques par marché et quitte")
    parser.add_argument("--test", action="store_true", help="lance les tests intégrés (sans réseau) et quitte")
    parser.add_argument("--init-config", action="store_true",
                        help="écrit le modèle de config dans --config s'il n'existe pas, et quitte")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH, help="chemin de config.json")
    args = parser.parse_args()

    if args.test:
        sys.exit(run_tests())

    if args.init_config:
        if write_example_config(args.config):
            print(f"Config d'exemple écrite : {args.config}")
            return
        print(f"{args.config} existe déjà : non écrasé.", file=sys.stderr)
        sys.exit(1)

    try:
        app = load_app_config(args.config)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"Configuration invalide : {exc}", file=sys.stderr)
        sys.exit(2)

    if args.stats:
        print_stats(app)
        return

    if websockets is None:
        print("Le paquet 'websockets' est requis : pip install -r requirements.txt", file=sys.stderr)
        sys.exit(2)

    configure_logging()
    try:
        asyncio.run(run(app))
    except KeyboardInterrupt:
        log_main.info("Arrêt demandé par l'utilisateur.")


# =============================================================================
# 11. TESTS INTÉGRÉS — python3 main.py --test (logique pure, aucun accès réseau)
# =============================================================================

T0 = 1_700_000_000 - (1_700_000_000 % 3600)     # aligné sur l'heure
M30, M5 = 1800, 300


class FakeNotifier(Notifier):
    def __init__(self):
        super().__init__(None)
        self.messages: list[str] = []
        self.photos: list = []

    def send(self, text: str, photo_path=None) -> None:
        self.messages.append(text)
        self.photos.append(photo_path)


class Harness:
    """Fabrique un MarketEngine avec des bougies pilotées à la main."""

    def __init__(self, tmp: Path, **cfg_kwargs):
        self.cfg = StrategyConfig(**cfg_kwargs)
        self.cfg.validate()
        self.candles: dict[str, list[Candle]] = {"M30": [], "M5": [], "H1": [], "M1": [], "M15": []}
        self.notifier = FakeNotifier()
        self.journal = Journal(tmp)
        self.now = 0.0
        self.engine = MarketEngine(
            "TEST", "TEST Index", "R_TEST", self.cfg,
            get_candles=lambda tf: self.candles[tf],
            notifier=self.notifier, journal=self.journal, now_fn=lambda: self.now)

    def close(self, tf: str, candle: Candle) -> None:
        self.candles[tf].append(candle)
        self.engine.on_candle_close(tf, candle)

    def prices(self, epoch0: int, *prices: float) -> None:
        for i, p in enumerate(prices):
            self.engine.on_price(p, epoch0 + i)


def sell_crt(h: Harness) -> int:
    """Bougie 1 puis bougie de sweep SELL. Retourne l'instant de confirmation."""
    c1 = Candle(T0, 100, 110, 90, 105)
    c2 = Candle(T0 + M30, 105, 115, 100, 104)          # high 115 > 110, close 104 < 110
    h.close("M30", c1)
    h.now = T0 + 2 * M30 + 5
    h.close("M30", c2)
    return T0 + 2 * M30


def bearish_fvg(h: Harness, confirm: int) -> None:
    """3 bougies M5 post-confirmation -> FVG baissière [98, 100]."""
    h.close("M5", Candle(confirm, 104, 104.5, 100, 101))
    h.close("M5", Candle(confirm + M5, 101, 101, 96, 97))
    h.close("M5", Candle(confirm + 2 * M5, 97, 98, 95, 96))


class TestCRT(unittest.TestCase):
    def test_sell(self):
        self.assertEqual(detect_crt(Candle(0, 100, 110, 90, 105), Candle(1, 105, 115, 100, 104)), "sell")

    def test_buy(self):
        self.assertEqual(detect_crt(Candle(0, 100, 110, 90, 105), Candle(1, 95, 100, 85, 96)), "buy")

    def test_close_outside_range_is_not_crt(self):
        # sweep du high mais clôture AU-DESSUS du high de la bougie 1 : simple cassure
        self.assertIsNone(detect_crt(Candle(0, 100, 110, 90, 105), Candle(1, 105, 118, 104, 116)))

    def test_no_sweep(self):
        self.assertIsNone(detect_crt(Candle(0, 100, 110, 90, 105), Candle(1, 100, 108, 92, 101)))

    def test_outside_bar_is_ambiguous(self):
        self.assertIsNone(detect_crt(Candle(0, 100, 110, 90, 105), Candle(1, 100, 115, 85, 100)))


class TestFVG(unittest.TestCase):
    def test_bearish(self):
        z = detect_fvg(Candle(0, 0, 104, 100, 0), Candle(1, 0, 0, 0, 0), Candle(2, 0, 98, 95, 0), "sell")
        self.assertEqual(z, (98, 100))

    def test_bullish(self):
        z = detect_fvg(Candle(0, 0, 100, 96, 0), Candle(1, 0, 0, 0, 0), Candle(2, 0, 106, 102, 0), "buy")
        self.assertEqual(z, (100, 102))

    def test_overlap_is_no_fvg(self):
        self.assertIsNone(detect_fvg(Candle(0, 0, 104, 100, 0), Candle(1, 0, 0, 0, 0), Candle(2, 0, 101, 95, 0), "sell"))

    def test_wrong_direction(self):
        self.assertIsNone(detect_fvg(Candle(0, 0, 104, 100, 0), Candle(1, 0, 0, 0, 0), Candle(2, 0, 98, 95, 0), "buy"))


class TestTrend(unittest.TestCase):
    def rising(self, n=260):
        out = []
        for i in range(n):
            p = 100 + i * 0.5
            out.append(Candle(T0 + i * M30, p, p + 1, p - 1, p + 0.5))
        return out

    def test_ema_value(self):
        self.assertAlmostEqual(ema([1, 2, 3, 4, 5], 5), 3.0)
        self.assertIsNone(ema([1, 2], 5))

    def test_ema_trend(self):
        up = self.rising()
        self.assertEqual(trend_from_ema(up, 50, 200), "buy")
        down = [Candle(c.epoch, -c.open, -c.low, -c.high, -c.close) for c in up]
        self.assertEqual(trend_from_ema(down, 50, 200), "sell")
        self.assertIsNone(trend_from_ema(up[:100], 50, 200))     # pas assez de données

    def test_structure(self):
        zig = []
        # zigzag haussier : creux/sommets de plus en plus hauts
        for i, base in enumerate([10, 14, 11, 16, 12, 18, 13, 20, 14, 22, 15, 24]):
            zig.append(Candle(i, base, base + 1, base - 1, base))
        self.assertEqual(trend_from_structure(zig, 1), "buy")

    def test_filter_blocks_and_allows(self):
        cfg = StrategyConfig(trend_filter="EMA")
        up = self.rising()
        self.assertTrue(trend_allows(cfg, "buy", up)[0])
        self.assertFalse(trend_allows(cfg, "sell", up)[0])
        self.assertFalse(trend_allows(cfg, "buy", up[:50])[0])   # indéterminé -> bloqué
        self.assertTrue(trend_allows(StrategyConfig(), "sell", [])[0])


class TestPaperTrade(unittest.TestCase):
    def make(self, direction="sell", **kw):
        entry, sl = (99.0, 115.0) if direction == "sell" else (101.0, 85.0)
        risk = 16.0
        tp = entry - 3 * risk if direction == "sell" else entry + 3 * risk
        return PaperTrade(
            id="x", market_key="T", label="T", symbol="T", direction=direction,
            crt_tf="M30", entry_tf="M5", mode="FVG", trend_filter="OFF",
            c1_epoch=0, c1_high=110, c1_low=90, sweep_epoch=1, sweep_high=115, sweep_low=85,
            fvg_low=98, fvg_high=100, entry_epoch=2, entry=entry, sl=sl, tp=tp, risk=risk,
            rr_target=3.0, **kw)

    def test_sell_levels(self):
        t = self.make("sell")
        self.assertEqual(t.tp, 51.0)
        ev = t.on_price(83.0, 10)               # exactement RR1
        self.assertEqual(ev, [("RR", 1.0)])
        self.assertEqual(t.status, "OPEN")

    def test_gap_hits_several_milestones(self):
        t = self.make("buy")
        ev = t.on_price(200.0, 10)              # bien au-delà de RR3
        kinds = [e[0] for e in ev]
        self.assertEqual(kinds, ["RR", "RR", "RR", "CLOSE"])
        self.assertEqual(t.status, "WIN")
        self.assertEqual(t.r_result, 3.0)

    def test_loss(self):
        t = self.make("sell")
        t.on_price(90, 5)
        ev = t.on_price(115.0, 6)
        self.assertEqual(ev, [("CLOSE", "LOSS")])
        self.assertEqual(t.r_result, -1.0)

    def test_break_even_off_keeps_full_loss(self):
        t = self.make("sell", be_rr=0.0)
        t.on_price(83.0, 5)                     # RR1 atteint
        t.on_price(115.0, 6)
        self.assertEqual(t.status, "LOSS")

    def test_break_even_on(self):
        t = self.make("sell", be_rr=1.0)
        ev = t.on_price(83.0, 5)
        self.assertIn(("BE_ACTIVATED", 1.0), ev)
        self.assertEqual(t.sl_current, 99.0)
        ev = t.on_price(99.0, 6)
        self.assertEqual(ev, [("CLOSE", "BE")])
        self.assertEqual(t.r_result, 0.0)

    def test_closed_trade_ignores_prices(self):
        t = self.make("sell")
        t.on_price(200, 1)
        self.assertEqual(t.on_price(10, 2), [])


class TestEngine(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_full_sell_flow_to_rr3(self):
        h = Harness(self.tmp)
        confirm = sell_crt(h)
        self.assertIsNotNone(h.engine.setup)
        self.assertEqual(h.engine.setup.direction, "sell")

        bearish_fvg(h, confirm)
        s = h.engine.setup
        self.assertEqual((s.fvg_low, s.fvg_high), (98, 100))

        e = confirm + 3 * M5
        h.prices(e, 96.5, 97.5)                 # sous la FVG : pas d'entrée
        self.assertEqual(h.engine.open_trades, [])
        h.prices(e + 10, 99.0)                  # retest : entrée
        self.assertEqual(len(h.engine.open_trades), 1)

        t = h.engine.open_trades[0]
        self.assertEqual((t.entry, t.sl, t.risk, t.tp), (99.0, 115.0, 16.0, 51.0))
        self.assertEqual(t.mode, "FVG")
        self.assertIsNone(h.engine.setup)

        h.prices(e + 20, 90, 83, 67, 51)
        self.assertEqual(t.status, "WIN")
        self.assertEqual(sorted(t.rr_hits), ["1", "2", "3"])
        self.assertEqual(h.engine.open_trades, [])

        text = "\n".join(h.notifier.messages)
        for expected in ("SELL", "RR 1 atteint", "RR 2 atteint", "TP RR3 atteint", "Trade WIN"):
            self.assertIn(expected, text)

        rec = h.journal.read_closed()[0]
        self.assertEqual(rec["result"], "WIN")
        self.assertEqual(rec["r_result"], 3.0)
        self.assertEqual(rec["sweep_high"], 115)
        self.assertIsNotNone(rec["rr1_time"]); self.assertIsNotNone(rec["rr3_time"])

        stats = compute_stats(h.journal.read_closed())
        self.assertEqual(stats["ALL"]["total_r"], 3.0)
        self.assertEqual(stats["TEST Index"]["wins"], 1)

    def test_full_sell_flow_with_m15_crt(self):
        """Même scénario que test_full_sell_flow_to_rr3, mais avec crt_tf=M15
        (nouveau réglage pour obtenir des signaux plus rapidement en test)."""
        h = Harness(self.tmp, crt_tf="M15")
        M15 = 900
        c1 = Candle(T0, 100, 110, 90, 105)
        c2 = Candle(T0 + M15, 105, 115, 100, 104)       # sweep du high, clôture dans le range
        h.close("M15", c1)
        h.now = T0 + 2 * M15 + 5
        h.close("M15", c2)
        confirm = T0 + 2 * M15
        self.assertEqual(h.engine.setup.direction, "sell")

        bearish_fvg(h, confirm)
        s = h.engine.setup
        self.assertEqual((s.fvg_low, s.fvg_high), (98, 100))

        e = confirm + 3 * M5
        h.prices(e + 10, 99.0)                          # retest : entrée
        self.assertEqual(len(h.engine.open_trades), 1)

        t = h.engine.open_trades[0]
        self.assertEqual(t.crt_tf, "M15")
        self.assertEqual((t.entry, t.sl, t.risk, t.tp), (99.0, 115.0, 16.0, 51.0))

    def test_full_buy_flow_loss(self):
        h = Harness(self.tmp)
        h.close("M30", Candle(T0, 100, 110, 90, 105))
        h.now = T0 + 2 * M30 + 5
        h.close("M30", Candle(T0 + M30, 95, 100, 85, 96))       # low 85 < 90, close 96 > 90
        confirm = T0 + 2 * M30
        self.assertEqual(h.engine.setup.direction, "buy")
        h.close("M5", Candle(confirm, 96, 100, 95, 99))         # high 100
        h.close("M5", Candle(confirm + M5, 99, 104, 99, 103))
        h.close("M5", Candle(confirm + 2 * M5, 103, 106, 102, 105))   # low 102 > 100 -> FVG [100,102]
        s = h.engine.setup
        self.assertEqual((s.fvg_low, s.fvg_high), (100, 102))

        e = confirm + 3 * M5
        h.prices(e, 104, 103)
        h.prices(e + 5, 102)                                    # retest
        t = h.engine.open_trades[0]
        self.assertEqual((t.entry, t.sl, t.risk, t.tp), (102.0, 85.0, 17.0, 153.0))
        h.prices(e + 10, 95, 85)
        self.assertEqual(t.status, "LOSS")
        self.assertEqual(h.journal.read_closed()[0]["r_result"], -1.0)

    def test_setup_cancelled_if_sweep_level_hit_before_entry(self):
        h = Harness(self.tmp)
        confirm = sell_crt(h)
        bearish_fvg(h, confirm)
        h.prices(confirm + 3 * M5, 116.0)                       # au-dessus du high du sweep
        self.assertIsNone(h.engine.setup)
        self.assertEqual(h.engine.open_trades, [])

    def test_no_entry_before_fvg_exists(self):
        h = Harness(self.tmp)
        sell_crt(h)
        h.prices(T0 + 3 * M30, 99, 100, 105)                    # aucune FVG -> aucune entrée
        self.assertEqual(h.engine.open_trades, [])
        self.assertIsNotNone(h.engine.setup)

    def test_fvg_before_confirmation_is_ignored(self):
        h = Harness(self.tmp)
        confirm = sell_crt(h)
        # FVG complète formée PENDANT la bougie de sweep (avant la confirmation) : ignorée
        h.close("M5", Candle(confirm - 3 * M5, 104, 104.5, 100, 101))
        h.close("M5", Candle(confirm - 2 * M5, 101, 101, 96, 97))
        h.close("M5", Candle(confirm - 1 * M5, 97, 98, 95, 96))
        self.assertIsNone(h.engine.setup.fvg_low)

    def test_fvg_starting_inside_sweep_candle_is_ignored(self):
        h = Harness(self.tmp)
        confirm = sell_crt(h)
        # 1re bougie de la FVG = dernière M5 DU sweep, les 2 autres après : ignorée
        h.close("M5", Candle(confirm - M5, 104, 104.5, 100, 101))
        h.close("M5", Candle(confirm, 101, 101, 96, 97))
        h.close("M5", Candle(confirm + M5, 97, 98, 95, 96))
        self.assertIsNone(h.engine.setup.fvg_low)

    def test_no_entry_after_setup_expiry_even_on_tick(self):
        h = Harness(self.tmp, setup_expiry_crt_candles=1)
        confirm = sell_crt(h)
        bearish_fvg(h, confirm)
        h.prices(confirm + M30 + 1, 99.0)                       # retest, mais trop tard
        self.assertEqual(h.engine.open_trades, [])
        self.assertIsNone(h.engine.setup)

    def test_non_consecutive_fvg_candles_ignored(self):
        h = Harness(self.tmp)
        confirm = sell_crt(h)
        h.close("M5", Candle(confirm, 104, 104.5, 100, 101))
        h.close("M5", Candle(confirm + M5, 101, 101, 96, 97))
        h.close("M5", Candle(confirm + 3 * M5, 97, 98, 95, 96))  # trou d'une bougie
        self.assertIsNone(h.engine.setup.fvg_low)

    def test_direct_entry_when_fvg_off(self):
        h = Harness(self.tmp, fvg_enabled=False, direct_entry=True)
        confirm = sell_crt(h)
        h.close("M5", Candle(confirm, 104, 104, 101, 102))
        h.close("M5", Candle(confirm + M5, 102, 103, 100, 101))
        h.close("M5", Candle(confirm + 2 * M5, 101, 101, 97, 98))   # baissière et casse le micro-range
        self.assertEqual(len(h.engine.open_trades), 1)
        t = h.engine.open_trades[0]
        self.assertEqual(t.mode, "DIRECT")
        self.assertEqual(t.entry, 98)
        self.assertEqual(t.sl, 115)
        self.assertEqual(t.tp, 98 - 3 * 17)

    def test_direct_entry_off_by_default(self):
        h = Harness(self.tmp)
        confirm = sell_crt(h)
        h.close("M5", Candle(confirm, 104, 104, 101, 102))
        h.close("M5", Candle(confirm + M5, 102, 103, 100, 101))
        h.close("M5", Candle(confirm + 2 * M5, 101, 101, 97, 98))
        self.assertEqual(h.engine.open_trades, [])

    def test_direct_fallback_waits_when_fvg_on(self):
        h = Harness(self.tmp, fvg_enabled=True, direct_entry=True, direct_fallback_bars=5)
        confirm = sell_crt(h)
        # pas de FVG (bougies qui se chevauchent) mais confirmation directe dès la 3e bougie
        h.close("M5", Candle(confirm, 104, 104, 101, 102))
        h.close("M5", Candle(confirm + M5, 102, 103, 100, 101))
        h.close("M5", Candle(confirm + 2 * M5, 101, 102, 99.5, 99.8))
        self.assertEqual(h.engine.open_trades, [])               # trop tôt (< 5 bougies)
        h.close("M5", Candle(confirm + 3 * M5, 99.8, 100.5, 99.5, 99.9))
        h.close("M5", Candle(confirm + 4 * M5, 99.9, 100, 98, 98.5))
        self.assertEqual(len(h.engine.open_trades), 1)

    def test_trend_filter_blocks_counter_trend_crt(self):
        h = Harness(self.tmp, trend_filter="EMA")
        base = []
        for i in range(258):
            p = 100 + i * 0.5
            base.append(Candle(T0 - (260 - i) * M30, p, p + 1, p - 1, p + 0.5))
        h.candles["M30"] = base
        p = 100 + 258 * 0.5
        c1 = Candle(T0, p, p + 3, p - 2, p + 1)
        c2 = Candle(T0 + M30, p + 1, p + 5, p, p + 0.5)          # sweep du high, clôture dedans
        h.close("M30", c1)
        h.now = T0 + 2 * M30 + 5
        h.close("M30", c2)
        self.assertIsNone(h.engine.setup)                        # SELL contre tendance haussière

        h2 = Harness(self.tmp / "b", trend_filter="OFF")
        sell_crt(h2)
        self.assertIsNotNone(h2.engine.setup)

    def test_non_consecutive_crt_candles_ignored(self):
        h = Harness(self.tmp)
        h.close("M30", Candle(T0, 100, 110, 90, 105))
        h.now = T0 + 3 * M30 + 5
        h.close("M30", Candle(T0 + 2 * M30, 105, 115, 100, 104))  # une bougie manque
        self.assertIsNone(h.engine.setup)

    def test_stale_crt_ignored(self):
        h = Harness(self.tmp)
        h.close("M30", Candle(T0, 100, 110, 90, 105))
        h.now = T0 + 10 * M30                                     # données anciennes
        h.close("M30", Candle(T0 + M30, 105, 115, 100, 104))
        self.assertIsNone(h.engine.setup)

    def test_setup_expires(self):
        h = Harness(self.tmp, setup_expiry_crt_candles=1)
        confirm = sell_crt(h)
        h.close("M5", Candle(confirm + 6 * M5, 104, 105, 103, 104))   # = confirm + 1 bougie CRT
        self.assertIsNone(h.engine.setup)

    def test_new_crt_replaces_old_setup(self):
        h = Harness(self.tmp)
        sell_crt(h)
        old = h.engine.setup
        h.now = T0 + 3 * M30 + 5
        h.close("M30", Candle(T0 + 2 * M30, 104, 104.5, 84, 102))  # sweep du low de c2 (100), clôture dedans : BUY
        self.assertEqual(h.engine.setup.direction, "buy")
        self.assertIsNot(h.engine.setup, old)

    def test_max_positions_per_market(self):
        h = Harness(self.tmp, max_positions=1)
        confirm = sell_crt(h)
        bearish_fvg(h, confirm)
        h.prices(confirm + 3 * M5, 99.0)
        self.assertEqual(len(h.engine.open_trades), 1)
        # second CRT pendant que le trade est ouvert
        h.now = T0 + 3 * M30 + 5
        h.close("M30", Candle(T0 + 2 * M30, 104, 118, 100, 110))   # sweep du high de c2 (115), clôture dedans
        self.assertIsNotNone(h.engine.setup)
        confirm2 = T0 + 3 * M30
        h.close("M5", Candle(confirm2, 110, 110.5, 106, 107))
        h.close("M5", Candle(confirm2 + M5, 107, 107, 102, 103))
        h.close("M5", Candle(confirm2 + 2 * M5, 103, 104, 101, 102))  # FVG [104, 106]
        self.assertIsNotNone(h.engine.setup.fvg_low)
        h.prices(confirm2 + 3 * M5, 105.0)
        self.assertEqual(len(h.engine.open_trades), 1)            # pas de 2e trade

    def test_sl_buffer_and_rr(self):
        h = Harness(self.tmp, sl_buffer=1.0, rr=2.0)
        confirm = sell_crt(h)
        bearish_fvg(h, confirm)
        h.prices(confirm + 3 * M5, 99.0)
        t = h.engine.open_trades[0]
        self.assertEqual(t.sl, 116.0)
        self.assertEqual(t.risk, 17.0)
        self.assertEqual(t.tp, 99 - 2 * 17)

    def test_open_trades_survive_restart(self):
        h = Harness(self.tmp)
        confirm = sell_crt(h)
        bearish_fvg(h, confirm)
        h.prices(confirm + 3 * M5, 99.0)
        saved = Journal(self.tmp).load_open()
        trades = [PaperTrade.from_dict(d) for d in saved["TEST"]]
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].entry, 99.0)
        h2 = Harness(self.tmp)
        h2.engine.restore_trades(trades)
        h2.prices(confirm + 10 * M5, 51)
        self.assertEqual(trades[0].status, "WIN")


class FakeClient:
    """Client Deriv factice : historique + active_symbols, sans réseau."""

    def __init__(self, history: dict[int, list[dict]], active_symbols: list[dict] | None = None):
        self.handlers: dict[str, list] = {}
        self.history = history
        self.active_symbols = active_symbols or []

    def on(self, msg_type, handler):
        self.handlers.setdefault(msg_type, []).append(handler)

    async def request(self, payload, timeout=15.0):
        if "active_symbols" in payload:
            return {"active_symbols": self.active_symbols}
        return {"candles": self.history[payload["granularity"]]}

    async def subscribe(self, payload):
        pass


def raw(epoch, o, h, l, c):
    return {"epoch": epoch, "open": o, "high": h, "low": l, "close": c}


class TestPipelineFromTicks(unittest.TestCase):
    """Ticks -> bougies -> CRT -> FVG -> retest -> paper trade -> WIN, via le vrai
    câblage (CandleStore + TickCache + wire_engines)."""

    def test_end_to_end(self):
        import asyncio
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            # historique : c1 complète ; c2 = bougie M30 EN COURS (high 112 pour l'instant)
            c2_epoch = T0 + M30
            hist = {
                1800: [raw(T0, 100, 110, 90, 105), raw(c2_epoch, 105, 112, 100, 108)],
                300: [raw(T0 + 1200, 106, 107, 104, 105), raw(T0 + 3300, 108, 109, 107, 108)],  # M5 en cours
            }
            client = FakeClient(hist)
            tick_cache = TickCache(client)
            store = CandleStore(client, tick_cache)
            symbol = "R_TEST"
            tfs = {"M5": 300, "M30": 1800}
            store.register_timeframes(symbol, tfs)
            asyncio.run(store.load_history(symbol, tfs, 500))

            # la bougie en cours n'est PAS dans les clôturées, mais reste complétable
            self.assertEqual([c.epoch for c in store.get_closed_candles(symbol, "M30")], [T0])
            self.assertEqual(store.get_forming_candle(symbol, "M30").high, 112)

            notifier = FakeNotifier()
            journal = Journal(tmp)
            market = MarketSymbol("TEST", "TEST Index", "TEST", symbol, "synthetic_index", True)
            engines = wire_engines(StrategyConfig(), {"TEST": market}, store, tick_cache,
                                     notifier, journal, now_fn=lambda: T0 + 2 * M30 + 5)
            engine = engines[symbol]

            def tick(epoch, quote):
                asyncio.run(tick_cache._on_tick_message(
                    {"tick": {"symbol": symbol, "quote": quote, "epoch": epoch}}))

            confirm = T0 + 2 * M30
            # fin de la bougie c2 : sweep du high (115) puis clôture dedans (104)
            tick(T0 + 3330, 115)
            tick(T0 + 3500, 104)
            self.assertIsNone(engine.setup)                     # c2 pas encore clôturée
            tick(confirm + 5, 101)                               # 1er tick du nouveau bucket -> clôture c2
            self.assertIsNotNone(engine.setup)
            self.assertEqual(engine.setup.direction, "sell")
            self.assertEqual(engine.setup.sweep_high, 115)      # high complet, pas une bougie partielle

            # M5 post-confirmation : a (h104.5 l100), b (l96), c (h98 l95) -> FVG [98, 100]
            for dt, q in ((10, 104.5), (100, 100), (250, 101)):
                tick(confirm + dt, q)
            for dt, q in ((M5 + 5, 101), (M5 + 100, 96), (M5 + 250, 97)):
                tick(confirm + dt, q)
            for dt, q in ((2 * M5 + 5, 97), (2 * M5 + 50, 98), (2 * M5 + 100, 95), (2 * M5 + 250, 96)):
                tick(confirm + dt, q)
            self.assertIsNone(engine.setup.fvg_low)             # c pas encore clôturée
            tick(confirm + 3 * M5 + 5, 96.5)                     # clôture c -> FVG détectée
            self.assertEqual((engine.setup.fvg_low, engine.setup.fvg_high), (98, 100))

            tick(confirm + 3 * M5 + 30, 97.5)
            self.assertEqual(engine.open_trades, [])
            tick(confirm + 3 * M5 + 60, 99.0)                    # retest
            self.assertEqual(len(engine.open_trades), 1)
            for i, q in enumerate((83, 67, 51)):
                tick(confirm + 3 * M5 + 100 + i, q)
            self.assertEqual(journal.read_closed()[0]["result"], "WIN")
            self.assertEqual(journal.read_closed()[0]["sweep_high"], 115)

    def test_discover_symbols_prefers_regular_index_and_skips_boom(self):
        import asyncio
        active = [
            {"symbol": "1HZ75V", "display_name": "Volatility 75 (1s) Index", "market": "synthetic_index"},
            {"symbol": "R_75", "display_name": "Volatility 75 Index", "market": "synthetic_index"},
            {"symbol": "R_25", "display_name": "Volatility 25 Index", "market": "synthetic_index"},
            {"symbol": "BOOM500", "display_name": "Boom 500 Index", "market": "synthetic_index"},
            {"symbol": "CRASH500", "display_name": "Crash 500 Index", "market": "synthetic_index"},
            {"symbol": "frxXAUUSD", "display_name": "Gold/USD", "market": "commodities"},
            {"symbol": "cryBTCUSD", "display_name": "BTC/USD", "market": "cryptocurrency"},
        ]
        found = asyncio.run(discover_symbols(FakeClient({}, active), ALL_MARKETS))
        self.assertEqual({k: v.symbol for k, v in found.items()},
                         {"V75": "R_75", "V25": "R_25", "GOLD": "frxXAUUSD", "BTC": "cryBTCUSD"})


class TestConfig(unittest.TestCase):
    def test_defaults_are_the_v1_test_setup(self):
        c = StrategyConfig()
        self.assertEqual((c.crt_tf, c.entry_tf, c.rr, c.fvg_enabled, c.direct_entry, c.trend_filter, c.be_rr, c.sl_buffer),
                         ("M30", "M5", 3.0, True, False, "OFF", 0.0, 0.0))

    def test_invalid_values(self):
        for bad in ({"crt_tf": "M10"}, {"entry_tf": "H1"}, {"rr": 0}, {"trend_filter": "MACD"},
                    {"break_even": "3"}, {"fvg_enabled": False, "direct_entry": False},
                    {"ema_fast": 200, "ema_slow": 50}, {"typo_param": 1}):
            with self.assertRaises(ValueError, msg=str(bad)):
                StrategyConfig.from_dict(bad)

    def test_m15_crt_tf_is_accepted(self):
        c = StrategyConfig.from_dict({"crt_tf": "M15"})
        self.assertEqual(c.crt_tf, "M15")
        self.assertEqual(TF_SECONDS["M15"], 900)

    def test_break_even_parsing(self):
        self.assertEqual(StrategyConfig(break_even="1.5").be_rr, 1.5)
        self.assertEqual(StrategyConfig(break_even="RR2").be_rr, 2.0)
        self.assertEqual(StrategyConfig(break_even="OFF").be_rr, 0.0)

    def test_only_allowed_markets(self):
        self.assertEqual(ALL_MARKET_KEYS, ["V75", "V25", "GOLD", "BTC"])
        self.assertFalse(any("boom" in a or "crash" in a for d in ALL_MARKETS for a in d["aliases"]))

    def test_load_config_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.json"
            p.write_text(json.dumps({"strategy": {"crt_tf": "H1", "rr": 2}, "markets": ["GOLD"]}))
            app = load_app_config(p)
            self.assertEqual((app.strategy.crt_tf, app.strategy.rr, app.markets), ("H1", 2, ["GOLD"]))
            p.write_text(json.dumps({"markets": ["BOOM500"]}))
            with self.assertRaises(ValueError):
                load_app_config(p)

    def test_telegram_and_report_defaults(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.json"
            p.write_text(json.dumps({}))
            app = load_app_config(p)
            self.assertFalse(app.telegram_enabled)
            self.assertTrue(app.chart_enabled)
            self.assertEqual((app.report_hour, app.report_minute), (21, 0))

    def test_telegram_and_report_custom_values(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.json"
            p.write_text(json.dumps({
                "telegram": {"enabled": True}, "chart_enabled": False,
                "report": {"hour": 22, "minute": 30},
            }))
            app = load_app_config(p)
            self.assertTrue(app.telegram_enabled)
            self.assertFalse(app.chart_enabled)
            self.assertEqual((app.report_hour, app.report_minute), (22, 30))

    def test_bad_report_time_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.json"
            p.write_text(json.dumps({"report": {"hour": 25, "minute": 0}}))
            with self.assertRaises(ValueError):
                load_app_config(p)


class TestStore(unittest.TestCase):
    def test_needed_timeframes(self):
        self.assertEqual(list(needed_timeframes(StrategyConfig())), ["M5", "M30"])
        self.assertEqual(list(needed_timeframes(StrategyConfig(crt_tf="H1", trend_filter="EMA"))), ["M5", "H1"])
        self.assertEqual(list(needed_timeframes(StrategyConfig(trend_filter="EMA", trend_tf="H1"))), ["M5", "M30", "H1"])
        self.assertEqual(list(needed_timeframes(StrategyConfig(crt_tf="M15"))), ["M5", "M15"])


def make_trade(direction="sell", fvg=True, **kw):
    """Même fabrique que TestPaperTrade.make, réutilisable pour le rendu de graphique."""
    entry, sl = (99.0, 115.0) if direction == "sell" else (101.0, 85.0)
    risk = 16.0
    tp = entry - 3 * risk if direction == "sell" else entry + 3 * risk
    return PaperTrade(
        id="chart-test", market_key="T", label="TEST Index", symbol="R_TEST", direction=direction,
        crt_tf="M15", entry_tf="M5", mode="FVG", trend_filter="OFF",
        c1_epoch=T0, c1_high=110, c1_low=90, sweep_epoch=T0 + 900, sweep_high=115, sweep_low=85,
        fvg_low=98 if fvg else None, fvg_high=100 if fvg else None,
        entry_epoch=T0 + 2700, entry=entry, sl=sl, tp=tp, risk=risk, rr_target=3.0, **kw)


def make_candles(start: int, step: int, n: int) -> list:
    out = []
    p = 100.0
    for i in range(n):
        o, c = p, p + (1 if i % 2 == 0 else -1)
        out.append(Candle(start + i * step, o, max(o, c) + 1, min(o, c) - 1, c))
        p = c
    return out


class TestChart(unittest.TestCase):
    def test_renders_png_for_sell_with_fvg(self):
        trade = make_trade("sell", fvg=True)
        crt_candles = make_candles(T0 - 10 * 900, 900, 12)
        entry_candles = make_candles(T0 + 900, 300, 15)
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "chart.png"
            result = render_signal_chart(trade, crt_candles, entry_candles, StrategyConfig(crt_tf="M15"), out)
            self.assertEqual(result, out)
            self.assertTrue(out.exists())
            self.assertGreater(out.stat().st_size, 0)

    def test_renders_png_for_buy_without_fvg(self):
        """mode DIRECT (pas de FVG) : ne doit pas planter faute de fvg_low/high."""
        trade = make_trade("buy", fvg=False)
        crt_candles = make_candles(T0 - 10 * 900, 900, 12)
        entry_candles = make_candles(T0 + 900, 300, 15)
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "chart.png"
            result = render_signal_chart(trade, crt_candles, entry_candles, StrategyConfig(crt_tf="M15"), out)
            self.assertTrue(out.exists())

    def test_handles_empty_candle_lists_gracefully(self):
        trade = make_trade("sell", fvg=True)
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "chart.png"
            result = render_signal_chart(trade, [], [], StrategyConfig(crt_tf="M15"), out)
            self.assertTrue(out.exists())


class FakeResponse:
    def __init__(self, ok=True, status_code=200, text=""):
        self.ok, self.status_code, self.text = ok, status_code, text


class FakeRequests:
    """Remplace le module 'requests' : capture les appels sans toucher au réseau."""
    def __init__(self):
        self.calls: list[dict] = []

    def post(self, url, data=None, files=None, timeout=None):
        self.calls.append({"url": url, "data": data, "files": files})
        return FakeResponse()


class TestTelegramNotifier(unittest.TestCase):
    def setUp(self):
        self.fake = FakeRequests()
        self._orig = requests
        globals()["requests"] = self.fake

    def tearDown(self):
        globals()["requests"] = self._orig

    def test_send_text_only_calls_sendmessage(self):
        n = TelegramNotifier("TOKEN", "12345")
        n.send("hello")
        self.assertEqual(len(self.fake.calls), 1)
        call = self.fake.calls[0]
        self.assertIn("sendMessage", call["url"])
        self.assertEqual(call["data"]["chat_id"], "12345")
        self.assertEqual(call["data"]["text"], "hello")

    def test_send_with_photo_calls_sendphoto(self):
        with tempfile.TemporaryDirectory() as d:
            photo = Path(d) / "x.png"
            photo.write_bytes(b"\x89PNG\r\n")
            n = TelegramNotifier("TOKEN", "12345")
            n.send("caption text", photo_path=photo)
            self.assertEqual(len(self.fake.calls), 1)
            call = self.fake.calls[0]
            self.assertIn("sendPhoto", call["url"])
            self.assertEqual(call["data"]["caption"], "caption text")
            self.assertIsNotNone(call["files"])

    def test_missing_photo_file_falls_back_to_text(self):
        n = TelegramNotifier("TOKEN", "12345")
        n.send("hello", photo_path=Path("/no/such/file.png"))
        self.assertIn("sendMessage", self.fake.calls[0]["url"])

    def test_network_error_does_not_raise(self):
        def boom(*a, **kw):
            raise ConnectionError("no network")
        self.fake.post = boom
        n = TelegramNotifier("TOKEN", "12345")
        n.send("hello")   # ne doit pas lever d'exception

    def test_requests_missing_logs_and_returns(self):
        globals()["requests"] = None
        n = TelegramNotifier("TOKEN", "12345")
        n.send("hello")   # ne doit pas lever d'exception


class TestJournalSignals(unittest.TestCase):
    def test_append_and_read_signals(self):
        with tempfile.TemporaryDirectory() as d:
            journal = Journal(Path(d))
            trade = make_trade("sell")
            journal.append_signal(trade)
            rows = journal.read_signals()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["direction"], "SELL")
            self.assertEqual(rows[0]["market_key"], "T")
            self.assertEqual(rows[0]["date"], iso(trade.entry_epoch)[:10])

    def test_open_trade_writes_a_signal_row(self):
        """_open_trade doit journaliser un signal en plus d'ouvrir le trade paper."""
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            h = Harness(tmp)
            confirm = sell_crt(h)
            bearish_fvg(h, confirm)
            h.prices(confirm + 3 * M5 + 10, 99.0)
            rows = h.journal.read_signals()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_key"], "TEST")


class TestDailyReport(unittest.TestCase):
    def test_next_report_time_same_day(self):
        now = datetime(2026, 9, 19, 14, 0, 0)
        target = _next_report_time(now, 21, 0)
        self.assertEqual(target, datetime(2026, 9, 19, 21, 0, 0))

    def test_next_report_time_rolls_to_tomorrow(self):
        now = datetime(2026, 9, 19, 21, 30, 0)
        target = _next_report_time(now, 21, 0)
        self.assertEqual(target, datetime(2026, 9, 20, 21, 0, 0))

    def test_format_daily_report_counts(self):
        signals = [
            {"market_key": "V75", "direction": "BUY"},
            {"market_key": "V75", "direction": "SELL"},
            {"market_key": "GOLD", "direction": "SELL"},
        ]
        closed = [
            {"market_key": "V75", "result": "WIN", "r_result": 3.0, "rr1_time": "t", "rr2_time": "t", "rr3_time": "t"},
            {"market_key": "V75", "result": "LOSS", "r_result": -1.0, "rr1_time": None, "rr2_time": None, "rr3_time": None},
            {"market_key": "GOLD", "result": "WIN", "r_result": 3.0, "rr1_time": "t", "rr2_time": "t", "rr3_time": "t"},
        ]
        text = format_daily_report("19/09/2026", ["V75", "GOLD"], signals, closed)
        self.assertIn("Total : 3", text)
        self.assertIn("BUY : 1", text)
        self.assertIn("SELL : 2", text)
        self.assertIn("WIN : 2", text)
        self.assertIn("LOSS : 1", text)
        self.assertIn("Winrate : 66.7%", text)
        self.assertIn("RR1 atteint : 2", text)
        self.assertIn("V75", text)
        self.assertIn("GOLD", text)

    def test_format_daily_report_with_no_data(self):
        text = format_daily_report("19/09/2026", ["V75"], [], [])
        self.assertIn("Total : 0", text)
        self.assertIn("Winrate : 0.0%", text)


class TestV1SingleTimeframeDirectEntry(unittest.TestCase):
    """Nouveau mode V1 : crt_tf == entry_tf (ex. M5 partout), FVG désactivée,
    entrée directe immédiate au prix de clôture de la bougie de sweep, sans
    attendre une nouvelle bougie ni un retest."""

    def make_h(self, tmp, **kw):
        return Harness(tmp, crt_tf="M5", entry_tf="M5", fvg_enabled=False, direct_entry=True, **kw)

    def test_sell_enters_immediately_on_confirmation(self):
        with tempfile.TemporaryDirectory() as d:
            h = self.make_h(Path(d))
            c1 = Candle(T0, 100, 110, 90, 105)
            c2 = Candle(T0 + M5, 105, 115, 100, 104)   # sweep du high (115>110), clôture 104<110
            h.close("M5", c1)
            h.now = T0 + 2 * M5 + 1
            h.close("M5", c2)
            # entrée immédiate, dès la clôture de c2, sans bougie supplémentaire ni tick
            self.assertEqual(len(h.engine.open_trades), 1)
            t = h.engine.open_trades[0]
            self.assertEqual(t.direction, "sell")
            self.assertEqual(t.mode, "DIRECT")
            self.assertEqual(t.entry, 104)              # prix disponible à la confirmation = close(c2)
            self.assertEqual(t.sl, 115)                 # SL = plus haut du sweep
            self.assertEqual(t.risk, 11)
            self.assertEqual(t.tp, 104 - 3 * 11)         # RR = 3 (config par défaut)
            self.assertIsNone(h.engine.setup)            # pas de setup en attente

    def test_buy_enters_immediately_on_confirmation(self):
        with tempfile.TemporaryDirectory() as d:
            h = self.make_h(Path(d))
            c1 = Candle(T0, 100, 110, 90, 105)
            c2 = Candle(T0 + M5, 95, 100, 85, 96)      # sweep du low (85<90), clôture 96>90
            h.close("M5", c1)
            h.now = T0 + 2 * M5 + 1
            h.close("M5", c2)
            self.assertEqual(len(h.engine.open_trades), 1)
            t = h.engine.open_trades[0]
            self.assertEqual(t.direction, "buy")
            self.assertEqual(t.entry, 96)
            self.assertEqual(t.sl, 85)
            self.assertEqual(t.tp, 96 + 3 * 11)

    def test_scanner_continues_after_win_and_after_loss(self):
        """Après un WIN puis un LOSS, le scanner continue et un nouveau signal
        peut être pris immédiatement — jamais de pause."""
        with tempfile.TemporaryDirectory() as d:
            h = self.make_h(Path(d), max_positions=5)
            c1 = Candle(T0, 100, 110, 90, 105)
            c2 = Candle(T0 + M5, 105, 115, 100, 104)
            h.close("M5", c1); h.now = T0 + 2 * M5 + 1; h.close("M5", c2)
            t1 = h.engine.open_trades[0]
            h.prices(T0 + 2 * M5 + 10, t1.tp)          # TP atteint -> WIN
            self.assertEqual(t1.status, "WIN")
            self.assertEqual(h.engine.open_trades, [])

            # nouveau CRT juste après : le scanner doit toujours répondre
            c3 = Candle(T0 + 2 * M5, 104, 106, 84, 102)   # sweep du low de c2 (100) -> BUY
            h.now = T0 + 3 * M5 + 1
            h.close("M5", c3)
            self.assertEqual(len(h.engine.open_trades), 1)
            t2 = h.engine.open_trades[0]
            self.assertEqual(t2.direction, "buy")
            h.prices(T0 + 3 * M5 + 10, t2.sl)          # SL atteint -> LOSS
            self.assertEqual(t2.status, "LOSS")

            # encore un CRT ensuite : toujours actif
            c4 = Candle(T0 + 3 * M5, 102, 118, 100, 103)  # sweep du high de c3 (106) -> SELL
            h.now = T0 + 4 * M5 + 1
            h.close("M5", c4)
            self.assertEqual(len(h.engine.open_trades), 1)

    def test_no_immediate_entry_when_fvg_enabled(self):
        """Le mode entrée directe immédiate ne s'applique que si fvg_enabled=False :
        avec FVG activée, même en timeframe unique, on repasse par l'ancien flux."""
        with tempfile.TemporaryDirectory() as d:
            h = Harness(Path(d), crt_tf="M5", entry_tf="M5", fvg_enabled=True, direct_entry=False)
            c1 = Candle(T0, 100, 110, 90, 105)
            c2 = Candle(T0 + M5, 105, 115, 100, 104)
            h.close("M5", c1); h.now = T0 + 2 * M5 + 1; h.close("M5", c2)
            self.assertEqual(h.engine.open_trades, [])
            self.assertIsNotNone(h.engine.setup)


class TestMaxPositionsByMarket(unittest.TestCase):
    def test_override_replaces_global_value(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            cfg = StrategyConfig(crt_tf="M5", entry_tf="M5", fvg_enabled=False,
                                 direct_entry=True, max_positions=1)
            cfg.validate()
            candles: dict[str, list] = {"M5": []}
            notifier = FakeNotifier()
            journal = Journal(tmp)
            engine = MarketEngine("V75", "Volatility 75", "R_75", cfg,
                                    get_candles=lambda tf: candles[tf],
                                    notifier=notifier, journal=journal,
                                    now_fn=lambda: T0 + 10 * M5, max_positions=3)
            self.assertEqual(engine.max_positions, 3)   # override, pas cfg.max_positions (1)

    def test_no_override_keeps_global_value(self):
        cfg = StrategyConfig(max_positions=2)
        engine = MarketEngine("GOLD", "Gold", "frxXAUUSD", cfg,
                                get_candles=lambda tf: [],
                                notifier=FakeNotifier(), journal=None)
        self.assertEqual(engine.max_positions, 2)

    def test_config_file_parses_overrides(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.json"
            p.write_text(json.dumps({"max_positions_by_market": {"V75": 3, "GOLD": 1}}))
            app = load_app_config(p)
            self.assertEqual(app.max_positions_by_market, {"V75": 3, "GOLD": 1})

    def test_config_file_rejects_unknown_market(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.json"
            p.write_text(json.dumps({"max_positions_by_market": {"BOOM500": 1}}))
            with self.assertRaises(ValueError):
                load_app_config(p)


class TestReconnectSchedule(unittest.TestCase):
    def test_delays_match_spec(self):
        settings = ConnectionSettings()
        self.assertEqual(settings.reconnect_delays, (5.0, 10.0, 20.0, 30.0, 60.0))

    def test_run_forever_never_raises_and_uses_schedule(self):
        """Le client ne doit jamais lever d'exception : erreurs réseau et
        erreurs inattendues déclenchent toutes une reconnexion planifiée."""
        import asyncio

        class FlakyWS:
            calls = 0

            async def _run_once(self):
                FlakyWS.calls += 1
                if FlakyWS.calls == 1:
                    raise ConnectionError("boom")
                if FlakyWS.calls == 2:
                    raise ValueError("erreur inattendue")
                client._stop = True   # 3e tentative : on arrête proprement le test

        client = DerivClient(ConnectionSettings())
        client._run_once = FlakyWS()._run_once

        sleeps = []
        orig_sleep = asyncio.sleep
        async def fake_sleep(sec):
            sleeps.append(sec)
        asyncio.sleep = fake_sleep
        try:
            asyncio.run(client.run_forever())
        finally:
            asyncio.sleep = orig_sleep
        self.assertEqual(sleeps, [5.0, 10.0])   # palier progressif, jamais de crash


class TestPatAuth(unittest.TestCase):
    ENV = ("DERIV_PAT", "DERIV_PAT_APP_ID", "DERIV_ACCOUNT_ID")

    def test_disabled_without_env(self):
        with mock.patch.dict(os.environ):
            for k in self.ENV:
                os.environ.pop(k, None)
            st = ConnectionSettings()
            self.assertFalse(st.pat_enabled)
            client = DerivClient(st)
            self.assertEqual(asyncio.run(client._resolve_ws_url()), st.deriv_ws_url)

    def test_enabled_uses_otp_url_and_headers(self):
        env = {"DERIV_PAT": "pat_x", "DERIV_PAT_APP_ID": "123", "DERIV_ACCOUNT_ID": "DOT1"}
        with mock.patch.dict(os.environ, env):
            st = ConnectionSettings()
            self.assertTrue(st.pat_enabled)
            self.assertTrue(st.otp_url.endswith("/trading/v1/options/accounts/DOT1/otp"))

            class Resp:
                status_code = 200
                text = ""
                def json(self): return {"data": {"url": "wss://api.derivws.com/x/ws/demo?otp=abc"}}

            class Req:
                calls = []
                @staticmethod
                def post(url, headers=None, timeout=None):
                    Req.calls.append((url, headers))
                    return Resp()

            orig = globals()["requests"]
            globals()["requests"] = Req
            try:
                url = asyncio.run(DerivClient(st)._resolve_ws_url())
            finally:
                globals()["requests"] = orig
            self.assertEqual(url, "wss://api.derivws.com/x/ws/demo?otp=abc")
            self.assertEqual(Req.calls[0][1]["Authorization"], "Bearer pat_x")
            self.assertEqual(Req.calls[0][1]["Deriv-App-ID"], "123")

    def test_otp_http_error_never_leaks_token(self):
        env = {"DERIV_PAT": "pat_secret", "DERIV_PAT_APP_ID": "123", "DERIV_ACCOUNT_ID": "DOT1"}
        with mock.patch.dict(os.environ, env):
            class Resp:
                status_code = 401
                text = "Unauthorized"
                def json(self): return {}

            class Req:
                @staticmethod
                def post(url, headers=None, timeout=None): return Resp()

            orig = globals()["requests"]
            globals()["requests"] = Req
            try:
                with self.assertRaises(ConnectionError) as cm:
                    DerivClient(ConnectionSettings())._fetch_otp_url()
            finally:
                globals()["requests"] = orig
            self.assertNotIn("pat_secret", str(cm.exception))


class TestExampleConfig(unittest.TestCase):
    """Le modèle intégré (ex config_example.json) doit rester valide."""

    def test_example_config_loads_and_matches_defaults(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.json"
            p.write_text(EXAMPLE_CONFIG_JSON, encoding="utf-8")
            app = load_app_config(p)
        self.assertEqual(app.markets, ALL_MARKET_KEYS)
        self.assertEqual(app.strategy, StrategyConfig())

    def test_write_example_config_never_overwrites(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "sub" / "config.json"
            self.assertTrue(write_example_config(p))
            self.assertEqual(p.read_text(encoding="utf-8"), EXAMPLE_CONFIG_JSON)
            p.write_text("{}", encoding="utf-8")
            self.assertFalse(write_example_config(p))
            self.assertEqual(p.read_text(encoding="utf-8"), "{}")


class TestTelegramCredentials(unittest.TestCase):
    """Valeurs par défaut intégrées, remplacées par l'environnement (.env) quand il est défini."""

    KEYS = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "TELEGRAM_ADMIN_ID")

    def _clean_env(self):
        for k in self.KEYS:
            os.environ.pop(k, None)

    def test_defaults_when_env_absent(self):
        with mock.patch.dict(os.environ):
            self._clean_env()
            self.assertEqual(telegram_credentials(),
                             (DEFAULT_TELEGRAM_BOT_TOKEN, DEFAULT_TELEGRAM_CHAT_ID, DEFAULT_TELEGRAM_ADMIN_ID))

    def test_env_overrides_defaults(self):
        env = {"TELEGRAM_BOT_TOKEN": "T", "TELEGRAM_CHAT_ID": "-1", "TELEGRAM_ADMIN_ID": "2"}
        with mock.patch.dict(os.environ, env):
            self.assertEqual(telegram_credentials(), ("T", "-1", "2"))

    def test_empty_env_value_falls_back_to_default(self):
        with mock.patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": ""}):
            self.assertEqual(telegram_credentials()[0], DEFAULT_TELEGRAM_BOT_TOKEN)

    def test_build_notifier_falls_back_without_credentials(self):
        """Sans .env et sans défaut codé en dur (sécurité), build_notifier ne doit
        jamais fabriquer un TelegramNotifier avec un token vide : repli fichier."""
        orig = requests
        globals()["requests"] = FakeRequests()          # aucun accès réseau
        try:
            with mock.patch.dict(os.environ), tempfile.TemporaryDirectory() as d:
                self._clean_env()
                app = AppConfig(strategy=StrategyConfig(), markets=[], data_dir=Path(d), telegram_enabled=True)
                n = build_notifier(app)
                self.assertIsInstance(n, Notifier)
                self.assertNotIsInstance(n, TelegramNotifier)
        finally:
            globals()["requests"] = orig

    def test_build_notifier_uses_env_when_set(self):
        orig = requests
        globals()["requests"] = FakeRequests()
        try:
            env = {"TELEGRAM_BOT_TOKEN": "T", "TELEGRAM_CHAT_ID": "-1"}
            with mock.patch.dict(os.environ, env), tempfile.TemporaryDirectory() as d:
                app = AppConfig(strategy=StrategyConfig(), markets=[], data_dir=Path(d), telegram_enabled=True)
                n = build_notifier(app)
                self.assertIsInstance(n, TelegramNotifier)
                self.assertEqual((n.bot_token, n.chat_id), ("T", "-1"))
        finally:
            globals()["requests"] = orig


def run_tests(verbosity: int = 1) -> int:
    """Lance les tests intégrés. Retourne 0 si tout passe, 1 sinon."""
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(verbosity=verbosity).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    main()

