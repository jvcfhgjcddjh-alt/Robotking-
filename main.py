"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   BINANCE FUTURES ELITE BOT  v3  ·  SMC · 14 CRYPTOS · $20 · 20×          ║
║                                                                              ║
║  OBJECTIF : 20 $ → 200 $  en compound  (objectif journalier +30 %)         ║
║                                                                              ║
║  CAPITAL & LEVIER                                                            ║
║  ✦ Capital réel    : 20 $  (ISOLATED MARGIN par trade)                      ║
║  ✦ Levier          : 20×  →  Position dynamique = capital × 20              ║
║  ✦ Risque / trade  : 8 % du capital courant (COMPOUND agressif)             ║
║                                                                              ║
║  FRAIS (Binance Futures USDM taker)                                          ║
║  ✦ Taker fee       : 0,05 %  par côté  → 0,10 % aller-retour               ║
║  ✦ Trades/jour     : illimité — stoppe quand +30 % du solde atteint         ║
║                                                                              ║
║  SIGNAUX (SMC multi-timeframe H4 → M15 → M5)                               ║
║  ✦ 14 marchés crypto scannés 24/7                                           ║
║  ✦ Score minimum   : 82 / 100                                               ║
║  ✦ RR minimum      : 3,0  (pour couvrir frais + profit net)                 ║
║  ✦ TP partiel : 40 % @ TP1 · 35 % @ TP2 · 25 % @ TP3                      ║
║  ✦ SL déplacé au break-even après TP1                                       ║
║                                                                              ║
║  MODE DÉMO (défaut si pas de clé API)                                        ║
║  ✦ Prix réels Binance — ordres simulés localement                           ║
║  ✦ Aucune clé API requise en démo                                           ║
║                                                                              ║
║  ENVOI TELEGRAM                                                              ║
║  ✦ Image du chart M15 + niveaux SMC à chaque signal                        ║
║  ✦ Image equity curve après chaque trade fermé                              ║
║  ✦ Rapport quotidien à 23h UTC                                              ║
║                                                                              ║
║  DÉPLOIEMENT RENDER                                                          ║
║  ✦ Serveur Flask port 10000 (keep-alive inclus)                             ║
║                                                                              ║
║  Installation :                                                              ║
║      pip install ccxt pandas numpy matplotlib requests flask colorama        ║
║                                                                              ║
║  Variables d'environnement (Render → Environment) :                         ║
║      BINANCE_API_KEY     → clé API Binance Futures  (optionnel en démo)     ║
║      BINANCE_API_SECRET  → secret API               (optionnel en démo)     ║
║      TG_TOKEN            → token bot Telegram                               ║
║      TG_LEADER_ID        → ton ID perso Telegram                            ║
║      TG_GROUP_ID         → ID du groupe (optionnel)                         ║
║                                                                              ║
║  Usage :                                                                     ║
║      python btc_futures_elite.py            # auto-démo si pas de clé API  ║
║      python btc_futures_elite.py --paper    # forcer démo                  ║
║      python btc_futures_elite.py --live     # live (clés API obligatoires) ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════
import os, sys, time, json, logging, argparse, threading, io
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import requests

try:
    import ccxt
except ImportError:
    sys.exit("❌ pip install ccxt")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    print("⚠ matplotlib manquant — charts désactivés  (pip install matplotlib)")

from flask import Flask, jsonify

try:
    from colorama import Fore, Style, init as _ci
    _ci(autoreset=True)
    _C = True
except ImportError:
    _C = False


# ═══════════════════════════════════════════════════════════════
#  ██████  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

# ── Capital & levier ─────────────────────────────────────────
CAPITAL_INIT     = 20.0      # capital de départ en USDT
LEVERAGE         = 20        # levier (Isolated Margin)
RISK_PCT         = 0.08      # 8 % du capital courant par trade (compound agressif)
TAKER_FEE        = 0.0005    # 0,05 % par côté (Binance Futures taker)
MAX_TRADES_DAY   = 9999      # illimité — stoppé uniquement par l'objectif journalier
DAILY_TARGET_PCT = 0.30      # +30 % du solde du jour → stop trading ce jour
MIN_RR           = 3.0       # RR minimum absolu
SCORE_MIN        = 82        # score SMC minimum
TARGET_USD       = 200.0     # objectif total ($) pour arrêt du bot

# ── Limites de perte (Risk Management) ───────────────────────
DAILY_LOSS_LIMIT_PCT  = 0.15   # L1 : -15% du capital du jour  → pause jusqu'à demain
MAX_CONSEC_LOSSES     = 3      # L2 : 3 SL consécutifs          → cooldown 2h
CONSEC_LOSS_PAUSE_SEC = 7200   # L2 : durée du cooldown en secondes (2h)
CIRCUIT_BREAKER_PCT   = 0.60   # L3 : capital < 60% initial     → arrêt total bot

# ── TP partiels ───────────────────────────────────────────────
TP1_QTY_PCT = 0.40   # 40 % @ TP1 (1× R) → SL → BE
TP2_QTY_PCT = 0.35   # 35 % @ TP2 (2× R)
TP3_QTY_PCT = 0.25   # 25 % @ TP3 (3× R)

# ── Paramètres SMC ───────────────────────────────────────────
HTF = "4h"
MTF = "15m"
LTF = "5m"
FVG_MIN_RATIO        = 0.0002
OB_LOOKBACK          = 15
SEPTUPLE_MIN_CANDLES = 5
AMD_LOOKBACK         = 50
SD_MIN_IMPULSE_RATIO = 1.5
SD_ZONE_BUFFER       = 0.15

# ── 14 marchés crypto Binance Futures ────────────────────────
MARKETS: list[tuple[str, str]] = [
    ("BTC/USDT:USDT",  "Bitcoin"),
    ("ETH/USDT:USDT",  "Ethereum"),
    ("BNB/USDT:USDT",  "BNB"),
    ("SOL/USDT:USDT",  "Solana"),
    ("XRP/USDT:USDT",  "XRP"),
    ("DOGE/USDT:USDT", "Dogecoin"),
    ("ADA/USDT:USDT",  "Cardano"),
    ("AVAX/USDT:USDT", "Avalanche"),
    ("LINK/USDT:USDT", "Chainlink"),
    ("DOT/USDT:USDT",  "Polkadot"),
    ("LTC/USDT:USDT",  "Litecoin"),
    ("MATIC/USDT:USDT","Polygon"),
    ("ATOM/USDT:USDT", "Cosmos"),
    ("NEAR/USDT:USDT", "NEAR"),
]

# ── Env ───────────────────────────────────────────────────────
TG_TOKEN       = os.environ.get("TG_TOKEN", "METS_TON_NOUVEAU_TOKEN_ICI")
TG_LEADER_ID   = os.environ.get("TG_LEADER_ID", "6982051442")
TG_GROUP_ID    = os.environ.get("TG_GROUP_ID", "")
API_KEY        = os.environ.get("BINANCE_API_KEY", "")
API_SECRET     = os.environ.get("BINANCE_API_SECRET", "")
SCAN_INTERVAL  = 30   # secondes entre chaque cycle de scan


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════
def _mk_log() -> logging.Logger:
    lg = logging.getLogger("elite_bot")
    lg.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s",
                            datefmt="%H:%M:%S")
    for h in [logging.StreamHandler(sys.stdout),
               logging.FileHandler("elite_bot.log", encoding="utf-8")]:
        h.setFormatter(fmt)
        lg.addHandler(h)
    return lg

log = _mk_log()


def c(t: str, color: str = "green") -> str:
    if not _C:
        return t
    p = {"green": Fore.GREEN, "red": Fore.RED, "yellow": Fore.YELLOW,
         "cyan": Fore.CYAN, "white": Fore.WHITE, "magenta": Fore.MAGENTA}
    return p.get(color, "") + t + Style.RESET_ALL


# ═══════════════════════════════════════════════════════════════
#  DATACLASSES
# ═══════════════════════════════════════════════════════════════

@dataclass
class FVG:
    direction: str; top: float; bottom: float; index: int; filled: bool = False

@dataclass
class OrderBlock:
    direction: str; top: float; bottom: float; index: int; mitigated: bool = False

@dataclass
class SDZone:
    zone_type: str; top: float; bottom: float; index: int; impulse: float

@dataclass
class AmdPhase:
    phase: str; sub_phase: str; direction: str; confidence: int
    range_high: float; range_low: float
    sweep_level: Optional[float] = None
    reasons: list = field(default_factory=list)

@dataclass
class Signal:
    sym: str; name: str; direction: str
    entry: float; sl: float; tp1: float; tp2: float; tp3: float
    rr: float; score: int; timestamp: datetime; htf_bias: str
    mode: str = "SMC"; reasons: list = field(default_factory=list)
    df_m15: object = field(default=None, repr=False)
    fvg: object = field(default=None, repr=False)
    ob: object = field(default=None, repr=False)
    bos_lv: float = 0.0; choch_lv: float = 0.0

@dataclass
class Trade:
    sym: str; name: str; direction: str
    entry: float; sl: float; tp1: float; tp2: float; tp3: float
    qty_total: float; qty_rem: float
    capital_at_open: float; risk_usd: float; fee_open: float
    opened_at: datetime; order_id: str
    status: str = "open"   # open | tp1 | tp2 | closed | sl


# ═══════════════════════════════════════════════════════════════
#  ÉTAT GLOBAL
# ═══════════════════════════════════════════════════════════════

class BotState:
    """Thread-safe état du bot."""
    def __init__(self):
        self._lock        = threading.Lock()
        self.capital      = CAPITAL_INIT
        self.peak_capital = CAPITAL_INIT
        self.trades_today = 0
        self.fees_today   = 0.0
        self.pnl_today    = 0.0
        self.wins = self.losses = 0
        self.total_trades = 0
        self._day         = ""
        self._capital_start_day = CAPITAL_INIT   # capital au début du jour
        self.equity_curve : list[tuple[str, float]] = [
            (datetime.now(timezone.utc).strftime("%d/%m %H:%M"), CAPITAL_INIT)
        ]
        self.history: list[dict] = []
        self.scan_cycle   = 0
        self.active_trade : Optional[Trade] = None
        # ── Risk Management ──────────────────────────────────
        self.consec_losses     = 0              # L2 : compteur SL consécutifs
        self.consec_pause_until: float = 0.0   # L2 : timestamp fin de pause
        self.circuit_broken    = False          # L3 : circuit breaker déclenché

    def _chk_day(self):
        d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if d != self._day:
            self._day = d
            self.trades_today = 0
            self.fees_today   = 0.0
            self.pnl_today    = 0.0
            self._capital_start_day = self.capital
            self.consec_losses = 0          # reset série en début de journée
            log.info(f"  📅 Nouveau jour — capital de référence : {self.capital:.3f} $")

    # ── L1 : perte journalière ───────────────────────────────
    def daily_loss_triggered(self) -> bool:
        """Retourne True si la perte du jour dépasse DAILY_LOSS_LIMIT_PCT."""
        loss_pct = -self.pnl_today / max(self._capital_start_day, 0.01)
        return loss_pct >= DAILY_LOSS_LIMIT_PCT

    # ── L3 : circuit breaker global ──────────────────────────
    def check_circuit_breaker(self) -> bool:
        """Retourne True si le capital est passé sous le seuil critique."""
        if not self.circuit_broken:
            threshold = CAPITAL_INIT * CIRCUIT_BREAKER_PCT
            if self.capital < threshold:
                self.circuit_broken = True
                log.critical(
                    f"  🔴 CIRCUIT BREAKER — capital {self.capital:.3f} $ < "
                    f"{threshold:.2f} $ ({CIRCUIT_BREAKER_PCT*100:.0f}% initial)")
        return self.circuit_broken

    def can_trade(self) -> bool:
        with self._lock:
            self._chk_day()
            # L3 : circuit breaker global (priorité absolue)
            if self.check_circuit_breaker():
                return False
            # L1 : perte journalière max
            if self.daily_loss_triggered():
                return False
            # L1 : objectif journalier +30% atteint
            daily_gain_pct = self.pnl_today / max(self._capital_start_day, 0.01)
            if daily_gain_pct >= DAILY_TARGET_PCT:
                return False
            # L2 : cooldown après série de SL
            if time.time() < self.consec_pause_until:
                return False
            if self.trades_today >= MAX_TRADES_DAY:
                return False
            return True

    def open_trade(self, fee: float):
        with self._lock:
            self._chk_day()
            self.trades_today += 1
            self.fees_today   += fee
            self.total_trades += 1
            self.capital      -= fee   # frais d'ouverture déduits

    def close_trade(self, pnl_gross: float, fee: float, won: bool):
        with self._lock:
            self.fees_today += fee
            self.pnl_today  += pnl_gross
            self.capital    += pnl_gross - fee
            self.peak_capital = max(self.peak_capital, self.capital)
            if won:
                self.wins += 1
                self.consec_losses = 0   # reset la série sur un gain
            else:
                self.losses += 1
                self.consec_losses += 1
                # L2 : série de SL → cooldown
                if self.consec_losses >= MAX_CONSEC_LOSSES:
                    resume = time.time() + CONSEC_LOSS_PAUSE_SEC
                    self.consec_pause_until = resume
                    resume_dt = datetime.fromtimestamp(resume, tz=timezone.utc)
                    log.warning(
                        f"  ⚠ L2 — {MAX_CONSEC_LOSSES} SL consécutifs — "
                        f"pause jusqu'à {resume_dt.strftime('%H:%M UTC')}")
                    # Telegram alert L2
                    tg_text(
                        f"⚠️ <b>PAUSE FORCÉE — {MAX_CONSEC_LOSSES} SL consécutifs</b>\n"
                        f"Cooldown 2h — reprise à {resume_dt.strftime('%H:%M UTC')}\n"
                        f"Capital : {self.capital:.3f} $"
                    )
                    self.consec_losses = 0  # reset après déclenchement
            ts = datetime.now(timezone.utc).strftime("%d/%m %H:%M")
            self.equity_curve.append((ts, round(self.capital, 3)))
            self.history.append({
                "ts": ts, "won": won,
                "pnl": round(pnl_gross - fee, 3),
                "cap": round(self.capital, 3),
            })
            # L1 : vérif perte journalière après chaque close
            if not won and self.daily_loss_triggered():
                loss_pct = -self.pnl_today / max(self._capital_start_day, 0.01) * 100
                log.warning(
                    f"  🔴 L1 — Perte journalière {loss_pct:.1f}% atteinte — "
                    f"pause jusqu'à demain UTC")
                tg_text(
                    f"🔴 <b>LIMITE PERTE JOURNALIÈRE ATTEINTE</b>\n"
                    f"Perte : -{loss_pct:.1f}% du capital du jour\n"
                    f"Capital : {self.capital:.3f} $\n"
                    f"⏸ Pause trading jusqu'à 00:00 UTC"
                )
            # L3 : circuit breaker
            if self.check_circuit_breaker():
                threshold = CAPITAL_INIT * CIRCUIT_BREAKER_PCT
                tg_text(
                    f"🚨 <b>CIRCUIT BREAKER DÉCLENCHÉ</b>\n"
                    f"Capital {self.capital:.3f} $ < seuil {threshold:.2f} $\n"
                    f"({CIRCUIT_BREAKER_PCT*100:.0f}% du capital initial)\n"
                    f"🔴 BOT ARRÊTÉ — Intervention manuelle requise"
                )

    def summary(self) -> str:
        with self._lock:
            total = self.wins + self.losses
            wr = self.wins / total * 100 if total else 0
            net = self.pnl_today - self.fees_today
            growth = (self.capital - CAPITAL_INIT) / CAPITAL_INIT * 100
            loss_pct = -self.pnl_today / max(self._capital_start_day, 0.01) * 100
            cb_pct   = self.capital / CAPITAL_INIT * 100
            risk_lines = []
            if self.circuit_broken:
                risk_lines.append(f"🚨 CIRCUIT BREAKER actif ({cb_pct:.0f}% initial)")
            elif loss_pct >= DAILY_LOSS_LIMIT_PCT * 100:
                risk_lines.append(f"🔴 L1 perte jour -{loss_pct:.1f}% (limite {DAILY_LOSS_LIMIT_PCT*100:.0f}%)")
            if time.time() < self.consec_pause_until:
                resume = datetime.fromtimestamp(
                    self.consec_pause_until, tz=timezone.utc).strftime('%H:%M UTC')
                risk_lines.append(f"⚠️ L2 cooldown — reprise {resume}")
            risk_str = ("\n" + "\n".join(risk_lines)) if risk_lines else ""
            return (
                f"💼 Capital : <b>{self.capital:.2f} $</b>  "
                f"({growth:+.1f}% vs init)\n"
                f"📊 Aujourd'hui : {self.trades_today} trades | "
                f"WR {wr:.0f}% ({self.wins}W/{self.losses}L)\n"
                f"💸 Frais : -{self.fees_today:.3f} $ | Net : "
                f"{'🟢' if net >= 0 else '🔴'}{net:+.3f} ${risk_str}"
            )

STATE = BotState()


# ═══════════════════════════════════════════════════════════════
#  FLASK (Render keep-alive)
# ═══════════════════════════════════════════════════════════════

flask_app = Flask(__name__)

@flask_app.route("/")
def dashboard():
    with STATE._lock:
        cap   = STATE.capital
        cycle = STATE.scan_cycle
        wins  = STATE.wins; losses = STATE.losses
        total = wins + losses
        wr    = wins / total * 100 if total else 0
        growth = (cap - CAPITAL_INIT) / CAPITAL_INIT * 100
        hist  = list(reversed(STATE.history[-10:]))
        active = STATE.active_trade

    rows = ""
    for h in hist:
        g = "#2ecc71" if h["won"] else "#e74c3c"
        rows += (f"<tr><td>{h['ts']}</td>"
                 f"<td style='color:{g};font-weight:bold'>"
                 f"{'WIN' if h['won'] else 'LOSS'}</td>"
                 f"<td>{h['pnl']:+.3f} $</td>"
                 f"<td>{h['cap']:.3f} $</td></tr>")

    active_html = ""
    if active:
        ac = "#2ecc71" if active.direction == "LONG" else "#e74c3c"
        active_html = (
            f"<p>🔴 Trade actif : <b style='color:{ac}'>{active.direction}</b> "
            f"{active.name} @ {active.entry:,.2f} $  "
            f"SL {active.sl:,.2f}  TP1 {active.tp1:,.2f}</p>"
        )

    return f"""<!DOCTYPE html><html lang='fr'>
<head><meta charset='UTF-8'><meta http-equiv='refresh' content='30'>
<title>Elite Bot</title>
<style>body{{font-family:monospace;background:#0d1117;color:#c9d1d9;margin:2em}}
h1{{color:#58a6ff}}h2{{color:#8b949e;border-bottom:1px solid #30363d;padding-bottom:.3em}}
table{{border-collapse:collapse;width:100%}}
th{{background:#161b22;color:#8b949e;padding:.5em 1em;text-align:left}}
td{{padding:.4em 1em;border-bottom:1px solid #21262d}}
.g{{color:#2ecc71}}.r{{color:#e74c3c}}
.badge{{display:inline-block;padding:.2em .6em;border-radius:4px;font-size:.85em;font-weight:bold}}
</style></head><body>
<h1>⚡ Elite Futures Bot  ·  SMC · 14 Cryptos · $20 · 20×</h1>
<p>Cycle #{cycle}  ·  Scan toutes les {SCAN_INTERVAL}s  ·  ⟳ Refresh auto 30s</p>
{active_html}
<h2>📈 Capital</h2>
<table><tr>
<td><b>Capital actuel</b></td><td class='{'g' if growth>=0 else 'r'}'><b>{cap:.2f} $</b>  ({growth:+.1f}%)</td>
<td><b>Objectif</b></td><td>{TARGET_USD} $</td>
<td><b>WR global</b></td><td>{wr:.0f}%  ({wins}W / {losses}L)</td>
<td><b>Trades total</b></td><td>{total}</td>
</tr></table>
<h2>📋 10 derniers trades</h2>
{'<table><tr><th>Heure</th><th>Résultat</th><th>PnL net</th><th>Capital</th></tr>'
 + rows + '</table>' if rows else '<p style="color:#f39c12">Aucun trade encore.</p>'}
<h2>⚙️ Configuration</h2>
<table>
<tr><td>Capital init</td><td>{CAPITAL_INIT} $</td></tr>
<tr><td>Levier</td><td>{LEVERAGE}×</td></tr>
<tr><td>Risque/trade</td><td>{RISK_PCT*100:.0f}% du capital (compound)</td></tr>
<tr><td>Frais taker</td><td>{TAKER_FEE*100:.2f}% × 2 = {TAKER_FEE*2*100:.2f}% aller-retour</td></tr>
<tr><td>Trades/jour</td><td>Illimités — stop à +{DAILY_TARGET_PCT*100:.0f}% du solde</td></tr>
<tr><td>Score min</td><td>{SCORE_MIN}/100</td></tr>
<tr><td>RR min</td><td>1:{MIN_RR}</td></tr>
<tr><td>Marchés</td><td>{len(MARKETS)} cryptos 24/7</td></tr>
</table></body></html>"""

@flask_app.route("/status")
def status():
    with STATE._lock:
        return jsonify({
            "capital": STATE.capital, "cycle": STATE.scan_cycle,
            "wins": STATE.wins, "losses": STATE.losses,
            "trades_today": STATE.trades_today,
        })

def start_flask(port: int = 10000):
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

def start_selfping(port: int = 10000):
    url = os.environ.get("RENDER_EXTERNAL_URL", f"http://localhost:{port}") + "/status"
    def _loop():
        time.sleep(60)
        while True:
            try:
                requests.get(url, timeout=10)
            except Exception:
                pass
            time.sleep(240)
    threading.Thread(target=_loop, daemon=True, name="ping").start()


# ═══════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════

def _tg(method: str) -> str:
    return f"https://api.telegram.org/bot{TG_TOKEN}/{method}"

def tg_text(txt: str, cid: str = "") -> bool:
    if not TG_TOKEN: return False
    for chat in filter(None, [cid or TG_LEADER_ID, TG_GROUP_ID if not cid else ""]):
        try:
            requests.post(_tg("sendMessage"),
                          json={"chat_id": chat, "text": txt,
                                "parse_mode": "HTML",
                                "disable_web_page_preview": True},
                          timeout=10)
        except Exception:
            pass
    return True

def tg_photo(path: str, caption: str, cid: str = "") -> bool:
    if not TG_TOKEN or not HAS_MPL: return False
    for chat in filter(None, [cid or TG_LEADER_ID, TG_GROUP_ID if not cid else ""]):
        try:
            with open(path, "rb") as f:
                requests.post(_tg("sendPhoto"),
                              data={"chat_id": chat, "caption": caption,
                                    "parse_mode": "HTML"},
                              files={"photo": f}, timeout=30)
        except Exception:
            pass
    return True


# ═══════════════════════════════════════════════════════════════
#  CHART — Signal Setup
# ═══════════════════════════════════════════════════════════════

_BG   = "#0a0c10"; _BG2  = "#0d1117"
_GRN  = "#22c55e"; _RED  = "#ef4444"
_BLU  = "#3b82f6"; _PRP  = "#a855f7"
_GLD  = "#f59e0b"; _ORG  = "#f97316"
_GRY  = "#64748b"; _LGY  = "#94a3b8"
_MONO = "DejaVu Sans Mono"


def chart_signal(sig: Signal) -> Optional[str]:
    """Génère chart M15 SMC dark-theme → /tmp/*.png."""
    if not HAS_MPL or sig.df_m15 is None or len(sig.df_m15) < 10:
        return None
    try:
        df = sig.df_m15.tail(80).reset_index(drop=True)
        n  = len(df)
        fig, ax = plt.subplots(figsize=(13, 7), facecolor=_BG)
        ax.set_facecolor(_BG2)
        for s in ax.spines.values():
            s.set_color("#1e293b")

        prices = pd.concat([df["high"], df["low"]])
        p_min  = prices.min() * 0.9993
        p_max  = prices.max() * 1.0007

        # Grid
        for p in np.linspace(p_min, p_max, 8):
            ax.axhline(p, color="#1e293b", lw=0.5, ls="--", alpha=0.45)

        # FVG
        fvg = sig.fvg
        if fvg and p_min <= fvg.top <= p_max:
            x0 = max(0, fvg.index - 2)
            ax.add_patch(Rectangle((x0, fvg.bottom), n - x0, fvg.top - fvg.bottom,
                                   facecolor=_BLU, alpha=0.13, zorder=1))
            ax.add_patch(Rectangle((x0, fvg.bottom), n - x0, fvg.top - fvg.bottom,
                                   edgecolor=_BLU, facecolor="none", lw=1, ls="--",
                                   alpha=0.5, zorder=2))
            ax.text((x0 + min(x0 + 12, n)) / 2, (fvg.top + fvg.bottom) / 2,
                    "FVG", color=_BLU, fontsize=9, fontweight="bold",
                    ha="center", va="center", fontfamily=_MONO,
                    bbox=dict(fc=_BG2, ec=_BLU, boxstyle="round,pad=0.2", alpha=0.9))

        # OB
        ob = sig.ob
        if ob and p_min <= ob.top <= p_max:
            x0 = max(0, ob.index - 2); x1 = min(n, ob.index + 12)
            ax.add_patch(Rectangle((x0, ob.bottom), x1 - x0, ob.top - ob.bottom,
                                   facecolor=_PRP, alpha=0.16, zorder=1))
            ax.add_patch(Rectangle((x0, ob.bottom), x1 - x0, ob.top - ob.bottom,
                                   edgecolor=_PRP, facecolor="none", lw=1.5, zorder=2))
            ax.text((x0 + x1) / 2, (ob.top + ob.bottom) / 2, "OB",
                    color=_PRP, fontsize=9, fontweight="bold",
                    ha="center", va="center", fontfamily=_MONO,
                    bbox=dict(fc=_BG2, ec=_PRP, boxstyle="round,pad=0.2", alpha=0.9))

        # BOS / CHoCH
        if sig.bos_lv and p_min <= sig.bos_lv <= p_max:
            ax.axhline(sig.bos_lv, color=_RED, lw=1.4, ls="--",
                       xmin=0.0, xmax=0.5, zorder=3)
            ax.text(n * 0.22, sig.bos_lv * 1.0002, "BOS",
                    color=_RED, fontsize=9, fontweight="bold", fontfamily=_MONO)
        if sig.choch_lv and p_min <= sig.choch_lv <= p_max:
            ax.axhline(sig.choch_lv, color=_ORG, lw=1.4, ls=":",
                       xmin=0.45, xmax=0.78, zorder=3)
            ax.text(n * 0.58, sig.choch_lv * 1.0002, "CHoCH",
                    color=_ORG, fontsize=9, fontweight="bold", fontfamily=_MONO)

        # Niveaux SL / Entry / TP1 / TP2 / TP3
        dec = 2 if sig.entry > 10 else 5
        for price, lbl, col_ in [
            (sig.tp3,   f"TP3  {sig.tp3:,.{dec}f}",   _GRN),
            (sig.tp2,   f"TP2  {sig.tp2:,.{dec}f}",   _GRN),
            (sig.tp1,   f"TP1  {sig.tp1:,.{dec}f}",   _GRN),
            (sig.entry, f"ENTRY {sig.entry:,.{dec}f}", _GLD),
            (sig.sl,    f"SL   {sig.sl:,.{dec}f}",    _RED),
        ]:
            if p_min <= price <= p_max:
                ax.axhline(price, color=col_, lw=1.1, ls="--", alpha=0.85,
                           xmin=0.45, zorder=2)
                ax.text(n - 0.4, price, lbl, color=col_, fontsize=8.5,
                        va="center", ha="right", fontfamily=_MONO,
                        bbox=dict(fc=_BG2, alpha=0.85, pad=1, ec="none"))

        # Flèche entry
        ex = max(n - 14, n // 2)
        dist = abs(sig.entry - sig.sl)
        arr0 = sig.entry - dist * 0.7 if sig.direction == "LONG" else sig.entry + dist * 0.7
        ax.annotate("", xy=(ex, sig.entry), xytext=(ex, arr0),
                    arrowprops=dict(arrowstyle="->", color=_GRN, lw=2.4))

        # Bougies
        for i, row in df.iterrows():
            o, h_, l_, cl_ = row["open"], row["high"], row["low"], row["close"]
            up = cl_ >= o
            c_ = _GRN if up else _RED
            bh = max(abs(cl_ - o), (p_max - p_min) * 0.0007)
            ax.plot([i, i], [l_, h_], color=c_, lw=1.1, zorder=4)
            ax.add_patch(Rectangle((i - 0.38, min(cl_, o)), 0.76, bh,
                                   fc=c_ if up else "none", ec=c_, lw=1.1, zorder=5))

        # Titres
        dir_ico = "🟢 LONG" if sig.direction == "LONG" else "🔴 SHORT"
        ax.text(0.01, 0.98, f"{sig.name}  |  M15  |  {dir_ico}",
                transform=ax.transAxes, color=_LGY, fontsize=11,
                va="top", fontfamily=_MONO)
        ax.text(0.01, 0.91, f"Mode : {sig.mode}  |  Score : {sig.score}/100  |  RR 1:{sig.rr}",
                transform=ax.transAxes, color=_GRY, fontsize=9, va="top", fontfamily=_MONO)
        ax.text(0.01, 0.85, f"Capital : {STATE.capital:.2f} $  |  Levier : {LEVERAGE}×",
                transform=ax.transAxes, color=_GRY, fontsize=8, va="top", fontfamily=_MONO)

        ax.set_xlim(-1, n + 1); ax.set_ylim(p_min, p_max)
        ax.tick_params(colors=_GRY, labelsize=8)
        ax.yaxis.set_visible(False); ax.set_xticks([])

        plt.tight_layout(pad=0.4)
        safe = sig.sym.replace("/", "").replace(":", "")
        path = f"/tmp/chart_{safe}_{int(time.time())}.png"
        fig.savefig(path, dpi=130, bbox_inches="tight", facecolor=_BG)
        plt.close(fig)
        return path
    except Exception as e:
        log.warning(f"  [CHART] {e}")
        return None


# ═══════════════════════════════════════════════════════════════
#  CHART — Equity Curve
# ═══════════════════════════════════════════════════════════════

def chart_equity() -> Optional[str]:
    if not HAS_MPL: return None
    with STATE._lock:
        curve = list(STATE.equity_curve)
    if len(curve) < 2: return None
    try:
        labels = [x[0] for x in curve]
        vals   = [x[1] for x in curve]
        fig, ax = plt.subplots(figsize=(11, 5), facecolor=_BG)
        ax.set_facecolor(_BG2)
        for s in ax.spines.values():
            s.set_color("#1e293b")
        xs = range(len(vals))
        ax.plot(xs, vals, color=_GRN, lw=2.2, zorder=3)
        ax.fill_between(xs, CAPITAL_INIT, vals,
                        where=[v >= CAPITAL_INIT for v in vals],
                        color=_GRN, alpha=0.12)
        ax.fill_between(xs, CAPITAL_INIT, vals,
                        where=[v < CAPITAL_INIT for v in vals],
                        color=_RED, alpha=0.18)
        ax.axhline(CAPITAL_INIT, color=_GRY, lw=1, ls="--", alpha=0.6)
        ax.axhline(TARGET_USD, color=_GLD, lw=1.1, ls="--", alpha=0.7)
        ax.text(len(vals) - 1, TARGET_USD, f"  Objectif {TARGET_USD} $",
                color=_GLD, fontsize=8, va="center", fontfamily=_MONO)
        ax.set_xticks(range(0, len(labels), max(1, len(labels) // 8)))
        ax.set_xticklabels(
            [labels[i] for i in range(0, len(labels), max(1, len(labels) // 8))],
            fontsize=7, color=_GRY, rotation=30)
        ax.tick_params(colors=_GRY, labelsize=8)
        ax.set_ylabel("Capital ($)", color=_LGY, fontfamily=_MONO, fontsize=9)
        ax.yaxis.label.set_color(_LGY)
        ax.set_title("📈 Equity Curve — Elite Futures Bot",
                     color=_LGY, fontfamily=_MONO, fontsize=11)
        plt.tight_layout()
        path = f"/tmp/equity_{int(time.time())}.png"
        fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=_BG)
        plt.close(fig)
        return path
    except Exception as e:
        log.warning(f"  [EQUITY CHART] {e}")
        return None


# ═══════════════════════════════════════════════════════════════
#  FETCH OHLCV (ccxt Binance Futures)
# ═══════════════════════════════════════════════════════════════

def fetch_ohlcv(exchange: ccxt.binanceusdm,
                symbol: str, tf: str, limit: int = 200) -> pd.DataFrame:
    try:
        raw = exchange.fetch_ohlcv(symbol, tf, limit=limit)
        if not raw:
            return pd.DataFrame()
        df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df.set_index("ts", inplace=True)
        df = df.astype(float)
        return df
    except Exception as e:
        log.warning(f"  ⚠ OHLCV {symbol} {tf} : {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
#  HELPERS SMC
# ═══════════════════════════════════════════════════════════════

def atr(df: pd.DataFrame, p: int = 14) -> float:
    if len(df) < p: return 0.0
    return float((df["high"] - df["low"]).rolling(p).mean().iloc[-1])

def swing_highs(df: pd.DataFrame) -> list:
    return [(i, float(df["high"].iloc[i]))
            for i in range(1, len(df) - 1)
            if df["high"].iloc[i] > df["high"].iloc[i-1]
            and df["high"].iloc[i] > df["high"].iloc[i+1]]

def swing_lows(df: pd.DataFrame) -> list:
    return [(i, float(df["low"].iloc[i]))
            for i in range(1, len(df) - 1)
            if df["low"].iloc[i] < df["low"].iloc[i-1]
            and df["low"].iloc[i] < df["low"].iloc[i+1]]


# ═══════════════════════════════════════════════════════════════
#  HTF BIAS (H4)
# ═══════════════════════════════════════════════════════════════

def get_bias(df_h4: pd.DataFrame) -> str:
    if len(df_h4) < 20: return "NEUTRE"
    sh = swing_highs(df_h4); sl = swing_lows(df_h4)
    if len(sh) >= 2 and len(sl) >= 2:
        if sh[-1][1] > sh[-2][1] and sl[-1][1] > sl[-2][1]: return "BULLISH"
        if sh[-1][1] < sh[-2][1] and sl[-1][1] < sl[-2][1]: return "BEARISH"
    # EMA trend
    ema = df_h4["close"].ewm(span=20).mean()
    return "BULLISH" if float(df_h4["close"].iloc[-1]) > float(ema.iloc[-1]) else "BEARISH"


# ═══════════════════════════════════════════════════════════════
#  AMD PHASE
# ═══════════════════════════════════════════════════════════════

def detect_amd(df_h4: pd.DataFrame) -> Optional[AmdPhase]:
    if len(df_h4) < AMD_LOOKBACK: return None
    df   = df_h4.tail(AMD_LOOKBACK).reset_index(drop=True)
    rng  = df.tail(20)
    rh   = float(rng["high"].max()); rl = float(rng["low"].min())
    last = df.iloc[-1]
    lh   = float(last["high"]); ll = float(last["low"]); lc = float(last["close"])
    if lh > rh * 1.001 and lc < rh:   # sweep high → SHORT
        return AmdPhase("manipulation", "bear_manip", "SHORT", 78,
                        rh, rl, lh, ["AMD: Sweep range-high → SHORT attendu"])
    if ll < rl * 0.999 and lc > rl:   # sweep low → LONG
        return AmdPhase("manipulation", "bull_manip", "LONG", 78,
                        rh, rl, ll, ["AMD: Sweep range-low → LONG attendu"])
    return None


# ═══════════════════════════════════════════════════════════════
#  FVG / OB / BOS-CHOCH / SD / SEPTUPLE
# ═══════════════════════════════════════════════════════════════

def detect_fvg(df: pd.DataFrame, d: str) -> Optional[FVG]:
    for i in range(2, min(len(df), 35)):
        c1 = df.iloc[-(i+2)]; c3 = df.iloc[-i]
        if d == "LONG":
            gap = float(c3["low"]) - float(c1["high"])
            if gap / max(float(c1["high"]), 1) >= FVG_MIN_RATIO:
                return FVG("LONG", float(c3["low"]), float(c1["high"]), len(df) - i)
        else:
            gap = float(c1["low"]) - float(c3["high"])
            if gap / max(float(c3["high"]), 1) >= FVG_MIN_RATIO:
                return FVG("SHORT", float(c1["low"]), float(c3["high"]), len(df) - i)
    return None

def detect_ob(df: pd.DataFrame, d: str) -> Optional[OrderBlock]:
    lb = min(len(df) - 1, OB_LOOKBACK)
    for i in range(2, lb):
        o_ = float(df["open"].iloc[-i]); c_ = float(df["close"].iloc[-i])
        o2 = float(df["open"].iloc[-(i-1)]); c2 = float(df["close"].iloc[-(i-1)])
        if d == "LONG"  and c_ < o_ and c2 > o2:
            return OrderBlock("LONG",  float(df["high"].iloc[-i]),
                              float(df["low"].iloc[-i]),  len(df) - i)
        if d == "SHORT" and c_ > o_ and c2 < o2:
            return OrderBlock("SHORT", float(df["high"].iloc[-i]),
                              float(df["low"].iloc[-i]),  len(df) - i)
    return None

def detect_bos(df: pd.DataFrame, d: str) -> tuple[float, float]:
    sh = swing_highs(df); sl = swing_lows(df)
    if d == "LONG"  and sh and sl:
        return sh[-1][1], sl[-2][1] if len(sl) >= 2 else 0.0
    if d == "SHORT" and sh and sl:
        return sl[-1][1], sh[-2][1] if len(sh) >= 2 else 0.0
    return 0.0, 0.0

def detect_sd(df: pd.DataFrame, d: str, atr_val: float) -> bool:
    if atr_val == 0: return False
    price = float(df["close"].iloc[-1])
    for i in range(1, min(len(df)-1, 40)):
        body = abs(float(df["close"].iloc[-i]) - float(df["open"].iloc[-i]))
        if body < SD_MIN_IMPULSE_RATIO * atr_val: continue
        is_bull = float(df["close"].iloc[-i]) > float(df["open"].iloc[-i])
        lo_ = float(df["low"].iloc[-i]); hi_ = float(df["high"].iloc[-i])
        buf = atr_val * SD_ZONE_BUFFER
        if d == "LONG"  and is_bull and lo_ - buf <= price <= hi_ + buf: return True
        if d == "SHORT" and not is_bull and lo_ - buf <= price <= hi_ + buf: return True
    return False

def detect_sept(df: pd.DataFrame, d: str) -> bool:
    n = SEPTUPLE_MIN_CANDLES
    if len(df) < n: return False
    last = df.tail(n)
    if d == "LONG":
        return all(float(last["close"].iloc[i]) > float(last["open"].iloc[i]) for i in range(n))
    return all(float(last["close"].iloc[i]) < float(last["open"].iloc[i]) for i in range(n))


# ═══════════════════════════════════════════════════════════════
#  ANALYSE SMC COMPLÈTE  (H4 → M15 → M5)
# ═══════════════════════════════════════════════════════════════

def analyse(exchange: ccxt.binanceusdm,
            sym: str, name: str) -> Optional[Signal]:
    df_h4  = fetch_ohlcv(exchange, sym, HTF,  limit=120)
    df_m15 = fetch_ohlcv(exchange, sym, MTF,  limit=200)
    df_m5  = fetch_ohlcv(exchange, sym, LTF,  limit=120)
    if any(df.empty or len(df) < 20 for df in [df_h4, df_m15, df_m5]):
        return None

    price   = float(df_m5["close"].iloc[-1])
    atr_m5  = atr(df_m5)
    atr_h4  = atr(df_h4)
    if atr_m5 == 0: return None

    # ── Biais H4 ─────────────────────────────────────────────
    bias = get_bias(df_h4)
    if bias == "NEUTRE": return None
    direction = "LONG" if bias == "BULLISH" else "SHORT"

    # ── AMD ───────────────────────────────────────────────────
    amd = detect_amd(df_h4)
    amd_score = 0; amd_reasons = []
    if amd and amd.direction == direction:
        amd_score   = amd.confidence      # 78
        amd_reasons = amd.reasons
    elif amd and amd.direction not in (direction, "NEUTRE"):
        return None  # AMD en sens inverse → pas d'entrée

    # ── Indicateurs LTF / MTF ────────────────────────────────
    fvg  = detect_fvg(df_m5,  direction)
    ob   = detect_ob(df_m15,  direction)
    bos_, choch_ = detect_bos(df_m5, direction)
    in_sd  = detect_sd(df_h4, direction, atr_h4)
    sept   = detect_sept(df_h4, direction)

    fvg_sc  = 20 if fvg   else 0
    ob_sc   = 18 if ob    else 0
    bos_sc  = 15 if bos_  else 0
    sd_sc   = 22 if in_sd else 0
    sept_sc = 12 if sept  else 0

    # Score total normalisé sur 100
    raw   = amd_score + fvg_sc + ob_sc + bos_sc + sd_sc + sept_sc
    total = 78 + 20 + 18 + 15 + 22 + 12
    score = min(100, int(raw * 100 / total))

    reasons = (amd_reasons
               + ([f"FVG {direction} M5"] if fvg else [])
               + ([f"OB {direction} M15"]  if ob  else [])
               + ([f"BOS @ {bos_:.4g}"]    if bos_ else [])
               + (["Zone S/D H4"]          if in_sd else [])
               + ([f"Septuple {direction}"] if sept else []))

    if score < SCORE_MIN:
        return None

    # ── Entrée / SL / TP ─────────────────────────────────────
    # SL basé sur ATR M5 (1.5×) plafonné à 2 % de l'entrée
    sl_dist_raw = atr_m5 * 1.5
    sl_dist_max = price * 0.02           # max 2 % de distance SL
    sl_dist     = min(sl_dist_raw, sl_dist_max)
    if sl_dist == 0: return None

    if direction == "LONG":
        sl  = round(price - sl_dist, 6)
        tp1 = round(price + sl_dist * MIN_RR,       6)
        tp2 = round(price + sl_dist * MIN_RR * 2,   6)
        tp3 = round(price + sl_dist * MIN_RR * 3,   6)
    else:
        sl  = round(price + sl_dist, 6)
        tp1 = round(price - sl_dist * MIN_RR,       6)
        tp2 = round(price - sl_dist * MIN_RR * 2,   6)
        tp3 = round(price - sl_dist * MIN_RR * 3,   6)

    rr = round(abs(tp1 - price) / sl_dist, 1)
    if rr < MIN_RR: return None

    mode = ("AMD" if amd and amd.direction == direction
            else "SEPTUPLE" if sept
            else "SD"  if in_sd
            else "SMC")

    return Signal(
        sym=sym, name=name, direction=direction,
        entry=price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
        rr=rr, score=score, timestamp=datetime.now(timezone.utc),
        htf_bias=bias, mode=mode, reasons=reasons,
        df_m15=df_m15, fvg=fvg, ob=ob, bos_lv=bos_, choch_lv=choch_,
    )


# ═══════════════════════════════════════════════════════════════
#  MONEY MANAGEMENT — COMPOUND
# ═══════════════════════════════════════════════════════════════

def compute_qty(exchange: Optional[ccxt.binanceusdm],
                sym: str, entry: float, sl: float, paper: bool) -> float:
    """
    Compound sizing :
      risk_usd    = capital_actuel × RISK_PCT  (5 %)
      position    = capital_actuel × LEVERAGE  (plafonnée au solde disponible)
      qty         = risk_usd / sl_distance
      vérifie : position_value = qty × entry ≤ position_max
    """
    cap      = STATE.capital
    risk_usd = cap * RISK_PCT
    pos_max  = cap * LEVERAGE        # taille max de position

    sl_dist = abs(entry - sl)
    if sl_dist == 0: return 0.0

    qty_risk = risk_usd / sl_dist
    qty_max  = pos_max  / entry

    qty = min(qty_risk, qty_max)

    # Vérifier les contraintes Binance (min notional ≈ 5 $)
    if qty * entry < 5.0:
        log.warning(f"  ⚠ Notional trop faible ({qty*entry:.2f} $) — skip")
        return 0.0

    # Précision selon le symbole (BTC 3 déc., autres 2 ou 1)
    if "BTC" in sym:   return round(qty, 3)
    if "ETH" in sym:   return round(qty, 2)
    return round(qty, 1)


# ═══════════════════════════════════════════════════════════════
#  ORDRES BINANCE
# ═══════════════════════════════════════════════════════════════

def place_entry(exchange: Optional[ccxt.binanceusdm],
                direction: str, sym: str,
                qty: float, price: float, paper: bool) -> Optional[str]:
    side = "buy" if direction == "LONG" else "sell"
    if paper:
        oid = f"PAPER_{int(time.time()*1000)}"
        log.info(f"  📝 PAPER ENTRY {side.upper()} {qty} {sym} @ ~{price:,.4g}  id={oid}")
        return oid
    try:
        o = exchange.create_market_order(sym, side, qty)
        log.info(f"  ✅ ENTRY {side.upper()} {qty} {sym}  id={o['id']}")
        return str(o["id"])
    except Exception as e:
        log.error(f"  ❌ ENTRY {side.upper()} {sym}: {e}")
        tg_text(f"❌ <b>Erreur ordre {direction} {sym}</b>\n{e}")
        return None

def partial_close(exchange: Optional[ccxt.binanceusdm],
                  direction: str, sym: str,
                  qty: float, price: float, reason: str, paper: bool) -> bool:
    side = "sell" if direction == "LONG" else "buy"
    if paper:
        log.info(f"  📝 PAPER CLOSE {side.upper()} {qty} @ ~{price:,.4g}  [{reason}]")
        return True
    try:
        exchange.create_market_order(sym, side, qty,
                                     params={"reduceOnly": True})
        log.info(f"  ✅ PARTIAL CLOSE {reason} {qty} {sym}")
        return True
    except Exception as e:
        log.error(f"  ❌ CLOSE {reason} {sym}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
#  TELEGRAM — SIGNAL
# ═══════════════════════════════════════════════════════════════

def tg_notify_signal(sig: Signal, trade: Trade):
    cap = STATE.capital
    growth = (cap - CAPITAL_INIT) / CAPITAL_INIT * 100
    dec = 2 if sig.entry > 10 else 5
    d = sig.direction
    if d == "LONG":
        sl_pct  = (sig.sl  - sig.entry) / sig.entry * 100
        tp1_pct = (sig.tp1 - sig.entry) / sig.entry * 100
        tp2_pct = (sig.tp2 - sig.entry) / sig.entry * 100
        tp3_pct = (sig.tp3 - sig.entry) / sig.entry * 100
    else:
        sl_pct  =  (sig.sl  - sig.entry) / sig.entry * 100
        tp1_pct = -(sig.entry - sig.tp1) / sig.entry * 100
        tp2_pct = -(sig.entry - sig.tp2) / sig.entry * 100
        tp3_pct = -(sig.entry - sig.tp3) / sig.entry * 100

    fee_rt  = sig.entry * trade.qty_total * TAKER_FEE * 2
    pnl_tp1 = abs(sig.tp1 - sig.entry) * trade.qty_total * TP1_QTY_PCT - fee_rt * TP1_QTY_PCT

    txt = (
        f"⚡ <b>NOUVEAU TRADE — BINANCE FUTURES</b>\n"
        f"{'─'*34}\n"
        f"🪙 <b>{sig.name}</b>  ({sig.sym.split('/')[0]}USDT)\n"
        f"{'🟢 LONG' if d=='LONG' else '🔴 SHORT'}  ·  Mode : {sig.mode}  ·  Score : {sig.score}/100\n"
        f"{'─'*34}\n"
        f"💰 Entrée    : <code>{sig.entry:,.{dec}f} $</code>\n"
        f"🔴 Stop Loss : <code>{sig.sl:,.{dec}f} $</code>  ({sl_pct:+.2f}%)\n"
        f"{'─'*34}\n"
        f"🎯 TP1  : <code>{sig.tp1:,.{dec}f} $</code>  ({tp1_pct:+.2f}%)  → 40 %\n"
        f"🎯 TP2  : <code>{sig.tp2:,.{dec}f} $</code>  ({tp2_pct:+.2f}%)  → 35 %\n"
        f"🎯 TP3  : <code>{sig.tp3:,.{dec}f} $</code>  ({tp3_pct:+.2f}%)  → 25 %\n"
        f"📊 R:R  : 1:{sig.rr}\n"
        f"{'─'*34}\n"
        f"<b>Levier : {LEVERAGE}×  |  Isolated Margin</b>\n"
        f"Quantité  : {trade.qty_total} {sig.sym.split('/')[0]}\n"
        f"Position  : {trade.qty_total * sig.entry:,.2f} $\n"
        f"Risque    : {trade.risk_usd:.3f} $  ({RISK_PCT*100:.0f}% capital)\n"
        f"Frais AR  : ~{fee_rt:.3f} $\n"
        f"PnL net TP1 estimé : ~{pnl_tp1:+.3f} $\n"
        f"{'─'*34}\n"
        f"💼 Capital : <b>{cap:.3f} $</b>  (+{growth:.1f}%)\n"
        f"🎯 Objectif : {TARGET_USD} $\n"
        f"🕐 {sig.timestamp.strftime('%d/%m/%Y %H:%M UTC')}"
    )
    chart = chart_signal(sig)
    if chart:
        tg_photo(chart, txt)
        try: os.remove(chart)
        except Exception: pass
    else:
        tg_text(txt)


def tg_notify_close(trade: Trade, price: float,
                    pnl_gross: float, fee: float, reason: str):
    net  = pnl_gross - fee
    won  = net > 0
    ico  = "🟢" if won else "🔴"
    cap  = STATE.capital
    growth = (cap - CAPITAL_INIT) / CAPITAL_INIT * 100
    txt = (
        f"{ico} <b>TRADE FERMÉ — {reason}</b>\n"
        f"{'─'*32}\n"
        f"🪙 {trade.name}  {trade.direction}\n"
        f"Entrée : {trade.entry:,.4g} $  →  Sortie : {price:,.4g} $\n"
        f"PnL brut : {pnl_gross:+.4f} $\n"
        f"Frais    : -{fee:.4f} $\n"
        f"<b>Net : {net:+.4f} $</b>\n"
        f"{'─'*32}\n"
        f"{STATE.summary()}\n"
        f"📈 Progression : {growth:+.1f}% vs départ"
    )
    # Envoyer equity curve
    eq = chart_equity()
    if eq:
        tg_photo(eq, txt)
        try: os.remove(eq)
        except Exception: pass
    else:
        tg_text(txt)


# ═══════════════════════════════════════════════════════════════
#  MONITOR TRADE (thread dédié)
# ═══════════════════════════════════════════════════════════════

def monitor(trade: Trade, exchange: Optional[ccxt.binanceusdm], paper: bool):
    log.info(f"  👁️ Monitor {trade.name} {trade.direction} "
             f"entry={trade.entry:,.4g}  SL={trade.sl:,.4g}  TP1={trade.tp1:,.4g}")
    tp1_done = tp2_done = False
    check_sec = 10

    while trade.status == "open" and trade.qty_rem > 0:
        try:
            if paper:
                # simulation : on lit le dernier prix via ccxt public (sans auth)
                # On utilise exchange même en paper pour avoir le vrai prix
                try:
                    tk = exchange.fetch_ticker(trade.sym)
                    price = float(tk["last"])
                except Exception:
                    time.sleep(check_sec); continue
            else:
                tk = exchange.fetch_ticker(trade.sym)
                price = float(tk["last"])

            d   = trade.direction
            sl_ = (d == "LONG"  and price <= trade.sl) or \
                  (d == "SHORT" and price >= trade.sl)
            t1_ = (d == "LONG"  and price >= trade.tp1) or \
                  (d == "SHORT" and price <= trade.tp1)
            t2_ = (d == "LONG"  and price >= trade.tp2) or \
                  (d == "SHORT" and price <= trade.tp2)
            t3_ = (d == "LONG"  and price >= trade.tp3) or \
                  (d == "SHORT" and price <= trade.tp3)

            # ── STOP LOSS ─────────────────────────────────────
            if sl_:
                pnl  = (price - trade.entry) * trade.qty_rem * (1 if d=="LONG" else -1)
                fee  = price * trade.qty_rem * TAKER_FEE
                partial_close(exchange, d, trade.sym, trade.qty_rem, price, "SL", paper)
                trade.status = "sl"; trade.qty_rem = 0.0
                STATE.close_trade(pnl, fee, won=False)
                tg_notify_close(trade, price, pnl, fee, "🛑 STOP LOSS")
                log.info(f"  🛑 SL @ {price:,.4g}  PnL {pnl:+.4f} $")
                break

            # ── TP1 ────────────────────────────────────────────
            if t1_ and not tp1_done:
                q   = round(trade.qty_total * TP1_QTY_PCT, 6)
                q   = min(q, trade.qty_rem)
                pnl = abs(price - trade.entry) * q
                fee = price * q * TAKER_FEE
                if partial_close(exchange, d, trade.sym, q, price, "TP1", paper):
                    trade.qty_rem -= q
                    trade.sl       = trade.entry   # SL → break-even
                    STATE.close_trade(pnl, fee, won=True)
                    tg_notify_close(trade, price, pnl, fee, "🎯 TP1 (40%)")
                    log.info(f"  🎯 TP1 @ {price:,.4g}  SL → BE {trade.entry:,.4g}")
                trade.status = "tp1"; tp1_done = True

            # ── TP2 ────────────────────────────────────────────
            if t2_ and tp1_done and not tp2_done:
                q   = round(trade.qty_total * TP2_QTY_PCT, 6)
                q   = min(q, trade.qty_rem)
                pnl = abs(price - trade.entry) * q
                fee = price * q * TAKER_FEE
                if partial_close(exchange, d, trade.sym, q, price, "TP2", paper):
                    trade.qty_rem -= q
                    STATE.close_trade(pnl, fee, won=True)
                    tg_notify_close(trade, price, pnl, fee, "🎯 TP2 (35%)")
                    log.info(f"  🎯 TP2 @ {price:,.4g}")
                trade.status = "tp2"; tp2_done = True

            # ── TP3 ────────────────────────────────────────────
            if t3_ and tp1_done and tp2_done:
                q   = trade.qty_rem
                pnl = abs(price - trade.entry) * q
                fee = price * q * TAKER_FEE
                if partial_close(exchange, d, trade.sym, q, price, "TP3", paper):
                    trade.qty_rem = 0.0
                    trade.status  = "closed"
                    STATE.close_trade(pnl, fee, won=True)
                    tg_notify_close(trade, price, pnl, fee, "🏆 TP3 FULL (25%)")
                    log.info(f"  🏆 TP3 @ {price:,.4g} — Trade complet ✅")
                break

        except Exception as e:
            log.warning(f"  ⚠ Monitor {trade.name}: {e}")
        time.sleep(check_sec)

    with STATE._lock:
        STATE.active_trade = None
    log.info(f"  👁️ Monitor terminé — {trade.name} status={trade.status}")


# ═══════════════════════════════════════════════════════════════
#  RAPPORT QUOTIDIEN
# ═══════════════════════════════════════════════════════════════

def daily_report_loop():
    """Envoie le rapport + equity curve à 23 h UTC."""
    last_sent = ""
    while True:
        now = datetime.now(timezone.utc)
        key = now.strftime("%Y-%m-%d")
        if now.hour == 23 and key != last_sent:
            last_sent = key
            eq = chart_equity()
            txt = (
                f"🌙 <b>RAPPORT QUOTIDIEN — {now.strftime('%d/%m/%Y')}</b>\n"
                f"{'─'*32}\n"
                f"{STATE.summary()}\n"
                f"{'─'*32}\n"
                f"🎯 Objectif : {TARGET_USD} $\n"
                f"📈 Progression : {(STATE.capital-CAPITAL_INIT)/CAPITAL_INIT*100:+.1f}%\n"
                f"💎 Peak capital : {STATE.peak_capital:.3f} $"
            )
            if eq:
                tg_photo(eq, txt)
                try: os.remove(eq)
                except Exception: pass
            else:
                tg_text(txt)
        time.sleep(60)


# ═══════════════════════════════════════════════════════════════
#  BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════

def run(paper: bool = False):
    log.info("╔" + "═"*70 + "╗")
    log.info("║  ELITE FUTURES BOT  ·  14 CRYPTOS  ·  $20  ·  20×  ·  SMC      ║")
    log.info(f"║  Mode : {'📝 DÉMO (simulation, prix réels Binance)' if paper else '💰 LIVE (ordres réels)'}    ║")
    log.info(f"║  Objectif journalier : +{DAILY_TARGET_PCT*100:.0f}%  |  Risque {RISK_PCT*100:.0f}% compound      ║")
    log.info(f"║  Objectif global : {CAPITAL_INIT} $ → {TARGET_USD} $                             ║")
    log.info("╚" + "═"*70 + "╝\n")

    # Connexion exchange
    if paper:
        # Mode paper : exchange public (pas d'auth, prix réels)
        exchange = ccxt.binanceusdm({"options": {"defaultType": "future"}})
    else:
        if not API_KEY or not API_SECRET:
            log.error("  ❌ BINANCE_API_KEY / BINANCE_API_SECRET manquants"); return
        exchange = ccxt.binanceusdm({
            "apiKey": API_KEY, "secret": API_SECRET,
            "options": {"defaultType": "future"},
        })
        try:
            bal  = exchange.fetch_balance()
            usdt = bal.get("USDT", {}).get("free", 0.0)
            log.info(f"  ✅ Binance connecté — USDT libre : {usdt:.2f} $")
            if usdt < STATE.capital * 0.9:
                log.warning(f"  ⚠ Solde ({usdt:.2f} $) < capital bot ({STATE.capital:.2f} $)")
        except Exception as e:
            log.error(f"  ❌ Connexion Binance : {e}"); return

        # Configurer levier + Isolated pour chaque marché
        for sym, _ in MARKETS:
            try:
                exchange.set_leverage(LEVERAGE, sym)
                exchange.set_margin_mode("isolated", sym)
            except Exception:
                pass

    exchange.load_markets()

    # Telegram démarrage
    tg_text(
        f"🤖 <b>ELITE BOT DÉMARRÉ</b>\n"
        f"Capital : {STATE.capital:.2f} $  |  Levier : {LEVERAGE}×  |  Mode : "
        f"{'📝 DÉMO' if paper else '💰 LIVE'}\n"
        f"Marchés : {len(MARKETS)} cryptos  |  Score ≥ {SCORE_MIN}  |  RR ≥ {MIN_RR}\n"
        f"Trades/jour : illimités — stop à +{DAILY_TARGET_PCT*100:.0f}% du solde\n"
        f"Risque : {RISK_PCT*100:.0f}% compound  |  Objectif : {TARGET_USD} $\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M UTC')}"
    )

    # Thread rapport quotidien
    threading.Thread(target=daily_report_loop, daemon=True, name="daily").start()

    cooldown: dict[str, float] = {}   # évite les doublons de signal
    COOLDOWN_SEC = 900                # 15 min entre 2 signaux sur le même marché

    cycle = 0
    while True:
        try:
            cycle += 1
            with STATE._lock:
                STATE.scan_cycle = cycle
            now = datetime.now(timezone.utc)

            # ── Si trade actif, on attend ─────────────────────
            with STATE._lock:
                at = STATE.active_trade
            if at is not None:
                if cycle % 12 == 1:
                    log.info(f"  👁️ [{cycle}] Trade actif {at.name} {at.direction} "
                             f"status={at.status}  qty_rem={at.qty_rem}")
                time.sleep(SCAN_INTERVAL); continue

            # ── Limite journalière ────────────────────────────
            if not STATE.can_trade():
                if cycle % 20 == 1:
                    with STATE._lock:
                        cap        = STATE.capital
                        pnl_today  = STATE.pnl_today
                        cap_day    = STATE._capital_start_day
                        cb         = STATE.circuit_broken
                        pause_ts   = STATE.consec_pause_until
                    if cb:
                        log.critical(f"  🚨 [{cycle}] CIRCUIT BREAKER actif — capital {cap:.3f} $ — arrêt total")
                    elif time.time() < pause_ts:
                        resume = datetime.fromtimestamp(pause_ts, tz=timezone.utc).strftime('%H:%M UTC')
                        log.warning(f"  ⚠ [{cycle}] L2 cooldown actif — reprise à {resume}")
                    else:
                        daily_pct = pnl_today / max(cap_day, 0.01) * 100
                        label = "Objectif" if daily_pct >= 0 else "Limite perte"
                        log.info(f"  {'✅' if daily_pct >= 0 else '🔴'} [{cycle}] {label} journalier "
                                 f"({daily_pct:+.1f}%) — Capital {cap:.3f} $ — pause jusqu'à demain")
                time.sleep(SCAN_INTERVAL); continue

            # ── Circuit breaker L3 : arrêt dur ───────────────
            with STATE._lock:
                cb = STATE.circuit_broken
            if cb:
                log.critical(
                    f"  🚨 CIRCUIT BREAKER — arrêt total du bot."
                    f" Capital final : {STATE.capital:.3f} $")
                tg_text(
                    f"🚨 <b>BOT ARRÊTÉ — CIRCUIT BREAKER</b>\n"
                    f"{STATE.summary()}"
                )
                break

            # ── Objectif atteint ? ────────────────────────────
            if STATE.capital >= TARGET_USD:
                log.info(f"  🏆 Objectif {TARGET_USD} $ atteint !  "
                         f"Capital : {STATE.capital:.2f} $")
                tg_text(
                    f"🏆 <b>OBJECTIF ATTEINT !</b>\n"
                    f"Capital : <b>{STATE.capital:.2f} $</b>  "
                    f"(objectif : {TARGET_USD} $)\n"
                    f"Growth : +{(STATE.capital/CAPITAL_INIT-1)*100:.0f}%\n"
                    f"Trades : {STATE.total_trades}  |  WR : "
                    f"{STATE.wins/(STATE.wins+STATE.losses)*100:.0f}%"
                )
                break   # arrêt du bot

            # ── Scan 14 marchés ───────────────────────────────
            log.info(f"  🔍 [{cycle}] {now.strftime('%H:%M:%S')} — "
                     f"Scan {len(MARKETS)} cryptos  |  "
                     f"Capital : {STATE.capital:.3f} $")

            best_sig : Optional[Signal] = None

            for sym, name in MARKETS:
                # Cooldown par marché
                if time.time() - cooldown.get(sym, 0) < COOLDOWN_SEC:
                    continue
                try:
                    sig = analyse(exchange, sym, name)
                    if sig is None: continue
                    log.info(f"  ⚡ {name:10s} {sig.direction:5s} "
                             f"score={sig.score}  RR=1:{sig.rr}  mode={sig.mode}")
                    if best_sig is None or sig.score > best_sig.score:
                        best_sig = sig
                except Exception as e:
                    log.warning(f"  ⚠ {name}: {e}")
                time.sleep(1)   # anti-rate-limit

            if best_sig is None:
                log.info(f"  ℹ️  Aucun signal score≥{SCORE_MIN} — prochain scan dans {SCAN_INTERVAL}s")
                time.sleep(SCAN_INTERVAL); continue

            sig = best_sig
            log.info(f"\n  🏆 MEILLEUR SIGNAL : {sig.name} {sig.direction}  "
                     f"score={sig.score}  RR=1:{sig.rr}  mode={sig.mode}\n")

            # ── Sizing ────────────────────────────────────────
            qty = compute_qty(exchange, sig.sym, sig.entry, sig.sl, paper)
            if qty == 0.0:
                time.sleep(SCAN_INTERVAL); continue

            risk_usd  = abs(sig.entry - sig.sl) * qty
            fee_open  = sig.entry * qty * TAKER_FEE

            # ── Ordre d'entrée ────────────────────────────────
            oid = place_entry(exchange, sig.direction, sig.sym,
                              qty, sig.entry, paper)
            if oid is None:
                time.sleep(SCAN_INTERVAL); continue

            # ── Trade object ──────────────────────────────────
            trade = Trade(
                sym=sig.sym, name=sig.name, direction=sig.direction,
                entry=sig.entry, sl=sig.sl,
                tp1=sig.tp1, tp2=sig.tp2, tp3=sig.tp3,
                qty_total=qty, qty_rem=qty,
                capital_at_open=STATE.capital,
                risk_usd=risk_usd, fee_open=fee_open,
                opened_at=datetime.now(timezone.utc), order_id=oid,
            )

            STATE.open_trade(fee_open)
            cooldown[sig.sym] = time.time()

            with STATE._lock:
                STATE.active_trade = trade

            # ── Notification Telegram + chart ─────────────────
            tg_notify_signal(sig, trade)

            # ── Thread monitor ────────────────────────────────
            t = threading.Thread(target=monitor,
                                 args=(trade, exchange, paper),
                                 daemon=True, name=f"mon_{sig.sym[:4]}")
            t.start()
            log.info(f"  ✅ Trade lancé — monitor actif [{t.name}]")
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log.info("\n  ⏹ Arrêt manuel")
            tg_text(f"🔴 <b>Bot arrêté</b>\n{STATE.summary()}")
            break
        except Exception as e:
            log.error(f"  ❌ Erreur boucle principale : {e}")
            time.sleep(60)


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Elite Futures Bot — 14 cryptos · $20 · 20× · SMC")
    mode_grp = parser.add_mutually_exclusive_group()
    mode_grp.add_argument("--paper", action="store_true",
                          help="Forcer mode démo (simulation, sans ordres réels)")
    mode_grp.add_argument("--live",  action="store_true",
                          help="Forcer mode live (clés API Binance obligatoires)")
    args = parser.parse_args()

    # Auto-démo si aucune clé API fournie et pas de --live explicite
    if args.live:
        paper_mode = False
    elif args.paper:
        paper_mode = True
    else:
        paper_mode = not bool(API_KEY and API_SECRET)
        if paper_mode:
            log.info("  ℹ️  Pas de clé API détectée → mode DÉMO activé automatiquement")
        else:
            log.info("  ✅ Clés API détectées → mode LIVE")

    # Flask sur thread daemon (Render keep-alive)
    port = int(os.environ.get("PORT", 10000))
    threading.Thread(target=start_flask, args=(port,),
                     daemon=True, name="flask").start()
    time.sleep(2)
    start_selfping(port)
    log.info(f"  ✓ Flask dashboard → http://0.0.0.0:{port}")

    run(paper=paper_mode)

