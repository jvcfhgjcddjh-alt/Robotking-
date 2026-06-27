
"""
╔══════════════════════════════════════════════════════════════════════════╗
║   BACKTEST — SMC Signal Engine v8.6  (basket 6 mois, crypto + yfinance)  ║
║   Réutilise le VRAI moteur (smc_engine_v8_6.py) — pas une réécriture.    ║
╚══════════════════════════════════════════════════════════════════════════╝

PRINCIPE
────────
Ce script importe smc_engine_v8_6.py tel quel et rejoue son pipeline de
décision exact (kill zones → biais H4/H1 → scan_symbol → 7 checkers →
dédoublonnage par cycle → limite quotidienne → blocage BTC SELL →
correlation_guard) à chaque clôture de bougie M15, sur des données
HISTORIQUES — au lieu d'appeler tg_notify() en live, on enregistre le
trade directement dans une base SQLite de backtest et on simule sa
résolution (SL/TP1/TP2/TP3) bougie par bougie.

SOURCE DE DONNÉES — DOUBLE BACKEND (v6mo)
──────────────────────────────────────────
  • Symboles CRYPTO (eng.is_crypto_symbol → BTC-USD, ETH-USD, ...) :
    historique tiré de l'API publique Binance (klines REST, pas de clé
    requise). Binance conserve un historique illimité en M15/H4 → les
    6 mois complets sont couverts pour ces symboles.
    Mapping de symbole : "BTC-USD" → "BTCUSDT" (suffixe -USD retiré,
    "USD" remplacé par "USDT").

  • Symboles FOREX / MÉTAUX / INDICES (tout le reste) : yfinance,
    INCHANGÉ. yfinance ne fournit que ~60 jours d'historique en M15/M5.
    Sur une fenêtre de 6 mois, ces symboles n'auront donc des bougies
    M15/H4 que sur leurs ~60 derniers jours ; les ~4 premiers mois de
    la fenêtre seront vides pour eux (aucun signal généré sur cette
    portion — pas une erreur, juste l'absence de données en amont).

Quatre "monkeypatchs" rendent ça possible sans dupliquer la logique :

  1. eng.datetime  → une sous-classe dont .now() renvoie une horloge
     virtuelle qu'on avance pas à pas. Tout le moteur (kill zones,
     limite journalière, news, etc.) "croit" être à cette date.

  2. eng.fetch      → au lieu d'interroger yfinance pour TOUT, route
     par type de symbole : Binance (cache local) si crypto, yfinance
     (cache local) sinon. Dans les deux cas, on découpe les données
     déjà téléchargées (preload_history) jusqu'à l'horloge virtuelle
     (strictement AVANT, pour ne jamais voir une bougie pas encore
     close → pas de lookahead bias).

  3. eng.is_news_blackout → désactivé. L'API ForexFactory utilisée par
     le moteur ne donne que le calendrier de LA semaine en cours ; elle
     est inutilisable pour une date historique. Le filtre news n'est
     donc PAS simulé ici (à garder en tête en comparant aux résultats
     live, qui eux bloquent autour des annonces à fort impact).

LIMITES CONNUES (à lire avant de croire les chiffres au pied de la lettre)
───────────────────────────────────────────────────────────────────────
  • Résolution M15 : SL/TP sont vérifiés sur les high/low des bougies
    M15, pas tick par tick. Si SL et un TP tombent dans la même bougie,
    on suppose le SL touché en premier (hypothèse conservatrice).
  • Pas de slippage / requote / spread variable au-delà de ce que
    check_volatility() filtre déjà.
  • Forex/métaux/indices : ~60 jours réels de données sur les 6 mois
    demandés (limite dure yfinance M15). Le reste de la fenêtre est
    vide pour ces symboles — voir note ci-dessus.
  • Crypto (Binance) : couverture complète des 6 mois en M15/H4.
  • Filtre news désactivé (voir ci-dessus).
  • Nécessite une connexion internet pour le téléchargement initial
    (preload_history) — ensuite tout tourne en local, aucun réseau.

USAGE
─────
    python backtest_v9_6.py --cat forex --days 180
    python backtest_v9_6.py --symbol EURJPY=X --days 60
    python backtest_v9_6.py --cat all --days 180 --out results.json
    python backtest_v9_6.py --cat btc --days 180   # plein 6 mois (Binance)
"""

import os
import sys
import json
import csv
import time
import argparse
import itertools
import requests
import sqlite3
from datetime import datetime, timezone, timedelta

import pandas as pd

# ─────────────────────────────────────────────────────────────
#  0. ENV — avant l'import du moteur
# ─────────────────────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

BACKTEST_DB = os.path.join(_HERE, "backtest_trades.db")
if os.path.exists(BACKTEST_DB):
    os.remove(BACKTEST_DB)

os.environ["TRADE_DB_PATH"] = BACKTEST_DB
os.environ["TG_ENABLED"]    = "false"   # garde-fou : jamais d'envoi réel
os.environ.setdefault("TG_TOKEN", "x")  # évite le warning au démarrage

import smc_engine_v8_6 as eng  # noqa: E402

eng.TRADE_DB = BACKTEST_DB
eng._init_trade_db()

# Jamais de filtre news en backtest (calendrier ForexFactory = semaine
# courante uniquement, inutilisable sur des dates passées).
eng.is_news_blackout = lambda symbol: (False, "")


# ─────────────────────────────────────────────────────────────
#  1. HORLOGE VIRTUELLE
# ─────────────────────────────────────────────────────────────
class _FrozenDateTime(eng.datetime):
    """Sous-classe de datetime — .now() renvoie l'horloge figée du
    backtest au lieu de l'heure système réelle."""
    _frozen = None

    @classmethod
    def now(cls, tz=None):
        if cls._frozen is None:
            return super().now(tz)
        dt = cls._frozen
        if tz is not None and dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt


def set_virtual_now(dt: datetime) -> None:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    _FrozenDateTime._frozen = dt


eng.datetime = _FrozenDateTime  # rebind dans le namespace du moteur


# ─────────────────────────────────────────────────────────────
#  2. CACHE DE DONNÉES HISTORIQUES + fetch() patché
#     Double backend : Binance pour les cryptos (historique illimité),
#     yfinance pour tout le reste (forex/métaux/indices, ~60j max M15).
# ─────────────────────────────────────────────────────────────
_HIST_CACHE: dict[tuple[str, str], pd.DataFrame] = {}

# (interval, period) — period choisi au max raisonnable autorisé par
# yfinance pour cet interval (M15/M5 sont les facteurs limitants).
# Utilisé UNIQUEMENT pour les symboles non-crypto.
_INTERVAL_PERIODS = {
    "4h":  "730d",
    "1h":  "730d",
    "15m": "60d",
    "5m":  "60d",
}

# Intervalles Binance correspondants (mêmes codes que yfinance ici,
# mappage explicite quand même pour rester robuste à un futur ajout).
_BINANCE_INTERVAL_MAP = {
    "4h":  "4h",
    "1h":  "1h",
    "15m": "15m",
    "5m":  "5m",
}

_BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
_BINANCE_LIMIT       = 1000   # max bougies par requête (limite Binance)


def _to_binance_symbol(symbol: str) -> str:
    """'BTC-USD' → 'BTCUSDT' ; 'ETH-USDT' → 'ETHUSDT' ; etc."""
    s = symbol.replace("-", "").upper()
    if s.endswith("USD") and not s.endswith("USDT"):
        s = s[:-3] + "USDT"
    return s


def _fetch_binance_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """Télécharge l'historique Binance par pages de _BINANCE_LIMIT bougies
    entre start_ms et end_ms (timestamps epoch en millisecondes, UTC)."""
    bsym = _to_binance_symbol(symbol)
    bint = _BINANCE_INTERVAL_MAP.get(interval)
    if bint is None:
        return pd.DataFrame()

    rows: list[list] = []
    cursor = start_ms
    while cursor < end_ms:
        params = {
            "symbol": bsym, "interval": bint,
            "startTime": cursor, "endTime": end_ms,
            "limit": _BINANCE_LIMIT,
        }
        try:
            resp = requests.get(_BINANCE_KLINES_URL, params=params, timeout=20)
            resp.raise_for_status()
            batch = resp.json()
        except Exception as e:
            print(f"  [BINANCE] ⚠ {bsym} {bint} erreur fetch : {e}")
            break
        if not batch:
            break
        rows.extend(batch)
        last_open_ms = batch[-1][0]
        if len(batch) < _BINANCE_LIMIT:
            break
        cursor = last_open_ms + 1   # bougie suivante, évite la boucle infinie
        time.sleep(0.25)            # respecte le rate-limit public Binance

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "n_trades", "taker_base", "taker_quote", "ignore",
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("open_time")
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[["open", "high", "low", "close", "volume"]].dropna(subset=["close", "high", "low"])
    df = df[~df.index.duplicated(keep="first")].sort_index()
    return df


def preload_history(symbols: list[str], start: datetime, end: datetime) -> None:
    """Télécharge UNE FOIS toutes les données nécessaires. Après cet
    appel, plus aucune requête réseau n'est faite pendant le backtest.

    Route par type de symbole :
      • crypto  → Binance, couvre [start, end] en entier (illimité).
      • autre   → yfinance, couvre au mieux ~60j (limite native)."""
    print(f"  [DATA] Téléchargement historique — {len(symbols)} symboles…")
    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(end.timestamp() * 1000)

    for sym in symbols:
        is_crypto = eng.is_crypto_symbol(sym)
        for interval in _INTERVAL_PERIODS:
            if is_crypto:
                df = _fetch_binance_klines(sym, interval, start_ms, end_ms)
                src = "binance"
            else:
                period = _INTERVAL_PERIODS[interval]
                df = _real_fetch(sym, interval, period=period)
                df = _ensure_utc_index(df)
                src = "yfinance"
            _HIST_CACHE[(sym, interval)] = df
            span = f"{df.index.min().date()} → {df.index.max().date()}" if not df.empty else "—"
            print(f"    {sym:<10} {interval:<4} [{src:<8}] → {len(df)} bougies  ({span})")


def _ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if df.index.tz is None:
        df = df.tz_localize("UTC")
    else:
        df = df.tz_convert("UTC")
    return df


def _sliced(symbol: str, interval: str, as_of: datetime) -> pd.DataFrame:
    """Renvoie uniquement les bougies STRICTEMENT antérieures à as_of
    (pas de lookahead — la bougie en cours n'est jamais "connue")."""
    df = _HIST_CACHE.get((symbol, interval))
    if df is None or df.empty:
        return pd.DataFrame()
    return df[df.index < as_of]


def _patched_fetch(symbol: str, interval: str, period: str = "5d", **kwargs) -> pd.DataFrame:
    as_of = _FrozenDateTime._frozen
    if as_of is None:
        return pd.DataFrame()
    return _sliced(symbol, interval, as_of)


# garde une référence à la vraie fonction pour le téléchargement initial
_real_fetch = eng.fetch
eng.fetch = _patched_fetch


# ─────────────────────────────────────────────────────────────
#  3. DÉDOUBLONNAGE PAR NIVEAU DE PRIX (équivalent backtest de
#     is_price_level_duplicate, mais basé sur l'horloge virtuelle)
# ─────────────────────────────────────────────────────────────
_price_level_cache: dict[str, tuple[str, float, datetime]] = {}


def _is_price_level_duplicate(symbol: str, direction: str, entry: float, now: datetime) -> bool:
    cached = _price_level_cache.get(symbol)
    if not cached:
        return False
    c_dir, c_entry, c_ts = cached
    if (now - c_ts).total_seconds() > eng.PRICE_LEVEL_COOLDOWN:
        return False
    if c_dir != direction:
        return False
    return abs(entry - c_entry) / c_entry <= eng.PRICE_LEVEL_TOLERANCE


def _record_price_level(symbol: str, direction: str, entry: float, now: datetime) -> None:
    _price_level_cache[symbol] = (direction, entry, now)


# ─────────────────────────────────────────────────────────────
#  4. RÉSOLUTION DES TRADES OUVERTS (équivalent backtest du thread
#     _monitor_trades_loop, mais sur les bougies M15 historiques)
# ─────────────────────────────────────────────────────────────
_open_trades: dict[str, dict] = {}   # trade_id → trade dict (miroir local de la DB)
_signal_num_counter = itertools.count(1)


def _register(sig_v3, setup_type: str) -> str:
    num      = next(_signal_num_counter)
    trade_id = eng.register_trade(sig_v3, num, setup_type=setup_type)
    _open_trades[trade_id] = {
        "trade_id": trade_id, "symbol": sig_v3.symbol, "direction": sig_v3.direction,
        "entry": sig_v3.entry, "sl": sig_v3.sl, "tp1": sig_v3.tp,
        "tp2": sig_v3.tp2, "tp3": sig_v3.tp3,
        "tp1_hit": 0, "tp2_hit": 0, "tp3_hit": 0, "sl_hit": 0, "closed": 0,
    }
    return trade_id


def _resolve_open_trades_for_symbol(symbol: str, bar, bar_time: datetime) -> None:
    """Vérifie SL/TP1/TP2/TP3 pour tous les trades ouverts sur `symbol`
    contre la bougie M15 `bar` qui vient de clôturer à `bar_time`."""
    high, low = float(bar["high"]), float(bar["low"])
    for trade_id, t in list(_open_trades.items()):
        if t["symbol"] != symbol or t["closed"]:
            continue
        direction = t["direction"]

        # ── SL — toujours vérifié en premier (hypothèse conservatrice
        #        si SL et TP tombent dans la même bougie) ────────────
        sl_hit = (low <= t["sl"]) if direction == "LONG" else (high >= t["sl"])
        if sl_hit:
            eng.update_trade_field(trade_id, "sl_hit", 1)
            eng.update_trade_field(trade_id, "closed", 1)
            eng._update_stat_result(trade_id, "sl", t["sl"])
            t["sl_hit"] = 1
            t["closed"] = 1
            continue

        # ── TP1 ──────────────────────────────────────────────────────
        if not t["tp1_hit"] and t["tp1"] > 0:
            tp1_hit = (high >= t["tp1"]) if direction == "LONG" else (low <= t["tp1"])
            if tp1_hit:
                eng.update_trade_field(trade_id, "tp1_hit", 1)
                eng._update_stat_result(trade_id, "tp1", t["tp1"])
                t["tp1_hit"] = 1

        # ── TP2 (nécessite TP1) ──────────────────────────────────────
        if t["tp1_hit"] and not t["tp2_hit"] and t.get("tp2", 0) > 0:
            tp2_hit = (high >= t["tp2"]) if direction == "LONG" else (low <= t["tp2"])
            if tp2_hit:
                eng.update_trade_field(trade_id, "tp2_hit", 1)
                eng._update_stat_result(trade_id, "tp2", t["tp2"])
                t["tp2_hit"] = 1

        # ── TP3 (nécessite TP2) → clôture ────────────────────────────
        if t["tp2_hit"] and not t["tp3_hit"] and t.get("tp3", 0) > 0:
            tp3_hit = (high >= t["tp3"]) if direction == "LONG" else (low <= t["tp3"])
            if tp3_hit:
                eng.update_trade_field(trade_id, "tp3_hit", 1)
                eng.update_trade_field(trade_id, "closed", 1)
                eng._update_stat_result(trade_id, "tp3", t["tp3"])
                t["tp3_hit"] = 1
                t["closed"] = 1


def _force_close_remaining(last_known_prices: dict[str, float]) -> None:
    """À la fin du backtest, clôture au marché les trades encore ouverts
    (sinon ils restent 'open' dans les stats — ni gagnants ni perdants)."""
    for trade_id, t in _open_trades.items():
        if t["closed"]:
            continue
        px = last_known_prices.get(t["symbol"], t["entry"])
        eng.update_trade_field(trade_id, "closed", 1)
        eng._update_stat_result(trade_id, "open_end", px)


# ─────────────────────────────────────────────────────────────
#  5. BOUCLE PRINCIPALE DU BACKTEST
# ─────────────────────────────────────────────────────────────
def run_backtest(symbols: list[tuple[str, str]], start: datetime, end: datetime,
                  step_minutes: int = 15) -> None:
    sym_list = [s for s, _ in symbols]
    mkt_of   = dict(symbols)

    # Marge en amont du téléchargement : htf_bias() exige ≥25 bougies H4
    # avant de répondre — sans buffer, les tout premiers jours de la
    # fenêtre n'auraient pas de biais H4 valide (faux "NEUTRAL").
    preload_start = start - timedelta(days=10)
    preload_history(sym_list, preload_start, end)

    last_bar_seen: dict[str, pd.Timestamp] = {}
    last_known_price: dict[str, float] = {}
    grid = pd.date_range(start, end, freq=f"{step_minutes}min", tz="UTC")

    print(f"\n  [BT] {len(grid)} pas de {step_minutes} min — "
          f"{start.date()} → {end.date()}  ({len(sym_list)} marchés)\n")

    for n, t in enumerate(grid, 1):
        set_virtual_now(t.to_pydatetime())

        if n % 200 == 0:
            print(f"  [BT] … {t}  ({n}/{len(grid)})  "
                  f"trades ouverts={sum(1 for v in _open_trades.values() if not v['closed'])}")

        # ── Sélection des marchés actifs à cet instant (mêmes règles
        #    que la boucle live : weekend → crypto/gold seulement) ──
        if eng.is_weekend():
            active = [s for s in sym_list
                      if eng.is_crypto_symbol(s)
                      or (s in eng.GOLD_SYMBOLS and eng.is_gold_session_active())]
        elif not eng.is_session_active():
            active = [s for s in sym_list if eng.is_crypto_symbol(s)]
        else:
            active = sym_list

        cycle_signals: list[tuple[str, str, "eng.SetupSignal"]] = []

        for sym in active:
            # ── Résolution des trades ouverts sur ce symbole, contre
            #    la bougie M15 qui vient de clôturer ─────────────────
            m15 = _sliced(sym, "15m", t.to_pydatetime())
            if not m15.empty:
                bar = m15.iloc[-1]
                bar_ts = m15.index[-1]
                last_known_price[sym] = float(bar["close"])
                if last_bar_seen.get(sym) != bar_ts:
                    _resolve_open_trades_for_symbol(sym, bar, bar_ts)

            # ── Kill zone (par symbole) ──────────────────────────────
            kz_ok, _ = eng.is_kill_zone_active(sym)
            if not kz_ok:
                continue

            # ── Ne rescanne que si une NOUVELLE bougie M15 est dispo ─
            if m15.empty:
                continue
            bar_ts = m15.index[-1]
            if last_bar_seen.get(sym) == bar_ts:
                continue
            last_bar_seen[sym] = bar_ts

            # ── Biais H4 rapide (identique à la boucle live) ────────
            df_peek = eng.fetch(sym, "4h", period="5d")
            if df_peek.empty:
                continue
            bias = eng.htf_bias(df_peek)
            if bias == "NEUTRAL":
                continue

            # ── Scan complet (7 modules + ASM) ──────────────────────
            try:
                sigs = eng.scan_symbol(sym, mkt_of.get(sym, sym), min_rr=eng.MIN_RR)
            except Exception as e:
                print(f"  [BT] ⚠ {sym} erreur scan : {e}")
                continue

            for s in sigs:
                cycle_signals.append((mkt_of.get(sym, sym), sym, s))

        if not cycle_signals:
            continue

        # ── Dédoublonnage par cycle (max 3, 1 par paire) — identique
        #    à la boucle live ──────────────────────────────────────
        cycle_signals.sort(key=lambda x: (x[2].tier, -x[2].score))
        seen, deduped = set(), []
        for item in cycle_signals:
            if item[1] not in seen:
                seen.add(item[1])
                deduped.append(item)
        cycle_signals = deduped[:3]

        now_dt = t.to_pydatetime().replace(tzinfo=timezone.utc)

        for mkt, sym, setup_sig in cycle_signals:
            if not eng.check_daily_limit(sym):
                eng._reset_daily_if_needed()
                if eng._daily_global_count >= eng.MAX_SIGNALS_GLOBAL_PER_DAY:
                    break
                continue

            if eng.BTC_SELL_BLOCKED and sym == "BTC-USD" and setup_sig.direction in ("SHORT", "SELL"):
                continue

            corr_ok, _ = eng.correlation_guard(sym, setup_sig.direction)
            if not corr_ok:
                continue

            if _is_price_level_duplicate(sym, setup_sig.direction, setup_sig.entry, now_dt):
                continue

            eng.increment_daily_count(sym)
            sig_v3 = setup_sig.to_signal()
            _register(sig_v3, setup_type=setup_sig.setup_type)
            _record_price_level(sym, setup_sig.direction, setup_sig.entry, now_dt)

            print(f"  [BT] ⚡ {t}  {sym:<10} {setup_sig.direction:<5} "
                  f"[{setup_sig.setup_type}] score={setup_sig.score} RR=1:{setup_sig.rr}")

    _force_close_remaining(last_known_price)
    print("\n  [BT] Terminé.\n")


# ─────────────────────────────────────────────────────────────
#  6. RAPPORT — détail par trade (JSON + CSV) + mensuel × symbole
# ─────────────────────────────────────────────────────────────
def _load_full_trades() -> list[dict]:
    """signal_stats (résultat, pnl_r, durée) JOIN active_trades
    (tp2, tp3, quel TP a été touché) sur trade_id. Une ligne = un trade,
    avec tout ce qu'il faut pour l'audit (heure d'entrée, entry, SL,
    TP1/TP2/TP3, résultat final, R)."""
    stats = eng.get_signal_stats(limit=100000)
    con = sqlite3.connect(eng.TRADE_DB, check_same_thread=False)
    con.row_factory = sqlite3.Row
    extra = {
        r["trade_id"]: dict(r)
        for r in con.execute(
            "SELECT trade_id, tp2, tp3, tp1_hit, tp2_hit, tp3_hit, sl_hit "
            "FROM active_trades"
        ).fetchall()
    }
    con.close()

    rows = []
    for s in stats:
        a = extra.get(s["trade_id"], {})
        rows.append({
            "trade_id":    s["trade_id"],
            "symbol":      s["symbol"],
            "direction":   s["direction"],
            "setup_type":  s["setup_type"],
            "score":       s["score"],
            "entry_time":  s["timestamp"],          # heure d'entrée (horloge virtuelle backtest)
            "entry":       s["entry"],
            "sl":          s["sl"],
            "tp1":         s["tp1"],
            "tp2":         a.get("tp2", 0.0),
            "tp3":         a.get("tp3", 0.0),
            "tp1_hit":     bool(a.get("tp1_hit", 0)),
            "tp2_hit":     bool(a.get("tp2_hit", 0)),
            "tp3_hit":     bool(a.get("tp3_hit", 0)),
            "sl_hit":      bool(a.get("sl_hit", 0)),
            "result":      s["result"],             # tp1 | tp2 | tp3 | sl | open_end
            "exit_price":  s["exit_price"],
            "pnl_r":       s["pnl_r"],
            "duration_min": s["duration_min"],
        })
    # tri chronologique — plus simple à lire pour l'audit trade par trade
    rows.sort(key=lambda r: r["entry_time"])
    return rows


def _write_csv(rows: list[dict], path: str) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _month_key(iso_ts: str) -> str:
    """'2026-03-14T09:15:00+00:00' → '2026-03'."""
    return iso_ts[:7] if iso_ts else "?"


def _monthly_breakdown(rows: list[dict]) -> dict[tuple[str, str], dict]:
    """Agrège (mois, symbole) → {trades, wins, losses, total_r, winrate}.
    Ne compte que les trades clôturés (exclut les 'open' restants —
    en pratique il n'y en a plus après _force_close_remaining)."""
    closed = [r for r in rows if r["result"] != "open"]
    buckets: dict[tuple[str, str], list[dict]] = {}
    for r in closed:
        key = (_month_key(r["entry_time"]), r["symbol"])
        buckets.setdefault(key, []).append(r)

    out = {}
    for key, items in buckets.items():
        wins   = [r for r in items if r["result"] in ("tp1", "tp2", "tp3")]
        losses = [r for r in items if r["result"] == "sl"]
        decisive = wins + losses
        total_r  = sum(r["pnl_r"] for r in items)
        winrate  = (len(wins) / len(decisive) * 100) if decisive else 0.0
        out[key] = {
            "trades": len(items), "wins": len(wins), "losses": len(losses),
            "winrate_pct": round(winrate, 1), "total_r": round(total_r, 2),
        }
    return out


def print_report(out_path: str | None = None,
                  csv_path: str | None = None) -> None:
    rows   = _load_full_trades()
    closed = [r for r in rows if r["result"] != "open"]
    wins   = [r for r in closed if r["result"] in ("tp1", "tp2", "tp3")]
    losses = [r for r in closed if r["result"] == "sl"]
    forced = [r for r in closed if r["result"] == "open_end"]

    total_r  = sum(r["pnl_r"] for r in closed)
    decisive = wins + losses
    winrate  = (len(wins) / len(decisive) * 100) if decisive else 0.0

    print("═" * 78)
    print("  RAPPORT BACKTEST — SMC Signal Engine v8.6 — basket 6 mois")
    print("═" * 78)
    print(f"  Trades clôturés      : {len(closed)}  (dont {len(forced)} encore ouverts en fin de fenêtre)")
    print(f"  Gagnants / Perdants  : {len(wins)} ✅ / {len(losses)} ❌")
    print(f"  Winrate              : {winrate:.1f}%")
    print(f"  Total R              : {total_r:+.2f}R")
    print(f"  Moyenne R/trade      : {(total_r/len(closed) if closed else 0):+.2f}R")

    monthly = _monthly_breakdown(rows)
    months  = sorted({m for m, _ in monthly})
    symbols = sorted({s for _, s in monthly})

    print("\n  Mensuel × symbole :")
    for month in months:
        print(f"\n  ── {month} ──")
        month_total_r = 0.0
        month_trades  = 0
        for sym in symbols:
            agg = monthly.get((month, sym))
            if not agg:
                continue
            month_total_r += agg["total_r"]
            month_trades  += agg["trades"]
            print(f"    {sym:<10} {agg['trades']:>3} trades   "
                  f"WR {agg['winrate_pct']:5.1f}%   total {agg['total_r']:+6.2f}R")
        if month_trades:
            print(f"    {'TOTAL MOIS':<10} {month_trades:>3} trades   "
                  f"{'':<11}total {month_total_r:+6.2f}R")

    print("═" * 78)

    if out_path:
        with open(out_path, "w") as f:
            json.dump({
                "trades": rows,
                "monthly_by_symbol": {
                    f"{m}|{s}": v for (m, s), v in monthly.items()
                },
                "summary": {
                    "closed": len(closed), "wins": len(wins), "losses": len(losses),
                    "winrate_pct": winrate, "total_r": total_r,
                },
            }, f, indent=2, default=str)
        print(f"  Résultats détaillés (JSON) → {out_path}")

    if csv_path:
        _write_csv(rows, csv_path)
        print(f"  Détail trade par trade (CSV) → {csv_path}")


# ─────────────────────────────────────────────────────────────
#  7. CLI
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest SMC Signal Engine v8.6 — basket 6 mois")
    parser.add_argument("--symbol", default=None, help="Un seul symbole (ex: EURJPY=X)")
    parser.add_argument("--cat", default="all",
                         choices=["priority", "btc", "forex", "forex_all", "all"])
    parser.add_argument("--days", type=int, default=180,
                         help="Fenêtre de backtest en jours (défaut 180 = 6 mois). "
                              "Couverture complète pour les cryptos (Binance) ; "
                              "~60 derniers jours seulement pour forex/métaux/indices (yfinance).")
    parser.add_argument("--out", default="backtest_results.json",
                         help="Chemin du rapport JSON complet (tous les trades + mensuel × symbole).")
    parser.add_argument("--csv", default="backtest_trades.csv",
                         help="Chemin du CSV détaillé, une ligne par trade.")
    args = parser.parse_args()

    symbols = [(args.symbol, args.symbol)] if args.symbol else eng.get_symbols(args.cat)

    non_crypto = [s for s, _ in symbols if not eng.is_crypto_symbol(s)]
    if args.days > 60 and non_crypto:
        print(f"  ⚠ yfinance ne fournit pas plus de ~60 jours de données M15/M5 — "
              f"les {len(non_crypto)} symbole(s) forex/métaux/indices du panier "
              f"({', '.join(non_crypto[:6])}{'…' if len(non_crypto) > 6 else ''}) "
              f"n'auront des bougies que sur leurs ~60 derniers jours réels, "
              f"même si la fenêtre demandée ({args.days}j) remonte plus loin. "
              f"Seules les paires crypto (Binance) couvriront la fenêtre complète.")

    end_dt   = datetime.now(timezone.utc) - timedelta(minutes=15)  # vraie horloge, juste pour le préchargement
    start_dt = end_dt - timedelta(days=args.days)

    run_backtest(symbols, start_dt, end_dt)
    print_report(args.out, args.csv)
