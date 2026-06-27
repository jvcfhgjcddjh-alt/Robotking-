
#!/usr/bin/env python3
"""
backtest.py — Backtest fidèle du moteur SMC (smc_engine_v8_6.py) — v8.7
==========================================================================
Rejoue le moteur EXACT (scan_symbol, kill zones, profils ASM, score,
garde de corrélation corrigée) sur une fenêtre historique choisie.

Principe : walk-forward strict. À chaque bougie M15 simulée, le moteur
ne voit QUE les données jusqu'à cet instant (aucune fuite du futur).
Les SL/TP des trades ouverts sont vérifiés bougie par bougie avant de
chercher un nouveau signal.

[v8.7] Léger pour Pydroid — plus de yfinance. Les données viennent de
l'API JSON publique de Yahoo Finance via `requests` (déjà réécrit dans
smc_engine_v8_6.py : `fetch()`). Dépendances réelles : pandas, numpy,
requests. C'est tout.

⚠️ Toujours besoin d'INTERNET (l'API Yahoo, pas de fichier local) —
   mais ça fonctionne directement sur Pydroid 3 (Android), pas besoin
   d'un serveur/PC séparé. Le rapport est envoyé directement sur ton
   Telegram à la fin (même bot que les alertes de trade).

   Cet outil de développement où JE travaille n'a, lui, aucun accès
   internet — donc je ne peux toujours pas exécuter ce backtest et te
   donner des vrais chiffres moi-même. Lancé chez toi (Pydroid, VPS,
   PC...), les résultats seront réels.

Usage (sur Pydroid ou ailleurs) :
    pip install pandas numpy requests
    python3 backtest.py --start 2026-06-01 --end 2026-06-08
    python3 backtest.py --start 2026-06-15 --end 2026-06-22 --markets forex_all
    python3 backtest.py --start 2026-06-19 --end 2026-06-26 --markets all --symbols GC=F,BTC-USD
    python3 backtest.py --start 2026-06-01 --end 2026-06-08 --no-telegram   # affichage console seul

Limites connues de l'API Yahoo (pas un bug de ce script) :
  - Les bougies 5m/15m ne sont disponibles que sur les ~60 derniers
    jours. Si tu choisis une semaine trop ancienne, le téléchargement
    M15/M5 peut revenir vide pour certains symboles.
  - Les indices (^GDAXI, ^DJI, ^GSPC, ^NDX) ont parfois des trous de
    données les jours fériés US/EU — normal, ne pas confondre avec un
    bug du moteur.

Sortie :
  - backtest_trades.csv   : journal détaillé de chaque trade simulé
  - backtest_summary.txt  : résumé agrégé (winrate, R total, par marché)
  - Telegram              : même résumé + le CSV en pièce jointe
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import pandas as pd
import requests

import smc_engine_v8_6 as eng   # le moteur corrigé (v8.7, requests-only)


# ─────────────────────────────────────────────────────────────
#  FEED HISTORIQUE — remplace fetch() en mémoire, sans réseau
#  une fois les données téléchargées une seule fois au départ.
# ─────────────────────────────────────────────────────────────
class HistoricalFeed:
    """
    Sert des bougies déjà téléchargées, tronquées à l'instant
    `now_sim` courant. Empêche toute fuite d'information du futur :
    si `now_sim` = 14h00 le 12 juin, scan_symbol() ne verra jamais
    une bougie postérieure à 14h00 le 12 juin, exactement comme en
    conditions réelles.
    """

    def __init__(self, raw: dict):
        self.raw = raw          # {(symbol, interval): DataFrame}
        self.now_sim = None     # pd.Timestamp (UTC)

    def fetch(self, symbol, interval, period="5d", retries=3, retry_delay=15):
        df = self.raw.get((symbol, interval))
        if df is None or df.empty:
            return pd.DataFrame()
        if self.now_sim is None:
            return df
        return df[df.index <= self.now_sim]


INTERVALS = ["4h", "1h", "15m", "5m"]


def download_history(symbols, start: datetime, end: datetime) -> dict:
    """
    Télécharge une seule fois toutes les bougies nécessaires.
    [v8.7] N'utilise plus yfinance — réutilise directement la fonction
    `fetch()` du moteur (requests seul, API JSON publique Yahoo), pour
    rester léger et compatible Pydroid. On élargit juste la période
    demandée pour couvrir [start - 45j, end].
    """
    raw = {}
    span_days = (end - start).days + 46  # marge pour le biais H4/H1
    period_str = f"{span_days}d"
    for sym, mkt in symbols:
        for interval in INTERVALS:
            try:
                df = eng.fetch(sym, interval, period=period_str, retries=2, retry_delay=5)
                if df is None or df.empty:
                    print(f"  ⚠️  {sym} [{interval}] — vide (hors plage Yahoo ?)")
                    raw[(sym, interval)] = pd.DataFrame()
                    continue
                raw[(sym, interval)] = df
                print(f"  ✓ {sym:<10} [{interval:<3}] — {len(df)} bougies")
            except Exception as e:
                print(f"  ⚠️  {sym} [{interval}] — erreur : {e}")
                raw[(sym, interval)] = pd.DataFrame()
    return raw


# ─────────────────────────────────────────────────────────────
#  CARNET DE TRADES SIMULÉ — n'écrit JAMAIS dans la vraie DB
#  du bot (active_trades). Totalement isolé.
# ─────────────────────────────────────────────────────────────
class FakeTradeBook:
    """
    [v8.8] Génère et envoie de VRAIS graphiques pour chaque trade simulé —
    via generate_chart_image(), la même fonction que pour les signaux
    live. Le graphique est construit à partir des vraies bougies M15
    téléchargées (sig.df_chart), pas d'une image générique ou inventée.
    """

    def __init__(self, send_telegram: bool = True, max_charts: int = 20):
        self.trades = []   # liste de dicts
        self._id = 0
        self.send_telegram = send_telegram
        self.max_charts = max_charts
        self.charts_sent = 0

    def open(self, symbol, mkt, direction, entry, sl, tp1, tp2, tp3,
             score, setup_type, opened_at, sig_v3=None):
        self._id += 1
        trade = dict(
            trade_id=self._id, symbol=symbol, mkt=mkt, direction=direction,
            entry=entry, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
            score=score, setup_type=setup_type, opened_at=opened_at,
            closed=0, result=None, exit_price=None, closed_at=None, r=0.0,
        )
        self.trades.append(trade)
        self._maybe_send_chart(trade, sig_v3)
        return trade

    def _maybe_send_chart(self, trade, sig_v3):
        if not self.send_telegram or sig_v3 is None:
            return
        chat_id = eng.TELEGRAM_LEADER_ID
        if not chat_id:
            return
        if self.charts_sent >= self.max_charts:
            return
        try:
            chart_path = eng.generate_chart_image(sig_v3)
        except Exception as e:
            print(f"  ⚠️  Graphique #{trade['trade_id']} ({trade['symbol']}) : {e}")
            chart_path = None
        caption = (
            f"🧪 [BACKTEST] #{trade['trade_id']} {trade['mkt']} {trade['direction']}\n"
            f"{trade['setup_type']} · score {trade['score']}/100\n"
            f"Entry {trade['entry']} · SL {trade['sl']} · TP1 {trade['tp1']}\n"
            f"📅 {trade['opened_at']}"
        )
        if chart_path:
            ok = eng.tg_send_photo(chart_path, caption, chat_id)
        else:
            ok = eng.tg_send(caption + "\n(graphique indisponible)", chat_id)
        if ok:
            self.charts_sent += 1
        time.sleep(1.2)   # évite le flood-limit Telegram

    def _notify_close(self, trade):
        if not self.send_telegram:
            return
        chat_id = eng.TELEGRAM_LEADER_ID
        if not chat_id:
            return
        emoji = "✅" if trade["result"] == "TP1" else "❌" if trade["result"] == "SL" else "⏹"
        msg = (
            f"{emoji} [BACKTEST] #{trade['trade_id']} {trade['mkt']} {trade['direction']} "
            f"→ {trade['result']} ({trade['r']:+.2f}R)"
        )
        eng.tg_send(msg, chat_id)
        time.sleep(0.6)

    def active(self):
        return [t for t in self.trades if t["closed"] == 0]

    def active_for_corr_guard(self):
        """Format compatible avec get_active_trades() du moteur réel."""
        return [{"symbol": t["symbol"], "direction": t["direction"],
                  "trade_id": t["trade_id"]} for t in self.active()]

    def step(self, symbol, bar_time, bar):
        """Vérifie SL/TP pour les trades ouverts sur ce symbole, sur cette bougie M15."""
        hi, lo = float(bar["high"]), float(bar["low"])
        for t in self.trades:
            if t["closed"] or t["symbol"] != symbol:
                continue
            long_ = t["direction"] == "LONG"
            risk = abs(t["entry"] - t["sl"])
            if risk == 0:
                continue
            hit_sl = (lo <= t["sl"]) if long_ else (hi >= t["sl"])
            # Hypothèse conservatrice : si SL et TP sont touchés sur la même
            # bougie, on compte le SL (pire cas, on ne triche pas en sa faveur).
            hit_tp1 = (hi >= t["tp1"]) if long_ else (lo <= t["tp1"])
            if hit_sl:
                t.update(closed=1, result="SL", exit_price=t["sl"],
                         closed_at=bar_time, r=-1.0)
                self._notify_close(t)
            elif hit_tp1:
                r = abs(t["tp1"] - t["entry"]) / risk
                t.update(closed=1, result="TP1", exit_price=t["tp1"],
                         closed_at=bar_time, r=r)
                self._notify_close(t)

    def force_close_remaining(self, last_prices: dict):
        """Clôture au prix courant ce qui reste ouvert à la fin de la fenêtre testée."""
        for t in self.active():
            px = last_prices.get(t["symbol"])
            if px is None:
                t.update(closed=1, result="UNRESOLVED", r=0.0)
                continue
            long_ = t["direction"] == "LONG"
            risk = abs(t["entry"] - t["sl"])
            r = ((px - t["entry"]) if long_ else (t["entry"] - px)) / risk if risk else 0.0
            t.update(closed=1, result="OPEN_AT_END", exit_price=px, r=r)
            self._notify_close(t)


# ─────────────────────────────────────────────────────────────
#  CORRELATION GUARD — même logique que le moteur corrigé,
#  mais branchée sur le FakeTradeBook plutôt que sur la SQLite DB.
# ─────────────────────────────────────────────────────────────
def correlation_guard_sim(book: FakeTradeBook, symbol: str, direction: str):
    group = eng._CORR_GROUPS.get(symbol)
    if group is None:
        return True, ""
    for t in book.active_for_corr_guard():
        if t["symbol"] == symbol:
            continue
        if t["direction"] != direction:
            continue
        if eng._CORR_GROUPS.get(t["symbol"]) == group:
            return False, f"corrélation {group} {direction} déjà ouverte sur {t['symbol']}"
    return True, ""


def run_backtest(symbols, start: datetime, end: datetime, min_rr: float,
                  send_telegram: bool = True, max_charts: int = 20):
    print(f"\n📥 Téléchargement historique ({len(symbols)} marchés)…")
    raw = download_history(symbols, start, end)
    feed = HistoricalFeed(raw)

    # Monkey-patch : scan_symbol()/_fetch_data() appellent fetch() par son
    # nom global dans le module eng — on le remplace par le feed historique.
    eng.fetch = feed.fetch

    book = FakeTradeBook(send_telegram=send_telegram, max_charts=max_charts)

    # Référentiel temporel = la M15 du premier symbole dispo (toutes les
    # bougies M15 sont alignées en UTC par yfinance).
    m15_index = None
    for sym, _ in symbols:
        df = raw.get((sym, "15m"))
        if df is not None and not df.empty:
            m15_index = df.index
            break
    if m15_index is None:
        print("❌ Aucune donnée M15 disponible pour la fenêtre demandée. Abandon.")
        return book

    timeline = [t for t in m15_index if start <= t.to_pydatetime().replace(tzinfo=timezone.utc) <= end]
    print(f"\n⏱  {len(timeline)} bougies M15 à rejouer entre {start.date()} et {end.date()}\n")

    last_prices: dict[str, float] = {}

    for i, bar_time in enumerate(timeline):
        feed.now_sim = bar_time

        for sym, mkt in symbols:
            df_m15 = raw.get((sym, "15m"))
            if df_m15 is None or df_m15.empty or bar_time not in df_m15.index:
                continue
            bar = df_m15.loc[bar_time]
            last_prices[sym] = float(bar["close"])

            # 1) Mise à jour des trades ouverts sur ce symbole
            book.step(sym, bar_time, bar)

            # 2) Kill zone — même filtre que le moteur réel
            kz_ok, _ = eng.is_kill_zone_active(sym)
            if not kz_ok:
                continue

            # 3) Pas de doublon si un trade est déjà ouvert sur ce symbole
            if any(t["symbol"] == sym for t in book.active()):
                continue

            # 4) Scan réel (BREAKER/SD/OB/BOS/MSS/FVG/AMD + profil ASM)
            try:
                sigs = eng.scan_symbol(sym, mkt, min_rr=min_rr)
            except Exception as e:
                continue
            if not sigs:
                continue

            best = sigs[0]   # déjà trié par tier puis score dans scan_symbol()

            # 5) Filtre BTC SELL bloqué (même règle que le live)
            if eng.BTC_SELL_BLOCKED and sym == "BTC-USD" and best.direction in ("SHORT", "SELL"):
                continue

            # 6) Garde de corrélation corrigée (v8.6) — branchée sur le book simulé
            corr_ok, _ = correlation_guard_sim(book, sym, best.direction)
            if not corr_ok:
                continue

            book.open(sym, mkt, best.direction, best.entry, best.sl,
                      best.tp, best.tp2, best.tp3, best.score,
                      best.setup_type, bar_time, sig_v3=best.to_signal())

        if (i + 1) % 50 == 0:
            print(f"  … {i+1}/{len(timeline)} bougies rejouées "
                  f"({len(book.trades)} trades ouverts jusqu'ici)")

    book.force_close_remaining(last_prices)
    return book


def print_report(book: FakeTradeBook, start, end):
    trades = book.trades
    lines = []
    lines.append(f"BACKTEST — {start.date()} → {end.date()}")
    lines.append(f"Total trades simulés : {len(trades)}\n")

    if not trades:
        lines.append("Aucun trade généré sur cette fenêtre — soit le seuil de score "
                      "(74/100) n'a jamais été atteint, soit les kill zones/filtres "
                      "ont bloqué tous les setups cette semaine-là.")
        report = "\n".join(lines)
        print("\n" + report)
        return report

    resolved = [t for t in trades if t["result"] in ("SL", "TP1")]
    wins = [t for t in resolved if t["result"] == "TP1"]
    losses = [t for t in resolved if t["result"] == "SL"]
    total_r = sum(t["r"] for t in trades)
    winrate = (len(wins) / len(resolved) * 100) if resolved else 0.0

    lines.append(f"Résolus (SL ou TP1) : {len(resolved)}  |  "
                 f"En cours / coupés en fin de fenêtre : {len(trades) - len(resolved)}")
    lines.append(f"Winrate : {winrate:.1f}%  ({len(wins)}W / {len(losses)}L)")
    lines.append(f"Total R : {total_r:+.2f}R\n")

    by_sym = defaultdict(list)
    for t in trades:
        by_sym[t["mkt"]].append(t)

    lines.append("Détail par marché :")
    for mkt, ts in sorted(by_sym.items(), key=lambda kv: -sum(x["r"] for x in kv[1])):
        r_sum = sum(t["r"] for t in ts)
        res = [t for t in ts if t["result"] in ("SL", "TP1")]
        w = len([t for t in res if t["result"] == "TP1"])
        wr = (w / len(res) * 100) if res else 0.0
        lines.append(f"  {mkt:<16} {len(ts):>3} trades  |  winrate {wr:5.1f}%  |  {r_sum:+.2f}R")

    report = "\n".join(lines)
    print("\n" + report)
    return report


def send_to_telegram(report: str, csv_path: str = None):
    """
    Envoie le résumé directement sur Telegram, en réutilisant tg_send()
    du moteur (requests seul, déjà présent — aucune dépendance ajoutée).
    """
    chat_id = eng.TELEGRAM_LEADER_ID
    if not chat_id:
        print("  ⚠️  Pas de TG_LEADER_ID configuré — rapport non envoyé sur Telegram.")
        return
    header = "📊 <b>BACKTEST SMC — rapport</b>\n\n"
    ok = eng.tg_send(header + f"<pre>{report}</pre>", chat_id)
    print("  ✓ Rapport envoyé sur Telegram." if ok else "  ⚠️  Échec envoi Telegram (vérifie TG_BOT_TOKEN).")

    if csv_path and eng.TELEGRAM_TOKEN:
        try:
            with open(csv_path, "rb") as f:
                r = requests.post(
                    eng._tg_url("sendDocument"),
                    data={"chat_id": chat_id, "caption": "Journal détaillé des trades simulés"},
                    files={"document": (csv_path, f)},
                    timeout=20,
                )
            print("  ✓ CSV envoyé sur Telegram." if r.status_code == 200
                  else f"  ⚠️  Échec envoi CSV ({r.status_code}).")
        except Exception as e:
            print(f"  ⚠️  Échec envoi CSV : {e}")


def main():
    ap = argparse.ArgumentParser(description="Backtest fidèle du moteur SMC v8.7 (Pydroid-ready)")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--markets", default="all",
                     choices=["priority", "btc", "forex", "forex_all", "all"])
    ap.add_argument("--symbols", default=None,
                     help="Liste explicite ex: GC=F,BTC-USD,^GDAXI (remplace --markets)")
    ap.add_argument("--min-rr", type=float, default=eng.MIN_RR)
    ap.add_argument("--no-telegram", action="store_true",
                     help="N'envoie pas le rapport sur Telegram (affiche seulement dans la console)")
    ap.add_argument("--no-charts", action="store_true",
                     help="N'envoie pas les graphiques par trade — uniquement le résumé texte + CSV")
    ap.add_argument("--max-charts", type=int, default=20,
                     help="Nombre max de graphiques envoyés sur Telegram (évite le flood-limit). Défaut: 20")
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(
        hour=23, minute=45, tzinfo=timezone.utc)

    if args.symbols:
        all_known = dict(eng.get_symbols("all"))
        symbols = [(s.strip(), all_known.get(s.strip(), s.strip()))
                   for s in args.symbols.split(",")]
    else:
        symbols = eng.get_symbols(args.markets)

    send_charts = (not args.no_telegram) and (not args.no_charts)
    book = run_backtest(symbols, start, end, args.min_rr,
                         send_telegram=send_charts, max_charts=args.max_charts)
    report = print_report(book, start, end)

    csv_path = None
    if book.trades:
        csv_path = "backtest_trades.csv"
        # On exclut les champs internes non sérialisables (objets Signal) du CSV
        clean_rows = [{k: v for k, v in t.items() if not k.startswith("_")}
                      for t in book.trades]
        pd.DataFrame(clean_rows).to_csv(csv_path, index=False)
        print(f"\n💾 Journal détaillé → {csv_path}")
    with open("backtest_summary.txt", "w") as f:
        f.write(report)
    print("💾 Résumé → backtest_summary.txt")
    if book.trades and send_charts:
        print(f"📷 {book.charts_sent} graphique(s) réel(s) envoyé(s) sur Telegram "
              f"(plafond --max-charts {args.max_charts}).")

    if not args.no_telegram:
        send_to_telegram(report, csv_path)


if __name__ == "__main__":
    main()
