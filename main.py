# -*- coding: utf-8 -*-
"""
qml_sl.py — SL du QML gagnant, plusieurs modes, réglés MANUELLEMENT par toi.

MODES
  CANDLE : derrière la bougie d'entrée (avalement / pin bar) + buffer
  SWEEP  : derrière la mèche du sweep de liquidité + buffer
  ZONE   : derrière l'extrême de la zone QML + buffer
  ATR    : entrée -/+ (sl_atr_mult x ATR)     <- tu choisis le multiplicateur
  WIDEST : le plus large entre CANDLE, SWEEP, ZONE (plus sûr, RR plus bas)
  TIGHTEST : le plus serré valide entre CANDLE, SWEEP, ZONE (meilleur RR)

Si le mode choisi n'a pas sa donnée (ex. pas de sweep détecté), repli sur
sl_fallback (par défaut "ATR"). Rien n'est envoyé si le SL est trop large
(max_sl_atr) ou trop serré (min_sl_atr) : le signal est SKIP.

Intégration dans main.py :
    from qml_sl import load_cfg, save_cfg, compute_sl, tp_levels, \
                       sl_menu_keyboard, apply_sl_callback

    cfg = load_cfg()
    res = compute_sl("BUY", entry, ctx, cfg)
    if res["ok"]:
        sl = res["sl"]
        tps = tp_levels("BUY", entry, sl, rr_list=(1, 2, 4))
    else:
        print("SKIP :", res["reason"])

    ctx = {
        "atr": 3.2,                       # ATR du TF du QML (obligatoire)
        "candle_high": ..., "candle_low": ...,   # bougie d'entrée
        "sweep_high": ..., "sweep_low": ...,     # extrême du sweep
        "zone_high": ..., "zone_low": ...,       # bornes de la zone QML
        "spread": 0.0,                    # optionnel
    }
"""
import json
import os

CFG_FILE = "qml_sl_cfg.json"
MODES = ["CANDLE", "SWEEP", "ZONE", "ATR", "WIDEST", "TIGHTEST"]

DEFAULT_CFG = {
    "sl_mode": "CANDLE",
    "sl_atr_mult": 1.5,        # utilisé par le mode ATR (et le repli)
    "sl_buffer_atr": 0.10,     # marge derrière bougie/sweep/zone (x ATR)
    "sl_buffer_points": 0.0,   # marge fixe en plus (en prix)
    "sl_fallback": "ATR",      # si la donnée du mode manque
    "max_sl_atr": 4.0,         # SL plus large que ça => SKIP
    "min_sl_atr": 0.3,         # SL plus serré que ça => SKIP
}


# ───────────────────────── Config persistante ─────────────────────────
def load_cfg(path=CFG_FILE):
    cfg = dict(DEFAULT_CFG)
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg.update(json.load(f))
    except Exception:
        pass
    return cfg


def save_cfg(cfg, path=CFG_FILE):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"[qml_sl] sauvegarde impossible : {e}")


# ───────────────────────── Calcul du SL ─────────────────────────
def _buffer(cfg, atr):
    return cfg["sl_buffer_atr"] * atr + cfg["sl_buffer_points"]


def _candidate(mode, is_buy, entry, ctx, cfg):
    """Retourne le prix du SL pour un mode, ou None si donnée absente."""
    atr = ctx["atr"]
    buf = _buffer(cfg, atr) + ctx.get("spread", 0.0)
    if mode == "ATR":
        d = cfg["sl_atr_mult"] * atr
        return entry - d if is_buy else entry + d
    keys = {"CANDLE": ("candle_low", "candle_high"),
            "SWEEP": ("sweep_low", "sweep_high"),
            "ZONE": ("zone_low", "zone_high")}[mode]
    ext = ctx.get(keys[0] if is_buy else keys[1])
    if ext is None:
        return None
    return ext - buf if is_buy else ext + buf


def compute_sl(side, entry, ctx, cfg):
    """Retourne {ok, sl, mode_used, dist, dist_atr, reason}."""
    is_buy = side.upper() in ("BUY", "ACHAT", "LONG")
    atr = ctx.get("atr")
    if not atr or atr <= 0:
        return {"ok": False, "reason": "ATR indisponible"}

    mode = cfg["sl_mode"]
    used = mode
    sl = None

    if mode in ("WIDEST", "TIGHTEST"):
        cands = [(m, _candidate(m, is_buy, entry, ctx, cfg))
                 for m in ("CANDLE", "SWEEP", "ZONE")]
        # on ne garde que les SL du bon côté de l'entrée
        cands = [(m, p) for m, p in cands if p is not None
                 and ((p < entry) if is_buy else (p > entry))]
        if cands:
            pick = (min if (mode == "WIDEST") == is_buy else max)
            # BUY : le plus large = le plus bas ; SELL : le plus large = le plus haut
            if mode == "WIDEST":
                used, sl = (min(cands, key=lambda t: t[1]) if is_buy
                            else max(cands, key=lambda t: t[1]))
            else:
                used, sl = (max(cands, key=lambda t: t[1]) if is_buy
                            else min(cands, key=lambda t: t[1]))
    else:
        sl = _candidate(mode, is_buy, entry, ctx, cfg)

    # repli si donnée absente ou SL du mauvais côté
    wrong_side = sl is not None and ((sl >= entry) if is_buy else (sl <= entry))
    if sl is None or wrong_side:
        fb = cfg.get("sl_fallback", "ATR")
        sl = _candidate(fb, is_buy, entry, ctx, cfg)
        used = f"{fb} (repli de {mode})"
        if sl is None or ((sl >= entry) if is_buy else (sl <= entry)):
            return {"ok": False, "reason": f"SL impossible pour le mode {mode}"}

    dist = abs(entry - sl)
    dist_atr = dist / atr
    if dist_atr > cfg["max_sl_atr"]:
        return {"ok": False, "sl": sl, "mode_used": used, "dist": dist,
                "dist_atr": dist_atr,
                "reason": f"SL trop large ({dist_atr:.2f} ATR > {cfg['max_sl_atr']})"}
    if dist_atr < cfg["min_sl_atr"]:
        return {"ok": False, "sl": sl, "mode_used": used, "dist": dist,
                "dist_atr": dist_atr,
                "reason": f"SL trop serré ({dist_atr:.2f} ATR < {cfg['min_sl_atr']})"}

    return {"ok": True, "sl": sl, "mode_used": used, "dist": dist,
            "dist_atr": dist_atr, "reason": ""}


def tp_levels(side, entry, sl, rr_list=(1, 2, 4)):
    """TP calculés depuis la distance réelle du SL : [(rr, prix), ...]"""
    is_buy = side.upper() in ("BUY", "ACHAT", "LONG")
    d = abs(entry - sl)
    return [(rr, entry + rr * d if is_buy else entry - rr * d) for rr in rr_list]


# ───────────────────────── Menu Telegram (callbacks) ─────────────────────────
# Compatible python-telegram-bot / pyTelegramBotAPI : on renvoie une structure
# simple [[(texte, callback_data), ...], ...] à convertir en InlineKeyboard.
MODE_LABELS = {
    "CANDLE": "Bougie d'entrée", "SWEEP": "Derrière sweep", "ZONE": "Derrière zone",
    "ATR": "Multiple ATR", "WIDEST": "Le + large", "TIGHTEST": "Le + serré",
}


def sl_menu_keyboard(cfg):
    rows = []
    for i in range(0, len(MODES), 2):
        row = []
        for m in MODES[i:i + 2]:
            mark = "✅ " if cfg["sl_mode"] == m else ""
            row.append((f"{mark}{MODE_LABELS[m]}", f"slmode:{m}"))
        rows.append(row)
    rows.append([(f"ATR x {cfg['sl_atr_mult']:g}  ➖", "slatr:-0.25"),
                 ("➕", "slatr:+0.25")])
    rows.append([(f"Marge x {cfg['sl_buffer_atr']:g} ATR  ➖", "slbuf:-0.05"),
                 ("➕", "slbuf:+0.05")])
    rows.append([(f"SL max {cfg['max_sl_atr']:g} ATR  ➖", "slmax:-0.5"),
                 ("➕", "slmax:+0.5")])
    return rows


def menu_text(cfg):
    return (
        "🛡 <b>SL — QML gagnant</b>\n"
        f"Mode : <b>{MODE_LABELS[cfg['sl_mode']]}</b>\n"
        f"ATR x <b>{cfg['sl_atr_mult']:g}</b> (mode ATR / repli)\n"
        f"Marge : <b>{cfg['sl_buffer_atr']:g}</b> ATR\n"
        f"SL max : <b>{cfg['max_sl_atr']:g}</b> ATR • min : <b>{cfg['min_sl_atr']:g}</b> ATR\n"
        "Tu peux aussi saisir une valeur exacte : /slatr 2.3"
    )


def apply_sl_callback(cfg, data):
    """Applique un callback_data du menu. Retourne True si la config a changé."""
    try:
        key, val = data.split(":", 1)
    except ValueError:
        return False
    if key == "slmode" and val in MODES:
        cfg["sl_mode"] = val
    elif key == "slatr":
        cfg["sl_atr_mult"] = round(max(0.1, cfg["sl_atr_mult"] + float(val)), 2)
    elif key == "slbuf":
        cfg["sl_buffer_atr"] = round(max(0.0, cfg["sl_buffer_atr"] + float(val)), 2)
    elif key == "slmax":
        cfg["max_sl_atr"] = round(max(0.5, cfg["max_sl_atr"] + float(val)), 2)
    else:
        return False
    save_cfg(cfg)
    return True


def set_atr_mult(cfg, text):
    """Commande manuelle : /slatr 2.3 -> règle exactement le multiplicateur."""
    try:
        v = float(text.replace(",", ".").split()[-1])
    except Exception:
        return False
    if not (0.1 <= v <= 20):
        return False
    cfg["sl_atr_mult"] = round(v, 2)
    save_cfg(cfg)
    return True


# ───────────────────────── Test rapide ─────────────────────────
if __name__ == "__main__":
    cfg = load_cfg()
    ctx = {"atr": 3.0, "candle_low": 2649.2, "candle_high": 2651.0,
           "sweep_low": 2647.5, "sweep_high": 2652.0,
           "zone_low": 2648.0, "zone_high": 2651.5}
    for m in MODES:
        cfg["sl_mode"] = m
        r = compute_sl("BUY", 2650.4, ctx, cfg)
        print(m, "->", {k: (round(v, 2) if isinstance(v, float) else v)
                        for k, v in r.items()})

