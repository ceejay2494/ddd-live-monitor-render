
import os
import time
import math
import io
import re
import json
import hashlib
import textwrap
import threading
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont
from flask import Flask, jsonify, Response

os.environ.setdefault("DDD_DATA_ROOT", os.environ.get("DDD_DATA_ROOT", "/tmp/ddd"))

import ddd_engine_v72 as ddd

API_KEY = os.environ.get("API_FOOTBALL_KEY", "").strip()
SCAN_SECONDS = int(os.environ.get("DDD_SCAN_SECONDS", "300"))
IDLE_SCAN_SECONDS = int(os.environ.get("DDD_IDLE_SCAN_SECONDS", "900"))
DAILY_REQUEST_LIMIT = int(os.environ.get("DDD_DAILY_REQUEST_LIMIT", "6500"))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_SEND_EVERY_SCAN = os.environ.get("TELEGRAM_SEND_EVERY_SCAN", "1").strip() not in {"0","false","False"}

ROLLOVER_MODE = os.environ.get("DDD_ROLLOVER_MODE", "1").strip() not in {"0","false","False"}
ROLLOVER_FAST_TOLERANCE = float(os.environ.get("DDD_ROLLOVER_FAST_TOLERANCE", "0.03"))
ROLLOVER_MAX_PUSH = float(os.environ.get("DDD_ROLLOVER_MAX_PUSH", "0.10"))
ROLLOVER_STOP_ON_LOSS = os.environ.get("DDD_ROLLOVER_STOP_ON_LOSS", "1").strip() not in {"0","false","False"}
ROLLOVER_STATE_PATH = Path(os.environ.get("DDD_ROLLOVER_STATE_PATH", "/tmp/ddd/rollover_state.json"))
ROLLOVER_STRICT_MIN_ODDS = float(os.environ.get("DDD_ROLLOVER_MIN_ODDS", "1.25"))
ROLLOVER_PREF_MIN_ODDS = float(os.environ.get("DDD_ROLLOVER_PREF_MIN_ODDS", "1.30"))
ROLLOVER_PREF_MAX_ODDS = float(os.environ.get("DDD_ROLLOVER_PREF_MAX_ODDS", "1.55"))
ROLLOVER_EXCEPTION_MIN_ODDS = float(os.environ.get("DDD_ROLLOVER_EXCEPTION_MIN_ODDS", "1.20"))
ROLLOVER_EXCEPTION_MAX_ODDS = float(os.environ.get("DDD_ROLLOVER_EXCEPTION_MAX_ODDS", "1.24"))
ROLLOVER_EXCEPTION_MIN_PROB = float(os.environ.get("DDD_ROLLOVER_EXCEPTION_MIN_PROB", "0.90"))
ROLLOVER_EXCEPTION_MIN_EDGE = float(os.environ.get("DDD_ROLLOVER_EXCEPTION_MIN_EDGE", "0.06"))
ROLLOVER_EXCEPTION_MIN_EV = float(os.environ.get("DDD_ROLLOVER_EXCEPTION_MIN_EV", "0.05"))

ddd.MIN_ODDS = float(os.environ.get("DDD_MIN_ODDS", "1.20"))
ddd.MIN_MODEL_PROB = float(os.environ.get("DDD_MIN_MODEL_PROB", "0.65"))
ddd.HIGH_CONF_PROB = float(os.environ.get("DDD_HARD80", "0.80"))
ddd.WATCH_MIN_PROB = float(os.environ.get("DDD_WATCH_MIN", "0.75"))
ddd.MIN_EDGE = float(os.environ.get("DDD_MIN_EDGE", "0.04"))
ddd.MIN_EV = float(os.environ.get("DDD_MIN_EV", "0.025"))
ddd.PROBABILITY_FIRST = True
ddd.PERIOD_AWARE = True

app = Flask(__name__)

_lock = threading.RLock()
_state = {
    "healthy": False,
    "status": "starting",
    "last_scan_utc": None,
    "last_scan_age_seconds": None,
    "next_scan_seconds": None,
    "scan_interval_seconds": SCAN_SECONDS,
    "cycle": 0,
    "live_count": 0,
    "odds_rows": 0,
    "scored_markets": 0,
    "request_count_today": 0,
    "request_day_utc": datetime.now(timezone.utc).date().isoformat(),
    "quota_limit": DAILY_REQUEST_LIMIT,
    "quota_remaining_estimate": DAILY_REQUEST_LIMIT,
    "official": [],
    "fast_period": [],
    "momentum_fast": [],
    "watch": [],
    "closest": [],
    "fixtures": [],
    "telegram_configured": bool(TELEGRAM_BOT_TOKEN),
    "telegram_chat_resolved": bool(TELEGRAM_CHAT_ID),
    "telegram_last_sent_utc": None,
    "telegram_last_error": None,
    "rollover_mode": ROLLOVER_MODE,
    "rollover_active": None,
    "rollover_chain_no": 0,
    "rollover_stopped": False,
    "rollover_last_result": None,
    "error": None,
}

_original_api_get = ddd.api_get

def _reset_daily_counter_if_needed():
    today = datetime.now(timezone.utc).date().isoformat()
    with _lock:
        if _state["request_day_utc"] != today:
            _state["request_day_utc"] = today
            _state["request_count_today"] = 0

def counted_api_get(path, params=None, key=None):
    _reset_daily_counter_if_needed()
    with _lock:
        if _state["request_count_today"] >= DAILY_REQUEST_LIMIT:
            raise RuntimeError("DDD quota guard: local daily request ceiling reached")
        _state["request_count_today"] += 1
        _state["quota_remaining_estimate"] = max(
            0, DAILY_REQUEST_LIMIT - _state["request_count_today"]
        )
    return _original_api_get(path, params=params, key=key)

ddd.api_get = counted_api_get

def _clean_scalar(v):
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        v = float(v)
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (datetime,)):
        return v.isoformat()
    return v

def _records(df, limit=12, cols=None):
    if df is None or getattr(df, "empty", True):
        return []
    x = df.copy().head(limit)
    if cols:
        keep = [c for c in cols if c in x.columns]
        x = x[keep]
    out = []
    for row in x.to_dict(orient="records"):
        out.append({k: _clean_scalar(v) for k, v in row.items()})
    return out

CARD_COLS = [
    "fixture_id","selection_role","period_lane","minute","home","away","score",
    "market_group","market_family","market","selection","handicap","raw_value",
    "live_odds","model_probability_effective","push_probability_raw",
    "estimated_edge","estimated_ev","data_quality","market_data_quality",
    "corner_momentum_multiplier","recent_window_minutes",
    "recent_corners_delta","recent_shots_delta","recent_sot_delta"
]

FIXTURE_COLS = [
    "fixture_id","minute","status","home","away","home_goals","away_goals",
    "league","country","data_quality","live_odds_rows",
    "home_corners","away_corners","home_shots","away_shots","home_sot","away_sot"
]

def _warning_list(row):
    warnings = []
    p = pd.to_numeric(pd.Series([row.get("model_probability_effective")]), errors="coerce").iloc[0]
    edge = pd.to_numeric(pd.Series([row.get("estimated_edge")]), errors="coerce").iloc[0]
    ev = pd.to_numeric(pd.Series([row.get("estimated_ev")]), errors="coerce").iloc[0]
    odds = pd.to_numeric(pd.Series([row.get("live_odds")]), errors="coerce").iloc[0]
    push = pd.to_numeric(pd.Series([row.get("push_probability_raw")]), errors="coerce").iloc[0]
    dq = pd.to_numeric(pd.Series([row.get("data_quality")]), errors="coerce").iloc[0]

    if pd.isna(p):
        warnings.append("model probability unavailable")
    elif p < ddd.HIGH_CONF_PROB:
        warnings.append(f"{(ddd.HIGH_CONF_PROB-p)*100:.1f}pp below HARD80")
    if not pd.isna(edge) and edge < ddd.MIN_EDGE:
        warnings.append(f"edge {edge*100:.1f}% below {ddd.MIN_EDGE*100:.0f}% minimum")
    if not pd.isna(ev) and ev < ddd.MIN_EV:
        warnings.append(f"EV {ev*100:.1f}% below {ddd.MIN_EV*100:.1f}% minimum")
    if not pd.isna(odds) and odds < ddd.MIN_ODDS:
        warnings.append(f"odds {odds:.2f} below {ddd.MIN_ODDS:.2f} floor")
    if not pd.isna(push) and push >= 0.20:
        warnings.append(f"push-heavy line ({push*100:.0f}% push)")
    if not pd.isna(dq) and dq < 0.50:
        warnings.append("weak live-stat coverage")
    if not warnings:
        warnings.append("closest candidate; not selected by strict official gates")
    return warnings

def _closest_board(opp, limit=8):
    if opp is None or opp.empty:
        return []
    x = opp.copy()
    for c in ["model_probability_effective","estimated_edge","estimated_ev","live_odds"]:
        if c in x:
            x[c] = pd.to_numeric(x[c], errors="coerce")
    if "model_probability_effective" not in x:
        return []
    x = x[x["model_probability_effective"].notna()].copy()
    if "fixture_blocked" in x:
        x = x[x["fixture_blocked"] != True]
    if "fixture_finished" in x:
        x = x[x["fixture_finished"] != True]
    if x.empty:
        return []

    sort_cols = [c for c in ["model_probability_effective","estimated_edge","estimated_ev","live_odds"] if c in x]
    x = x.sort_values(sort_cols, ascending=[False]*len(sort_cols))
    if "fixture_id" in x:
        x = x.drop_duplicates("fixture_id", keep="first")
    x = x.head(limit)

    rows = _records(x, limit=limit, cols=CARD_COLS)
    for i, row in enumerate(rows, 1):
        row["closest_rank"] = i
        row["warnings"] = _warning_list(row)
    return rows


_telegram_chat_cache = TELEGRAM_CHAT_ID or None
_telegram_last_signature = None

def _font(size, bold=False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ]
    for p in candidates:
        try:
            return ImageFont.truetype(p, size=size)
        except Exception:
            pass
    return ImageFont.load_default()

def _fmt_pct(v):
    try:
        return f"{float(v)*100:.1f}%"
    except Exception:
        return "—"

def _fmt_num(v, nd=2):
    try:
        return f"{float(v):.{nd}f}"
    except Exception:
        return "—"

def _resolve_telegram_chat_id():
    global _telegram_chat_cache
    if _telegram_chat_cache:
        return str(_telegram_chat_cache)
    if not TELEGRAM_BOT_TOKEN:
        return None
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"limit": 20, "timeout": 0},
            timeout=15,
        )
        data = r.json()
        if not data.get("ok"):
            return None
        for upd in reversed(data.get("result", [])):
            msg = upd.get("message") or upd.get("channel_post") or upd.get("edited_message")
            if not msg:
                continue
            chat = msg.get("chat") or {}
            cid = chat.get("id")
            if cid is not None:
                _telegram_chat_cache = str(cid)
                with _lock:
                    _state["telegram_chat_resolved"] = True
                return _telegram_chat_cache
    except Exception as e:
        with _lock:
            _state["telegram_last_error"] = f"chat resolve: {type(e).__name__}: {e}"
    return None

def _official_signature(rows):
    bits = []
    for r in rows:
        bits.append("|".join([
            str(r.get("home","")),
            str(r.get("away","")),
            str(r.get("market","")),
            str(r.get("selection","")),
            str(r.get("live_odds","")),
            str(r.get("model_probability_effective","")),
            str(r.get("minute","")),
        ]))
    return hashlib.sha1("\n".join(bits).encode("utf-8")).hexdigest()

def _render_official_png(rows, scan_utc, live_count, odds_rows, scored_markets):
    # Mobile-friendly portrait image, one card per official pick.
    rows = list(rows or [])[:12]
    W = 1200
    header_h = 230
    card_h = 180
    gap = 18
    H = header_h + max(1, len(rows)) * (card_h + gap) + 70

    img = Image.new("RGB", (W, H), (11, 16, 32))
    d = ImageDraw.Draw(img)

    title_f = _font(50, True)
    sub_f = _font(26, False)
    match_f = _font(31, True)
    body_f = _font(25, False)
    prob_f = _font(44, True)

    d.text((42, 32), "DDD V7.2 — OFFICIAL ≥80%", font=title_f, fill=(238, 242, 255))
    d.text((42, 98), f"Scan: {scan_utc} UTC  •  LIVE {live_count}  •  ODDS {odds_rows}  •  SCORED {scored_markets}",
           font=sub_f, fill=(170, 184, 220))
    d.text((42, 140), "HARD80 actual-win  •  strict edge/EV  •  probability first",
           font=sub_f, fill=(100, 230, 167))
    d.text((42, 180), "Telegram board refreshes every 5 minutes when official qualifiers exist.",
           font=sub_f, fill=(170, 184, 220))

    y = header_h
    if not rows:
        d.rounded_rectangle((36, y, W-36, y+card_h), radius=24, fill=(18, 26, 48), outline=(52, 68, 111), width=3)
        d.text((70, y+55), "No strict OFFICIAL ≥80% qualifier this scan.", font=match_f, fill=(205, 211, 230))
    else:
        for i, r in enumerate(rows, 1):
            d.rounded_rectangle((36, y, W-36, y+card_h), radius=24, fill=(18, 26, 48), outline=(31, 157, 104), width=3)
            minute = r.get("minute")
            minute_txt = f"{minute}'" if minute is not None else "LIVE"
            match = f"{i}. {r.get('home','')} vs {r.get('away','')}  •  {minute_txt}  •  {r.get('score','')}"
            market = f"{r.get('market','') or r.get('market_family','')} — {r.get('selection','')}"
            meta = f"{r.get('period_lane','')}  •  Odds {_fmt_num(r.get('live_odds'),2)}  •  Edge {_fmt_pct(r.get('estimated_edge'))}  •  EV {_fmt_pct(r.get('estimated_ev'))}"
            p = _fmt_pct(r.get("model_probability_effective"))

            # Wrap long match/market names.
            match_lines = textwrap.wrap(match, width=54)[:2]
            for j, line in enumerate(match_lines):
                d.text((66, y+24+j*36), line, font=match_f, fill=(238, 242, 255))
            m_y = y + 24 + len(match_lines)*36 + 8
            market_lines = textwrap.wrap(market, width=67)[:2]
            for j, line in enumerate(market_lines):
                d.text((66, m_y+j*31), line, font=body_f, fill=(205, 211, 230))
            d.text((66, y+140), meta, font=body_f, fill=(170, 184, 220))
            d.text((W-250, y+58), p, font=prob_f, fill=(100, 230, 167))
            y += card_h + gap

    bio = io.BytesIO()
    img.save(bio, format="PNG", optimize=True)
    bio.seek(0)
    return bio

def _send_telegram_official(snapshot):
    global _telegram_last_signature
    rows = snapshot.get("official") or []
    if not TELEGRAM_BOT_TOKEN or not rows:
        return False

    chat_id = _resolve_telegram_chat_id()
    if not chat_id:
        with _lock:
            _state["telegram_last_error"] = "Waiting for Telegram chat. Open the bot and send /start once."
        print("TELEGRAM: waiting for /start (chat id not resolved yet)")
        return False

    sig = _official_signature(rows)
    if not TELEGRAM_SEND_EVERY_SCAN and sig == _telegram_last_signature:
        return False

    scan_utc = (snapshot.get("last_scan_utc") or datetime.now(timezone.utc).isoformat()).replace("+00:00","Z")
    png = _render_official_png(
        rows,
        scan_utc[:19].replace("T"," "),
        snapshot.get("live_count", 0),
        snapshot.get("odds_rows", 0),
        snapshot.get("scored_markets", 0),
    )

    caption = (
        f"DDD V7.2 OFFICIAL ≥80% — {len(rows)} qualifier(s)\n"
        f"{scan_utc[:19].replace('T',' ')} UTC\n"
        f"Next live scan: 5 minutes"
    )
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption},
            files={"photo": ("ddd_official_80plus.png", png, "image/png")},
            timeout=30,
        )
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description") or "Telegram sendPhoto failed")
        _telegram_last_signature = sig
        with _lock:
            _state["telegram_last_sent_utc"] = datetime.now(timezone.utc).isoformat()
            _state["telegram_last_error"] = None
            _state["telegram_chat_resolved"] = True
        print(f"TELEGRAM: sent OFFICIAL ≥80% board ({len(rows)} picks)")
        return True
    except Exception as e:
        with _lock:
            _state["telegram_last_error"] = f"{type(e).__name__}: {e}"
        print(f"TELEGRAM ERROR: {type(e).__name__}: {e}")
        return False


ROLLOVER_SUPPORTED_FAMILIES = {
    "goal_total", "goal_total_1h", "goal_total_2h",
    "team_goal_home", "team_goal_away",
    "btts_ft",
    "match_result_ft", "double_chance_ft", "draw_no_bet_ft",
    "match_result_1h", "match_result_2h",
    "corner_total_ft", "corner_total_1h", "corner_total_2h",
}

_rollover = {
    "active": None,
    "chain_no": 0,
    "stopped": False,
    "last_result": None,
}

def _roll_save():
    try:
        ROLLOVER_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        ROLLOVER_STATE_PATH.write_text(json.dumps(_rollover, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print("ROLLOVER STATE SAVE WARNING:", e)

def _roll_load():
    try:
        if ROLLOVER_STATE_PATH.exists():
            data = json.loads(ROLLOVER_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _rollover.update(data)
    except Exception as e:
        print("ROLLOVER STATE LOAD WARNING:", e)
    with _lock:
        _state["rollover_active"] = _rollover.get("active")
        _state["rollover_chain_no"] = int(_rollover.get("chain_no") or 0)
        _state["rollover_stopped"] = bool(_rollover.get("stopped"))
        _state["rollover_last_result"] = _rollover.get("last_result")

_roll_load()

def _n(v, default=None):
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default

def _line(row):
    for k in ("handicap", "line"):
        v = _n(row.get(k))
        if v is not None:
            return v
    txt = f"{row.get('selection','')} {row.get('market','')}"
    m = re.search(r"(?<!\d)(\d+(?:\.\d+)?)", txt)
    return float(m.group(1)) if m else None

def _settle_ou(selection, line, observed, three_way=False):
    if line is None or observed is None:
        return None
    s = str(selection).lower()
    if "over" in s:
        if observed > line:
            return "WIN"
        if observed < line:
            return "LOSS"
        return "LOSS" if three_way else "PUSH"
    if "under" in s:
        if observed < line:
            return "WIN"
        if observed > line:
            return "LOSS"
        return "LOSS" if three_way else "PUSH"
    return None

def _minutes_to_settle(row):
    lane = str(row.get("period_lane") or "").upper()
    minute = int(_n(row.get("minute"), 0) or 0)
    if lane == "1H":
        return max(0, 45 - minute)
    if lane in {"2H", "FT"}:
        return max(0, 90 - minute)
    return 999

def _select_rollover_candidate(rows):
    if not rows:
        return None

    cleaned = []
    for r0 in rows:
        r = dict(r0)
        fam = str(r.get("market_family") or "")
        p = _n(r.get("model_probability_effective"), 0.0)
        odds = _n(r.get("live_odds"), 0.0)
        edge = _n(r.get("estimated_edge"), 0.0)
        ev = _n(r.get("estimated_ev"), 0.0)
        push = _n(r.get("push_probability_raw"), 0.0)
        fid = r.get("fixture_id")

        if fam not in ROLLOVER_SUPPORTED_FAMILIES or fid in (None, ""):
            continue
        if p < ddd.HIGH_CONF_PROB or edge < ddd.MIN_EDGE or ev < ddd.MIN_EV:
            continue
        if push > ROLLOVER_MAX_PUSH:
            continue

        # Normal rollover lane: 1.25+
        normal_lane = odds >= ROLLOVER_STRICT_MIN_ODDS

        # Exceptional low-odds lane: 1.20-1.24 only when the signal is unusually strong.
        exception_lane = (
            ROLLOVER_EXCEPTION_MIN_ODDS <= odds <= ROLLOVER_EXCEPTION_MAX_ODDS
            and p >= ROLLOVER_EXCEPTION_MIN_PROB
            and edge >= ROLLOVER_EXCEPTION_MIN_EDGE
            and ev >= ROLLOVER_EXCEPTION_MIN_EV
            and push <= ROLLOVER_MAX_PUSH
        )
        if not (normal_lane or exception_lane):
            continue

        r["preferred_odds_zone"] = bool(
            ROLLOVER_PREF_MIN_ODDS <= odds <= ROLLOVER_PREF_MAX_ODDS
        )
        r["exception_low_odds_lane"] = bool(exception_lane and not normal_lane)
        r["minutes_to_settle"] = _minutes_to_settle(r)
        r["_p"] = p
        r["_odds"] = odds
        r["_edge"] = edge
        r["_ev"] = ev
        r["_minute"] = int(_n(r.get("minute"), 0) or 0)
        cleaned.append(r)

    if not cleaned:
        return None

    # Probability is the first ranking dimension.
    cleaned.sort(
        key=lambda r: (
            r["_p"],
            int(r["preferred_odds_zone"]),
            r["_ev"],
            r["_edge"],
            r["_odds"],
        ),
        reverse=True,
    )
    pmax = cleaned[0]["_p"]

    # Rollover tie-break: only inside the configured probability tolerance do
    # we allow faster settlement to outrank the marginally higher probability.
    near = [r for r in cleaned if r["_p"] >= pmax - ROLLOVER_FAST_TOLERANCE]
    near.sort(
        key=lambda r: (
            -r["minutes_to_settle"],
            int(r["preferred_odds_zone"]),
            r["_p"],
            r["_ev"],
            r["_edge"],
            r["_odds"],
        ),
        reverse=True,
    )
    chosen = near[0] if near else cleaned[0]

    for k in list(chosen):
        if k.startswith("_"):
            chosen.pop(k, None)
    return chosen

def _send_tg_text(text):
    chat_id = _resolve_telegram_chat_id()
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": chat_id, "text": text},
            timeout=20,
        )
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description") or "Telegram sendMessage failed")
        return True
    except Exception as e:
        with _lock:
            _state["telegram_last_error"] = f"{type(e).__name__}: {e}"
        print("TELEGRAM TEXT ERROR:", e)
        return False

def _render_rollover_png(r, chain_no, scan_utc):
    W, H = 1200, 720
    img = Image.new("RGB", (W, H), (11, 16, 32))
    d = ImageDraw.Draw(img)
    title = _font(52, True)
    h2 = _font(35, True)
    body = _font(28, False)
    prob = _font(64, True)

    d.text((48, 35), f"DDD ROLLOVER SIGNAL #{chain_no}", font=title, fill=(238,242,255))
    d.text((48, 102), "STRICT HARD80 • SINGLE ACTIVE SIGNAL", font=body, fill=(100,230,167))

    minute = r.get("minute")
    minute_txt = f"{minute}'" if minute is not None else "LIVE"
    match = f"{r.get('home','')} vs {r.get('away','')}"
    d.text((48, 180), match, font=h2, fill=(238,242,255))
    d.text((48, 235), f"{minute_txt}  •  Score {r.get('score','')}  •  {r.get('period_lane','')}", font=body, fill=(170,184,220))

    market = f"{r.get('market','') or r.get('market_family','')} — {r.get('selection','')}"
    for i, line in enumerate(textwrap.wrap(market, width=52)[:2]):
        d.text((48, 315 + i*42), line, font=h2 if i == 0 else body, fill=(205,211,230))

    d.text((48, 430), f"Odds: {_fmt_num(r.get('live_odds'),2)}", font=h2, fill=(238,242,255))
    d.text((48, 485), f"Edge: {_fmt_pct(r.get('estimated_edge'))}  •  EV: {_fmt_pct(r.get('estimated_ev'))}", font=body, fill=(170,184,220))
    d.text((48, 525), f"Est. settle: {r.get('minutes_to_settle', '—')} min  •  Preferred odds: {'YES' if r.get('preferred_odds_zone') else 'NO'}", font=body, fill=(170,184,220))
    d.text((48, 570), f"Scan: {scan_utc} UTC", font=body, fill=(170,184,220))
    d.text((830, 375), _fmt_pct(r.get("model_probability_effective")), font=prob, fill=(100,230,167))
    d.text((840, 455), "MODEL P", font=body, fill=(170,184,220))
    d.text((48, 635), "Model signal only — no outcome is guaranteed.", font=body, fill=(255,212,121))

    bio = io.BytesIO()
    img.save(bio, format="PNG", optimize=True)
    bio.seek(0)
    return bio

def _send_rollover_signal(candidate, snapshot):
    chat_id = _resolve_telegram_chat_id()
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    chain_no = int(_rollover.get("chain_no") or 0) + 1
    scan_utc = (snapshot.get("last_scan_utc") or datetime.now(timezone.utc).isoformat()).replace("+00:00","Z")
    png = _render_rollover_png(candidate, chain_no, scan_utc[:19].replace("T"," "))
    settle_at = "HT" if str(candidate.get("period_lane") or "") == "1H" else "FT"
    caption = (
        f"DDD ROLLOVER SIGNAL #{chain_no}\n"
        f"{candidate.get('home','')} vs {candidate.get('away','')}\n"
        f"{candidate.get('market','') or candidate.get('market_family','')} — {candidate.get('selection','')}\n"
        f"Model {_fmt_pct(candidate.get('model_probability_effective'))} | Odds {_fmt_num(candidate.get('live_odds'),2)}\n"
        f"Settlement target: {settle_at}\n"
        f"Only one rollover signal remains active at a time."
    )
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption},
            files={"photo": (f"ddd_rollover_{chain_no}.png", png, "image/png")},
            timeout=30,
        )
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description") or "Telegram sendPhoto failed")

        active = dict(candidate)
        active["chain_no"] = chain_no
        active["sent_at_utc"] = datetime.now(timezone.utc).isoformat()
        _rollover["active"] = active
        _rollover["chain_no"] = chain_no
        _rollover["last_result"] = None
        _roll_save()
        with _lock:
            _state["rollover_active"] = active
            _state["rollover_chain_no"] = chain_no
            _state["rollover_last_result"] = None
            _state["telegram_last_sent_utc"] = datetime.now(timezone.utc).isoformat()
            _state["telegram_last_error"] = None
        print(f"ROLLOVER: sent signal #{chain_no}")
        return True
    except Exception as e:
        with _lock:
            _state["telegram_last_error"] = f"{type(e).__name__}: {e}"
        print("ROLLOVER TELEGRAM ERROR:", e)
        return False

def _get_fixture(fid):
    data = ddd.api_get("/fixtures", {"id": int(fid)}, API_KEY)
    resp = (data or {}).get("response", [])
    return resp[0] if resp else None

def _active_settlement():
    r = _rollover.get("active")
    if not r:
        return None

    fid = r.get("fixture_id")
    fam = str(r.get("market_family") or "")
    lane = str(r.get("period_lane") or "")
    if fid in (None, ""):
        return None

    f = _get_fixture(fid)
    if not f:
        return None

    short = str((f.get("fixture") or {}).get("status", {}).get("short", ""))
    gh = (f.get("goals") or {}).get("home")
    ga = (f.get("goals") or {}).get("away")
    ht = (f.get("score") or {}).get("halftime", {}) or {}
    hth, hta = ht.get("home"), ht.get("away")

    end_1h = short in {"HT","2H","FT","AET","PEN"}
    end_ft = short in {"FT","AET","PEN"}

    if lane == "1H" and not end_1h:
        return None
    if lane != "1H" and not end_ft:
        return None

    observed = None
    result = None
    line = _line(r)
    market = str(r.get("market") or "")
    three_way = ("3 way" in market.lower()) or ("3way" in market.lower())

    if fam == "goal_total_1h" and None not in (hth, hta):
        observed = float(hth) + float(hta)
        result = _settle_ou(r.get("selection"), line, observed, three_way)

    elif fam == "match_result_1h" and None not in (hth, hta):
        observed = "home" if int(hth) > int(hta) else "away" if int(hta) > int(hth) else "draw"
        result = "WIN" if observed == str(r.get("raw_value","")).lower() else "LOSS"

    elif fam == "corner_total_1h":
        hs = ddd.api_get("/fixtures/statistics", {"fixture": int(fid), "half": "true"}, API_KEY)
        observed = ddd._stat_total((hs or {}).get("response", []), "Corner Kicks")
        result = _settle_ou(r.get("selection"), line, observed, three_way)

    elif fam == "goal_total" and None not in (gh, ga):
        observed = float(gh) + float(ga)
        result = _settle_ou(r.get("selection"), line, observed, three_way)

    elif fam == "goal_total_2h" and None not in (gh, ga, hth, hta):
        observed = max(0.0, (float(gh)+float(ga))-(float(hth)+float(hta)))
        result = _settle_ou(r.get("selection"), line, observed, three_way)

    elif fam == "team_goal_home" and gh is not None:
        observed = float(gh)
        result = _settle_ou(r.get("selection"), line, observed, three_way)

    elif fam == "team_goal_away" and ga is not None:
        observed = float(ga)
        result = _settle_ou(r.get("selection"), line, observed, three_way)

    elif fam == "btts_ft" and None not in (gh, ga):
        observed = "yes" if int(gh)>0 and int(ga)>0 else "no"
        want = str(r.get("raw_value","")).strip().lower()
        result = "WIN" if observed == want else "LOSS"

    elif fam in {"match_result_ft","match_result_2h"} and None not in (gh,ga):
        if fam == "match_result_2h" and None not in (hth,hta):
            xh, xa = int(gh)-int(hth), int(ga)-int(hta)
        else:
            xh, xa = int(gh), int(ga)
        observed = "home" if xh > xa else "away" if xa > xh else "draw"
        result = "WIN" if observed == str(r.get("raw_value","")).lower() else "LOSS"

    elif fam == "double_chance_ft" and None not in (gh,ga):
        observed = "home" if int(gh)>int(ga) else "away" if int(ga)>int(gh) else "draw"
        want = str(r.get("raw_value","")).lower()
        ok = (
            (want=="home or draw" and observed in {"home","draw"})
            or (want=="away or draw" and observed in {"away","draw"})
            or (want=="home or away" and observed in {"home","away"})
        )
        result = "WIN" if ok else "LOSS"

    elif fam == "draw_no_bet_ft" and None not in (gh,ga):
        observed = "home" if int(gh)>int(ga) else "away" if int(ga)>int(gh) else "draw"
        want = str(r.get("raw_value","")).lower()
        result = "PUSH" if observed=="draw" else ("WIN" if observed==want else "LOSS")

    elif fam in {"corner_total_ft","corner_total_2h"}:
        st = ddd.api_get("/fixtures/statistics", {"fixture": int(fid)}, API_KEY)
        ft_c = ddd._stat_total((st or {}).get("response", []), "Corner Kicks")
        if fam == "corner_total_2h":
            hs = ddd.api_get("/fixtures/statistics", {"fixture": int(fid), "half": "true"}, API_KEY)
            ht_c = ddd._stat_total((hs or {}).get("response", []), "Corner Kicks")
            observed = None if ft_c is None or ht_c is None else max(0.0, ft_c - ht_c)
        else:
            observed = ft_c
        result = _settle_ou(r.get("selection"), line, observed, three_way)

    if result:
        return {"result": result, "observed": observed, "status": short}
    return None

def _finish_rollover(settled):
    active = _rollover.get("active") or {}
    n = int(active.get("chain_no") or _rollover.get("chain_no") or 0)
    result = settled.get("result")
    icon = "✅" if result == "WIN" else "↔️" if result == "PUSH" else "❌"
    txt = (
        f"{icon} DDD ROLLOVER #{n} — {result}\n"
        f"{active.get('home','')} vs {active.get('away','')}\n"
        f"{active.get('market','') or active.get('market_family','')} — {active.get('selection','')}\n"
        f"Observed: {settled.get('observed')}\n"
    )
    if result == "WIN":
        txt += "Chain unlocked. The next qualifying scan may issue the next rollover signal."
    elif result == "PUSH":
        txt += "Push/void: chain unlocked with stake unchanged."
    else:
        if ROLLOVER_STOP_ON_LOSS:
            txt += "Chain stopped. No automatic recovery/chasing signal will be issued."
        else:
            txt += "Loss recorded. A new chain may start on a later qualifying scan."
    _send_tg_text(txt)

    _rollover["last_result"] = result
    _rollover["active"] = None
    if result == "LOSS" and ROLLOVER_STOP_ON_LOSS:
        _rollover["stopped"] = True
    _roll_save()
    with _lock:
        _state["rollover_active"] = None
        _state["rollover_last_result"] = result
        _state["rollover_stopped"] = bool(_rollover.get("stopped"))

def _rollover_cycle(snapshot):
    if not ROLLOVER_MODE:
        return

    # First settle the currently active signal. There can never be two active
    # rollover signals at the same time.
    if _rollover.get("active"):
        try:
            settled = _active_settlement()
        except Exception as e:
            print("ROLLOVER SETTLEMENT WARNING:", e)
            settled = None
        if settled:
            _finish_rollover(settled)

    if _rollover.get("active") or _rollover.get("stopped"):
        return

    candidate = _select_rollover_candidate(snapshot.get("official") or [])
    if candidate:
        _send_rollover_signal(candidate, snapshot)

def run_scan():
    fixtures, odds, opp, diag, raw, rawerr = ddd.scan_live(API_KEY)
    official, alts, high80, coverage = ddd.qualifying_boards(opp)
    fast, one, two, ft, safest = ddd.period_aware_boards(opp)
    momentum = ddd.momentum_fast_board(opp)
    watch = ddd.watch_board(opp, ddd.WATCH_MIN_PROB)
    picks = ddd.pick_independent(opp)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    try:
        ddd.append_research_history(opp, picks, stamp)
    except Exception:
        pass

    closest = _closest_board(opp, 8)

    live_count = len(fixtures) if fixtures is not None else 0
    if live_count == 0:
        interval = IDLE_SCAN_SECONDS
    else:
        interval = SCAN_SECONDS

    now = datetime.now(timezone.utc)
    snapshot = {
        "healthy": True,
        "status": "scanning",
        "last_scan_utc": now.isoformat(),
        "scan_interval_seconds": interval,
        "live_count": live_count,
        "odds_rows": len(odds) if odds is not None else 0,
        "scored_markets": len(opp) if opp is not None else 0,
        "official": _records(picks, 8, CARD_COLS),
        "fast_period": _records(fast, 8, CARD_COLS),
        "momentum_fast": _records(momentum, 8, CARD_COLS),
        "watch": _records(watch, 8, CARD_COLS),
        "closest": closest,
        "fixtures": _records(fixtures, 40, FIXTURE_COLS),
        "error": None,
    }
    return snapshot, interval

def scanner_loop():
    if not API_KEY:
        with _lock:
            _state.update({
                "healthy": False,
                "status": "missing_api_key",
                "error": "API_FOOTBALL_KEY is not configured"
            })
        return

    while True:
        started = time.time()
        with _lock:
            _state["status"] = "scanning"
            _state["cycle"] += 1
            cycle = _state["cycle"]
        try:
            snapshot, interval = run_scan()
            with _lock:
                quota_fields = {
                    "request_count_today": _state["request_count_today"],
                    "request_day_utc": _state["request_day_utc"],
                    "quota_limit": DAILY_REQUEST_LIMIT,
                    "quota_remaining_estimate": max(0, DAILY_REQUEST_LIMIT - _state["request_count_today"]),
                    "cycle": cycle,
                }
                _state.update(snapshot)
                _state.update(quota_fields)
                _state["next_scan_seconds"] = interval
            _rollover_cycle(snapshot)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            interval = 900 if "quota guard" in msg.lower() else SCAN_SECONDS
            with _lock:
                _state.update({
                    "healthy": False,
                    "status": "error",
                    "error": msg,
                    "scan_interval_seconds": interval,
                    "next_scan_seconds": interval,
                })

        elapsed = time.time() - started
        sleep_for = max(5, interval - elapsed)
        end_at = time.time() + sleep_for
        while time.time() < end_at:
            with _lock:
                _state["next_scan_seconds"] = max(0, int(end_at - time.time()))
                if _state["last_scan_utc"]:
                    try:
                        last = datetime.fromisoformat(_state["last_scan_utc"])
                        _state["last_scan_age_seconds"] = int((datetime.now(timezone.utc)-last).total_seconds())
                    except Exception:
                        pass
            time.sleep(min(5, max(1, end_at-time.time())))

threading.Thread(target=scanner_loop, daemon=True, name="ddd-scanner").start()

@app.get("/health")
def health():
    with _lock:
        return jsonify({
            "healthy": _state["healthy"],
            "status": _state["status"],
            "last_scan_utc": _state["last_scan_utc"],
            "last_scan_age_seconds": _state["last_scan_age_seconds"],
            "cycle": _state["cycle"],
            "live_count": _state["live_count"],
            "request_count_today": _state["request_count_today"],
            "quota_remaining_estimate": _state["quota_remaining_estimate"],
            "telegram_configured": _state["telegram_configured"],
            "telegram_chat_resolved": _state["telegram_chat_resolved"],
            "telegram_last_sent_utc": _state["telegram_last_sent_utc"],
            "telegram_last_error": _state["telegram_last_error"],
            "rollover_mode": _state["rollover_mode"],
            "rollover_active": _state["rollover_active"],
            "rollover_chain_no": _state["rollover_chain_no"],
            "rollover_stopped": _state["rollover_stopped"],
            "rollover_last_result": _state["rollover_last_result"],
            "rollover_min_odds": ROLLOVER_STRICT_MIN_ODDS,
            "rollover_preferred_odds": [ROLLOVER_PREF_MIN_ODDS, ROLLOVER_PREF_MAX_ODDS],
            "rollover_exception_odds": [ROLLOVER_EXCEPTION_MIN_ODDS, ROLLOVER_EXCEPTION_MAX_ODDS],
            "error": _state["error"],
        })

@app.get("/api/snapshot")
def snapshot():
    with _lock:
        return jsonify(dict(_state))

@app.get("/")
def dashboard():
    html = r"""<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DDD Live Monitor</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;margin:0;background:#0b1020;color:#eef2ff}
main{max-width:980px;margin:auto;padding:16px}
h1{font-size:24px;margin:0 0 8px}
.small{opacity:.72;font-size:13px}.bar{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0 18px}
.badge{background:#1c2745;border:1px solid #34446f;border-radius:999px;padding:7px 10px;font-size:13px}
.card{background:#121a30;border:1px solid #263556;border-radius:14px;padding:13px;margin:10px 0}
.good{border-color:#1f9d68}.warn{border-color:#c28a28}.bad{border-color:#b84c5a}
.row{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}.p{font-size:22px;font-weight:700}
.market{font-weight:700}.warns{color:#ffd479;font-size:13px;margin-top:6px}
section{margin-top:22px}h2{font-size:18px}.fixture{font-size:14px}
.empty{opacity:.65}.status-good{color:#64e6a7}.status-bad{color:#ff8d9a}
</style>
</head>
<body><main>
<h1>DDD V7.2 Live Monitor</h1>
<div class="small">HARD80 actual-win • probability first • fast period • recent momentum</div>
<div id="top" class="bar"></div>
<section><h2>OFFICIAL ≥80%</h2><div id="official"></div></section>
<section><h2>FAST / MOMENTUM</h2><div id="fast"></div></section>
<section><h2>CLOSEST IF NO OFFICIAL</h2><div id="closest"></div></section>
<section><h2>LIVE GAMES BEING SCANNED</h2><div id="fixtures"></div></section>
</main>
<script>
function pct(x){return x==null?'—':(x*100).toFixed(1)+'%'}
function odd(x){return x==null?'—':Number(x).toFixed(2)}
function card(r,cls=''){
 const w=(r.warnings||[]).map(x=>'<div>⚠ '+x+'</div>').join('');
 return `<div class="card ${cls}"><div class="row"><div>
 <div><b>${r.home||''}</b> vs <b>${r.away||''}</b> ${r.minute!=null?'· '+r.minute+"'":''}</div>
 <div class="market">${r.market||r.market_family||''} — ${r.selection||''}</div>
 <div class="small">${r.period_lane||''} · odds ${odd(r.live_odds)} · edge ${pct(r.estimated_edge)} · EV ${pct(r.estimated_ev)}</div>
 </div><div class="p">${pct(r.model_probability_effective)}</div></div>
 ${w?'<div class="warns">'+w+'</div>':''}</div>`;
}
async function load(){
 try{
  const r=await fetch('/api/snapshot',{cache:'no-store'}); const d=await r.json();
  document.getElementById('top').innerHTML=
   `<span class="badge ${d.healthy?'status-good':'status-bad'}">${d.healthy?'HEALTHY':'CHECK'}</span>`+
   `<span class="badge">Live ${d.live_count}</span><span class="badge">Odds ${d.odds_rows}</span>`+
   `<span class="badge">Scored ${d.scored_markets}</span><span class="badge">Next ${d.next_scan_seconds??'—'}s</span>`+
   `<span class="badge">API est. ${d.quota_remaining_estimate}/${d.quota_limit}</span>`+
   `<span class="badge">${d.telegram_configured?(d.telegram_chat_resolved?"Telegram ✓":"Telegram waiting /start"):"Telegram off"}</span>`+
   `<span class="badge">${d.rollover_stopped?"ROLLOVER STOPPED":(d.rollover_active?"ROLLOVER ACTIVE #"+d.rollover_chain_no:"ROLLOVER READY")}</span>`;
  const official=d.official||[];
  document.getElementById('official').innerHTML=official.length?official.map(x=>card(x,'good')).join(''):'<div class="empty">No strict HARD80 signal.</div>';
  const f=[...(d.fast_period||[]),...(d.momentum_fast||[])];
  document.getElementById('fast').innerHTML=f.length?f.map(x=>card(x,'good')).join(''):'<div class="empty">No fast-period/momentum qualifier.</div>';
  const c=d.closest||[];
  document.getElementById('closest').innerHTML=c.length?c.map(x=>card(x,'warn')).join(''):'<div class="empty">No priced candidate available.</div>';
  const fx=d.fixtures||[];
  document.getElementById('fixtures').innerHTML=fx.length?fx.map(x=>`<div class="card fixture"><b>${x.home}</b> ${x.home_goals??0}-${x.away_goals??0} <b>${x.away}</b> · ${x.minute??'—'}' · odds rows ${x.live_odds_rows??0} · quality ${pct(x.data_quality)}</div>`).join(''):'<div class="empty">No live fixtures in current scan.</div>';
 }catch(e){}
}
load(); setInterval(load,15000);
</script></body></html>"""
    return Response(html, mimetype="text/html")
