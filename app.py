
import os
import time
import math
import threading
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from flask import Flask, jsonify, Response

os.environ.setdefault("DDD_DATA_ROOT", os.environ.get("DDD_DATA_ROOT", "/tmp/ddd"))

import ddd_engine_v72 as ddd

API_KEY = os.environ.get("API_FOOTBALL_KEY", "").strip()
BASE_SCAN_SECONDS = int(os.environ.get("DDD_BASE_SCAN_SECONDS", "300"))
HOT_SCAN_SECONDS = int(os.environ.get("DDD_HOT_SCAN_SECONDS", "120"))
IDLE_SCAN_SECONDS = int(os.environ.get("DDD_IDLE_SCAN_SECONDS", "900"))
DAILY_REQUEST_LIMIT = int(os.environ.get("DDD_DAILY_REQUEST_LIMIT", "6500"))

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
    "scan_interval_seconds": BASE_SCAN_SECONDS,
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
    "selection_role","period_lane","minute","home","away","score",
    "market_group","market_family","market","selection","handicap",
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
    hot = (
        (picks is not None and not picks.empty)
        or (momentum is not None and not momentum.empty)
        or (fast is not None and not fast.empty)
        or any((r.get("model_probability_effective") or 0) >= 0.75 for r in closest)
    )
    if live_count == 0:
        interval = IDLE_SCAN_SECONDS
    elif hot:
        interval = HOT_SCAN_SECONDS
    else:
        interval = BASE_SCAN_SECONDS

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
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            interval = 900 if "quota guard" in msg.lower() else BASE_SCAN_SECONDS
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
   `<span class="badge">API est. ${d.quota_remaining_estimate}/${d.quota_limit}</span>`;
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
