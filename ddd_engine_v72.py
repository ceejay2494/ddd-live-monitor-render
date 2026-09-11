#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DDD LIVE VALUE ENGINE V7.2 — FAST MOMENTUM PERIOD-AWARE

ONE RUN:
1) Mount Google Drive.
2) Reuse existing API-Football key without printing it.
3) Commit this engine to Drive.
4) Fetch all live football fixtures.
5) Pull live statistics + live odds.
6) Score a broad live market universe where the current state supports a defensible model:
   goals, 1H goals/result, team goals, BTTS, 1X2, double chance, draw-no-bet,
   corners (2-way/3-way/Asian and corner 1X2), total/team shots and shots-on-target.
7) Apply market-specific reliability shrinkage, price/edge/EV gates and data-quality gates.
8) Rank probability first, then value. Keep one primary pick per fixture but ALSO surface
   up to 3 qualifying alternate markets per fixture so strong corners are never hidden.
9) Save high-confidence 80%+ board, qualifying fixture board, alternative market board,
   market-coverage audit, every raw opportunity and persistent research history.
10) Download a ZIP containing the complete scan.

Research engine: probability estimates are model estimates, not guarantees.
NO BET is valid.
"""

from pathlib import Path
from datetime import datetime, timezone
import os, re, json, math, shutil, zipfile
import urllib.request, urllib.parse
import numpy as np
import pandas as pd

BASE_URL = "https://v3.football.api-sports.io"

MIN_ODDS = 1.20
MAX_ODDS = 3.50
PREFERRED_ODDS_LOW = 1.28
PREFERRED_ODDS_HIGH = 1.90
MIN_MODEL_PROB = 0.65
HIGH_CONF_PROB = 0.80
WATCH_MIN_PROB = 0.75
MIN_EDGE = 0.040
MIN_EV = 0.025
MAX_LIVE_MATCHES = int(os.environ.get("DDD_MAX_LIVE_MATCHES", "80"))
MAX_DEEP_FIXTURES = int(os.environ.get("DDD_MAX_DEEP_FIXTURES", "6"))
MAX_FALLBACK_FIXTURES = int(os.environ.get("DDD_MAX_FALLBACK_FIXTURES", "2"))
MAX_RECOMMENDATIONS = 6
MAX_ALTERNATIVES_PER_FIXTURE = 6
MAX_SETTLEMENT_FIXTURES_PER_RUN = 24
REQUEST_TIMEOUT = 25

# Probability takes precedence over price. Value breaks close probability ties.
PROBABILITY_FIRST = True

# Period-aware policy: shorter-horizon markets get selection precedence only
# after independently meeting the high-confidence gate. Probability itself is never boosted.
PERIOD_AWARE = True
FAST_PERIOD_MIN_PROB = 0.80
LATE_1H_MINUTE = 36
MAX_1H_EXECUTION_MINUTE = 44

# V7.2 recent-momentum layer. It changes the expected future event rate, not the
# probability threshold. State lives in memory across repeated live scans.
RECENT_MOMENTUM_MAX_MINUTES = 8
CORNER_MOMENTUM_MIN_MULT = 0.82
CORNER_MOMENTUM_MAX_MULT = 1.32
MOMENTUM_FAST_MIN_MULT = 1.15
MOMENTUM_FAST_MIN_PROB = 0.80
MOMENTUM_FAST_MAX_MINUTES_TO_SETTLE = 20
_RECENT_MOMENTUM_STATE = {}

# Newer families are deliberately shrunk toward 50% until the persistent
# research DB earns stronger calibration. Core totals retain the most weight.
MARKET_RELIABILITY = {
    "goal_total": 1.00,
    "goal_total_1h": 0.98,
    "goal_total_2h": 0.94,
    "corner_total_ft": 0.98,
    "corner_total_1h": 0.96,
    "corner_total_2h": 0.93,
    "team_goal_home": 0.92,
    "team_goal_away": 0.92,
    "btts_ft": 0.90,
    "match_result_ft": 0.86,
    "double_chance_ft": 0.89,
    "draw_no_bet_ft": 0.86,
    "match_result_1h": 0.88,
    "match_result_2h": 0.84,
    "corner_result_ft": 0.84,
    "shot_total_ft": 0.82,
    "home_shot_total_ft": 0.80,
    "away_shot_total_ft": 0.80,
    "sot_total_ft": 0.84,
    "home_sot_total_ft": 0.82,
    "away_sot_total_ft": 0.82,
    "goals_odd_even": 0.78,
}

# Minimum relevant-stat coverage by family.
MARKET_MIN_QUALITY = {
    "goal_total": 0.35, "goal_total_1h": 0.40, "goal_total_2h": 0.50,
    "corner_total_ft": 0.45, "corner_total_1h": 0.50, "corner_total_2h": 0.55,
    "team_goal_home": 0.55, "team_goal_away": 0.55, "btts_ft": 0.60,
    "match_result_ft": 0.65, "double_chance_ft": 0.65, "draw_no_bet_ft": 0.65,
    "match_result_1h": 0.65, "match_result_2h": 0.68, "corner_result_ft": 0.60,
    "shot_total_ft": 0.70, "home_shot_total_ft": 0.65, "away_shot_total_ft": 0.65,
    "sot_total_ft": 0.70, "home_sot_total_ft": 0.65, "away_sot_total_ft": 0.65,
    "goals_odd_even": 0.60,
}

DRIVE_ROOT = Path(os.environ.get("DDD_DATA_ROOT", str(Path.cwd())))
ENGINE_DIR = DRIVE_ROOT / "DDD_LIVE_VALUE_ENGINE"
ARCHIVE_DIR = ENGINE_DIR / "LIVE_ARCHIVE"
RESULT_DIR = ENGINE_DIR / "RESULTS"

RESEARCH_DIR = ENGINE_DIR / "RESEARCH"
RESEARCH_DB = RESEARCH_DIR / "DDD_LIVE_RESEARCH_DB.csv"
RESEARCH_SETTLED_DB = RESEARCH_DIR / "DDD_LIVE_RESEARCH_SETTLED.csv"

def api_get(path, params=None, key=None):
    url = f"{BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={"x-apisports-key": key, "User-Agent": "DDD-Live-Value-Engine/7.2"},
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
        data = json.loads(r.read().decode("utf-8"))
    errors = data.get("errors") if isinstance(data, dict) else None
    if isinstance(errors, dict) and errors:
        raise RuntimeError(f"API error at {path}: {errors}")
    return data


def _validate_key(candidate):
    candidate = (candidate or "").strip()
    if len(candidate) < 16:
        return False
    try:
        data = api_get("/status", key=candidate)
        return isinstance(data, dict) and "response" in data and not data.get("errors")
    except Exception:
        return False

def _candidate_keys_from_text(txt):
    if not txt:
        return []

    patterns = [
        r"x-apisports-key[\"']?\s*[:=]\s*[\"']([^\"']{16,})[\"']",
        r"x-rapidapi-key[\"']?\s*[:=]\s*[\"']([^\"']{16,})[\"']",
        r"API[_-]?FOOTBALL[_-]?KEY\s*[:=]\s*[\"']([^\"']{16,})[\"']",
        r"API[_-]?SPORTS[_-]?KEY\s*[:=]\s*[\"']([^\"']{16,})[\"']",
        r"APISPORTS[_-]?KEY\s*[:=]\s*[\"']([^\"']{16,})[\"']",
        r"RAPIDAPI[_-]?KEY\s*[:=]\s*[\"']([^\"']{16,})[\"']",
        r"API[_-]?KEY\s*[:=]\s*[\"']([^\"']{16,})[\"']",
    ]

    out = []
    for pat in patterns:
        for m in re.finditer(pat, txt, flags=re.I):
            v = m.group(1).strip()
            if v not in out:
                out.append(v)
    return out

def discover_api_key():
    # 1. Environment variables
    for n in [
        "API_FOOTBALL_KEY", "APISPORTS_KEY", "API_SPORTS_KEY",
        "RAPIDAPI_KEY", "API_KEY"
    ]:
        v = os.environ.get(n)
        if _validate_key(v):
            return v.strip(), f"environment:{n}"

    # 2. Colab Secrets / userdata
    try:
        from google.colab import userdata
        for n in [
            "API_FOOTBALL_KEY", "APISPORTS_KEY", "API_SPORTS_KEY",
            "RAPIDAPI_KEY", "API_KEY"
        ]:
            try:
                v = userdata.get(n)
            except Exception:
                v = None
            if _validate_key(v):
                return v.strip(), f"Colab secret:{n}"
    except Exception:
        pass

    # 3. Previously saved private credential
    private_key_file = ENGINE_DIR / ".api_football_key"
    try:
        if private_key_file.exists():
            v = private_key_file.read_text(errors="ignore").strip()
            if _validate_key(v):
                return v, "DDD Live Value private credential"
    except Exception:
        pass

    seen = set()

    def test_text(txt, source):
        for cand in _candidate_keys_from_text(txt):
            if cand in seen:
                continue
            seen.add(cand)
            if _validate_key(cand):
                return cand, source
        return None

    allowed_suffixes = {
        ".py", ".txt", ".env", ".json", ".ini", ".cfg", ".ipynb",
        ".yaml", ".yml", ".md", ".log"
    }

    # 4. Existing DDD source/config/notebook files on Drive
    drive_candidates = []
    try:
        for root in DRIVE_ROOT.iterdir():
            if "DDD" not in root.name.upper():
                continue
            if root.is_file():
                drive_candidates.append(root)
            else:
                try:
                    for p in root.rglob("*"):
                        if p.is_file() and p.suffix.lower() in allowed_suffixes:
                            drive_candidates.append(p)
                except Exception:
                    pass

        drive_candidates.sort(
            key=lambda p: p.stat().st_mtime_ns if p.exists() else 0,
            reverse=True
        )
    except Exception:
        drive_candidates = []

    for p in drive_candidates[:1500]:
        try:
            if p.stat().st_size > 8_000_000:
                continue
            txt = p.read_text(errors="ignore")
            hit = test_text(txt, f"existing DDD file:{p.name}")
            if hit:
                return hit
        except Exception:
            continue

    # 5. Existing DDD ZIP packages
    zip_candidates = []
    try:
        zip_candidates = [
            p for p in DRIVE_ROOT.rglob("*.zip")
            if "DDD" in p.name.upper()
        ]
        zip_candidates.sort(key=lambda p: p.stat().st_mtime_ns, reverse=True)
    except Exception:
        pass

    for zp in zip_candidates[:25]:
        try:
            with zipfile.ZipFile(zp) as z:
                members = [
                    n for n in z.namelist()
                    if Path(n).suffix.lower() in allowed_suffixes
                ]
                for member in members[:300]:
                    try:
                        raw = z.read(member)
                        if len(raw) > 8_000_000:
                            continue
                        txt = raw.decode("utf-8", errors="ignore")
                    except Exception:
                        continue

                    hit = test_text(
                        txt,
                        f"existing DDD package:{zp.name}/{Path(member).name}"
                    )
                    if hit:
                        return hit
        except Exception:
            continue

    # 6. Current Colab /content files
    try:
        content_files = []
        for p in Path("/content").rglob("*"):
            if p.is_file() and p.suffix.lower() in allowed_suffixes:
                content_files.append(p)

        content_files.sort(key=lambda p: p.stat().st_mtime_ns, reverse=True)

        for p in content_files[:500]:
            try:
                if p.stat().st_size > 8_000_000:
                    continue
                txt = p.read_text(errors="ignore")
                hit = test_text(txt, f"current Colab file:{p.name}")
                if hit:
                    return hit
            except Exception:
                continue
    except Exception:
        pass

    # 7. Hidden one-time fallback, then persist for future one-touch runs
    import getpass
    print("\nAPI credential was not recoverable automatically from existing DDD files.")
    v = getpass.getpass(
        "Paste API-Football/API-Sports key once (input hidden): "
    ).strip()

    if not _validate_key(v):
        raise RuntimeError(
            "The supplied API key did not validate against API-Football /status."
        )

    try:
        ENGINE_DIR.mkdir(parents=True, exist_ok=True)
        private_key_file.write_text(v)
        try:
            os.chmod(private_key_file, 0o600)
        except Exception:
            pass
    except Exception:
        pass

    return v, "one-time hidden input"

def mount_drive():
    from google.colab import drive

    try:
        drive.mount("/content/drive", force_remount=False)
    except Exception:
        print("Drive initial mount failed; retrying clean remount...")
        drive.mount("/content/drive", force_remount=True)

    if not DRIVE_ROOT.exists():
        raise FileNotFoundError("/content/drive/MyDrive unavailable after mount.")

    for p in [ENGINE_DIR, ARCHIVE_DIR, RESULT_DIR]:
        p.mkdir(parents=True, exist_ok=True)

def commit_self_to_drive():
    """
    Commit a copy of the engine to Drive.
    Works both when executed as a normal .py file and when pasted/run as a Colab cell,
    where __file__ is not defined.
    """
    dest = ENGINE_DIR / "DDD_LIVE_VALUE_ENGINE_V7_BROAD_MARKETS.py"

    try:
        src = Path(__file__).resolve()
    except NameError:
        src = None

    if src is not None and src.exists():
        if src != dest:
            shutil.copy2(src, dest)
        return dest

    # Colab-cell fallback: if the uploaded engine exists in /content, copy it.
    candidates = [
        Path("/content/DDD_LIVE_VALUE_ENGINE_V7_BROAD_MARKETS.py"),
        Path("/mnt/data/DDD_LIVE_VALUE_ENGINE_V7_BROAD_MARKETS.py"),
    ]
    for candidate in candidates:
        try:
            if candidate.exists():
                shutil.copy2(candidate, dest)
                return dest
        except Exception:
            pass

    # Final fallback: do not block the live scan just because self-commit is unavailable.
    # The engine can still run and save all scan outputs to Drive.
    print("ENGINE COMMIT WARNING: source script path unavailable in this execution mode.")
    return dest

def fnum(x):
    if x is None:
        return np.nan
    if isinstance(x, (int,float)):
        return float(x)
    s = str(x).strip().replace("%","")
    try:
        return float(s)
    except Exception:
        return np.nan

def parse_stats(stats_resp):
    teams = stats_resp.get("response", []) or []
    out = []
    for team in teams[:2]:
        d = {}
        for item in team.get("statistics", []) or []:
            d[str(item.get("type","")).strip()] = fnum(item.get("value"))
        out.append(d)
    while len(out) < 2:
        out.append({})
    h, a = out[:2]

    def get(d, name):
        v = d.get(name, np.nan)
        return float(v) if not pd.isna(v) else np.nan

    return {
        "home_sot": get(h,"Shots on Goal"),
        "away_sot": get(a,"Shots on Goal"),
        "home_shots": get(h,"Total Shots"),
        "away_shots": get(a,"Total Shots"),
        "home_corners": get(h,"Corner Kicks"),
        "away_corners": get(a,"Corner Kicks"),
        "home_poss": get(h,"Ball Possession"),
        "away_poss": get(a,"Ball Possession"),
        "home_red": get(h,"Red Cards"),
        "away_red": get(a,"Red Cards"),
        "home_yellow": get(h,"Yellow Cards"),
        "away_yellow": get(a,"Yellow Cards"),
    }


def flatten_live_odds(odds_resp):
    """
    Parse API-Football /odds/live.

    IMPORTANT:
    Live odds DO NOT use the pre-match bookmakers[] -> bets[] structure.
    Current live shape is:
      response[] -> fixture{}, status{}, update, odds[] ->
        {id, name, values[{value, odd, handicap, main, suspended}]}

    The parser also tolerates the pre-match/bookmaker shape as a defensive fallback.
    """
    rows = []

    for block in odds_resp.get("response", []) or []:
        fixture = block.get("fixture")
        fid = fixture.get("id") if isinstance(fixture, dict) else fixture

        live_status = block.get("status") or {}
        blocked = live_status.get("blocked")
        stopped = live_status.get("stopped")
        finished = live_status.get("finished")
        update = block.get("update")

        # Correct LIVE schema.
        if isinstance(block.get("odds"), list):
            for bet in block.get("odds", []) or []:
                bet_id = bet.get("id")
                bet_name = str(bet.get("name", "")).strip()

                for val in bet.get("values", []) or []:
                    odd = fnum(val.get("odd"))
                    if pd.isna(odd):
                        continue

                    suspended = val.get("suspended")
                    main = val.get("main")
                    handicap = val.get("handicap")
                    value = str(val.get("value", "")).strip()

                    # Suspended selections are never executable.
                    if suspended is True:
                        continue

                    rows.append({
                        "fixture_id": fid,
                        "bookmaker": "API-Football Live",
                        "bet_id": bet_id,
                        "bet_name": bet_name,
                        "value": value,
                        "handicap": "" if handicap is None else str(handicap).strip(),
                        "odd": float(odd),
                        "main": main,
                        "suspended": suspended,
                        "fixture_blocked": blocked,
                        "fixture_stopped": stopped,
                        "fixture_finished": finished,
                        "update": update,
                    })
            continue

        # Defensive fallback for pre-match/bookmaker-shaped payloads.
        for bm in block.get("bookmakers", []) or []:
            for bet in bm.get("bets", []) or []:
                for val in bet.get("values", []) or []:
                    odd = fnum(val.get("odd"))
                    if pd.isna(odd):
                        continue
                    if val.get("suspended") is True:
                        continue
                    rows.append({
                        "fixture_id": fid,
                        "bookmaker": bm.get("name"),
                        "bet_id": bet.get("id"),
                        "bet_name": str(bet.get("name", "")).strip(),
                        "value": str(val.get("value", "")).strip(),
                        "handicap": "" if val.get("handicap") is None else str(val.get("handicap")).strip(),
                        "odd": float(odd),
                        "main": val.get("main"),
                        "suspended": val.get("suspended"),
                        "fixture_blocked": blocked,
                        "fixture_stopped": stopped,
                        "fixture_finished": finished,
                        "update": update,
                    })

    return pd.DataFrame(rows)

def live_odds_diagnostic(resp):
    """Return a compact diagnostic dict without exposing credentials."""
    d = {
        "get": resp.get("get") if isinstance(resp, dict) else None,
        "results": resp.get("results") if isinstance(resp, dict) else None,
        "errors": resp.get("errors") if isinstance(resp, dict) else "non-dict-response",
        "response_items": len(resp.get("response", []) or []) if isinstance(resp, dict) else 0,
    }

    sample = None
    if isinstance(resp, dict) and resp.get("response"):
        b = resp["response"][0]
        sample = {
            "top_level_keys": sorted(list(b.keys())),
            "has_live_odds_array": isinstance(b.get("odds"), list),
            "live_odds_count": len(b.get("odds", []) or []) if isinstance(b.get("odds"), list) else 0,
            "has_bookmakers_array": isinstance(b.get("bookmakers"), list),
        }
    d["sample_shape"] = sample
    return d

def clamp(x, lo=0.01, hi=0.99):
    return max(lo, min(hi, float(x)))


def poisson_pmf(k, lam):
    if k < 0:
        return 0.0
    return math.exp(-lam) * (lam ** int(k)) / math.factorial(int(k))


def poisson_cdf(k, lam):
    if k < 0:
        return 0.0
    return sum(poisson_pmf(i, lam) for i in range(int(k)+1))


def poisson_tail_at_least(k, lam):
    if k <= 0:
        return 1.0
    return 1.0 - poisson_cdf(k-1, lam)


def live_goal_lambda(elapsed, stats, score_total):
    elapsed = max(1.0, min(89.0, float(elapsed)))
    rem = 90.0 - elapsed
    base_remaining = 2.55 * rem / 90.0
    sot = np.nansum([stats.get("home_sot"), stats.get("away_sot")])
    shots = np.nansum([stats.get("home_shots"), stats.get("away_shots")])
    exp_sot = max(0.4, 8.4 * elapsed / 90.0)
    exp_shots = max(0.8, 24.0 * elapsed / 90.0)
    raw_tempo = 0.60*(sot/exp_sot) + 0.40*(shots/exp_shots)
    tempo = 0.55 + 0.45*np.clip(raw_tempo, 0.35, 2.0)
    reds = np.nansum([stats.get("home_red"), stats.get("away_red")])
    if reds >= 1:
        tempo *= 1.08
    if score_total >= 1:
        tempo *= 1.04
    return max(0.05, base_remaining*tempo)


def first_half_goal_lambda(elapsed, stats, score_total):
    if elapsed <= 0 or elapsed >= 45:
        return np.nan
    rem = 45.0 - float(elapsed)
    sot = np.nansum([stats.get("home_sot"), stats.get("away_sot")])
    shots = np.nansum([stats.get("home_shots"), stats.get("away_shots")])
    exp_sot = max(0.25, 4.0*elapsed/45.0)
    exp_shots = max(0.5, 11.5*elapsed/45.0)
    raw = 0.60*(sot/exp_sot) + 0.40*(shots/exp_shots)
    tempo = 0.58 + 0.42*np.clip(raw, 0.35, 2.0)
    if score_total >= 1:
        tempo *= 1.03
    return max(0.03, 1.10*rem/45.0*tempo)


def second_half_goal_lambda(elapsed, stats, second_half_score_total):
    """Expected goals remaining in the second half using second-half-only pressure."""
    if elapsed <= 45 or elapsed >= 90:
        return np.nan
    ep = float(elapsed) - 45.0
    rem = 45.0 - ep
    sot = np.nansum([stats.get("second_half_home_sot"), stats.get("second_half_away_sot")])
    shots = np.nansum([stats.get("second_half_home_shots"), stats.get("second_half_away_shots")])
    exp_sot = max(0.25, 4.4*ep/45.0)
    exp_shots = max(0.5, 12.5*ep/45.0)
    raw = 0.60*(sot/exp_sot) + 0.40*(shots/exp_shots)
    tempo = 0.58 + 0.42*np.clip(raw, 0.35, 2.0)
    if second_half_score_total >= 1:
        tempo *= 1.03
    return max(0.03, 1.45*rem/45.0*tempo)


def second_half_goal_side_lambdas(elapsed, stats, shg, sag):
    total = second_half_goal_lambda(elapsed, stats, shg+sag)
    if pd.isna(total):
        return np.nan, np.nan
    hsot = _safe_stat(stats,"second_half_home_sot")
    asot = _safe_stat(stats,"second_half_away_sot")
    hsh = _safe_stat(stats,"second_half_home_shots")
    ash = _safe_stat(stats,"second_half_away_shots")
    hp, ap = _safe_stat(stats,"home_poss",50.0), _safe_stat(stats,"away_poss",50.0)
    h_pressure = 1.08 + 1.70*hsot + 0.48*hsh + 0.010*hp
    a_pressure = 1.00 + 1.70*asot + 0.48*ash + 0.010*ap
    if shg < sag:
        h_pressure *= 1.07
    elif sag < shg:
        a_pressure *= 1.07
    share = clamp(h_pressure/(h_pressure+a_pressure), 0.18, 0.82)
    return max(0.01,total*share), max(0.01,total*(1.0-share))


def _safe_stat(stats, key, default=0.0):
    v = fnum(stats.get(key))
    return default if pd.isna(v) else float(v)


def goal_side_lambdas(elapsed, stats, hg, ag, half=False):
    total = first_half_goal_lambda(elapsed, stats, hg+ag) if half else live_goal_lambda(elapsed, stats, hg+ag)
    if pd.isna(total):
        return np.nan, np.nan

    hsot, asot = _safe_stat(stats,"home_sot"), _safe_stat(stats,"away_sot")
    hsh, ash = _safe_stat(stats,"home_shots"), _safe_stat(stats,"away_shots")
    hp, ap = _safe_stat(stats,"home_poss",50.0), _safe_stat(stats,"away_poss",50.0)

    # Smoothed live attacking-pressure split. The small home prior prevents a
    # zero-stat opening from becoming an artificial 50/50 neutral venue.
    h_pressure = 1.08 + 1.70*hsot + 0.48*hsh + 0.012*hp
    a_pressure = 1.00 + 1.70*asot + 0.48*ash + 0.012*ap
    if hg < ag:
        h_pressure *= 1.07
    elif ag < hg:
        a_pressure *= 1.07
    share = clamp(h_pressure/(h_pressure+a_pressure), 0.18, 0.82)
    return max(0.01,total*share), max(0.01,total*(1.0-share))


def corner_lambda(elapsed, stats):
    elapsed = max(1.0, min(89.0, float(elapsed)))
    rem = 90.0 - elapsed
    current = np.nansum([stats.get("home_corners"), stats.get("away_corners")])
    observed90 = (current/elapsed)*90.0 if elapsed >= 8 else 9.5
    projected = 0.60*9.5 + 0.40*np.clip(observed90, 4.0, 17.0)
    return max(0.10, projected*rem/90.0)


def corner_side_lambdas(elapsed, stats):
    total = corner_lambda(elapsed, stats)
    hc, ac = _safe_stat(stats,"home_corners"), _safe_stat(stats,"away_corners")
    hs, ass = _safe_stat(stats,"home_shots"), _safe_stat(stats,"away_shots")
    hp, ap = _safe_stat(stats,"home_poss",50.0), _safe_stat(stats,"away_poss",50.0)
    h = 1.0 + 1.15*hc + 0.18*hs + 0.008*hp
    a = 1.0 + 1.15*ac + 0.18*ass + 0.008*ap
    share = clamp(h/(h+a), 0.15, 0.85)
    return total*share, total*(1.0-share)


def ht_draw_probability(elapsed, hg, ag, stats):
    if elapsed <= 0 or elapsed >= 45:
        return np.nan
    lh, la = goal_side_lambdas(elapsed, stats, hg, ag, half=True)
    probs = result_probabilities(hg, ag, lh, la)
    return probs.get("draw", np.nan)


def data_quality(stats):
    keys = ["home_sot","away_sot","home_shots","away_shots",
            "home_corners","away_corners","home_poss","away_poss"]
    present = sum(not pd.isna(stats.get(k,np.nan)) for k in keys)
    q = present/len(keys)
    core = ["home_sot","away_sot","home_shots","away_shots"]
    if sum(not pd.isna(stats.get(k,np.nan)) for k in core) < 2:
        q *= 0.65
    return float(q)


def market_data_quality(kind, stats):
    base = data_quality(stats)
    def present(keys):
        return sum(not pd.isna(stats.get(k,np.nan)) for k in keys)/max(1,len(keys))
    if kind == "corner_total_2h":
        return float(0.70*present(["second_half_corners"]) + 0.30*base)
    if kind in {"goal_total_2h","match_result_2h"}:
        return float(0.70*present(["second_half_home_sot","second_half_away_sot","second_half_home_shots","second_half_away_shots"]) + 0.30*base)
    if "corner" in str(kind):
        return float(0.65*present(["home_corners","away_corners"]) + 0.35*base)
    if "shot_total" in str(kind) and "sot" not in str(kind):
        if kind.startswith("home_"):
            return float(0.70*present(["home_shots"]) + 0.30*base)
        if kind.startswith("away_"):
            return float(0.70*present(["away_shots"]) + 0.30*base)
        return float(0.70*present(["home_shots","away_shots"]) + 0.30*base)
    if "sot_total" in str(kind):
        if kind.startswith("home_"):
            return float(0.70*present(["home_sot"]) + 0.30*base)
        if kind.startswith("away_"):
            return float(0.70*present(["away_sot"]) + 0.30*base)
        return float(0.70*present(["home_sot","away_sot"]) + 0.30*base)
    return base


def _parse_number(x):
    if x is None:
        return None
    s = str(x).strip()
    m = re.search(r'-?[0-9]+(?:\.[0-9]+)?', s)
    return float(m.group(0)) if m else None


def _is_quarter_line(line):
    if line is None:
        return False
    return abs(float(line)*2 - round(float(line)*2)) > 1e-8


def parse_total_value(value, handicap=None):
    v = str(value or "").strip()
    m = re.search(r'(Over|Under)\s*([0-9]+(?:\.[0-9]+)?)', v, re.I)
    if m:
        return m.group(1).lower(), float(m.group(2))
    if v.lower() in {"over", "under"}:
        line = _parse_number(handicap)
        if line is not None:
            return v.lower(), float(line)
    return None


def classify_market(bet_name, value, handicap=None):
    """V7 broad taxonomy. Returns (family, detail-dict) or (None, None)."""
    b = " ".join(str(bet_name or "").lower().split())
    v = " ".join(str(value or "").lower().split())
    tv = parse_total_value(value, handicap)

    # ---- corners ----
    if b == "corners 1x2" and v in {"home","draw","away"}:
        return "corner_result_ft", {"side":v}

    if "corner" in b and tv:
        side, line = tv
        if _is_quarter_line(line):
            # Quarter-line Asian settlement is intentionally research-only for now.
            return None, None
        three_way = ("3 way" in b) or ("3way" in b) or b == "match corners"
        asian = "asian" in b
        push_mode = (not three_way) and asian and abs(line-round(line)) < 1e-8
        detail = {"side":side,"line":line,"three_way":three_way,"push_mode":push_mode}
        if "1st half" in b or "first half" in b:
            return "corner_total_1h", detail
        if "2nd half" in b or "second half" in b:
            return "corner_total_2h", detail
        if b in {"total corners","asian corners","match corners"} or "total corners" in b:
            return "corner_total_ft", detail

    # ---- shot volume ----
    if tv and b == "total shots":
        return "shot_total_ft", {"side":tv[0],"line":tv[1],"three_way":False,"push_mode":False}
    if tv and b == "home total shots":
        return "home_shot_total_ft", {"side":tv[0],"line":tv[1],"three_way":False,"push_mode":False}
    if tv and b == "away total shots":
        return "away_shot_total_ft", {"side":tv[0],"line":tv[1],"three_way":False,"push_mode":False}
    if tv and b == "total shots on goal":
        return "sot_total_ft", {"side":tv[0],"line":tv[1],"three_way":False,"push_mode":False}
    if tv and b == "home total shots on goal":
        return "home_sot_total_ft", {"side":tv[0],"line":tv[1],"three_way":False,"push_mode":False}
    if tv and b == "away total shots on goal":
        return "away_sot_total_ft", {"side":tv[0],"line":tv[1],"three_way":False,"push_mode":False}

    # ---- team goals ----
    if tv and b == "home team goals":
        return "team_goal_home", {"side":tv[0],"line":tv[1],"three_way":False,"push_mode":False}
    if tv and b == "away team goals":
        return "team_goal_away", {"side":tv[0],"line":tv[1],"three_way":False,"push_mode":False}

    # ---- BTTS ----
    if b == "both teams to score" and v in {"yes","no"}:
        return "btts_ft", {"side":v}

    # ---- result markets ----
    if b == "fulltime result" and v in {"home","draw","away"}:
        return "match_result_ft", {"side":v}
    if b == "double chance" and v in {"home or draw","away or draw","home or away"}:
        return "double_chance_ft", {"side":v}
    if b == "draw no bet" and v in {"home","away"}:
        return "draw_no_bet_ft", {"side":v}
    if b in {"1x2 (1st half)","1st half result","first half result","1st half winner","first half winner"} and v in {"home","draw","away"}:
        return "match_result_1h", {"side":v}
    if b in {"1x2 (2nd half)","2nd half result","second half result","2nd half winner","second half winner"} and v in {"home","draw","away"}:
        return "match_result_2h", {"side":v}
    if b == "goals odd/even" and v in {"odd","even"}:
        return "goals_odd_even", {"side":v}

    # ---- goal totals ----
    if tv:
        side, line = tv
        if _is_quarter_line(line):
            return None, None
        if b in {"over/under (1st half)","over/under line (1st half)","goals over/under (1st half)","total goals (1st half)"} or (("1st half" in b or "first half" in b) and ("over/under" in b or "total goals" in b)):
            push_mode = abs(line-round(line)) < 1e-8
            return "goal_total_1h", {"side":side,"line":line,"three_way":False,"push_mode":push_mode}
        if b in {"over/under (2nd half)","over/under line (2nd half)","goals over/under (2nd half)","total goals (2nd half)"} or (("2nd half" in b or "second half" in b) and ("over/under" in b or "total goals" in b)):
            push_mode = abs(line-round(line)) < 1e-8
            return "goal_total_2h", {"side":side,"line":line,"three_way":False,"push_mode":push_mode}
        if b in {"match goals","over/under line","goals over/under","goal over/under","over/under goals","total goals","goals total","asian total goals","over/under","over under","over-under"}:
            push_mode = (b == "over/under line") and abs(line-round(line)) < 1e-8
            return "goal_total", {"side":side,"line":line,"three_way":False,"push_mode":push_mode}

    # Keep unsupported/player/event-specific markets out of the official model.
    return None, None


def _sum_known(stats, a, b):
    x, y = fnum(stats.get(a)), fnum(stats.get(b))
    if pd.isna(x) or pd.isna(y):
        return np.nan
    return float(x) + float(y)


def _update_recent_momentum(fid, elapsed, stats):
    """Compute a bounded recent 1-8 minute pressure signal from consecutive scans.

    Corner acceleration dominates. Shots/SOT are supporting evidence. A first scan
    stays neutral. The state is intentionally tiny and in-memory for speed.
    """
    cur = {
        "minute": float(elapsed),
        "corners": _sum_known(stats, "home_corners", "away_corners"),
        "shots": _sum_known(stats, "home_shots", "away_shots"),
        "sot": _sum_known(stats, "home_sot", "away_sot"),
    }
    prev = _RECENT_MOMENTUM_STATE.get(int(fid))
    dm = np.nan; dc = ds = dst = np.nan
    mult = 1.0; signal = 1.0

    if prev is not None:
        dm = float(cur["minute"] - prev.get("minute", cur["minute"]))
        if 1.0 <= dm <= float(RECENT_MOMENTUM_MAX_MINUTES):
            def delta(k):
                a, b = cur.get(k), prev.get(k)
                if pd.isna(a) or pd.isna(b):
                    return np.nan
                return max(0.0, float(a) - float(b))
            dc, ds, dst = delta("corners"), delta("shots"), delta("sot")

            # Ratios versus normal per-minute football event rates. Extreme bursts
            # are clipped so one noisy API update cannot dominate the model.
            cr = 1.0 if pd.isna(dc) else np.clip((dc/dm)/(9.5/90.0), 0.0, 3.0)
            sr = 1.0 if pd.isna(ds) else np.clip((ds/dm)/(24.0/90.0), 0.0, 2.5)
            tr = 1.0 if pd.isna(dst) else np.clip((dst/dm)/(8.4/90.0), 0.0, 3.0)
            signal = 0.65*cr + 0.22*sr + 0.13*tr
            mult = float(np.clip(1.0 + 0.22*(signal-1.0),
                                 CORNER_MOMENTUM_MIN_MULT,
                                 CORNER_MOMENTUM_MAX_MULT))

    stats["recent_window_minutes"] = dm
    stats["recent_corners_delta"] = dc
    stats["recent_shots_delta"] = ds
    stats["recent_sot_delta"] = dst
    stats["corner_momentum_signal"] = signal
    stats["corner_momentum_multiplier"] = mult
    _RECENT_MOMENTUM_STATE[int(fid)] = cur
    return mult


def _bootstrap_recent_momentum_from_research(max_rows=12000):
    """Seed the tiny momentum cache from the latest stored snapshot after a restart."""
    if _RECENT_MOMENTUM_STATE or not RESEARCH_DB.exists():
        return
    try:
        cols=["fixture_id","minute","home_corners","away_corners","home_shots","away_shots","home_sot","away_sot","snapshot_ts"]
        d=pd.read_csv(RESEARCH_DB, usecols=lambda c:c in cols).tail(max_rows)
        if d.empty or "fixture_id" not in d.columns: return
        if "snapshot_ts" in d.columns:
            d=d.sort_values("snapshot_ts")
        d=d.drop_duplicates("fixture_id",keep="last")
        for _,r in d.iterrows():
            fid=fnum(r.get("fixture_id")); minute=fnum(r.get("minute"))
            if pd.isna(fid) or pd.isna(minute): continue
            def pair(a,b):
                x,y=fnum(r.get(a)),fnum(r.get(b))
                return np.nan if pd.isna(x) or pd.isna(y) else float(x)+float(y)
            _RECENT_MOMENTUM_STATE[int(fid)]={
                "minute":float(minute),
                "corners":pair("home_corners","away_corners"),
                "shots":pair("home_shots","away_shots"),
                "sot":pair("home_sot","away_sot"),
            }
    except Exception:
        pass


def _period_total_probs(side, line, current, lam, three_way=False, push_mode=False):
    if line is None or current is None or pd.isna(current) or pd.isna(lam):
        return np.nan, 0.0
    current = int(current)
    line = float(line)
    exact_future = int(round(line-current))
    p_exact = poisson_pmf(exact_future, lam) if exact_future >= 0 and abs(line-round(line)) < 1e-8 else 0.0

    if side == "over":
        needed = math.floor(line+1e-9)+1-current
        p_win = clamp(poisson_tail_at_least(needed, lam), 0.0, 1.0)
    else:
        max_final = math.ceil(line)-1
        max_future = max_final-current
        p_win = clamp(poisson_cdf(max_future, lam), 0.0, 1.0) if max_future >= 0 else 0.0

    p_push = p_exact if push_mode and not three_way else 0.0
    p_win = min(p_win, 1.0-p_push)
    return float(p_win), float(p_push)


def _corner_period_probability(side, line, current, elapsed_period, period_length, baseline_period, three_way=False, push_mode=False, momentum_mult=1.0):
    if pd.isna(current) or elapsed_period < 0 or elapsed_period >= period_length:
        return np.nan, 0.0
    current = float(current)
    rem = period_length - float(elapsed_period)
    elapsed_rate_base = max(float(elapsed_period), 6.0)
    expected_so_far = baseline_period * elapsed_rate_base / period_length
    ratio = clamp((current + 1.25) / (expected_so_far + 1.25), 0.55, 1.75)
    lam = max(0.02, baseline_period * rem / period_length * (0.64 + 0.36*ratio))
    # Recent acceleration/cooling affects the future corner intensity, bounded to
    # prevent a single burst from overwhelming the long-run rate.
    lam *= clamp(fnum(momentum_mult) if not pd.isna(fnum(momentum_mult)) else 1.0,
                 CORNER_MOMENTUM_MIN_MULT, CORNER_MOMENTUM_MAX_MULT)
    return _period_total_probs(side, line, current, lam, three_way, push_mode)


def _poisson_count_distribution(lam, max_k=10):
    vals = [poisson_pmf(k,lam) for k in range(max_k)]
    vals.append(max(0.0,1.0-sum(vals)))
    return vals


def result_probabilities(hg, ag, lam_h, lam_a, max_future=10):
    if pd.isna(lam_h) or pd.isna(lam_a):
        return {"home":np.nan,"draw":np.nan,"away":np.nan}
    ph = _poisson_count_distribution(float(lam_h),max_future)
    pa = _poisson_count_distribution(float(lam_a),max_future)
    out = {"home":0.0,"draw":0.0,"away":0.0}
    for i,pi in enumerate(ph):
        for j,pj in enumerate(pa):
            fh,fa = int(hg)+i, int(ag)+j
            if fh>fa: out["home"] += pi*pj
            elif fh<fa: out["away"] += pi*pj
            else: out["draw"] += pi*pj
    z=sum(out.values()) or 1.0
    return {k:v/z for k,v in out.items()}


def corner_result_probabilities(elapsed, stats, max_future=14):
    hc,ac = int(_safe_stat(stats,"home_corners")), int(_safe_stat(stats,"away_corners"))
    lh,la = corner_side_lambdas(elapsed,stats)
    return result_probabilities(hc,ac,lh,la,max_future=max_future)


def _count_remaining_lambda(current, elapsed, baseline90):
    elapsed=max(1.0,min(89.0,float(elapsed)))
    rem=90.0-elapsed
    current=float(current)
    expected=max(0.25,baseline90*elapsed/90.0)
    ratio=clamp((current+1.0)/(expected+1.0),0.45,2.10)
    projected_remaining=baseline90*rem/90.0*(0.60+0.40*ratio)
    return max(0.02,projected_remaining)


def estimate_market_probability(kind, detail, elapsed, hg, ag, stats):
    """Return raw (win_probability, push_probability)."""
    side = detail.get("side") if isinstance(detail,dict) else None
    line = detail.get("line") if isinstance(detail,dict) else None
    three_way = bool(detail.get("three_way",False)) if isinstance(detail,dict) else False
    push_mode = bool(detail.get("push_mode",False)) if isinstance(detail,dict) else False

    if kind == "goal_total":
        lam = live_goal_lambda(elapsed, stats, hg+ag)
        return _period_total_probs(side,line,hg+ag,lam,three_way,push_mode)

    if kind == "goal_total_1h":
        if elapsed >= 45: return np.nan,0.0
        lam = first_half_goal_lambda(elapsed,stats,hg+ag)
        return _period_total_probs(side,line,hg+ag,lam,three_way,push_mode)

    if kind == "goal_total_2h":
        if elapsed <= 45: return np.nan,0.0
        shg = fnum(stats.get("second_half_home_goals"))
        sag = fnum(stats.get("second_half_away_goals"))
        if pd.isna(shg) or pd.isna(sag): return np.nan,0.0
        cur = int(shg+sag)
        lam = second_half_goal_lambda(elapsed,stats,cur)
        return _period_total_probs(side,line,cur,lam,three_way,push_mode)

    if kind in {"team_goal_home","team_goal_away"}:
        lh,la=goal_side_lambdas(elapsed,stats,hg,ag)
        cur,lam=(hg,lh) if kind.endswith("home") else (ag,la)
        return _period_total_probs(side,line,cur,lam,three_way,push_mode)

    if kind == "btts_ft":
        lh,la=goal_side_lambdas(elapsed,stats,hg,ag)
        p_h = 1.0 if hg>0 else 1.0-math.exp(-lh)
        p_a = 1.0 if ag>0 else 1.0-math.exp(-la)
        yes=clamp(p_h*p_a,0.0,1.0)
        return (yes if side=="yes" else 1.0-yes),0.0

    if kind in {"match_result_ft","double_chance_ft","draw_no_bet_ft"}:
        lh,la=goal_side_lambdas(elapsed,stats,hg,ag)
        pr=result_probabilities(hg,ag,lh,la)
        if kind=="match_result_ft": return pr.get(side,np.nan),0.0
        if kind=="double_chance_ft":
            if side=="home or draw": return pr["home"]+pr["draw"],0.0
            if side=="away or draw": return pr["away"]+pr["draw"],0.0
            if side=="home or away": return pr["home"]+pr["away"],0.0
        if kind=="draw_no_bet_ft":
            return pr.get(side,np.nan),pr.get("draw",0.0)

    if kind == "match_result_1h":
        if elapsed >=45: return np.nan,0.0
        lh,la=goal_side_lambdas(elapsed,stats,hg,ag,half=True)
        pr=result_probabilities(hg,ag,lh,la)
        return pr.get(side,np.nan),0.0

    if kind == "match_result_2h":
        if elapsed <=45: return np.nan,0.0
        shg = fnum(stats.get("second_half_home_goals"))
        sag = fnum(stats.get("second_half_away_goals"))
        if pd.isna(shg) or pd.isna(sag): return np.nan,0.0
        lh,la=second_half_goal_side_lambdas(elapsed,stats,int(shg),int(sag))
        pr=result_probabilities(int(shg),int(sag),lh,la)
        return pr.get(side,np.nan),0.0

    if kind == "corner_total_ft":
        current=np.nansum([stats.get("home_corners"),stats.get("away_corners")])
        return _corner_period_probability(side,line,current,elapsed,90.0,9.5,three_way,push_mode,stats.get("corner_momentum_multiplier",1.0))

    if kind == "corner_total_1h":
        if elapsed>=45: return np.nan,0.0
        current=np.nansum([stats.get("home_corners"),stats.get("away_corners")])
        return _corner_period_probability(side,line,current,elapsed,45.0,4.65,three_way,push_mode,stats.get("corner_momentum_multiplier",1.0))

    if kind == "corner_total_2h":
        if elapsed<=45: return np.nan,0.0
        second_half_current=stats.get("second_half_corners",np.nan)
        if pd.isna(second_half_current): return np.nan,0.0
        return _corner_period_probability(side,line,second_half_current,elapsed-45.0,45.0,4.85,three_way,push_mode,stats.get("corner_momentum_multiplier",1.0))

    if kind == "corner_result_ft":
        pr=corner_result_probabilities(elapsed,stats)
        return pr.get(side,np.nan),0.0

    if kind in {"shot_total_ft","home_shot_total_ft","away_shot_total_ft"}:
        if kind=="shot_total_ft":
            cur=np.nansum([stats.get("home_shots"),stats.get("away_shots")]); base=24.0
        elif kind=="home_shot_total_ft": cur=_safe_stat(stats,"home_shots"); base=12.5
        else: cur=_safe_stat(stats,"away_shots"); base=11.5
        lam=_count_remaining_lambda(cur,elapsed,base)
        return _period_total_probs(side,line,cur,lam,False,False)

    if kind in {"sot_total_ft","home_sot_total_ft","away_sot_total_ft"}:
        if kind=="sot_total_ft":
            cur=np.nansum([stats.get("home_sot"),stats.get("away_sot")]); base=8.4
        elif kind=="home_sot_total_ft": cur=_safe_stat(stats,"home_sot"); base=4.5
        else: cur=_safe_stat(stats,"away_sot"); base=3.9
        lam=_count_remaining_lambda(cur,elapsed,base)
        return _period_total_probs(side,line,cur,lam,False,False)

    if kind == "goals_odd_even":
        lam=live_goal_lambda(elapsed,stats,hg+ag)
        cur=hg+ag
        # parity of Poisson future count: P(even)=(1+e^-2lambda)/2.
        p_future_even=(1.0+math.exp(-2.0*lam))/2.0
        p_final_even=p_future_even if cur%2==0 else 1.0-p_future_even
        return (p_final_even if side=="even" else 1.0-p_final_even),0.0

    return np.nan,0.0


def reliability_adjusted_probabilities(kind, p_win, p_push, q):
    """Shrink conditional win probability toward 50%; preserve push mass."""
    if pd.isna(p_win):
        return np.nan,np.nan,np.nan
    p_push=0.0 if pd.isna(p_push) else clamp(p_push,0.0,0.98)
    nonpush=max(1e-9,1.0-p_push)
    cond=clamp(float(p_win)/nonpush,0.0,1.0)
    rel=MARKET_RELIABILITY.get(kind,0.75)
    shrink=(0.55+0.45*clamp(q,0.0,1.0))*rel
    eff_cond=0.50+(cond-0.50)*shrink
    eff_cond=clamp(eff_cond,0.01,0.99)
    eff_win=eff_cond*nonpush
    eff_loss=(1.0-eff_cond)*nonpush
    return float(eff_cond),float(eff_win),float(eff_loss)

def scan_live(key):
    _bootstrap_recent_momentum_from_research()
    fixtures_resp = api_get("/fixtures", {"live":"all"}, key)
    fixtures = fixtures_resp.get("response", []) or []
    fixtures = [
        f for f in fixtures
        if ((f.get("fixture") or {}).get("status") or {}).get("short")
        in {"1H","HT","2H"}
    ][:MAX_LIVE_MATCHES]

    print("LIVE MATCHES FOUND:", len(fixtures))

    fixture_ids = {
        (f.get("fixture") or {}).get("id")
        for f in fixtures
        if (f.get("fixture") or {}).get("id") is not None
    }

    # -------------------------------------------------------------
    # LIVE ODDS: ONE GLOBAL CALL FIRST.
    # This is both more efficient and the best way to discover actual
    # in-play odds coverage. Then retain only the fixtures we are scanning.
    # -------------------------------------------------------------
    global_odds_resp = None
    global_odds_error = None
    try:
        global_odds_resp = api_get("/odds/live", key=key)
        print("LIVE ODDS GLOBAL DIAGNOSTIC:", live_odds_diagnostic(global_odds_resp))
        global_odds_df = flatten_live_odds(global_odds_resp)
    except Exception as e:
        global_odds_error = repr(e)
        print("LIVE ODDS GLOBAL ERROR:", global_odds_error)
        global_odds_df = pd.DataFrame()

    if not global_odds_df.empty and "fixture_id" in global_odds_df.columns:
        global_odds_df = global_odds_df[
            global_odds_df["fixture_id"].isin(fixture_ids)
        ].copy()

    print("LIVE ODDS ROWS MATCHING LIVE FIXTURES:", len(global_odds_df))

    # FAST V7.2: do expensive statistics calls primarily for fixtures that actually
    # have live odds. Keep only a small fallback lane for provider coverage gaps.
    covered_ids = set(global_odds_df["fixture_id"].dropna().tolist()) if not global_odds_df.empty else set()
    if covered_ids:
        covered_fixtures = [f for f in fixtures if (f.get("fixture") or {}).get("id") in covered_ids][:MAX_DEEP_FIXTURES]
        uncovered = [f for f in fixtures if (f.get("fixture") or {}).get("id") not in covered_ids]
        def _fallback_priority(f):
            st=(f.get("fixture") or {}).get("status") or {}
            e=fnum(st.get("elapsed")); e=0 if pd.isna(e) else float(e)
            # Prefer live windows where period-aware opportunities are actionable.
            return 0 if 15 <= e <= 44 else 1 if 46 <= e <= 85 else 2
        uncovered = sorted(uncovered, key=_fallback_priority)[:MAX_FALLBACK_FIXTURES]
        fixtures = covered_fixtures + uncovered
        print(f"FAST DEEP-SCAN FIXTURES: {len(fixtures)} ({len(covered_fixtures)} odds-covered + {len(uncovered)} fallback)")

    fixture_rows = []
    opportunity_rows = []
    odds_frames = []
    odds_diag_rows = []

    for i, f in enumerate(fixtures, 1):
        fixture = f.get("fixture") or {}
        fid = fixture.get("id")
        status = fixture.get("status") or {}
        elapsed = fnum(status.get("elapsed"))
        if pd.isna(elapsed):
            continue
        elapsed = int(elapsed)

        home = ((f.get("teams") or {}).get("home") or {}).get("name")
        away = ((f.get("teams") or {}).get("away") or {}).get("name")
        goals = f.get("goals") or {}
        hg = int(goals.get("home") or 0)
        ag = int(goals.get("away") or 0)
        league = f.get("league") or {}

        print(f"[{i}/{len(fixtures)}] {elapsed:02d}' {home} {hg}-{ag} {away}")

        # Live stats
        try:
            stats_resp = api_get("/fixtures/statistics", {"fixture":fid}, key)
            stats = parse_stats(stats_resp)
            stats_error = None
            for k in [
                "second_half_corners","second_half_home_sot","second_half_away_sot",
                "second_half_home_shots","second_half_away_shots",
                "second_half_home_goals","second_half_away_goals"
            ]:
                stats[k] = np.nan

            ht_score = (f.get("score") or {}).get("halftime") or {}
            hth = fnum(ht_score.get("home")); hta = fnum(ht_score.get("away"))
            if status.get("short") == "2H" and not pd.isna(hth) and not pd.isna(hta):
                stats["second_half_home_goals"] = max(0.0, float(hg)-float(hth))
                stats["second_half_away_goals"] = max(0.0, float(ag)-float(hta))

            # API-Football supports half=true statistics. In 2H this gives
            # halftime cumulative corners; subtract from current match corners
            # to obtain the genuine second-half-only count.
            if status.get("short") == "2H":
                try:
                    half_resp = api_get(
                        "/fixtures/statistics",
                        {"fixture":fid, "half":"true"},
                        key
                    )
                    half_stats = parse_stats(half_resp)
                    ft_h = fnum(stats.get("home_corners"))
                    ft_a = fnum(stats.get("away_corners"))
                    ht_h = fnum(half_stats.get("home_corners"))
                    ht_a = fnum(half_stats.get("away_corners"))
                    if not any(pd.isna(x) for x in (ft_h,ft_a,ht_h,ht_a)):
                        stats["second_half_corners"] = max(0.0, (ft_h+ft_a)-(ht_h+ht_a))
                    for cur_key, half_key, out_key in [
                        ("home_sot","home_sot","second_half_home_sot"),
                        ("away_sot","away_sot","second_half_away_sot"),
                        ("home_shots","home_shots","second_half_home_shots"),
                        ("away_shots","away_shots","second_half_away_shots"),
                    ]:
                        cv=fnum(stats.get(cur_key)); hv=fnum(half_stats.get(half_key))
                        if not pd.isna(cv) and not pd.isna(hv):
                            stats[out_key]=max(0.0,float(cv)-float(hv))
                except Exception:
                    pass
        except Exception as e:
            stats_error = repr(e)
            stats = {
                k: np.nan for k in [
                    "home_sot","away_sot","home_shots","away_shots",
                    "home_corners","away_corners","home_poss","away_poss",
                    "home_red","away_red","home_yellow","away_yellow","second_half_corners",
                    "second_half_home_sot","second_half_away_sot","second_half_home_shots","second_half_away_shots",
                    "second_half_home_goals","second_half_away_goals"
                ]
            }

        # Prefer global live-odds snapshot.
        if not global_odds_df.empty:
            odds = global_odds_df[
                global_odds_df["fixture_id"] == fid
            ].copy()
        else:
            odds = pd.DataFrame()

        source = "GLOBAL"

        # If the global response does not contain this fixture, do one
        # fixture-specific fallback call. Crucially, record exactly what
        # the provider returned rather than silently swallowing it.
        if odds.empty:
            source = "FIXTURE_FALLBACK"
            try:
                one_resp = api_get("/odds/live", {"fixture":fid}, key)
                diag = live_odds_diagnostic(one_resp)
                one_df = flatten_live_odds(one_resp)

                odds_diag_rows.append({
                    "fixture_id": fid,
                    "home": home,
                    "away": away,
                    "minute": elapsed,
                    "source": source,
                    "results": diag.get("results"),
                    "response_items": diag.get("response_items"),
                    "errors": json.dumps(diag.get("errors"), ensure_ascii=False),
                    "sample_shape": json.dumps(diag.get("sample_shape"), ensure_ascii=False),
                })

                if not one_df.empty:
                    odds = one_df.copy()
            except Exception as e:
                odds_diag_rows.append({
                    "fixture_id": fid,
                    "home": home,
                    "away": away,
                    "minute": elapsed,
                    "source": source,
                    "results": None,
                    "response_items": 0,
                    "errors": repr(e),
                    "sample_shape": None,
                })

        _update_recent_momentum(fid, elapsed, stats)
        q = data_quality(stats)

        fixture_rows.append({
            "fixture_id":fid,
            "minute":elapsed,
            "status":status.get("short"),
            "home":home,
            "away":away,
            "home_goals":hg,
            "away_goals":ag,
            "league":league.get("name"),
            "country":league.get("country"),
            "data_quality":q,
            "live_odds_rows":len(odds),
            "stats_error":stats_error,
            **stats
        })

        if odds.empty:
            continue

        odds = odds.assign(
            home=home,
            away=away,
            minute=elapsed,
            score=f"{hg}-{ag}",
            league=league.get("name"),
            country=league.get("country"),
        )
        odds_frames.append(odds)

        # ---------------------------------------------------------
        # Evaluate every supported market.
        # For LIVE odds, value may be "Over"/"Under" and the line is
        # carried in handicap. This was the second V1/V2 incompatibility.
        # ---------------------------------------------------------
        for _, quote in odds.iterrows():
            med = fnum(quote.get("odd"))
            if pd.isna(med):
                continue
            med = float(med)

            if med < MIN_ODDS or med > MAX_ODDS:
                continue

            # Never act on a provider-blocked/finished/suspended market.
            if quote.get("fixture_blocked") is True:
                continue
            if quote.get("fixture_finished") is True:
                continue
            if quote.get("suspended") is True:
                continue

            bet_name = str(quote.get("bet_name",""))
            value = str(quote.get("value",""))
            handicap = quote.get("handicap")

            kind, detail = classify_market(
                bet_name, value, handicap
            )
            if not kind:
                continue

            p_win_raw, p_push_raw = estimate_market_probability(
                kind, detail, elapsed, hg, ag, stats
            )
            if pd.isna(p_win_raw):
                continue

            mq = market_data_quality(kind, stats)
            effective_p, effective_win_p, effective_loss_p = reliability_adjusted_probabilities(
                kind, p_win_raw, p_push_raw, mq
            )
            if pd.isna(effective_p):
                continue

            # V7.2 integrity fix: HARD80 uses ACTUAL win probability. For markets
            # with a push, the break-even win requirement is (1-push)/odds.
            official_p = float(effective_win_p)
            implied = (1.0 - float(p_push_raw)) / med
            edge = official_p - implied
            # Push-aware expected value: win returns decimal odds, push returns stake.
            ev = official_p * med + float(p_push_raw) - 1.0

            if PREFERRED_ODDS_LOW <= med <= PREFERRED_ODDS_HIGH:
                odds_fit = 1.0
            elif med < PREFERRED_ODDS_LOW:
                odds_fit = 0.86
            else:
                odds_fit = 0.92

            # Probability dominates. Value and price are tie-breakers, not the main objective.
            rank_score = (
                0.70*official_p
                + 0.12*max(0.0, min(edge, 0.40))
                + 0.10*max(0.0, min(ev, 0.80))
                + 0.05*mq
                + 0.03*odds_fit
            )

            family_q_min = MARKET_MIN_QUALITY.get(kind, 0.70)
            minutes_to_settle = max(0, (45-elapsed) if kind.endswith("_1h") else (90-elapsed))
            eligible = (
                med >= MIN_ODDS
                and official_p >= MIN_MODEL_PROB
                and edge >= MIN_EDGE
                and ev >= MIN_EV
                and mq >= family_q_min
            )
            # Separate sitter/research lane: recent corner acceleration can surface a
            # short-horizon 80%+ opportunity even if the strict 4% edge gate misses it.
            # It remains explicitly distinct from the HARD80 official board.
            momentum_fast_candidate = bool(
                kind in {"corner_total_1h","corner_total_2h"}
                and official_p >= MOMENTUM_FAST_MIN_PROB
                and med >= MIN_ODDS
                and fnum(stats.get("corner_momentum_multiplier")) >= MOMENTUM_FAST_MIN_MULT
                and minutes_to_settle <= MOMENTUM_FAST_MAX_MINUTES_TO_SETTLE
                and mq >= family_q_min
                and ev >= 0.0
            )

            # Human-readable live selection including handicap/line.
            selection_display = value
            if str(handicap or "").strip():
                selection_display = f"{value} {handicap}".strip()

            opportunity_rows.append({
                "fixture_id":fid,
                "minute":elapsed,
                "status":status.get("short"),
                "home":home,
                "away":away,
                "score":f"{hg}-{ag}",
                "league":league.get("name"),
                "country":league.get("country"),
                "bet_id":quote.get("bet_id"),
                "market":bet_name,
                "selection":selection_display,
                "raw_value":value,
                "handicap":handicap,
                "market_family":kind,
                "period_lane":("1H" if kind.endswith("_1h") else "2H" if kind.endswith("_2h") else "FT"),
                "minutes_to_settle":minutes_to_settle,
                "taxonomy_version":"FAST_MOMENTUM_V7_2",
                "live_odds":med,
                "model_probability_raw":p_win_raw,
                "push_probability_raw":p_push_raw,
                "model_probability_effective":official_p,
                "conditional_probability_effective":effective_p,
                "effective_win_probability":official_p,
                "effective_loss_probability":effective_loss_p,
                "implied_probability":implied,
                "estimated_edge":edge,
                "estimated_ev":ev,
                "data_quality":q,
                "market_data_quality":mq,
                "market_reliability":MARKET_RELIABILITY.get(kind,0.75),
                "rank_score":rank_score,
                "high_confidence_80":bool(eligible and official_p >= HIGH_CONF_PROB),
                "momentum_fast_candidate":momentum_fast_candidate,
                "eligible":eligible,
                "main":quote.get("main"),
                "update":quote.get("update"),
                "home_sot":stats.get("home_sot"),
                "away_sot":stats.get("away_sot"),
                "home_shots":stats.get("home_shots"),
                "away_shots":stats.get("away_shots"),
                "home_corners":stats.get("home_corners"),
                "away_corners":stats.get("away_corners"),
                "recent_window_minutes":stats.get("recent_window_minutes"),
                "recent_corners_delta":stats.get("recent_corners_delta"),
                "recent_shots_delta":stats.get("recent_shots_delta"),
                "recent_sot_delta":stats.get("recent_sot_delta"),
                "corner_momentum_signal":stats.get("corner_momentum_signal"),
                "corner_momentum_multiplier":stats.get("corner_momentum_multiplier"),
                "home_poss":stats.get("home_poss"),
                "away_poss":stats.get("away_poss"),
            })

    fixtures_df = pd.DataFrame(fixture_rows)
    odds_df = pd.concat(odds_frames, ignore_index=True) if odds_frames else pd.DataFrame()
    opp_df = pd.DataFrame(opportunity_rows)
    odds_diag_df = pd.DataFrame(odds_diag_rows)

    if not opp_df.empty:
        opp_df = opp_df.sort_values(
            ["eligible","model_probability_effective","rank_score","estimated_ev","live_odds"],
            ascending=[False,False,False,False,False]
        ).reset_index(drop=True)

    return fixtures_df, odds_df, opp_df, odds_diag_df, global_odds_resp, global_odds_error

def _dedupe_equivalent_quotes(df):
    """Keep the best price/value quote for the same underlying fixture/family/side/line."""
    if df is None or df.empty:
        return df
    x=df.copy()
    keys=[c for c in ["fixture_id","market_family","raw_value","handicap"] if c in x.columns]
    if keys:
        x=x.sort_values(
            ["model_probability_effective","estimated_ev","live_odds"],
            ascending=[False,False,False]
        ).drop_duplicates(keys,keep="first")
    return x


def _market_group(family):
    f=str(family or "")
    if f.startswith("corner_"):
        return "CORNERS"
    if "shot_total" in f or "sot_total" in f:
        return "SHOTS"
    if f in {"match_result_ft","double_chance_ft","draw_no_bet_ft","match_result_1h","match_result_2h"}:
        return "RESULTS"
    return "GOALS"


def _period_lane(row):
    fam=str(row.get("market_family", ""))
    if fam.endswith("_1h"):
        return "1H"
    if fam.endswith("_2h"):
        return "2H"
    return "FT"


def period_aware_boards(opp_df):
    """Return FAST_PERIOD, 1H, 2H, FT and SAFEST boards without changing model probabilities."""
    if opp_df is None or opp_df.empty:
        e=opp_df.iloc[0:0] if opp_df is not None else pd.DataFrame()
        return e,e,e,e,e
    e=_dedupe_equivalent_quotes(opp_df[opp_df["eligible"]].copy())
    if e.empty:
        return e,e,e,e,e
    e["period_lane"]=e.apply(_period_lane,axis=1)
    e=e.sort_values(["model_probability_effective","estimated_ev","live_odds"],ascending=False).reset_index(drop=True)
    one=e[e["period_lane"]=="1H"].copy()
    two=e[e["period_lane"]=="2H"].copy()
    ft=e[e["period_lane"]=="FT"].copy()
    fast=e[(e["period_lane"].isin(["1H","2H"])) & (e["model_probability_effective"]>=FAST_PERIOD_MIN_PROB)].copy()
    if not fast.empty:
        # First-half opportunities settle sooner than FT; in 2H, period-specific markets
        # are still isolated as a distinct thesis, but probability remains untouched.
        fast["period_priority"] = fast["period_lane"].map({"1H":2,"2H":1}).fillna(0)
        fast=fast.sort_values(["period_priority","model_probability_effective","estimated_ev","live_odds"],ascending=[False,False,False,False]).reset_index(drop=True)
        fast["fast_period_rank"]=range(1,len(fast)+1)
    safest=e.drop_duplicates("fixture_id",keep="first").copy().reset_index(drop=True)
    safest["safest_rank"]=range(1,len(safest)+1)
    return fast,one,two,ft,safest


def qualifying_boards(opp_df):
    """Official board is HARD-GATED at HIGH_CONF_PROB (80% by default).

    Period precedence is applied only AFTER a market has cleared the official
    probability threshold. Lower-probability eligible markets remain research/
    watch candidates and can never appear as official picks.
    """
    if opp_df is None or opp_df.empty:
        e=opp_df.iloc[0:0] if opp_df is not None else pd.DataFrame()
        return e,e,e,e

    eligible=_dedupe_equivalent_quotes(opp_df[opp_df["eligible"]].copy())
    if eligible.empty:
        return eligible,eligible,eligible,eligible

    eligible["market_group"]=eligible["market_family"].map(_market_group)
    eligible["period_lane"]=eligible.apply(_period_lane,axis=1)
    eligible=eligible.sort_values(
        ["model_probability_effective","rank_score","estimated_ev","live_odds"],
        ascending=[False,False,False,False]
    ).reset_index(drop=True)

    # HARD official gate. No 65-79.9% fallback can enter fixture_board.
    official_pool=eligible[
        pd.to_numeric(eligible["model_probability_effective"],errors="coerce") >= HIGH_CONF_PROB
    ].copy()

    prim=[]
    for fid,g in official_pool.groupby("fixture_id",sort=False):
        g=g.sort_values(["model_probability_effective","estimated_ev","live_odds"],ascending=False)
        period=g[g["period_lane"].isin(["1H","2H"])].copy()
        if not period.empty:
            period["_period_priority"]=period["period_lane"].map({"1H":2,"2H":1}).fillna(0)
            period=period.sort_values(
                ["_period_priority","model_probability_effective","estimated_ev","live_odds"],
                ascending=[False,False,False,False]
            )
            r=period.iloc[0].copy(); r["selection_role"]="FAST_PERIOD"; r["period_precedence"]=1
        else:
            r=g.iloc[0].copy(); r["selection_role"]="SAFEST_80PLUS"; r["period_precedence"]=0
        prim.append(r)

    fixture_board=pd.DataFrame(prim)
    if not fixture_board.empty:
        fixture_board=fixture_board.sort_values(
            ["period_precedence","model_probability_effective","estimated_ev","live_odds"],
            ascending=[False,False,False,False]
        ).reset_index(drop=True)
        fixture_board["probability_rank"]=range(1,len(fixture_board)+1)

    # Alternatives remain research-oriented, but official/high80 status is explicit.
    alt_rows=[]
    for fid,g in eligible.groupby("fixture_id",sort=False):
        g=g.sort_values(["model_probability_effective","estimated_ev","live_odds"],ascending=False)
        chosen=[]; seen=set()
        def add_row(row,role):
            key=(row.get("market_family"),str(row.get("raw_value")),str(row.get("handicap")))
            if key in seen or len(chosen)>=MAX_ALTERNATIVES_PER_FIXTURE:
                return
            r=row.copy(); r["alternative_role"]=role
            r["official_80plus"] = bool(float(r.get("model_probability_effective",0) or 0) >= HIGH_CONF_PROB)
            chosen.append(r); seen.add(key)

        fastg=g[(g["period_lane"].isin(["1H","2H"])) & (g["model_probability_effective"]>=HIGH_CONF_PROB)]
        if not fastg.empty:
            fastg=fastg.assign(_pp=fastg["period_lane"].map({"1H":2,"2H":1}).fillna(0)).sort_values(
                ["_pp","model_probability_effective","estimated_ev"],ascending=False
            )
            add_row(fastg.iloc[0],"FAST_PERIOD_80PLUS")
        add_row(g.iloc[0],"SAFEST_PROBABILITY")
        for lane in ["1H","2H","FT"]:
            gg=g[g["period_lane"]==lane]
            if not gg.empty: add_row(gg.iloc[0],f"BEST_{lane}")
        for grp in ["CORNERS","RESULTS","SHOTS","GOALS"]:
            gg=g[g["market_group"]==grp]
            if not gg.empty: add_row(gg.iloc[0],f"BEST_{grp}")
        gev=g.sort_values(["estimated_ev","model_probability_effective","live_odds"],ascending=False)
        if not gev.empty: add_row(gev.iloc[0],"BEST_VALUE")
        for _,r in g.iterrows():
            add_row(r,"NEXT_PROBABILITY")
            if len(chosen)>=MAX_ALTERNATIVES_PER_FIXTURE: break
        alt_rows.extend(chosen)

    alternatives=pd.DataFrame(alt_rows).reset_index(drop=True)
    if not alternatives.empty:
        alternatives["fixture_alt_rank"]=alternatives.groupby("fixture_id").cumcount()+1

    high80=official_pool.copy().reset_index(drop=True)
    if not high80.empty:
        high80["high_conf_rank"]=range(1,len(high80)+1)

    coverage=(opp_df.groupby(["market_family","market"],dropna=False)
              .agg(scored_quotes=("fixture_id","size"),eligible_quotes=("eligible","sum"),max_probability=("model_probability_effective","max"),max_odds=("live_odds","max"))
              .reset_index().sort_values(["eligible_quotes","scored_quotes"],ascending=[False,False]))
    return fixture_board,alternatives,high80,coverage


def momentum_fast_board(opp_df):
    """Short-horizon 1H/2H corner acceleration board; separate from official HARD80."""
    if opp_df is None or opp_df.empty or "momentum_fast_candidate" not in opp_df.columns:
        return opp_df.iloc[0:0] if opp_df is not None else pd.DataFrame()
    e=opp_df[opp_df["momentum_fast_candidate"].fillna(False)].copy()
    if e.empty: return e
    e=_dedupe_equivalent_quotes(e)
    e=e.sort_values(["model_probability_effective","corner_momentum_multiplier","estimated_ev","live_odds"],
                    ascending=[False,False,False,False]).reset_index(drop=True)
    e["momentum_fast_rank"]=range(1,len(e)+1)
    e["selection_role"]="MOMENTUM_FAST_80PLUS_RESEARCH"
    return e


def watch_board(opp_df, watch_min_prob=None):
    """Best 75-79.9% candidate per fixture. Never used as an official pick."""
    if watch_min_prob is None:
        watch_min_prob = WATCH_MIN_PROB
    if opp_df is None or opp_df.empty:
        return opp_df.iloc[0:0] if opp_df is not None else pd.DataFrame()
    e=_dedupe_equivalent_quotes(opp_df[opp_df["eligible"]].copy())
    if e.empty:
        return e
    p=pd.to_numeric(e["model_probability_effective"],errors="coerce")
    e=e[(p>=watch_min_prob) & (p<HIGH_CONF_PROB)].copy()
    if e.empty:
        return e
    e["period_lane"]=e.apply(_period_lane,axis=1)
    e["market_group"]=e["market_family"].map(_market_group)
    e=e.sort_values(["model_probability_effective","estimated_ev","live_odds"],ascending=False)
    e=e.drop_duplicates("fixture_id",keep="first").reset_index(drop=True)
    e["watch_rank"]=range(1,len(e)+1)
    e["selection_role"]="WATCH_75_79"
    return e

def pick_independent(opp_df):
    fixture_board,_,_,_=qualifying_boards(opp_df)
    if fixture_board is None or fixture_board.empty:
        return fixture_board
    out=fixture_board.head(MAX_RECOMMENDATIONS).copy().reset_index(drop=True)
    out["live_value_rank"]=range(1,len(out)+1)
    return out

def append_research_history(opp_df, picks, stamp):
    """Append this scan to a cumulative Drive research database."""
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    if opp_df is None or opp_df.empty:
        return

    cur = opp_df.copy()
    cur["snapshot_ts"] = stamp
    cur["engine_version"] = "V7_2_FAST_MOMENTUM_RENDER"
    cur["selected_pick"] = False

    if picks is not None and not picks.empty:
        keys = [c for c in [
            "fixture_id","market_family","market","selection","handicap","live_odds"
        ] if c in cur.columns and c in picks.columns]
        if keys:
            pk = picks[keys].drop_duplicates().copy()
            pk["_picked"] = True
            cur = cur.merge(pk, on=keys, how="left")
            cur["selected_pick"] = cur["_picked"].fillna(False)
            cur = cur.drop(columns=["_picked"])

    if RESEARCH_DB.exists():
        try:
            old = pd.read_csv(RESEARCH_DB)
            cur = pd.concat([old, cur], ignore_index=True, sort=False)
        except Exception as e:
            print("Research DB read warning:", e)

    dedupe = [c for c in [
        "fixture_id","market_family","market","selection","handicap","snapshot_ts"
    ] if c in cur.columns]
    if dedupe:
        cur = cur.drop_duplicates(subset=dedupe, keep="last")

    cur.to_csv(RESEARCH_DB, index=False)
    print(f"RESEARCH DB: {len(cur)} rows")


def _stat_sides(response, stat_type):
    vals=[]
    for team in response or []:
        found=np.nan
        for s in team.get("statistics",[]):
            if str(s.get("type","")).strip().lower()==stat_type.lower():
                found=fnum(s.get("value")); break
        vals.append(found)
    while len(vals)<2: vals.append(np.nan)
    return vals[0],vals[1]


def _stat_total(response, stat_type):
    h,a=_stat_sides(response,stat_type)
    if pd.isna(h) or pd.isna(a): return None
    return float(h)+float(a)


def _line_from_row(r):
    # V5 stores period-total line in handicap; selection also contains it.
    try:
        if pd.notna(r.get("handicap")):
            return float(r.get("handicap"))
    except Exception:
        pass
    m = re.search(r'(\d+(?:\.\d+)?)', str(r.get("selection","")))
    return float(m.group(1)) if m else None


def _settle_ou(selection, line, observed, three_way=False):
    if line is None or observed is None:
        return None
    s = str(selection).lower()
    if "over" in s:
        if observed > line: return "WIN"
        if observed < line: return "LOSS"
        return "LOSS" if three_way else "PUSH"
    if "under" in s:
        if observed < line: return "WIN"
        if observed > line: return "LOSS"
        return "LOSS" if three_way else "PUSH"
    return None


def settle_research_history(key):
    """
    On every scan, settle previously archived rows whose fixtures have finished.
    Uncertain/unsupported rows stay PENDING rather than being guessed.
    """
    if not RESEARCH_DB.exists():
        return

    try:
        d = pd.read_csv(RESEARCH_DB)
    except Exception as e:
        print("Research settlement read warning:", e)
        return

    if d.empty:
        return

    for c, default in [
        ("settlement",""),("settled_at",""),("final_observed",pd.NA)
    ]:
        if c not in d.columns:
            d[c] = default

    pending = d[d["settlement"].fillna("").eq("")].copy()
    if pending.empty:
        d.to_csv(RESEARCH_SETTLED_DB, index=False)
        return

    # Keep repeated live scans fast: settle the oldest unresolved fixtures first,
    # in a bounded batch. Later scans continue catching up automatically.
    if "snapshot_ts" in pending.columns:
        pending = pending.sort_values("snapshot_ts", kind="stable")
    fixture_ids = (
        pd.to_numeric(pending["fixture_id"], errors="coerce")
        .dropna().astype(int).drop_duplicates().tolist()
    )[:MAX_SETTLEMENT_FIXTURES_PER_RUN]
    print(f"SETTLEMENT BATCH: checking {len(fixture_ids)} oldest pending fixtures")

    for fid in fixture_ids:
        try:
            fx = api_get("/fixtures", {"id": int(fid)}, key)
            resp = (fx or {}).get("response", [])
            if not resp:
                continue
            f = resp[0]
            short = str(f.get("fixture",{}).get("status",{}).get("short",""))
            if short not in {"FT","AET","PEN"}:
                continue

            gh = f.get("goals",{}).get("home")
            ga = f.get("goals",{}).get("away")
            ft_goals = None if gh is None or ga is None else float(gh) + float(ga)

            ht = f.get("score",{}).get("halftime",{}) or {}
            hth, hta = ht.get("home"), ht.get("away")

            st = api_get("/fixtures/statistics", {"fixture": int(fid)}, key)
            ft_corners = _stat_total((st or {}).get("response", []), "Corner Kicks")

            rows = d.index[
                (pd.to_numeric(d["fixture_id"], errors="coerce") == int(fid)) &
                d["settlement"].fillna("").eq("")
            ]

            # Only request half=true when a period-corner market exists.
            fams = set(d.loc[rows, "market_family"].astype(str))
            ht_corners = None
            if {"corner_total_1h","corner_total_2h","goal_total_1h","match_result_1h"} & fams:
                hs = api_get("/fixtures/statistics", {"fixture": int(fid), "half": "true"}, key)
                ht_corners = _stat_total((hs or {}).get("response", []), "Corner Kicks")

            for idx in rows:
                r = d.loc[idx]
                fam = str(r.get("market_family",""))
                market = str(r.get("market",""))
                selection = str(r.get("selection",""))
                line = _line_from_row(r)
                three_way = ("3 way" in market.lower()) or ("3way" in market.lower())
                result = None
                observed = None

                if fam == "goal_total":
                    observed = ft_goals
                    result = _settle_ou(selection, line, observed, three_way)

                elif fam == "goal_total_1h" and hth is not None and hta is not None:
                    observed = float(hth)+float(hta)
                    result = _settle_ou(selection,line,observed,three_way)

                elif fam == "goal_total_2h" and None not in (gh,ga,hth,hta):
                    observed = max(0.0,(float(gh)+float(ga))-(float(hth)+float(hta)))
                    result = _settle_ou(selection,line,observed,three_way)

                elif fam == "team_goal_home" and gh is not None:
                    observed=float(gh); result=_settle_ou(selection,line,observed,three_way)
                elif fam == "team_goal_away" and ga is not None:
                    observed=float(ga); result=_settle_ou(selection,line,observed,three_way)

                elif fam == "btts_ft" and gh is not None and ga is not None:
                    observed = "YES" if int(gh)>0 and int(ga)>0 else "NO"
                    want_yes = str(r.get("raw_value","")).strip().lower()=="yes"
                    result = "WIN" if ((observed=="YES") == want_yes) else "LOSS"

                elif fam == "match_result_ft" and gh is not None and ga is not None:
                    observed = "home" if int(gh)>int(ga) else "away" if int(ga)>int(gh) else "draw"
                    result = "WIN" if observed==str(r.get("raw_value","")).lower() else "LOSS"

                elif fam == "double_chance_ft" and gh is not None and ga is not None:
                    observed = "home" if int(gh)>int(ga) else "away" if int(ga)>int(gh) else "draw"
                    want=str(r.get("raw_value","")).lower()
                    ok=(want=="home or draw" and observed in {"home","draw"}) or (want=="away or draw" and observed in {"away","draw"}) or (want=="home or away" and observed in {"home","away"})
                    result="WIN" if ok else "LOSS"

                elif fam == "draw_no_bet_ft" and gh is not None and ga is not None:
                    observed = "home" if int(gh)>int(ga) else "away" if int(ga)>int(gh) else "draw"
                    want=str(r.get("raw_value","")).lower()
                    result="PUSH" if observed=="draw" else ("WIN" if observed==want else "LOSS")

                elif fam == "match_result_1h" and hth is not None and hta is not None:
                    observed = "home" if int(hth)>int(hta) else "away" if int(hta)>int(hth) else "draw"
                    result="WIN" if observed==str(r.get("raw_value","")).lower() else "LOSS"

                elif fam == "match_result_2h" and None not in (gh,ga,hth,hta):
                    shg=int(gh)-int(hth); sag=int(ga)-int(hta)
                    observed = "home" if shg>sag else "away" if sag>shg else "draw"
                    result="WIN" if observed==str(r.get("raw_value","")).lower() else "LOSS"

                elif fam == "corner_total_ft":
                    observed = ft_corners
                    result = _settle_ou(selection, line, observed, three_way)

                elif fam == "corner_total_1h" and ht_corners is not None:
                    observed = ht_corners
                    result = _settle_ou(selection, line, observed, three_way)

                elif fam == "corner_total_2h" and ht_corners is not None and ft_corners is not None:
                    observed = max(0.0, ft_corners - ht_corners)
                    result = _settle_ou(selection, line, observed, three_way)

                elif fam == "corner_result_ft":
                    ch,ca=_stat_sides((st or {}).get("response",[]),"Corner Kicks")
                    if not pd.isna(ch) and not pd.isna(ca):
                        observed="home" if ch>ca else "away" if ca>ch else "draw"
                        result="WIN" if observed==str(r.get("raw_value","")).lower() else "LOSS"

                elif fam in {"shot_total_ft","home_shot_total_ft","away_shot_total_ft"}:
                    sh,sa=_stat_sides((st or {}).get("response",[]),"Total Shots")
                    observed=(sh+sa) if fam=="shot_total_ft" and not(pd.isna(sh) or pd.isna(sa)) else sh if fam=="home_shot_total_ft" else sa
                    result=_settle_ou(selection,line,observed,three_way) if observed is not None and not pd.isna(observed) else None

                elif fam in {"sot_total_ft","home_sot_total_ft","away_sot_total_ft"}:
                    sh,sa=_stat_sides((st or {}).get("response",[]),"Shots on Goal")
                    observed=(sh+sa) if fam=="sot_total_ft" and not(pd.isna(sh) or pd.isna(sa)) else sh if fam=="home_sot_total_ft" else sa
                    result=_settle_ou(selection,line,observed,three_way) if observed is not None and not pd.isna(observed) else None

                elif fam == "goals_odd_even" and ft_goals is not None:
                    observed="even" if int(ft_goals)%2==0 else "odd"
                    result="WIN" if observed==str(r.get("raw_value","")).lower() else "LOSS"

                elif fam == "ht_draw" and hth is not None and hta is not None:
                    observed = f"{hth}-{hta}"
                    result = "WIN" if int(hth) == int(hta) else "LOSS"

                if result:
                    d.at[idx,"settlement"] = result
                    d.at[idx,"settled_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    d.at[idx,"final_observed"] = observed

        except Exception as e:
            print(f"Settlement warning fixture {fid}: {e}")

    d.to_csv(RESEARCH_DB, index=False)
    settled = d[d["settlement"].fillna("").isin(["WIN","LOSS","PUSH"])].copy()
    settled.to_csv(RESEARCH_SETTLED_DB, index=False)

    wl = settled[settled["settlement"].isin(["WIN","LOSS"])]
    if not wl.empty:
        wins = int((wl["settlement"]=="WIN").sum())
        print(f"SETTLED RESEARCH: {len(wl)} W/L rows | {wins} wins | hit {wins/len(wl):.1%}")


def print_threshold_audit():
    if not RESEARCH_SETTLED_DB.exists():
        return
    try:
        d = pd.read_csv(RESEARCH_SETTLED_DB)
    except Exception:
        return

    if "model_probability_effective" not in d.columns:
        return

    x = d[d["settlement"].isin(["WIN","LOSS"])].copy()
    x["model_probability_effective"] = pd.to_numeric(
        x["model_probability_effective"], errors="coerce"
    )
    x["live_odds"] = pd.to_numeric(x.get("live_odds"), errors="coerce")
    x = x.dropna(subset=["model_probability_effective"])

    if x.empty:
        return

    print("\nCUMULATIVE RESEARCH — MIN PROBABILITY THRESHOLDS")
    print(" floor |   n | hit rate | 1u ROI")
    for th in [0.55,0.60,0.65,0.70,0.75,0.80,0.85,0.90,0.95]:
        y = x[x["model_probability_effective"] >= th]
        if y.empty:
            continue
        wins = int((y["settlement"]=="WIN").sum())
        hit = wins / len(y)
        profit = 0.0
        valid = 0
        for _,r in y.iterrows():
            if pd.isna(r["live_odds"]):
                continue
            valid += 1
            profit += (float(r["live_odds"])-1.0) if r["settlement"]=="WIN" else -1.0
        roi = profit/valid if valid else float("nan")
        print(f" {th:>4.0%} | {len(y):>3} | {hit:>8.1%} | {roi:>6.1%}")

def main():
    mount_drive()
    dest = commit_self_to_drive()
    key, source = discover_api_key()
    settle_research_history(key)
    print_threshold_audit()

    print("="*110)
    print("DDD LIVE VALUE ENGINE V7.2 — FAST MOMENTUM PERIOD-AWARE")
    print("="*110)
    print("ENGINE COMMITTED:", dest)
    print("API KEY: FOUND (hidden)")
    print("KEY SOURCE:", source)
    print(f"MIN ODDS: {MIN_ODDS:.2f}")
    print(f"MIN EFFECTIVE PROBABILITY: {MIN_MODEL_PROB:.0%}")
    print(f"HIGH CONFIDENCE BOARD: {HIGH_CONF_PROB:.0%}+")
    print("RANKING: PROBABILITY FIRST; VALUE SECOND")
    print(f"MIN ESTIMATED EDGE: {MIN_EDGE:.1%}")
    print("="*110)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    fixtures_df, odds_df, opp_df, odds_diag_df, raw_global_odds, raw_global_odds_error = scan_live(key)
    fixture_board, alternatives, high80, coverage = qualifying_boards(opp_df)
    fast_period, first_half_board, second_half_board, full_time_board, safest_board = period_aware_boards(opp_df)
    picks = pick_independent(opp_df)
    append_research_history(opp_df, picks, stamp)

    f_csv = ARCHIVE_DIR/f"LIVE_FIXTURES_{stamp}.csv"
    o_csv = ARCHIVE_DIR/f"LIVE_ODDS_{stamp}.csv"
    a_csv = ARCHIVE_DIR/f"LIVE_ALL_OPPORTUNITIES_{stamp}.csv"
    p_csv = RESULT_DIR/f"LIVE_VALUE_PICKS_{stamp}.csv"
    q_csv = RESULT_DIR/f"LIVE_QUALIFYING_FIXTURE_BOARD_{stamp}.csv"
    alt_csv = RESULT_DIR/f"LIVE_ALTERNATIVE_MARKETS_{stamp}.csv"
    h_csv = RESULT_DIR/f"LIVE_HIGH_CONFIDENCE_80_{stamp}.csv"
    c_csv = RESULT_DIR/f"LIVE_MARKET_COVERAGE_{stamp}.csv"
    fp_csv = RESULT_DIR/f"LIVE_FAST_PERIOD_{stamp}.csv"
    h1_csv = RESULT_DIR/f"LIVE_FIRST_HALF_{stamp}.csv"
    h2_csv = RESULT_DIR/f"LIVE_SECOND_HALF_{stamp}.csv"
    ft_csv = RESULT_DIR/f"LIVE_FULL_TIME_{stamp}.csv"
    safe_csv = RESULT_DIR/f"LIVE_SAFEST_{stamp}.csv"
    d_csv = ARCHIVE_DIR/f"LIVE_ODDS_DIAGNOSTIC_{stamp}.csv"
    r_json = ARCHIVE_DIR/f"LIVE_ODDS_RAW_GLOBAL_{stamp}.json"

    fixtures_df.to_csv(f_csv,index=False)
    odds_df.to_csv(o_csv,index=False)
    opp_df.to_csv(a_csv,index=False)
    picks.to_csv(p_csv,index=False)
    fixture_board.to_csv(q_csv,index=False)
    alternatives.to_csv(alt_csv,index=False)
    high80.to_csv(h_csv,index=False)
    coverage.to_csv(c_csv,index=False)
    fast_period.to_csv(fp_csv,index=False)
    first_half_board.to_csv(h1_csv,index=False)
    second_half_board.to_csv(h2_csv,index=False)
    full_time_board.to_csv(ft_csv,index=False)
    safest_board.to_csv(safe_csv,index=False)
    odds_diag_df.to_csv(d_csv,index=False)

    # Preserve the exact provider response for deterministic diagnosis.
    # It contains no API key.
    raw_payload = raw_global_odds if raw_global_odds is not None else {
        "global_odds_error": raw_global_odds_error
    }
    r_json.write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2))

    print("\n"+"="*110)
    print("LIVE VALUE DECISION — PERIOD-AWARE, PROBABILITY FIRST")
    print("="*110)
    print(f"QUALIFYING FIXTURES: {len(fixture_board)} | QUALIFYING MARKET ALTERNATIVES: {len(alternatives)} | 80%+ MARKETS: {len(high80)}")

    if picks.empty:
        print("NO BET")
        print("No independent live market met probability + price + value requirements.")
    else:
        cols = [c for c in [
            "live_value_rank","minute","home","away","score","market_family","market","selection",
            "live_odds","model_probability_effective",
            "implied_probability","estimated_edge","estimated_ev","data_quality",
            "home_sot","away_sot","home_shots","away_shots","home_corners","away_corners"
        ] if c in picks.columns]
        print(picks[cols].to_string(index=False))

        rollover = float(np.prod(picks["live_odds"]))
        print("\nINDEPENDENT PICKS:",len(picks))
        print(f"THEORETICAL FULL ROLLOVER ODDS: {rollover:.2f}")
        print("\nSEQUENTIAL ROLLOVER ORDER:")
        for _,r in picks.iterrows():
            print(
                f"{int(r['live_value_rank'])}. {r['home']} vs {r['away']} | "
                f"{r['selection']} @ {r['live_odds']:.2f} | "
                f"p {r['model_probability_effective']:.1%} | "
                f"edge {r['estimated_edge']:.1%}"
            )
        print("\nTOP ALTERNATIVES BY FIXTURE (up to 3 each):")
        show_cols=[c for c in ["minute","home","away","market_family","market","selection","live_odds","model_probability_effective","estimated_ev","fixture_alt_rank"] if c in alternatives.columns]
        if not alternatives.empty:
            print(alternatives[show_cols].to_string(index=False))
        print("\nRe-scan before each rollover leg; do not pre-commit later live legs.")

    bundle = RESULT_DIR/f"DDD_LIVE_VALUE_SCAN_{stamp}.zip"
    with zipfile.ZipFile(bundle,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f_csv,f_csv.name)
        z.write(o_csv,o_csv.name)
        z.write(a_csv,a_csv.name)
        z.write(p_csv,p_csv.name)
        z.write(q_csv,q_csv.name)
        z.write(alt_csv,alt_csv.name)
        z.write(h_csv,h_csv.name)
        z.write(c_csv,c_csv.name)
        z.write(fp_csv,fp_csv.name)
        z.write(h1_csv,h1_csv.name)
        z.write(h2_csv,h2_csv.name)
        z.write(ft_csv,ft_csv.name)
        z.write(safe_csv,safe_csv.name)
        z.write(d_csv,d_csv.name)
        z.write(r_json,r_json.name)

        if RESEARCH_DB.exists():
            z.write(RESEARCH_DB, RESEARCH_DB.name)
        if RESEARCH_SETTLED_DB.exists():
            z.write(RESEARCH_SETTLED_DB, RESEARCH_SETTLED_DB.name)

    print("\nRESULT ZIP:",bundle)
    print("LIVE HISTORY:",ARCHIVE_DIR)

    try:
        from google.colab import files
        files.download(str(bundle))
    except Exception:
        pass

if __name__ == "__main__":
    main()
