# DDD V7.2 Live Monitor — Render

Render-ready live football market monitor.

## Runtime
- Base live scan: 5 minutes
- Hot scan: 2 minutes when a >=75% candidate/official signal exists
- Idle scan: 15 minutes when no live fixtures exist
- HARD80 official gate
- FAST PERIOD and MOMENTUM FAST lanes
- Closest-candidate board with explicit warnings
- Public mobile dashboard + `/api/snapshot` + `/health`
- Local daily API request guard

## Required Render secret
`API_FOOTBALL_KEY`

## Suggested Render environment
- DDD_BASE_SCAN_SECONDS=300
- DDD_HOT_SCAN_SECONDS=120
- DDD_IDLE_SCAN_SECONDS=900
- DDD_DAILY_REQUEST_LIMIT=6500
- DDD_MAX_DEEP_FIXTURES=6
- DDD_MAX_FALLBACK_FIXTURES=2

Start command:
`gunicorn -w 1 --threads 4 -b 0.0.0.0:$PORT app:app`
