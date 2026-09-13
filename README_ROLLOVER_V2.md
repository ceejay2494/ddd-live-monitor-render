# DDD V7.2 — Top-1 Rollover V2 odds patch

Changes
- Normal rollover minimum odds: 1.25
- Preferred odds zone: 1.30–1.55
- 1.20–1.24 allowed only when probability >=90%, edge >=6%, EV >=5%, push <=10%
- Engine scan floor remains 1.20 so exceptional candidates are still visible
- Corrected settlement countdown: 1H=45-minute, 2H/FT=90-minute
- Probability remains primary; faster settlement is only a tie-break inside the 3pp tolerance
- One active rollover signal at a time
- LOSS stops the chain by default

Recommended Render environment variables
DDD_ROLLOVER_MODE=1
DDD_ROLLOVER_MIN_ODDS=1.25
DDD_ROLLOVER_PREF_MIN_ODDS=1.30
DDD_ROLLOVER_PREF_MAX_ODDS=1.55
DDD_ROLLOVER_EXCEPTION_MIN_ODDS=1.20
DDD_ROLLOVER_EXCEPTION_MAX_ODDS=1.24
DDD_ROLLOVER_EXCEPTION_MIN_PROB=0.90
DDD_ROLLOVER_EXCEPTION_MIN_EDGE=0.06
DDD_ROLLOVER_EXCEPTION_MIN_EV=0.05
DDD_ROLLOVER_MAX_PUSH=0.10
DDD_ROLLOVER_FAST_TOLERANCE=0.03
DDD_ROLLOVER_STOP_ON_LOSS=1
DDD_SCAN_SECONDS=300
