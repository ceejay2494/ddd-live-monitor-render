# DDD V7.3 — Pressure-First Live Update

Deployment bundle for the Render service `ddd-live-monitor`.

## What changed
- 5-minute and 10-minute rolling attacking-pressure windows.
- Pressure acceleration (recent 5m vs broader 10m window).
- Provider xG ingestion when available.
- Conservative recent xG proxy fallback from shots on target, non-SOT shots and corners.
- Minute-phase prior applied conservatively to remaining-goal rate.
- Bounded goal-pressure multiplier; it adjusts the Poisson remaining-goal lambda without replacing model probability.
- Separate Goal Imminence research lane. Its 0–100 score is a pressure index, **not** a win probability.
- Adaptive scanning: 300s normal, 120s when a hot pressure candidate is present, 60s for critical pressure; fast scanning is disabled when estimated remaining daily API quota drops below 1,200.
- Existing HARD80 official gate is preserved: actual model probability, edge and EV continue to determine official selections.
- Existing probability-first ranking, period-aware logic, broad market competition and one-primary-pick discipline remain intact.

## Goal-imminence research gate
- Goal total market and Over side.
- Model probability >= 70%.
- Live odds >= existing minimum odds floor.
- Goal-imminence pressure score >= 70/100.
- Expected settlement horizon <= 30 minutes.
- Market data-quality gate satisfied.
- EV must be non-negative.

This lane is research/signal context; it does not bypass the official HARD80 gate.

## Files
- `app.py` — V7.3 dashboard + adaptive scan + pressure-lane display.
- `ddd_engine_v72.py` — V7.3 pressure-first engine kept under the existing import filename for deployment compatibility.
- `requirements.txt` — unchanged dependency set.

## Verification performed
- Both Python files compile successfully with `python -m py_compile`.
- Synthetic live-state test confirmed pressure-window creation, pressure acceleration, xG-proxy generation, goal-pressure multiplier and goal-imminence board selection.
