# Iron Condor Advisory Module

Advisory workbook for Iron Condors: checklist, strikes, thresholds, polls, alerts, journal. **Does not submit broker orders.**

## Run locally

Backend depends on Postgres (via `DATABASE_URL`) and authenticated user token (same JWT as `/api/iron-…` flows).

Serve static UI: `frontend/public/iron-condor.html`.

## Primary HTTP surface

- `GET /iron-condor/session` — market window, banner, quote-feed streak, holdings verify prompt  
- `GET /iron-condor/workspace` — settings, enriched positions (`card_peak_severity`), alerts, dashboard rollup  
- `POST /iron-condor/checklist` — PASS/FAIL chips (optional declared earnings date → 25-day rule)  
- `POST /iron-condor/analyze-detailed` — strikes + economics; optional `strike_overrides`  
- `POST /iron-condor/confirm-entry` — requires `placed_orders_confirmed`; persists `ACTIVE` rows  
- `POST /iron-condor/poll` — IST window poller + alert engine + streak counter  
- `POST /iron-condor/session/verify-positions-held` — clears daily holdings prompt  
- `POST /iron-condor/positions/{id}/log-adjustment` — manual adjustment log & recomputed thresholds  
- `POST /iron-condor/close-with-journal` — requires `squaring_confirmed`  
- `GET /iron-condor/equity-curve` — cumulative realized from journal  

## Extend safely

1. Strike math lives in `backend/services/iron_condor_service.py` (`_pick_buy_wing`, `analyze_iron_condor`).  
2. Alert rules live in `backend/services/iron_condor_extended.py` (`evaluate_active_position`). Skip price-sensitive rules automatically when `_fresh_chain_quotes` fails; earnings proximity still evaluates.  
3. New UI strings only in `frontend/public/iron-condor.{html,js,css}` plus menu links as needed.

## Automated tests

```bash
python3 test_iron_condor_advisory.py
python3 test_iron_condor_v1_scenarios.py
```
