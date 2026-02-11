# Credits Guide (Odds API Free Tier)

Free tier budget: `500 credits / month`.

## Cost Formulas

- Featured odds (`/sports/{sport}/odds`):
  - `credits = number_of_markets x regions_equivalent`
- Event odds (`/sports/{sport}/events/{eventId}/odds`):
  - worst-case `credits = number_of_events x number_of_markets x regions_equivalent`
  - actual can be lower if requested markets are unavailable for some events

`regions_equivalent`:
- if `--bookmakers` is used: `ceil(bookmaker_count / 10)`
- else: number of regions in `--regions` (e.g., `us` = 1)

## Practical Monthly Budgets

Conservative daily slate snapshots:
- 1 run/day, `markets=spreads,totals`, `regions=us`
- ~2 credits/day => ~60 credits/month

Conservative props snapshots:
- 1 run/day, 10 events, `markets=player_points`, `regions=us`
- ~10 credits/day => ~300 credits/month

Combined:
- ~360 credits/month (leaves ~140 credits headroom)

## Recommended Operating Pattern

1. Run slate snapshot daily.
2. Run props snapshot once near decision time.
3. Reuse snapshot data in reruns (`--offline`) instead of refreshing.
4. Keep `--max-credits` enabled; only use `--force` when intentional.

## CLI Helpers

```bash
uv run prop-ev credits budget --events 10 --markets player_points --regions us
uv run prop-ev credits report --month 2026-02
```
