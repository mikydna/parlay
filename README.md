# nba-prop-ev

Scaffold for an NBA player-props expected-value pipeline.

No fetching, modeling, or scraping is implemented yet.

## Install

```bash
uv sync --all-groups
```

## Environment

```bash
cp .env.example .env
```

Set `ODDS_API_KEY` in `.env`.

## Run

```bash
uv run prop-ev --help
make ci
```
