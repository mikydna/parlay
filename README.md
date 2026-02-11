# nba-prop-ev

[![CI](https://github.com/<OWNER>/<REPO>/actions/workflows/ci.yml/badge.svg)](https://github.com/<OWNER>/<REPO>/actions/workflows/ci.yml)

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

## CI Badge

Replace `<OWNER>/<REPO>` in the badge URL after pushing to GitHub.
