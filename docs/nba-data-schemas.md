# NBA Data Schemas (`schema_v1`)

Clean parquet outputs are written under:

- `data/nba_data/clean/schema_v1/games/`
- `data/nba_data/clean/schema_v1/boxscore_players/`
- `data/nba_data/clean/schema_v1/pbp_events/`
- `data/nba_data/clean/schema_v1/possessions/`

All tables are partitioned by:

- `season`
- `season_type`

## `games`

- `season` (string)
- `season_type` (string)
- `game_id` (string)
- `date` (string)
- `home_team_id` (string)
- `away_team_id` (string)

## `boxscore_players`

- `season` (string)
- `season_type` (string)
- `game_id` (string)
- `team_id` (string)
- `player_id` (string)
- `minutes` (float)
- `points` (float)
- `rebounds` (float)
- `assists` (float)

## `pbp_events`

- `season` (string)
- `season_type` (string)
- `game_id` (string)
- `event_num` (int64)
- `clock` (string)
- `event_type` (string)
- `team_id` (string)
- `player_id` (string)
- `description` (string)

## `possessions`

- `season` (string)
- `season_type` (string)
- `game_id` (string)
- `possession_id` (int64)
- `start_event_num` (int64)
- `end_event_num` (int64)
- `offense_team_id` (string)
- `defense_team_id` (string)

## Verify checks

`nba-data verify` reads schema_v1 and reports:

- referential integrity (`game_id` exists in `games`)
- duplicate PK checks:
  - `pbp_events`: (`game_id`, `event_num`)
  - `possessions`: (`game_id`, `possession_id`)
- minute sanity warnings/failures
- optional lineup ID sanity (when lineup IDs are present)
