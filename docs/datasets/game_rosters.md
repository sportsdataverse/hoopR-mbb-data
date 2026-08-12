# `game_rosters`

`game_rosters` reshaper -- release tag [`espn_mens_college_basketball_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_09_game_rosters_creation.py`](../../python/espn_mbb_09_game_rosters_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters) |
| **File stem** | `game_rosters_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-08-07 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 56 |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `mbb_data_build`; `-l R` is the retained rollback path over the R creation scripts.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `game_id` | String | |
| `team_id` | Int32 | |
| `team_slug` | String | |
| `team_abbreviation` | String | |
| `team_display_name` | String | |
| `home_away` | String | |
| `athlete_id` | Int32 | |
| `athlete_uid` | String | |
| `athlete_guid` | String | |
| `athlete_display_name` | String | |
| `athlete_short_name` | String | |
| `athlete_first_name` | String | |
| `athlete_last_name` | String | |
| `athlete_jersey` | String | |
| `athlete_position` | String | |
| `athlete_headshot` | String | |
| `starter` | Boolean | |
| `did_not_play` | Boolean | |
| `active` | Boolean | |
| `ejected` | Boolean | |
| `reason` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2025 | 207,716 |
| 2026 | 196,876 |
