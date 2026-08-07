# `game_rosters`

`game_rosters` reshaper -- release tag [`espn_mens_college_basketball_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters).

| | |
|---|---|
| **Builder** | [`python/espn_mbb_09_game_rosters_creation.py`](../../python/espn_mbb_09_game_rosters_creation.py) |
| **Release tag** | [`espn_mens_college_basketball_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters) |
| **File stem** | `game_rosters_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_mbb.yml` -- daily cron, running `scripts/daily_mbb_R_processor.sh` (R, the current daily producer). `scripts/daily_mbb_python_processor.sh` runs the parity-tested Python build in `mbb_data_build` but does not yet drive daily CI.

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
